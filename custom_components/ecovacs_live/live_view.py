"""ECOVACS Kinesis WebRTC Live View runtime."""
from __future__ import annotations

import asyncio
import base64
import hashlib
from io import BytesIO
import hmac
from datetime import datetime, timezone
import json
import logging
import secrets
import string
import time
import uuid
from dataclasses import dataclass
from fractions import Fraction
from types import MethodType
from typing import Any
from urllib.parse import urlparse
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import websockets
from aiortc import (
    MediaStreamTrack,
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCRtpSender,
    RTCSessionDescription,
)
from av import AudioFrame

from homeassistant.core import HomeAssistant

from .pin import encode_live_view_pin

_LOGGER = logging.getLogger(__name__)

APP_ID = "ecovacs"
APP_VERSION = "3.14.0"
APP_CHANNEL = "google_play"
VIDEO_API_VERSION = "2.1.0"
SIGNING_PREFIX = "ecovacs2ea31cf06e6711eaa0aff7b9558a534e"
REALM = "ecouser.net"
BOOTSTRAP_APP_HOST = "api-app.ww.ecouser.net"


def _frame_to_jpeg(frame: Any) -> bytes:
    """Convert one decoded AV frame to JPEG outside the HA event loop."""
    image = frame.to_image()
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=82)
    return buffer.getvalue()


@dataclass(frozen=True)
class KinesisSession:
    region: str
    channel_name: str
    client_id: str
    access_key_id: str
    secret_access_key: str
    session_token: str


class SilentAudioTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self) -> None:
        super().__init__()
        self.sample_rate = 48_000
        self.samples_per_frame = 960
        self._timestamp = 0
        self._started_at: float | None = None

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_running_loop()
        if self._started_at is None:
            self._started_at = loop.time()
        else:
            target = self._started_at + self._timestamp / self.sample_rate
            delay = target - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)

        frame = AudioFrame(
            format="s16",
            layout="stereo",
            samples=self.samples_per_frame,
        )
        frame.sample_rate = self.sample_rate
        frame.pts = self._timestamp
        frame.time_base = Fraction(1, self.sample_rate)
        for plane in frame.planes:
            plane.update(bytes(plane.buffer_size))
        self._timestamp += self.samples_per_frame
        return frame


def _random_track_id(length: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _signature(timestamp_ms: str) -> str:
    return hashlib.sha1(
        f"{SIGNING_PREFIX}{timestamp_ms}".encode("utf-8")
    ).hexdigest()




def _normalise_host(value: str) -> str:
    """Return a bare hostname from an ECOVACS service value."""
    value = value.strip().rstrip("/")
    if not value:
        return ""
    if "://" in value:
        parsed = urlparse(value)
        return parsed.netloc or parsed.path
    return value.split("/", 1)[0]


async def async_discover_magw(
    session: Any,
    *,
    country: str,
    user_id: str,
) -> str:
    """Discover the regional ECOVACS application gateway used by Live View."""
    params = {
        "area": country,
        "uid": user_id,
        "t": str(int(time.time() * 1000)),
    }
    url = f"https://{BOOTSTRAP_APP_HOST}/api/appsvr/service/list"

    async with session.get(
        url,
        params=params,
        headers={"Accept": "application/json"},
        timeout=30,
    ) as response:
        raw = await response.text()
        status = response.status

    if status >= 400:
        raise RuntimeError(
            f"ECOVACS service-list discovery failed with HTTP {status}"
        )

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "ECOVACS service-list discovery returned invalid JSON"
        ) from exc

    if not isinstance(payload, dict):
        raise RuntimeError(
            "ECOVACS service-list discovery returned a non-object response"
        )

    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(
            "ECOVACS service-list discovery did not return a data object"
        )

    magw = _normalise_host(str(data.get("magw", "")))
    if not magw:
        raise RuntimeError(
            "ECOVACS service-list discovery did not return data.magw"
        )

    return magw


async def async_start_watch(
    session: Any,
    *,
    rest_config: Any,
    login_client_id: str,
    credentials: Any,
    country: str,
    pin: str,
    robot: dict[str, Any],
) -> KinesisSession:
    """Create a fresh session using the exact GET flow proven on Windows."""
    track_id = _random_track_id()
    timestamp_ms = str(int(time.time() * 1000))

    auth = {
        "with": "users",
        "userid": credentials.user_id,
        "realm": REALM,
        "token": credentials.token,
        "resource": login_client_id,
    }

    params = {
        "videoTrackId": track_id,
        "lang": "en",
        "plat": "Android",
        "av": VIDEO_API_VERSION,
        "did": robot["did"],
        "mid": robot["class"],
        "res": robot["resource"],
        "pwd": encode_live_view_pin(pin, robot),
        "auth": json.dumps(auth, separators=(",", ":")),
        "channel": APP_CHANNEL,
    }

    headers = {
        "country": country,
        "Accept": "application/json",
        "sign": _signature(timestamp_ms),
        "userid": credentials.user_id,
        "token": credentials.token,
        "Authorization": f"Bearer {credentials.token}",
        "v": APP_VERSION,
        "appid": APP_ID,
        "plat": "android",
        "lang": "EN",
        "Content-Type": "application/json",
        "ts": timestamp_ms,
    }

    # Match the proven Windows runner exactly: start_watch is sent through
    # the portal URL returned by create_rest_config after classic login.
    url = f"{rest_config.portal_url.rstrip('/')}/api/appsvr/akvs/start_watch/v2"

    # Public-build diagnostic: deliberately avoid logging account details,
    # robot identifiers, authentication material, PIN hashes, or AWS credentials.
    portal_host = urlparse(str(rest_config.portal_url)).hostname or "<unknown>"
    _LOGGER.debug(
        "ECOVACS start_watch request prepared: portal_host=%s",
        portal_host,
    )

    async with session.get(
        url,
        params=params,
        headers=headers,
        timeout=30,
    ) as response:
        raw = await response.text()
        status = response.status

    if status >= 400:
        # Do not include tokens, request URL, PIN hash, or credentials in logs.
        raise RuntimeError(f"ECOVACS start_watch failed with HTTP {status}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"ECOVACS start_watch returned invalid JSON (HTTP {status})"
        ) from exc

    if not isinstance(data, dict):
        raise RuntimeError("ECOVACS start_watch returned a non-object response")

    creds = data.get("credentials")

    if (
        str(data.get("ret", "")).lower() != "ok"
        or not isinstance(creds, dict)
    ):
        safe_keys = sorted(
            key
            for key in data.keys()
            if key not in {"credentials", "session", "token", "auth"}
        )
        raise RuntimeError(
            "ECOVACS start_watch did not return Kinesis credentials "
            f"(ret={data.get('ret')!r}, code={data.get('code')!r}, "
            f"message={(data.get('message') or data.get('msg'))!r}, "
            f"response_keys={safe_keys})"
        )

    required_top = ["region", "channel", "client_id", "session"]
    missing = [key for key in required_top if not data.get(key)]
    missing += [
        f"credentials.{key}"
        for key in ("AccessKeyId", "SecretAccessKey", "SessionToken")
        if not creds.get(key)
    ]
    if missing:
        raise RuntimeError(
            "ECOVACS start_watch response missing required fields: "
            + ", ".join(missing)
        )

    return KinesisSession(
        region=str(data["region"]),
        channel_name=str(data["channel"]),
        client_id=str(data["client_id"]),
        access_key_id=str(creds["AccessKeyId"]),
        secret_access_key=str(creds["SecretAccessKey"]),
        session_token=str(creds["SessionToken"]),
    )


async def async_send_app_ping(
    session: Any,
    *,
    credentials: Any,
    robot: dict[str, Any],
) -> None:
    """Send the app's stream-start appping command."""
    service = robot.get("service") or {}
    host = str(service.get("mqs") or "").strip()
    if not host:
        _LOGGER.debug("Robot did not advertise an NG-IoT mqs host")
        return

    service_id = _random_track_id(16)
    timestamp_ms = str(int(time.time() * 1000))
    params = {
        "si": service_id,
        "ct": "m",
        "eid": robot["did"],
        "et": robot["class"],
        "er": robot["resource"],
        "apn": "appping",
        "fmt": "j",
    }
    body = {
        "body": {},
        "header": {
            "channel": "Android",
            "m": "request",
            "pri": 2,
            "reqid": _random_track_id(6),
            "ts": timestamp_ms,
            "tzc": "UTC",
            "tzm": 0,
            "ver": "0.0.22",
        },
    }
    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "X-ECO-REQUEST-ID": service_id,
        "Content-Type": "application/octet-stream",
        "Accept": "application/json",
        "User-Agent": "okhttp/4.9.1",
    }
    async with session.post(
        f"https://{host}/api/iot/endpoint/control",
        params=params,
        data=json.dumps(body, separators=(",", ":")).encode(),
        headers=headers,
        timeout=20,
    ) as response:
        await response.read()
        if response.status >= 400:
            _LOGGER.warning(
                "ECOVACS appping returned HTTP %s", response.status
            )



def _aws_quote(value: str) -> str:
    from urllib.parse import quote
    return quote(value, safe="-_.~")


def _signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = hmac.new(("AWS4" + secret_key).encode(), date_stamp.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


def _sigv4_headers(
    info: KinesisSession,
    *,
    method: str,
    url: str,
    service: str,
    body: bytes = b"",
    extra_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    parsed = urlparse(url)
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    query_pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    canonical_query = "&".join(
        f"{_aws_quote(str(k))}={_aws_quote(str(v))}"
        for k, v in query_pairs
    )

    signing_headers: dict[str, str] = {
        "host": parsed.netloc,
        "x-amz-date": amz_date,
        "x-amz-security-token": info.session_token,
    }
    if extra_headers:
        signing_headers.update(
            {k.lower(): str(v).strip() for k, v in extra_headers.items()}
        )

    names = sorted(signing_headers)
    canonical_headers = "".join(
        f"{name}:{' '.join(signing_headers[name].split())}\n"
        for name in names
    )
    signed_headers = ";".join(names)
    payload_hash = hashlib.sha256(body).hexdigest()

    canonical_request = "\n".join(
        [
            method.upper(),
            parsed.path or "/",
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{date_stamp}/{info.region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(info.secret_access_key, date_stamp, info.region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()

    output = {
        "Host": parsed.netloc,
        "X-Amz-Date": amz_date,
        "X-Amz-Security-Token": info.session_token,
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential={info.access_key_id}/{scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        ),
    }
    if extra_headers:
        output.update(extra_headers)
    return output


async def _aws_json_post(
    http: Any,
    info: KinesisSession,
    *,
    url: str,
    payload: dict[str, Any],
    service: str,
) -> dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    extra = {"Content-Type": "application/json"}
    headers = _sigv4_headers(
        info,
        method="POST",
        url=url,
        service=service,
        body=body,
        extra_headers=extra,
    )
    async with http.post(url, data=body, headers=headers, timeout=30) as response:
        raw = await response.text()
        if response.status >= 400:
            raise RuntimeError(
                f"AWS {service} request failed HTTP {response.status}: {raw[:200]}"
            )
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("AWS response was not a JSON object")
        return value


async def _describe_channel(http: Any, info: KinesisSession) -> str:
    response = await _aws_json_post(
        http,
        info,
        url=f"https://kinesisvideo.{info.region}.amazonaws.com/describeSignalingChannel",
        payload={"ChannelName": info.channel_name},
        service="kinesisvideo",
    )
    arn = (response.get("ChannelInfo") or {}).get("ChannelARN")
    if not arn:
        raise RuntimeError("Kinesis did not return ChannelARN")
    return str(arn)


async def _endpoints(
    http: Any,
    info: KinesisSession,
    arn: str,
) -> tuple[str, str]:
    response = await _aws_json_post(
        http,
        info,
        url=f"https://kinesisvideo.{info.region}.amazonaws.com/getSignalingChannelEndpoint",
        payload={
            "ChannelARN": arn,
            "SingleMasterChannelEndpointConfiguration": {
                "Protocols": ["WSS", "HTTPS"],
                "Role": "VIEWER",
            },
        },
        service="kinesisvideo",
    )
    values = {
        str(item["Protocol"]): str(item["ResourceEndpoint"])
        for item in response.get("ResourceEndpointList", [])
        if item.get("Protocol") and item.get("ResourceEndpoint")
    }
    return values["WSS"], values["HTTPS"]


async def _ice_servers(
    http: Any,
    info: KinesisSession,
    arn: str,
    https_endpoint: str,
) -> list[RTCIceServer]:
    response = await _aws_json_post(
        http,
        info,
        url=f"{https_endpoint.rstrip('/')}/v1/get-ice-server-config",
        payload={
            "ChannelARN": arn,
            "ClientId": info.client_id,
            "Service": "TURN",
        },
        service="kinesisvideo",
    )
    servers = [
        RTCIceServer(
            urls=[f"stun:stun.kinesisvideo.{info.region}.amazonaws.com:443"]
        )
    ]
    for server in response.get("IceServerList", []):
        uris = server.get("Uris") or []
        if uris:
            servers.append(
                RTCIceServer(
                    urls=[str(uri) for uri in uris],
                    username=server.get("Username"),
                    credential=server.get("Password"),
                )
            )
    return servers


def _signed_wss(info: KinesisSession, arn: str, endpoint: str) -> str:
    parsed = urlparse(endpoint)
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    service = "kinesisvideo"
    scope = f"{date_stamp}/{info.region}/{service}/aws4_request"

    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params.update(
        {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-ChannelARN": arn,
            "X-Amz-ClientId": info.client_id,
            "X-Amz-Credential": f"{info.access_key_id}/{scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": "299",
            "X-Amz-Security-Token": info.session_token,
            "X-Amz-SignedHeaders": "host",
        }
    )

    canonical_query = "&".join(
        f"{_aws_quote(str(k))}={_aws_quote(str(v))}"
        for k, v in sorted(params.items())
    )
    canonical_request = "\n".join(
        [
            "GET",
            parsed.path or "/",
            canonical_query,
            f"host:{parsed.netloc}\n",
            "host",
            hashlib.sha256(b"").hexdigest(),
        ]
    )
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(info.secret_access_key, date_stamp, info.region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()

    return urlunparse(
        (
            "wss",
            parsed.netloc,
            parsed.path,
            parsed.params,
            f"{canonical_query}&X-Amz-Signature={signature}",
            parsed.fragment,
        )
    )

def _encode(value: str) -> str:
    return base64.b64encode(value.encode()).decode("ascii")


def _decode(value: str) -> str:
    return base64.b64decode(value).decode("utf-8", errors="replace")


def _message(
    action: str,
    payload: str,
    correlation_id: str | None = None,
) -> str:
    data = {
        "action": action,
        "recipientClientId": "MASTER",
        "messagePayload": _encode(payload),
    }
    if correlation_id:
        data["correlationId"] = correlation_id
    return json.dumps(data, separators=(",", ":"))


async def _wait_ice(pc: RTCPeerConnection, timeout: float = 20) -> None:
    if pc.iceGatheringState == "complete":
        return
    event = asyncio.Event()

    @pc.on("icegatheringstatechange")
    async def _changed() -> None:
        if pc.iceGatheringState == "complete":
            event.set()

    await asyncio.wait_for(event.wait(), timeout=timeout)


def _rewrite_offer(sdp: str) -> str:
    """Use the codec/extension layout from the successful Android offer."""
    lines = sdp.replace("\r\n", "\n").split("\n")
    session_lines: list[str] = []
    sections: list[tuple[str, list[str]]] = []
    kind: str | None = None
    body: list[str] = []

    for line in lines:
        if line.startswith("m="):
            if kind is not None:
                sections.append((kind, body))
            kind = line.split("=", 1)[1].split(" ", 1)[0]
            body = [line]
        elif kind is None:
            if line:
                session_lines.append(line)
        else:
            body.append(line)
    if kind is not None:
        sections.append((kind, body))

    if "a=extmap-allow-mixed" not in session_lines:
        idx = next(
            (
                i + 1
                for i, line in enumerate(session_lines)
                if line.startswith("a=group:BUNDLE")
            ),
            len(session_lines),
        )
        session_lines.insert(idx, "a=extmap-allow-mixed")
    session_lines = [
        "a=msid-semantic: WMS KvsLocalMediaStream"
        if line.startswith("a=msid-semantic:")
        else line
        for line in session_lines
    ]

    rebuilt = list(session_lines)
    for media_kind, section in sections:
        if media_kind not in {"audio", "video"}:
            rebuilt.extend(section)
            continue

        parts = section[0].split()
        if media_kind == "audio":
            rebuilt.append(
                " ".join(
                    parts[:3] + ["111", "63", "9", "0", "8", "13", "110", "126"]
                )
            )
        else:
            rebuilt.append(
                " ".join(parts[:3] + ["96", "97", "98", "99", "100", "35"])
            )

        for line in section[1:]:
            if line.startswith(
                (
                    "a=extmap:",
                    "a=rtpmap:",
                    "a=rtcp-fb:",
                    "a=fmtp:",
                    "a=msid:",
                    "a=ssrc:",
                )
            ):
                continue
            if line == "a=rtcp-rsize":
                continue
            rebuilt.append(line)

        rebuilt.append("a=rtcp-rsize")
        if media_kind == "audio":
            rebuilt.extend(
                [
                    "a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                    "a=extmap:2 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
                    "a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
                    "a=extmap:4 urn:ietf:params:rtp-hdrext:sdes:mid",
                    "a=msid:KvsLocalMediaStream KvsAudioTrack",
                    "a=rtpmap:111 opus/48000/2",
                    "a=rtcp-fb:111 transport-cc",
                    "a=fmtp:111 minptime=10;useinbandfec=1",
                    "a=rtpmap:63 red/48000/2",
                    "a=fmtp:63 111/111",
                    "a=rtpmap:9 G722/8000",
                    "a=rtpmap:0 PCMU/8000",
                    "a=rtpmap:8 PCMA/8000",
                    "a=rtpmap:13 CN/8000",
                    "a=rtpmap:110 telephone-event/48000",
                    "a=rtpmap:126 telephone-event/8000",
                ]
            )
        else:
            rebuilt.extend(
                [
                    "a=extmap:14 urn:ietf:params:rtp-hdrext:toffset",
                    "a=extmap:2 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
                    "a=extmap:13 urn:3gpp:video-orientation",
                    "a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
                    "a=extmap:5 http://www.webrtc.org/experiments/rtp-hdrext/playout-delay",
                    "a=extmap:6 http://www.webrtc.org/experiments/rtp-hdrext/video-content-type",
                    "a=extmap:7 http://www.webrtc.org/experiments/rtp-hdrext/video-timing",
                    "a=extmap:8 http://www.webrtc.org/experiments/rtp-hdrext/color-space",
                    "a=extmap:4 urn:ietf:params:rtp-hdrext:sdes:mid",
                    "a=extmap:10 urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
                    "a=extmap:11 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
                    "a=rtpmap:96 H264/90000",
                    "a=rtcp-fb:96 goog-remb",
                    "a=rtcp-fb:96 transport-cc",
                    "a=rtcp-fb:96 ccm fir",
                    "a=rtcp-fb:96 nack",
                    "a=rtcp-fb:96 nack pli",
                    "a=fmtp:96 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
                    "a=rtpmap:97 rtx/90000",
                    "a=fmtp:97 apt=96",
                    "a=rtpmap:98 red/90000",
                    "a=rtpmap:99 rtx/90000",
                    "a=fmtp:99 apt=98",
                    "a=rtpmap:100 ulpfec/90000",
                    "a=rtpmap:35 flexfec-03/90000",
                    "a=rtcp-fb:35 goog-remb",
                    "a=rtcp-fb:35 transport-cc",
                    "a=fmtp:35 repair-window=10000000",
                ]
            )
    return "\r\n".join(rebuilt).rstrip("\r\n") + "\r\n"


def _ensure_ice_options(section: list[str]) -> list[str]:
    if any(line.startswith("a=ice-options:") for line in section):
        return section
    idx = next(
        (
            i + 1
            for i, line in enumerate(section)
            if line.startswith("a=ice-pwd:")
        ),
        1,
    )
    copy = list(section)
    copy.insert(idx, "a=ice-options:trickle renomination")
    return copy


def _trickle_offer(
    gathered_sdp: str,
    audio_ssrc: int,
    cname: str,
) -> tuple[str, list[tuple[str, int, str]]]:
    source = gathered_sdp.replace("\r\n", "\n").split("\n")
    candidates: list[tuple[str, int, str]] = []
    mid: str | None = None
    mline = -1
    for line in source:
        if line.startswith("m="):
            mline += 1
            mid = None
        elif line.startswith("a=mid:"):
            mid = line.split(":", 1)[1]
        elif line.startswith("a=candidate:"):
            candidates.append((mid or str(mline), mline, line[2:]))

    lines = [
        line
        for line in source
        if line
        and not line.startswith("a=candidate:")
        and line != "a=end-of-candidates"
    ]

    rebuilt: list[str] = []
    kind: str | None = None
    ssrc_added = False
    for line in lines:
        if line.startswith("m="):
            kind = line.split("=", 1)[1].split(" ", 1)[0]
            rebuilt.append(line)
            continue
        if line.startswith("a=ice-options:"):
            rebuilt.append("a=ice-options:trickle renomination")
            continue
        if line.startswith("a=fingerprint:"):
            if "sha-256" in line.lower():
                rebuilt.append(line)
            continue
        rebuilt.append(line)
        if (
            kind == "audio"
            and line == "a=msid:KvsLocalMediaStream KvsAudioTrack"
            and not ssrc_added
        ):
            rebuilt.append(f"a=ssrc:{audio_ssrc} cname:{cname}")
            rebuilt.append(
                f"a=ssrc:{audio_ssrc} msid:KvsLocalMediaStream KvsAudioTrack"
            )
            ssrc_added = True

    final: list[str] = []
    session: list[str] = []
    section: list[str] = []
    for line in rebuilt:
        if line.startswith("m="):
            if section:
                final.extend(_ensure_ice_options(section))
            else:
                final.extend(session)
            section = [line]
        elif section:
            section.append(line)
        else:
            session.append(line)
    if section:
        final.extend(_ensure_ice_options(section))
    else:
        final.extend(session)

    return "\r\n".join(final).rstrip("\r\n") + "\r\n", candidates


def _candidate_payload(candidate: str, mid: str, index: int) -> str:
    return json.dumps(
        {
            "candidate": candidate,
            "sdpMid": mid,
            "sdpMLineIndex": index,
        },
        separators=(",", ":"),
    )


def _prefer_h264(transceiver: Any) -> None:
    capabilities = RTCRtpSender.getCapabilities("video")
    h264 = [
        codec
        for codec in capabilities.codecs
        if codec.mimeType.lower() == "video/h264"
    ]
    if h264:
        transceiver.setCodecPreferences(h264)


class EcovacsLiveViewSession:
    """One live-view WebRTC session for a discovered robot."""

    # Synthetic Opus silence payload used only to keep the sendrecv audio
    # transceiver active. It contains no user/session media.
    _OPUS_SILENCE = b"\xF8\xFF\xFE"

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        session_info: KinesisSession,
        robot: dict[str, Any],
    ) -> None:
        self.hass = hass
        self.session_info = session_info
        self.robot = robot
        self.pc: RTCPeerConnection | None = None
        self.websocket: Any = None
        self.silent_audio = SilentAudioTrack()
        self.stop_event = asyncio.Event()
        self.first_frame_event = asyncio.Event()
        self.video_frames = 0
        self.audio_frames = 0
        self.latest_jpeg: bytes | None = None
        self.latest_jpeg_sequence = 0
        self._last_jpeg_monotonic = 0.0
        self.status = "starting"
        self.error: str | None = None
        self._media_tasks: list[asyncio.Task[Any]] = []
        self._receive_task: asyncio.Task[Any] | None = None
        self._opus_count = 0
        self._wrapped: set[int] = set()
        self._cname = uuid.uuid4().hex[:16]

    def _install_opus_rewriter(self) -> None:
        if self.pc is None:
            return
        transceiver = next(
            (
                item
                for item in self.pc.getTransceivers()
                if item.kind == "audio"
            ),
            None,
        )
        if transceiver is None:
            return
        transport = getattr(transceiver.sender, "transport", None)
        if transport is None or id(transport) in self._wrapped:
            return
        original = getattr(transport, "_send_rtp", None)
        if original is None:
            return
        viewer = self

        async def rewritten(
            transport_self: Any,
            data: bytes,
            *,
            _original: Any = original,
        ) -> Any:
            raw = bytes(data)
            if (
                len(raw) >= 12
                and (raw[0] >> 6) == 2
                and (raw[1] & 0x7F) == 111
                and not (192 <= raw[1] <= 223)
            ):
                payload = viewer._OPUS_SILENCE
                viewer._opus_count += 1
                header = bytearray(raw[:12])
                header[0] = 0x80
                header[1] = 0x6F
                if viewer._opus_count == 1:
                    header[1] |= 0x80
                return await _original(bytes(header) + payload)
            return await _original(raw)

        transport._send_rtp = MethodType(rewritten, transport)
        self._wrapped.add(id(transport))

    async def _consume_video(self, track: Any) -> None:
        try:
            while not self.stop_event.is_set():
                frame = await track.recv()
                self.video_frames += 1

                # Keep a browser-friendly JPEG copy for the Home Assistant
                # camera entity.  Limit conversion to ~5 fps to avoid wasting
                # CPU while still providing smooth enough dashboard video.
                now = time.monotonic()
                if now - self._last_jpeg_monotonic >= 0.2:
                    try:
                        self.latest_jpeg = await self.hass.async_add_executor_job(
                            _frame_to_jpeg, frame
                        )
                        self.latest_jpeg_sequence += 1
                        self._last_jpeg_monotonic = now
                    except Exception:
                        _LOGGER.exception(
                            "ECOVACS camera frame JPEG conversion failed"
                        )

                if self.video_frames == 1:
                    self.status = "video_received"
                    _LOGGER.debug(
                        "ECOVACS WebRTC first video frame received"
                    )
                    self.first_frame_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.stop_event.is_set():
                self.error = str(exc)
                self.status = "error"
                _LOGGER.exception(
                    "ECOVACS WebRTC video-track consumer failed"
                )

    async def _consume_audio(self, track: Any) -> None:
        try:
            while not self.stop_event.is_set():
                await track.recv()
                self.audio_frames += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _handle(self, raw: str | bytes) -> None:
        if raw is None:
            _LOGGER.debug("Ignoring null ECOVACS signaling frame")
            return

        if isinstance(raw, bytes):
            if not raw:
                _LOGGER.debug("Ignoring empty binary ECOVACS signaling frame")
                return
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                _LOGGER.warning(
                    "Ignoring non-UTF-8 ECOVACS signaling frame: byte_len=%d",
                    len(raw),
                )
                return

        if not raw:
            _LOGGER.debug("Ignoring empty ECOVACS signaling frame")
            return

        try:
            message = json.loads(raw)
        except json.JSONDecodeError as exc:
            _LOGGER.warning(
                "Ignoring non-JSON ECOVACS signaling frame: char_len=%d error=%s",
                len(raw),
                exc,
            )
            return
        kind = str(
            message.get("messageType") or message.get("action") or ""
        ).upper()
        encoded = str(message.get("messagePayload") or "")

        _LOGGER.debug(
            "ECOVACS WebRTC signaling message received: kind=%s encoded_len=%d",
            kind or "<unknown>",
            len(encoded),
        )

        if not encoded:
            if kind == "GO_AWAY":
                _LOGGER.warning("ECOVACS WebRTC signaling received GO_AWAY")
                self.stop_event.set()
            return

        payload = _decode(encoded)

        if kind == "SDP_ANSWER":
            if self.pc is None:
                _LOGGER.warning(
                    "ECOVACS WebRTC SDP answer arrived before peer connection existed"
                )
                return
            answer = json.loads(payload)
            answer_sdp = str(answer["sdp"])
            _LOGGER.debug(
                "ECOVACS WebRTC SDP answer received: type=%s sdp_len=%d",
                str(answer.get("type", "answer")),
                len(answer_sdp),
            )
            await self.pc.setRemoteDescription(
                RTCSessionDescription(
                    sdp=answer_sdp,
                    type=str(answer.get("type", "answer")),
                )
            )
            _LOGGER.debug(
                "ECOVACS WebRTC remote description applied: signaling_state=%s "
                "ice_state=%s connection_state=%s",
                self.pc.signalingState,
                self.pc.iceConnectionState,
                self.pc.connectionState,
            )
        elif kind == "ICE_CANDIDATE":
            _LOGGER.debug(
                "ECOVACS WebRTC remote ICE_CANDIDATE received (currently diagnostic only): "
                "payload_len=%d",
                len(payload),
            )
        elif kind == "GO_AWAY":
            _LOGGER.warning("ECOVACS WebRTC signaling received GO_AWAY")
            self.stop_event.set()

    async def start(self, timeout: float = 30) -> bool:
        """Start WebRTC and wait for the first video frame."""
        self.status = "connecting"
        http = self.hass.data["ecovacs_live"][next(
            entry_id
            for entry_id, runtime in self.hass.data["ecovacs_live"].items()
            if self.robot in runtime.get("devices", [])
        )]["manager"].http
        _LOGGER.debug(
            "ECOVACS WebRTC stage: describing Kinesis signaling channel"
        )
        arn = await _describe_channel(http, self.session_info)
        _LOGGER.debug(
            "ECOVACS WebRTC stage: channel described; requesting signaling endpoints"
        )
        wss_endpoint, https_endpoint = await _endpoints(
            http, self.session_info, arn
        )
        _LOGGER.debug(
            "ECOVACS WebRTC stage: signaling endpoints received; requesting ICE servers"
        )
        ice_servers = await _ice_servers(
            http, self.session_info, arn, https_endpoint
        )
        _LOGGER.debug(
            "ECOVACS WebRTC stage: ICE configuration ready; server_count=%d",
            len(ice_servers),
        )

        self.pc = RTCPeerConnection(
            configuration=RTCConfiguration(iceServers=ice_servers)
        )
        setattr(self.pc, "_RTCPeerConnection__cname", self._cname)

        audio = self.pc.addTransceiver(
            self.silent_audio,
            direction="sendrecv",
        )
        video = self.pc.addTransceiver("video", direction="recvonly")
        _prefer_h264(video)
        self._install_opus_rewriter()

        @self.pc.on("track")
        def _on_track(track: Any) -> None:
            _LOGGER.debug(
                "ECOVACS WebRTC remote track received: kind=%s",
                track.kind,
            )
            if track.kind == "video":
                self._media_tasks.append(
                    asyncio.create_task(self._consume_video(track))
                )
            elif track.kind == "audio":
                self._media_tasks.append(
                    asyncio.create_task(self._consume_audio(track))
                )

        @self.pc.on("connectionstatechange")
        async def _connection_state() -> None:
            if self.pc is None:
                return
            _LOGGER.debug(
                "ECOVACS WebRTC connection state changed: %s",
                self.pc.connectionState,
            )
            if self.pc.connectionState == "connected":
                if self.status != "video_received":
                    self.status = "connected_waiting_video"
            elif self.pc.connectionState in {"failed", "closed"}:
                if self.status != "video_received":
                    self.status = self.pc.connectionState
                self.stop_event.set()

        @self.pc.on("iceconnectionstatechange")
        async def _ice_connection_state() -> None:
            if self.pc is not None:
                _LOGGER.debug(
                    "ECOVACS WebRTC ICE connection state changed: %s",
                    self.pc.iceConnectionState,
                )

        @self.pc.on("icegatheringstatechange")
        async def _ice_gathering_state() -> None:
            if self.pc is not None:
                _LOGGER.debug(
                    "ECOVACS WebRTC ICE gathering state changed: %s",
                    self.pc.iceGatheringState,
                )

        @self.pc.on("signalingstatechange")
        async def _signaling_state() -> None:
            if self.pc is not None:
                _LOGGER.debug(
                    "ECOVACS WebRTC signaling state changed: %s",
                    self.pc.signalingState,
                )

        signed = _signed_wss(
            self.session_info,
            arn,
            wss_endpoint,
        )
        _LOGGER.debug(
            "ECOVACS WebRTC stage: connecting Kinesis signaling WebSocket"
        )
        self.websocket = await websockets.connect(
            signed,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=8 * 1024 * 1024,
        )
        _LOGGER.debug(
            "ECOVACS WebRTC stage: Kinesis signaling WebSocket connected"
        )

        _LOGGER.debug("ECOVACS WebRTC stage: creating SDP offer")
        offer = await self.pc.createOffer()
        offer = RTCSessionDescription(
            sdp=_rewrite_offer(offer.sdp),
            type=offer.type,
        )
        await self.pc.setLocalDescription(offer)
        _LOGGER.debug(
            "ECOVACS WebRTC stage: local SDP applied; gathering ICE"
        )
        await _wait_ice(self.pc)
        _LOGGER.debug(
            "ECOVACS WebRTC stage: ICE gathering complete"
        )

        local = self.pc.localDescription
        if local is None:
            raise RuntimeError("WebRTC did not produce a local offer")

        audio_ssrc = int(getattr(audio.sender, "_ssrc"))
        trickle_sdp, candidates = _trickle_offer(
            local.sdp,
            audio_ssrc,
            self._cname,
        )

        offer_payload = json.dumps(
            {"type": local.type, "sdp": trickle_sdp},
            separators=(",", ":"),
        )
        _LOGGER.debug(
            "ECOVACS WebRTC stage: sending SDP offer; sdp_len=%d candidate_count=%d",
            len(trickle_sdp),
            len(candidates),
        )
        await self.websocket.send(
            _message(
                "SDP_OFFER",
                offer_payload,
                correlation_id=str(uuid.uuid4()),
            )
        )
        _LOGGER.debug("ECOVACS WebRTC stage: SDP offer sent")

        sent: set[tuple[str, int, str]] = set()
        for mid, index, candidate in candidates:
            key = (mid, index, candidate)
            if key in sent:
                continue
            sent.add(key)
            await self.websocket.send(
                _message(
                    "ICE_CANDIDATE",
                    _candidate_payload(candidate, mid, index),
                )
            )

        _LOGGER.debug(
            "ECOVACS WebRTC stage: sent %d unique local ICE candidates",
            len(sent),
        )

        async def _receive() -> None:
            _LOGGER.debug(
                "ECOVACS WebRTC signaling receive loop started"
            )
            try:
                async for incoming in self.websocket:
                    await self._handle(incoming)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self.stop_event.is_set():
                    self.error = str(exc)
                    self.status = "error"
                    _LOGGER.exception(
                        "ECOVACS WebRTC signaling receive loop failed"
                    )
                    self.stop_event.set()

        self._receive_task = asyncio.create_task(_receive())

        try:
            await asyncio.wait_for(
                self.first_frame_event.wait(),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            self.status = "timeout_waiting_video"
            _LOGGER.warning(
                "ECOVACS WebRTC timed out waiting for first video frame: "
                "connection_state=%s ice_state=%s signaling_state=%s "
                "video_frames=%d audio_frames=%d error=%r",
                self.pc.connectionState if self.pc is not None else "<none>",
                self.pc.iceConnectionState if self.pc is not None else "<none>",
                self.pc.signalingState if self.pc is not None else "<none>",
                self.video_frames,
                self.audio_frames,
                self.error,
            )
            return False

    async def stop(self) -> None:
        self.stop_event.set()
        if self._receive_task is not None:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

        for task in self._media_tasks:
            task.cancel()
        for task in self._media_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._media_tasks.clear()

        try:
            self.silent_audio.stop()
        except Exception:
            pass

        if self.websocket is not None:
            try:
                await self.websocket.close()
            except Exception:
                pass
            self.websocket = None

        if self.pc is not None:
            try:
                await self.pc.close()
            except Exception:
                pass
            self.pc = None

        if self.status != "error":
            self.status = "stopped"


class LiveViewManager:
    """Manage per-robot live-view test sessions."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        aiohttp_session: Any,
        rest_config: Any,
        credentials: Any,
        login_client_id: str,
        country: str,
        pin: str,
        auto_stop_minutes: int = 10,
    ) -> None:
        self.hass = hass
        self.http = aiohttp_session
        self.rest_config = rest_config
        self.credentials = credentials
        self.login_client_id = login_client_id
        self.country = country
        self.pin = pin
        self.auto_stop_minutes = max(0, int(auto_stop_minutes))
        self.sessions: dict[str, EcovacsLiveViewSession] = {}
        self.status: dict[str, str] = {}
        self.errors: dict[str, str | None] = {}
        self.frames: dict[str, int] = {}
        self.last_images: dict[str, bytes] = {}
        self.started_monotonic: dict[str, float] = {}
        self._auto_stop_tasks: dict[str, asyncio.Task[Any]] = {}
        self._listeners: list[Any] = []

    def get_latest_image(self, did: str) -> bytes | None:
        """Return the most recent JPEG received for a robot."""
        live = self.sessions.get(did)
        if live is not None and live.latest_jpeg is not None:
            return live.latest_jpeg
        return self.last_images.get(did)

    def get_image_sequence(self, did: str) -> int:
        """Return a monotonically increasing frame sequence while live."""
        live = self.sessions.get(did)
        if live is None:
            return -1
        return live.latest_jpeg_sequence

    def is_streaming(self, did: str) -> bool:
        """Return whether a live WebRTC session currently exists."""
        live = self.sessions.get(did)
        return (
            live is not None
            and not live.stop_event.is_set()
            and live.status in {
                "connecting",
                "connected_waiting_video",
                "video_received",
            }
        )

    def get_auto_stop_remaining(self, did: str) -> int | None:
        """Return seconds until automatic stop, or None when disabled/inactive."""
        if self.auto_stop_minutes <= 0 or did not in self.started_monotonic:
            return None
        elapsed = time.monotonic() - self.started_monotonic[did]
        remaining = int((self.auto_stop_minutes * 60) - elapsed)
        return max(0, remaining)

    def get_session_elapsed(self, did: str) -> int | None:
        """Return active session duration in seconds."""
        started = self.started_monotonic.get(did)
        if started is None:
            return None
        return max(0, int(time.monotonic() - started))

    def _cancel_auto_stop(self, did: str) -> None:
        task = self._auto_stop_tasks.pop(did, None)
        if task is not None and not task.done():
            task.cancel()

    def _schedule_auto_stop(
        self,
        robot: dict[str, Any],
        live: EcovacsLiveViewSession,
    ) -> None:
        did = str(robot["did"])
        self._cancel_auto_stop(did)
        if self.auto_stop_minutes <= 0:
            return

        async def _auto_stop() -> None:
            try:
                await asyncio.sleep(self.auto_stop_minutes * 60)
                # Only stop if this is still the same active session.
                if self.sessions.get(did) is not live:
                    return
                _LOGGER.info(
                    "Automatically stopping ECOVACS Live View for %s after %d minute(s)",
                    did,
                    self.auto_stop_minutes,
                )
                await self.stop(robot, reason="auto_stopped")
            except asyncio.CancelledError:
                return

        self._auto_stop_tasks[did] = asyncio.create_task(_auto_stop())

    def subscribe(self, callback: Any) -> Any:
        self._listeners.append(callback)

        def _remove() -> None:
            if callback in self._listeners:
                self._listeners.remove(callback)

        return _remove

    def _notify(self) -> None:
        for callback in list(self._listeners):
            callback()

    async def start(self, robot: dict[str, Any]) -> bool:
        did = str(robot["did"])
        existing = self.sessions.pop(did, None)
        if existing is not None:
            self._cancel_auto_stop(did)
            if existing.latest_jpeg is not None:
                self.last_images[did] = existing.latest_jpeg
            await existing.stop()

        self.started_monotonic.pop(did, None)
        self.status[did] = "requesting_session"
        self.errors[did] = None
        self._notify()

        try:
            session_info = await async_start_watch(
                self.http,
                rest_config=self.rest_config,
                login_client_id=self.login_client_id,
                credentials=self.credentials,
                country=self.country,
                pin=self.pin,
                robot=robot,
            )
            await async_send_app_ping(
                self.http,
                credentials=self.credentials,
                robot=robot,
            )

            live = EcovacsLiveViewSession(
                self.hass,
                session_info=session_info,
                robot=robot,
            )
            self.sessions[did] = live
            self.status[did] = "connecting"
            self._notify()

            success = await live.start(timeout=30)
            self.frames[did] = live.video_frames
            self.status[did] = live.status
            self.errors[did] = live.error

            if success:
                self.started_monotonic[did] = time.monotonic()
                self._schedule_auto_stop(robot, live)
            else:
                # Failed/timeout sessions must not remain alive indefinitely.
                if live.latest_jpeg is not None:
                    self.last_images[did] = live.latest_jpeg
                await live.stop()
                if self.sessions.get(did) is live:
                    self.sessions.pop(did, None)

            self._notify()
            return success
        except Exception as exc:
            _LOGGER.exception("ECOVACS Live View start failed")
            self._cancel_auto_stop(did)
            self.started_monotonic.pop(did, None)
            failed = self.sessions.pop(did, None)
            if failed is not None:
                try:
                    if failed.latest_jpeg is not None:
                        self.last_images[did] = failed.latest_jpeg
                    await failed.stop()
                except Exception:
                    _LOGGER.exception("Error cleaning failed ECOVACS Live View session")
            self.status[did] = "error"
            self.errors[did] = str(exc)
            self._notify()
            return False

    async def stop(
        self,
        robot: dict[str, Any],
        *,
        reason: str = "stopped",
    ) -> None:
        did = str(robot["did"])
        self._cancel_auto_stop(did)
        self.started_monotonic.pop(did, None)
        live = self.sessions.pop(did, None)
        if live is not None:
            if live.latest_jpeg is not None:
                self.last_images[did] = live.latest_jpeg
            await live.stop()
            self.frames[did] = live.video_frames
        self.status[did] = reason
        self._notify()

    async def stop_all(self) -> None:
        for did in list(self._auto_stop_tasks):
            self._cancel_auto_stop(did)
        self.started_monotonic.clear()
        for did, live in list(self.sessions.items()):
            if live.latest_jpeg is not None:
                self.last_images[did] = live.latest_jpeg
            await live.stop()
            self.frames[did] = live.video_frames
        self.sessions.clear()
        self._notify()
