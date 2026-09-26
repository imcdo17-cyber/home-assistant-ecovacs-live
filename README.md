# ECOVACS Live View for Home Assistant

[![HACS validation](https://github.com/imcdo17-cyber/home-assistant-ecovacs-live/actions/workflows/validate.yml/badge.svg)](https://github.com/imcdo17-cyber/home-assistant-ecovacs-live/actions/workflows/validate.yml)
[![Hassfest](https://github.com/imcdo17-cyber/home-assistant-ecovacs-live/actions/workflows/hassfest.yml/badge.svg)](https://github.com/imcdo17-cyber/home-assistant-ecovacs-live/actions/workflows/hassfest.yml)

Unofficial Home Assistant custom integration that adds ECOVACS robot Live View
camera support using the ECOVACS Kinesis/WebRTC flow.

## Status

**Tested with:** ECOVACS DEEBOT T90 OMNI and GOAT O1000 LiDAR Pro

This project uses private/undocumented ECOVACS APIs and may stop working if
ECOVACS changes its application or backend services.

## Features

- ECOVACS account login through Home Assistant config flow
- Automatic robot discovery using `GetGlobalDeviceList`
- No hardcoded robot DID, class, resource or serial number required
- Live View PIN verification
- Device-family-specific Live View PIN encoding for GOAT and DEEBOT models
- AWS Kinesis/WebRTC video session
- Home Assistant camera entity
- MJPEG dashboard stream
- Start Live View button
- Stop Live View button
- Live View status sensor
- Configurable automatic stream shutdown (default: 10 minutes)
- Automatic cleanup of failed/timed-out video sessions
- Safe migration/cleanup of stale `ecovacs_live` device-registry records

## Installation

### HACS custom repository

Until this integration is accepted into the default HACS catalogue:

1. Open **HACS** in Home Assistant.
2. Open the menu and choose **Custom repositories**.
3. Add:
   `https://github.com/imcdo17-cyber/home-assistant-ecovacs-live`
4. Select **Integration** as the category.
5. Install **ECOVACS Live View**.
6. Restart Home Assistant.

### Manual installation

Copy:

```text
custom_components/ecovacs_live/
```

to:

```text
/config/custom_components/ecovacs_live/
```

or, on Home Assistant OS when using the Terminal & SSH add-on:

```text
/homeassistant/custom_components/ecovacs_live/
```

Restart Home Assistant, then go to:

**Settings → Devices & services → Add integration → ECOVACS Live View**

You will be asked for:

- ECOVACS account/email
- ECOVACS password
- two-letter country code
- robot Live View PIN

If ECOVACS requires device verification, the integration will prompt for the
email verification code.

## Using Live View

1. Press **Start live view**.
2. Wait for the Live View status to report `video_received`.
3. Open the **Live view** camera entity or add it to a dashboard.
4. Press **Stop live view** when finished.

The integration can automatically stop an active session. The default is
10 minutes. Change it under the integration's **Configure**/Options screen.
Set the value to `0` to disable automatic stopping.

## Privacy and security

The source code contains **no user-specific robot identifiers, account details,
camera PINs, ECOVACS tokens, AWS credentials, local IP addresses, or device
serial numbers**.

At runtime, your ECOVACS account credentials and Live View PIN are stored in
Home Assistant's config-entry storage in the same way other credential-based
custom integrations store their configuration.

Temporary ECOVACS/AWS credentials are held in memory only for the active
session by this integration and are not intentionally written to its logs.

**Do not publish Home Assistant debug logs without reviewing/redacting them.**
Third-party libraries such as `deebot-client` can produce very verbose debug
output, and service responses may contain temporary credentials.

## Known scope

This release focuses on Live View. It does not attempt to replace the normal
Home Assistant Ecovacs integration for vacuum controls, maps, consumables or
cleaning sensors.

## Disclaimer

This is an independent community project and is not affiliated with, endorsed
by, or supported by ECOVACS.

Use at your own risk. Live View involves a camera inside your home, so review
the code and your Home Assistant security configuration before use.

## Support

Please use the GitHub issue tracker for bugs:

https://github.com/imcdo17-cyber/home-assistant-ecovacs-live/issues

Before posting logs, read `SECURITY.md` and remove all account/device identifiers
and temporary credentials.
