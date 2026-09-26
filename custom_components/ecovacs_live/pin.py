"""Live-view PIN encoding for ECOVACS device families."""

from __future__ import annotations

import hashlib
from typing import Any


def encode_live_view_pin(pin: str, robot: dict[str, Any]) -> str:
    """Encode the PIN using the device family's live-view scheme."""
    model_names = (robot.get("device_name"), robot.get("model"))
    is_goat = any(
        isinstance(name, str) and name.strip().casefold().startswith("goat ")
        for name in model_names
    )
    if is_goat:
        return hashlib.sha256(f"goat_{pin}".encode("utf-8")).hexdigest()
    return hashlib.md5(f"eco_{pin}".encode("utf-8")).hexdigest()
