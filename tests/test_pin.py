"""Tests for live-view PIN encoding without Home Assistant dependencies."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "custom_components" / "ecovacs_live"))

from pin import encode_live_view_pin


class EncodeLiveViewPinTests(unittest.TestCase):
    def test_goat_device_name_uses_goat_sha256(self) -> None:
        robot = {"device_name": "GOAT O1000 LiDAR Pro", "nick": "Garden Robot"}
        self.assertEqual(
            encode_live_view_pin("0123", robot),
            "9ade4ccda243014c79c8c4cbebe780a551f3535698a8148cb624685db11dbf28",
        )

    def test_goat_model_is_used_when_device_name_is_missing(self) -> None:
        robot = {"model": "  goat G1  "}
        self.assertEqual(
            encode_live_view_pin("0123", robot),
            "9ade4ccda243014c79c8c4cbebe780a551f3535698a8148cb624685db11dbf28",
        )

    def test_non_goat_keeps_existing_eco_md5_encoding(self) -> None:
        robot = {"device_name": "DEEBOT T90 OMNI", "model": "T90"}
        self.assertEqual(
            encode_live_view_pin("0123", robot),
            "34a29b5c4d5420d0eb4ac968dcb08baf",
        )

    def test_nickname_does_not_select_goat_encoding(self) -> None:
        robot = {"device_name": "DEEBOT T90 OMNI", "nick": "GOAT The Vacuum"}
        self.assertEqual(
            encode_live_view_pin("0123", robot),
            "34a29b5c4d5420d0eb4ac968dcb08baf",
        )


if __name__ == "__main__":
    unittest.main()
