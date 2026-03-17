# Copyright (c) 2020 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

from .. import cbor
from .base import Ctap2, Info
from .pin import PinProtocol, _PinUv

from typing import Optional, List
from enum import IntEnum, unique
import struct


class Config:
    """Implementation of the CTAP2.1 Authenticator Config API.

    :param ctap: An instance of a CTAP2 object.
    :param pin_uv_protocol: An instance of a PinUvAuthProtocol.
    :param pin_uv_token: A valid PIN/UV Auth Token for the current CTAP session.
    """

    @unique
    class CMD(IntEnum):
        ENABLE_ENTERPRISE_ATT = 0x01
        TOGGLE_ALWAYS_UV = 0x02
        SET_MIN_PIN_LENGTH = 0x03
        VENDOR_PROTOTYPE = 0xFF

    @unique
    class PARAM(IntEnum):
        NEW_MIN_PIN_LENGTH = 0x01
        MIN_PIN_LENGTH_RPIDS = 0x02
        FORCE_CHANGE_PIN = 0x03

    @staticmethod
    def is_supported(info: Info) -> bool:
        return info.options.get("authnrCfg") is True

    def __init__(
        self,
        ctap: Ctap2,
        pin_uv_protocol: Optional[PinProtocol] = None,
        pin_uv_token: Optional[bytes] = None,
    ):
        if not self.is_supported(ctap.info):
            raise ValueError("Authenticator does not support Config")

        self.ctap = ctap
        self.pin_uv = (
            _PinUv(pin_uv_protocol, pin_uv_token)
            if pin_uv_protocol and pin_uv_token
            else None
        )

    def _call(self, sub_cmd, params=None):
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        else:
            params = None
        if self.pin_uv:
            msg = (
                b"\xff" * 32
                + b"\x0d"
                + struct.pack("<B", sub_cmd)
                + (cbor.encode(params) if params else b"")
            )
            pin_uv_protocol = self.pin_uv.protocol.VERSION
            pin_uv_param = self.pin_uv.protocol.authenticate(self.pin_uv.token, msg)
        else:
            pin_uv_protocol = None
            pin_uv_param = None
        return self.ctap.config(sub_cmd, params, pin_uv_protocol, pin_uv_param)

    def enable_enterprise_attestation(self) -> None:
        """Enables Enterprise Attestation.

        If already enabled, this command is ignored.
        """
        self._call(Config.CMD.ENABLE_ENTERPRISE_ATT)

    def toggle_always_uv(self) -> None:
        """Toggle the alwaysUV setting.

        When true, the Authenticator always requires UV for credential assertion.
        """
        self._call(Config.CMD.TOGGLE_ALWAYS_UV)

    def set_min_pin_length(
        self,
        min_pin_length: Optional[int] = None,
        rp_ids: Optional[List[str]] = None,
        force_change_pin: bool = False,
    ) -> None:
        """Set the minimum PIN length allowed when setting/changing the PIN.

        :param min_pin_length: The minimum PIN length the Authenticator should allow.
        :param rp_ids: A list of RP IDs which should be allowed to get the current
            minimum PIN length.
        :param force_change_pin: True if the Authenticator should enforce changing the
            PIN before the next use.
        """
        self._call(
            Config.CMD.SET_MIN_PIN_LENGTH,
            {
                Config.PARAM.NEW_MIN_PIN_LENGTH: min_pin_length,
                Config.PARAM.MIN_PIN_LENGTH_RPIDS: rp_ids,
                Config.PARAM.FORCE_CHANGE_PIN: force_change_pin,
            },
        )
