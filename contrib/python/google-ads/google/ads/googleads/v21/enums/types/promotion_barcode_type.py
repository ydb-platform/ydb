# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations


import proto  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v21.enums",
    marshal="google.ads.googleads.v21",
    manifest={
        "PromotionBarcodeTypeEnum",
    },
)


class PromotionBarcodeTypeEnum(proto.Message):
    r"""Container for enum describing a promotion barcode type."""

    class PromotionBarcodeType(proto.Enum):
        r"""A promotion barcode type.

        Values:
            UNSPECIFIED (0):
                Not specified.
            UNKNOWN (1):
                Used for return value only. Represents value
                unknown in this version.
            AZTEC (2):
                Aztec 2D barcode format.
                Max 350 characters and no links
            CODABAR (3):
                CODABAR 1D format.
                Max 12 characters and no links. Supported
                characters include 0123456789-$:/.+ and optional
                start and end guards from ABCD.
            CODE39 (4):
                Code 39 1D format. Max 8 characters and no links. Supported
                characters include 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-.
                \*$/+%.
            CODE128 (5):
                Code 128 1D format.
                Max 18 ASCII characters only and no links
            DATA_MATRIX (6):
                Data Matrix 2D barcode format.
                Max 525 ISO-8859-1 characters only and no links
            EAN8 (7):
                EAN-8 1D format.
                The barcode value should be 7 digits (the check
                digit will be computed automatically) or 8
                digits (if you are providing your own check
                digit).
            EAN13 (8):
                EAN-13 1D format.
                The barcode value should be 12 digits (the check
                digit will be computed automatically) or 13
                digits (if you are providing your own check
                digit).
            ITF (9):
                ITF (Interleaved Two of Five) 1D format.
                Must be 14 digits long
            PDF417 (10):
                PDF417 format.
                Max 140 characters and no links
            UPC_A (11):
                UPC-A 1D format.
                The barcode value should be 11 digits (the check
                digit will be computed automatically) or 12
                digits (if you are providing your own check
                digit).
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        AZTEC = 2
        CODABAR = 3
        CODE39 = 4
        CODE128 = 5
        DATA_MATRIX = 6
        EAN8 = 7
        EAN13 = 8
        ITF = 9
        PDF417 = 10
        UPC_A = 11


__all__ = tuple(sorted(__protobuf__.manifest))
