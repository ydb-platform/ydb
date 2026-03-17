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
    package="google.ads.googleads.v21.errors",
    marshal="google.ads.googleads.v21",
    manifest={
        "FunctionErrorEnum",
    },
)


class FunctionErrorEnum(proto.Message):
    r"""Container for enum describing possible function errors."""

    class FunctionError(proto.Enum):
        r"""Enum describing possible function errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            INVALID_FUNCTION_FORMAT (2):
                The format of the function is not recognized
                as a supported function format.
            DATA_TYPE_MISMATCH (3):
                Operand data types do not match.
            INVALID_CONJUNCTION_OPERANDS (4):
                The operands cannot be used together in a
                conjunction.
            INVALID_NUMBER_OF_OPERANDS (5):
                Invalid numer of Operands.
            INVALID_OPERAND_TYPE (6):
                Operand Type not supported.
            INVALID_OPERATOR (7):
                Operator not supported.
            INVALID_REQUEST_CONTEXT_TYPE (8):
                Request context type not supported.
            INVALID_FUNCTION_FOR_CALL_PLACEHOLDER (9):
                The matching function is not allowed for call
                placeholders
            INVALID_FUNCTION_FOR_PLACEHOLDER (10):
                The matching function is not allowed for the
                specified placeholder
            INVALID_OPERAND (11):
                Invalid operand.
            MISSING_CONSTANT_OPERAND_VALUE (12):
                Missing value for the constant operand.
            INVALID_CONSTANT_OPERAND_VALUE (13):
                The value of the constant operand is invalid.
            INVALID_NESTING (14):
                Invalid function nesting.
            MULTIPLE_FEED_IDS_NOT_SUPPORTED (15):
                The Feed ID was different from another Feed
                ID in the same function.
            INVALID_FUNCTION_FOR_FEED_WITH_FIXED_SCHEMA (16):
                The matching function is invalid for use with
                a feed with a fixed schema.
            INVALID_ATTRIBUTE_NAME (17):
                Invalid attribute name.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_FUNCTION_FORMAT = 2
        DATA_TYPE_MISMATCH = 3
        INVALID_CONJUNCTION_OPERANDS = 4
        INVALID_NUMBER_OF_OPERANDS = 5
        INVALID_OPERAND_TYPE = 6
        INVALID_OPERATOR = 7
        INVALID_REQUEST_CONTEXT_TYPE = 8
        INVALID_FUNCTION_FOR_CALL_PLACEHOLDER = 9
        INVALID_FUNCTION_FOR_PLACEHOLDER = 10
        INVALID_OPERAND = 11
        MISSING_CONSTANT_OPERAND_VALUE = 12
        INVALID_CONSTANT_OPERAND_VALUE = 13
        INVALID_NESTING = 14
        MULTIPLE_FEED_IDS_NOT_SUPPORTED = 15
        INVALID_FUNCTION_FOR_FEED_WITH_FIXED_SCHEMA = 16
        INVALID_ATTRIBUTE_NAME = 17


__all__ = tuple(sorted(__protobuf__.manifest))
