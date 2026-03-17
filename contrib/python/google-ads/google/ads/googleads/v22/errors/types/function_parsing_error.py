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
    package="google.ads.googleads.v22.errors",
    marshal="google.ads.googleads.v22",
    manifest={
        "FunctionParsingErrorEnum",
    },
)


class FunctionParsingErrorEnum(proto.Message):
    r"""Container for enum describing possible function parsing
    errors.

    """

    class FunctionParsingError(proto.Enum):
        r"""Enum describing possible function parsing errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            NO_MORE_INPUT (2):
                Unexpected end of function string.
            EXPECTED_CHARACTER (3):
                Could not find an expected character.
            UNEXPECTED_SEPARATOR (4):
                Unexpected separator character.
            UNMATCHED_LEFT_BRACKET (5):
                Unmatched left bracket or parenthesis.
            UNMATCHED_RIGHT_BRACKET (6):
                Unmatched right bracket or parenthesis.
            TOO_MANY_NESTED_FUNCTIONS (7):
                Functions are nested too deeply.
            MISSING_RIGHT_HAND_OPERAND (8):
                Missing right-hand-side operand.
            INVALID_OPERATOR_NAME (9):
                Invalid operator/function name.
            FEED_ATTRIBUTE_OPERAND_ARGUMENT_NOT_INTEGER (10):
                Feed attribute operand's argument is not an
                integer.
            NO_OPERANDS (11):
                Missing function operands.
            TOO_MANY_OPERANDS (12):
                Function had too many operands.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_MORE_INPUT = 2
        EXPECTED_CHARACTER = 3
        UNEXPECTED_SEPARATOR = 4
        UNMATCHED_LEFT_BRACKET = 5
        UNMATCHED_RIGHT_BRACKET = 6
        TOO_MANY_NESTED_FUNCTIONS = 7
        MISSING_RIGHT_HAND_OPERAND = 8
        INVALID_OPERATOR_NAME = 9
        FEED_ATTRIBUTE_OPERAND_ARGUMENT_NOT_INTEGER = 10
        NO_OPERANDS = 11
        TOO_MANY_OPERANDS = 12


__all__ = tuple(sorted(__protobuf__.manifest))
