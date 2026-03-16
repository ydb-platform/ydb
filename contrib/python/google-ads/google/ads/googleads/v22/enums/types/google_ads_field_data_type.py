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
    package="google.ads.googleads.v22.enums",
    marshal="google.ads.googleads.v22",
    manifest={
        "GoogleAdsFieldDataTypeEnum",
    },
)


class GoogleAdsFieldDataTypeEnum(proto.Message):
    r"""Container holding the various data types."""

    class GoogleAdsFieldDataType(proto.Enum):
        r"""These are the various types a GoogleAdsService artifact may
        take on.

        Values:
            UNSPECIFIED (0):
                Unspecified
            UNKNOWN (1):
                Unknown
            BOOLEAN (2):
                Maps to google.protobuf.BoolValue

                Applicable operators:  =, !=
            DATE (3):
                Maps to google.protobuf.StringValue. It can
                be compared using the set of operators specific
                to dates however.

                Applicable operators:  =, <, >, <=, >=, BETWEEN,
                DURING, and IN
            DOUBLE (4):
                Maps to google.protobuf.DoubleValue

                Applicable operators:  =, !=, <, >, IN, NOT IN
            ENUM (5):
                Maps to an enum. It's specific definition can be found at
                type_url.

                Applicable operators: =, !=, IN, NOT IN
            FLOAT (6):
                Maps to google.protobuf.FloatValue

                Applicable operators:  =, !=, <, >, IN, NOT IN
            INT32 (7):
                Maps to google.protobuf.Int32Value

                Applicable operators:  =, !=, <, >, <=, >=,
                BETWEEN, IN, NOT IN
            INT64 (8):
                Maps to google.protobuf.Int64Value

                Applicable operators:  =, !=, <, >, <=, >=,
                BETWEEN, IN, NOT IN
            MESSAGE (9):
                Maps to a protocol buffer message type. The data type's
                details can be found in type_url.

                No operators work with MESSAGE fields.
            RESOURCE_NAME (10):
                Maps to google.protobuf.StringValue. Represents the resource
                name (unique id) of a resource or one of its foreign keys.

                No operators work with RESOURCE_NAME fields.
            STRING (11):
                Maps to google.protobuf.StringValue.

                Applicable operators:  =, !=, LIKE, NOT LIKE,
                IN, NOT IN
            UINT64 (12):
                Maps to google.protobuf.UInt64Value

                Applicable operators:  =, !=, <, >, <=, >=,
                BETWEEN, IN, NOT IN
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        BOOLEAN = 2
        DATE = 3
        DOUBLE = 4
        ENUM = 5
        FLOAT = 6
        INT32 = 7
        INT64 = 8
        MESSAGE = 9
        RESOURCE_NAME = 10
        STRING = 11
        UINT64 = 12


__all__ = tuple(sorted(__protobuf__.manifest))
