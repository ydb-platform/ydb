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
        "UserListErrorEnum",
    },
)


class UserListErrorEnum(proto.Message):
    r"""Container for enum describing possible user list errors."""

    class UserListError(proto.Enum):
        r"""Enum describing possible user list errors.

        Values:
            UNSPECIFIED (0):
                Enum unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            EXTERNAL_REMARKETING_USER_LIST_MUTATE_NOT_SUPPORTED (2):
                Creating and updating external remarketing
                user lists is not supported.
            CONCRETE_TYPE_REQUIRED (3):
                Concrete type of user list is required.
            CONVERSION_TYPE_ID_REQUIRED (4):
                Creating/updating user list conversion types
                requires specifying the conversion type Id.
            DUPLICATE_CONVERSION_TYPES (5):
                Remarketing user list cannot have duplicate
                conversion types.
            INVALID_CONVERSION_TYPE (6):
                Conversion type is invalid/unknown.
            INVALID_DESCRIPTION (7):
                User list description is empty or invalid.
            INVALID_NAME (8):
                User list name is empty or invalid.
            INVALID_TYPE (9):
                Type of the UserList does not match.
            CAN_NOT_ADD_LOGICAL_LIST_AS_LOGICAL_LIST_OPERAND (10):
                Embedded logical user lists are not allowed.
            INVALID_USER_LIST_LOGICAL_RULE_OPERAND (11):
                User list rule operand is invalid.
            NAME_ALREADY_USED (12):
                Name is already being used for another user
                list for the account.
            NEW_CONVERSION_TYPE_NAME_REQUIRED (13):
                Name is required when creating a new
                conversion type.
            CONVERSION_TYPE_NAME_ALREADY_USED (14):
                The given conversion type name has been used.
            OWNERSHIP_REQUIRED_FOR_SET (15):
                Only an owner account may edit a user list.
            USER_LIST_MUTATE_NOT_SUPPORTED (16):
                Creating user list without setting type in oneof user_list
                field, or creating/updating read-only user list types is not
                allowed.
            INVALID_RULE (17):
                Rule is invalid.
            INVALID_DATE_RANGE (27):
                The specified date range is empty.
            CAN_NOT_MUTATE_SENSITIVE_USERLIST (28):
                A UserList which is privacy sensitive or
                legal rejected cannot be mutated by external
                users.
            MAX_NUM_RULEBASED_USERLISTS (29):
                Maximum number of rulebased user lists a
                customer can have.
            CANNOT_MODIFY_BILLABLE_RECORD_COUNT (30):
                BasicUserList's billable record field cannot
                be modified once it is set.
            APP_ID_NOT_SET (31):
                crm_based_user_list.app_id field must be set when
                upload_key_type is MOBILE_ADVERTISING_ID.
            USERLIST_NAME_IS_RESERVED_FOR_SYSTEM_LIST (32):
                Name of the user list is reserved for system
                generated lists and cannot be used.
            ADVERTISER_NOT_ON_ALLOWLIST_FOR_USING_UPLOADED_DATA (37):
                Advertiser needs to be on the allow-list to
                use remarketing lists created from advertiser
                uploaded data (for example, Customer Match
                lists).
            RULE_TYPE_IS_NOT_SUPPORTED (34):
                The provided rule_type is not supported for the user list.
            CAN_NOT_ADD_A_SIMILAR_USERLIST_AS_LOGICAL_LIST_OPERAND (35):
                Similar user list cannot be used as a logical
                user list operand.
            CAN_NOT_MIX_CRM_BASED_IN_LOGICAL_LIST_WITH_OTHER_LISTS (36):
                Logical user list should not have a mix of
                CRM based user list and other types of lists in
                its rules.
            APP_ID_NOT_ALLOWED (39):
                crm_based_user_list.app_id field can only be set when
                upload_key_type is MOBILE_ADVERTISING_ID.
            CANNOT_MUTATE_SYSTEM_LIST (40):
                Google system generated user lists cannot be
                mutated.
            MOBILE_APP_IS_SENSITIVE (41):
                The mobile app associated with the
                remarketing list is sensitive.
            SEED_LIST_DOES_NOT_EXIST (42):
                One or more given seed lists do not exist.
            INVALID_SEED_LIST_ACCESS_REASON (43):
                One or more given seed lists are not
                accessible to the current user.
            INVALID_SEED_LIST_TYPE (44):
                One or more given seed lists have an
                unsupported type.
            INVALID_COUNTRY_CODES (45):
                One or more invalid country codes are added
                to Lookalike UserList.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        EXTERNAL_REMARKETING_USER_LIST_MUTATE_NOT_SUPPORTED = 2
        CONCRETE_TYPE_REQUIRED = 3
        CONVERSION_TYPE_ID_REQUIRED = 4
        DUPLICATE_CONVERSION_TYPES = 5
        INVALID_CONVERSION_TYPE = 6
        INVALID_DESCRIPTION = 7
        INVALID_NAME = 8
        INVALID_TYPE = 9
        CAN_NOT_ADD_LOGICAL_LIST_AS_LOGICAL_LIST_OPERAND = 10
        INVALID_USER_LIST_LOGICAL_RULE_OPERAND = 11
        NAME_ALREADY_USED = 12
        NEW_CONVERSION_TYPE_NAME_REQUIRED = 13
        CONVERSION_TYPE_NAME_ALREADY_USED = 14
        OWNERSHIP_REQUIRED_FOR_SET = 15
        USER_LIST_MUTATE_NOT_SUPPORTED = 16
        INVALID_RULE = 17
        INVALID_DATE_RANGE = 27
        CAN_NOT_MUTATE_SENSITIVE_USERLIST = 28
        MAX_NUM_RULEBASED_USERLISTS = 29
        CANNOT_MODIFY_BILLABLE_RECORD_COUNT = 30
        APP_ID_NOT_SET = 31
        USERLIST_NAME_IS_RESERVED_FOR_SYSTEM_LIST = 32
        ADVERTISER_NOT_ON_ALLOWLIST_FOR_USING_UPLOADED_DATA = 37
        RULE_TYPE_IS_NOT_SUPPORTED = 34
        CAN_NOT_ADD_A_SIMILAR_USERLIST_AS_LOGICAL_LIST_OPERAND = 35
        CAN_NOT_MIX_CRM_BASED_IN_LOGICAL_LIST_WITH_OTHER_LISTS = 36
        APP_ID_NOT_ALLOWED = 39
        CANNOT_MUTATE_SYSTEM_LIST = 40
        MOBILE_APP_IS_SENSITIVE = 41
        SEED_LIST_DOES_NOT_EXIST = 42
        INVALID_SEED_LIST_ACCESS_REASON = 43
        INVALID_SEED_LIST_TYPE = 44
        INVALID_COUNTRY_CODES = 45


__all__ = tuple(sorted(__protobuf__.manifest))
