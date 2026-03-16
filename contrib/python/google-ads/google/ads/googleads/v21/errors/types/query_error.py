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
        "QueryErrorEnum",
    },
)


class QueryErrorEnum(proto.Message):
    r"""Container for enum describing possible query errors."""

    class QueryError(proto.Enum):
        r"""Enum describing possible query errors.

        Values:
            UNSPECIFIED (0):
                Name unspecified.
            UNKNOWN (1):
                The received error code is not known in this
                version.
            QUERY_ERROR (50):
                Returned if all other query error reasons are
                not applicable.
            BAD_ENUM_CONSTANT (18):
                A condition used in the query references an
                invalid enum constant.
            BAD_ESCAPE_SEQUENCE (7):
                Query contains an invalid escape sequence.
            BAD_FIELD_NAME (12):
                Field name is invalid.
            BAD_LIMIT_VALUE (15):
                Limit value is invalid (for example, not a
                number)
            BAD_NUMBER (5):
                Encountered number can not be parsed.
            BAD_OPERATOR (3):
                Invalid operator encountered.
            BAD_PARAMETER_NAME (61):
                Parameter unknown or not supported.
            BAD_PARAMETER_VALUE (62):
                Parameter have invalid value.
            BAD_RESOURCE_TYPE_IN_FROM_CLAUSE (45):
                Invalid resource type was specified in the
                FROM clause.
            BAD_SYMBOL (2):
                Non-ASCII symbol encountered outside of
                strings.
            BAD_VALUE (4):
                Value is invalid.
            DATE_RANGE_TOO_WIDE (36):
                Date filters fail to restrict date to a range
                smaller than 31 days. Applicable if the query is
                segmented by date.
            DATE_RANGE_TOO_NARROW (60):
                Filters on date/week/month/quarter have a
                start date after end date.
            EXPECTED_AND (30):
                Expected AND between values with BETWEEN
                operator.
            EXPECTED_BY (14):
                Expecting ORDER BY to have BY.
            EXPECTED_DIMENSION_FIELD_IN_SELECT_CLAUSE (37):
                There was no dimension field selected.
            EXPECTED_FILTERS_ON_DATE_RANGE (55):
                Missing filters on date related fields.
            EXPECTED_FROM (44):
                Missing FROM clause.
            EXPECTED_LIST (41):
                The operator used in the conditions requires
                the value to be a list.
            EXPECTED_REFERENCED_FIELD_IN_SELECT_CLAUSE (16):
                Fields used in WHERE or ORDER BY clauses are
                missing from the SELECT clause.
            EXPECTED_SELECT (13):
                SELECT is missing at the beginning of query.
            EXPECTED_SINGLE_VALUE (42):
                A list was passed as a value to a condition
                whose operator expects a single value.
            EXPECTED_VALUE_WITH_BETWEEN_OPERATOR (29):
                Missing one or both values with BETWEEN
                operator.
            INVALID_DATE_FORMAT (38):
                Invalid date format. Expected 'YYYY-MM-DD'.
            MISALIGNED_DATE_FOR_FILTER (64):
                Misaligned date value for the filter. The
                date should be the start of a week/month/quarter
                if the filtered field is
                segments.week/segments.month/segments.quarter.
            INVALID_STRING_VALUE (57):
                Value passed was not a string when it should
                have been. For example, it was a number or
                unquoted literal.
            INVALID_VALUE_WITH_BETWEEN_OPERATOR (26):
                A String value passed to the BETWEEN operator
                does not parse as a date.
            INVALID_VALUE_WITH_DURING_OPERATOR (22):
                The value passed to the DURING operator is
                not a Date range literal
            INVALID_VALUE_WITH_LIKE_OPERATOR (56):
                An invalid value was passed to the LIKE
                operator.
            OPERATOR_FIELD_MISMATCH (35):
                An operator was provided that is inapplicable
                to the field being filtered.
            PROHIBITED_EMPTY_LIST_IN_CONDITION (28):
                A Condition was found with an empty list.
            PROHIBITED_ENUM_CONSTANT (54):
                A condition used in the query references an
                unsupported enum constant.
            PROHIBITED_FIELD_COMBINATION_IN_SELECT_CLAUSE (31):
                Fields that are not allowed to be selected
                together were included in the SELECT clause.
            PROHIBITED_FIELD_IN_ORDER_BY_CLAUSE (40):
                A field that is not orderable was included in
                the ORDER BY clause.
            PROHIBITED_FIELD_IN_SELECT_CLAUSE (23):
                A field that is not selectable was included
                in the SELECT clause.
            PROHIBITED_FIELD_IN_WHERE_CLAUSE (24):
                A field that is not filterable was included
                in the WHERE clause.
            PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE (43):
                Resource type specified in the FROM clause is
                not supported by this service.
            PROHIBITED_RESOURCE_TYPE_IN_SELECT_CLAUSE (48):
                A field that comes from an incompatible
                resource was included in the SELECT clause.
            PROHIBITED_RESOURCE_TYPE_IN_WHERE_CLAUSE (58):
                A field that comes from an incompatible
                resource was included in the WHERE clause.
            PROHIBITED_METRIC_IN_SELECT_OR_WHERE_CLAUSE (49):
                A metric incompatible with the main resource
                or other selected segmenting resources was
                included in the SELECT or WHERE clause.
            PROHIBITED_SEGMENT_IN_SELECT_OR_WHERE_CLAUSE (51):
                A segment incompatible with the main resource
                or other selected segmenting resources was
                included in the SELECT or WHERE clause.
            PROHIBITED_SEGMENT_WITH_METRIC_IN_SELECT_OR_WHERE_CLAUSE (53):
                A segment in the SELECT clause is
                incompatible with a metric in the SELECT or
                WHERE clause.
            LIMIT_VALUE_TOO_LOW (25):
                The value passed to the limit clause is too
                low.
            PROHIBITED_NEWLINE_IN_STRING (8):
                Query has a string containing a newline
                character.
            PROHIBITED_VALUE_COMBINATION_IN_LIST (10):
                List contains values of different types.
            PROHIBITED_VALUE_COMBINATION_WITH_BETWEEN_OPERATOR (21):
                The values passed to the BETWEEN operator are
                not of the same type.
            STRING_NOT_TERMINATED (6):
                Query contains unterminated string.
            TOO_MANY_SEGMENTS (34):
                Too many segments are specified in SELECT
                clause.
            UNEXPECTED_END_OF_QUERY (9):
                Query is incomplete and cannot be parsed.
            UNEXPECTED_FROM_CLAUSE (47):
                FROM clause cannot be specified in this
                query.
            UNRECOGNIZED_FIELD (32):
                Query contains one or more unrecognized
                fields.
            UNEXPECTED_INPUT (11):
                Query has an unexpected extra part.
            REQUESTED_METRICS_FOR_MANAGER (59):
                Metrics cannot be requested for a manager
                account. To retrieve metrics, issue separate
                requests against each client account under the
                manager account.
            FILTER_HAS_TOO_MANY_VALUES (63):
                The number of values (right-hand-side
                operands) in a filter exceeds the limit.
        """

        UNSPECIFIED = 0
        UNKNOWN = 1
        QUERY_ERROR = 50
        BAD_ENUM_CONSTANT = 18
        BAD_ESCAPE_SEQUENCE = 7
        BAD_FIELD_NAME = 12
        BAD_LIMIT_VALUE = 15
        BAD_NUMBER = 5
        BAD_OPERATOR = 3
        BAD_PARAMETER_NAME = 61
        BAD_PARAMETER_VALUE = 62
        BAD_RESOURCE_TYPE_IN_FROM_CLAUSE = 45
        BAD_SYMBOL = 2
        BAD_VALUE = 4
        DATE_RANGE_TOO_WIDE = 36
        DATE_RANGE_TOO_NARROW = 60
        EXPECTED_AND = 30
        EXPECTED_BY = 14
        EXPECTED_DIMENSION_FIELD_IN_SELECT_CLAUSE = 37
        EXPECTED_FILTERS_ON_DATE_RANGE = 55
        EXPECTED_FROM = 44
        EXPECTED_LIST = 41
        EXPECTED_REFERENCED_FIELD_IN_SELECT_CLAUSE = 16
        EXPECTED_SELECT = 13
        EXPECTED_SINGLE_VALUE = 42
        EXPECTED_VALUE_WITH_BETWEEN_OPERATOR = 29
        INVALID_DATE_FORMAT = 38
        MISALIGNED_DATE_FOR_FILTER = 64
        INVALID_STRING_VALUE = 57
        INVALID_VALUE_WITH_BETWEEN_OPERATOR = 26
        INVALID_VALUE_WITH_DURING_OPERATOR = 22
        INVALID_VALUE_WITH_LIKE_OPERATOR = 56
        OPERATOR_FIELD_MISMATCH = 35
        PROHIBITED_EMPTY_LIST_IN_CONDITION = 28
        PROHIBITED_ENUM_CONSTANT = 54
        PROHIBITED_FIELD_COMBINATION_IN_SELECT_CLAUSE = 31
        PROHIBITED_FIELD_IN_ORDER_BY_CLAUSE = 40
        PROHIBITED_FIELD_IN_SELECT_CLAUSE = 23
        PROHIBITED_FIELD_IN_WHERE_CLAUSE = 24
        PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE = 43
        PROHIBITED_RESOURCE_TYPE_IN_SELECT_CLAUSE = 48
        PROHIBITED_RESOURCE_TYPE_IN_WHERE_CLAUSE = 58
        PROHIBITED_METRIC_IN_SELECT_OR_WHERE_CLAUSE = 49
        PROHIBITED_SEGMENT_IN_SELECT_OR_WHERE_CLAUSE = 51
        PROHIBITED_SEGMENT_WITH_METRIC_IN_SELECT_OR_WHERE_CLAUSE = 53
        LIMIT_VALUE_TOO_LOW = 25
        PROHIBITED_NEWLINE_IN_STRING = 8
        PROHIBITED_VALUE_COMBINATION_IN_LIST = 10
        PROHIBITED_VALUE_COMBINATION_WITH_BETWEEN_OPERATOR = 21
        STRING_NOT_TERMINATED = 6
        TOO_MANY_SEGMENTS = 34
        UNEXPECTED_END_OF_QUERY = 9
        UNEXPECTED_FROM_CLAUSE = 47
        UNRECOGNIZED_FIELD = 32
        UNEXPECTED_INPUT = 11
        REQUESTED_METRICS_FOR_MANAGER = 59
        FILTER_HAS_TOO_MANY_VALUES = 63


__all__ = tuple(sorted(__protobuf__.manifest))
