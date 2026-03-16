#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import json
from typing import Any

from elasticsearch.dsl.document_base import InstrumentedExpression
from elasticsearch.esql.esql import ESQLBase, ExpressionType


def _render(v: Any) -> str:
    return (
        json.dumps(v)
        if not isinstance(v, InstrumentedExpression)
        else ESQLBase._format_expr(v)
    )


def abs(number: ExpressionType) -> InstrumentedExpression:
    """Returns the absolute value.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ABS({_render(number)})")


def acos(number: ExpressionType) -> InstrumentedExpression:
    """Returns the arccosine of `n` as an angle, expressed in radians.

    :param number: Number between -1 and 1. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ACOS({_render(number)})")


def asin(number: ExpressionType) -> InstrumentedExpression:
    """Returns the arcsine of the input numeric expression as an angle,
    expressed in radians.

    :param number: Number between -1 and 1. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ASIN({_render(number)})")


def atan(number: ExpressionType) -> InstrumentedExpression:
    """Returns the arctangent of the input numeric expression as an angle,
    expressed in radians.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ATAN({_render(number)})")


def atan2(
    y_coordinate: ExpressionType, x_coordinate: ExpressionType
) -> InstrumentedExpression:
    """The angle between the positive x-axis and the ray from the origin to the
    point (x , y) in the Cartesian plane, expressed in radians.

    :param y_coordinate: y coordinate. If `null`, the function returns `null`.
    :param x_coordinate: x coordinate. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(
        f"ATAN2({_render(y_coordinate)}, {_render(x_coordinate)})"
    )


def avg(number: ExpressionType) -> InstrumentedExpression:
    """The average of a numeric field.

    :param number: Expression that outputs values to average.
    """
    return InstrumentedExpression(f"AVG({_render(number)})")


def avg_over_time(number: ExpressionType) -> InstrumentedExpression:
    """The average over time of a numeric field.

    :param number: Expression that outputs values to average.
    """
    return InstrumentedExpression(f"AVG_OVER_TIME({_render(number)})")


def bit_length(string: ExpressionType) -> InstrumentedExpression:
    """Returns the bit length of a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"BIT_LENGTH({_render(string)})")


def bucket(
    field: ExpressionType,
    buckets: ExpressionType,
    from_: ExpressionType,
    to: ExpressionType,
) -> InstrumentedExpression:
    """Creates groups of values - buckets - out of a datetime or numeric input.
    The size of the buckets can either be provided directly, or chosen based on
    a recommended count and values range.

    :param field: Numeric or date expression from which to derive buckets.
    :param buckets: Target number of buckets, or desired bucket size if `from`
                    and `to` parameters are omitted.
    :param from_: Start of the range. Can be a number, a date or a date expressed
                  as a string.
    :param to: End of the range. Can be a number, a date or a date expressed as a string.
    """
    return InstrumentedExpression(
        f"BUCKET({_render(field)}, {_render(buckets)}, {_render(from_)}, {_render(to)})"
    )


def byte_length(string: ExpressionType) -> InstrumentedExpression:
    """Returns the byte length of a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"BYTE_LENGTH({_render(string)})")


def case(*conditions: ExpressionType) -> InstrumentedExpression:
    """Accepts pairs of conditions and values. The function returns the value
    that belongs to the first condition that evaluates to `true`.  If the
    number of arguments is odd, the last argument is the default value which is
    returned when no condition matches. If the number of arguments is even, and
    no condition matches, the function returns `null`.
    """
    return InstrumentedExpression(
        f'CASE({", ".join([_render(c) for c in conditions])})'
    )


def categorize(field: ExpressionType) -> InstrumentedExpression:
    """Groups text messages into categories of similarly formatted text values.

    :param field: Expression to categorize
    """
    return InstrumentedExpression(f"CATEGORIZE({_render(field)})")


def cbrt(number: ExpressionType) -> InstrumentedExpression:
    """Returns the cube root of a number. The input can be any numeric value,
    the return value is always a double. Cube roots of infinities are null.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"CBRT({_render(number)})")


def ceil(number: ExpressionType) -> InstrumentedExpression:
    """Round a number up to the nearest integer.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"CEIL({_render(number)})")


def cidr_match(ip: ExpressionType, block_x: ExpressionType) -> InstrumentedExpression:
    """Returns true if the provided IP is contained in one of the provided CIDR blocks.

    :param ip: IP address of type `ip` (both IPv4 and IPv6 are supported).
    :param block_x: CIDR block to test the IP against.
    """
    return InstrumentedExpression(f"CIDR_MATCH({_render(ip)}, {_render(block_x)})")


def coalesce(first: ExpressionType, rest: ExpressionType) -> InstrumentedExpression:
    """Returns the first of its arguments that is not null. If all arguments
    are null, it returns `null`.

    :param first: Expression to evaluate.
    :param rest: Other expression to evaluate.
    """
    return InstrumentedExpression(f"COALESCE({_render(first)}, {_render(rest)})")


def concat(*strings: ExpressionType) -> InstrumentedExpression:
    """Concatenates two or more strings."""
    return InstrumentedExpression(
        f'CONCAT({", ".join([f"{_render(s)}" for s in strings])})'
    )


def cos(angle: ExpressionType) -> InstrumentedExpression:
    """Returns the cosine of an angle.

    :param angle: An angle, in radians. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"COS({_render(angle)})")


def cosh(number: ExpressionType) -> InstrumentedExpression:
    """Returns the hyperbolic cosine of a number.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"COSH({_render(number)})")


def count(field: ExpressionType) -> InstrumentedExpression:
    """Returns the total number (count) of input values.

    :param field: Expression that outputs values to be counted. If omitted,
                  equivalent to `COUNT(*)` (the number of rows).
    """
    return InstrumentedExpression(f"COUNT({_render(field)})")


def count_distinct(
    field: ExpressionType, precision: ExpressionType
) -> InstrumentedExpression:
    """Returns the approximate number of distinct values.

    :param field: Column or literal for which to count the number of distinct values.
    :param precision: Precision threshold. The maximum supported value is 40000. Thresholds
                      above this number will have the same effect as a threshold of 40000.
                      The default value is 3000.
    """
    return InstrumentedExpression(
        f"COUNT_DISTINCT({_render(field)}, {_render(precision)})"
    )


def count_distinct_over_time(
    field: ExpressionType, precision: ExpressionType
) -> InstrumentedExpression:
    """The count of distinct values over time for a field.

    :param field:
    :param precision: Precision threshold. The maximum supported value is 40000. Thresholds
                      above this number will have the same effect as a threshold of 40000. The
                      default value is 3000.
    """
    return InstrumentedExpression(
        f"COUNT_DISTINCT_OVER_TIME({_render(field)}, {_render(precision)})"
    )


def count_over_time(field: ExpressionType) -> InstrumentedExpression:
    """The count over time value of a field.

    :param field:
    """
    return InstrumentedExpression(f"COUNT_OVER_TIME({_render(field)})")


def date_diff(
    unit: ExpressionType, start_timestamp: ExpressionType, end_timestamp: ExpressionType
) -> InstrumentedExpression:
    """Subtracts the `startTimestamp` from the `endTimestamp` and returns the
    difference in multiples of `unit`. If `startTimestamp` is later than the
    `endTimestamp`, negative values are returned.

    :param unit: Time difference unit
    :param start_timestamp: A string representing a start timestamp
    :param end_timestamp: A string representing an end timestamp
    """
    return InstrumentedExpression(
        f"DATE_DIFF({_render(unit)}, {_render(start_timestamp)}, {_render(end_timestamp)})"
    )


def date_extract(
    date_part: ExpressionType, date: ExpressionType
) -> InstrumentedExpression:
    """Extracts parts of a date, like year, month, day, hour.

    :param date_part: Part of the date to extract.  Can be:
        `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`,
        `aligned_week_of_month`, `aligned_week_of_year`, `ampm_of_day`,
        `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`,
        `day_of_year`, `epoch_day`, `era`, `hour_of_ampm`, `hour_of_day`,
        `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
        `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`,
        `nano_of_day`, `nano_of_second`, `offset_seconds`, `proleptic_month`,
        `second_of_day`, `second_of_minute`, `year`, or `year_of_era`. If `null`,
        the function returns `null`.
    :param date: Date expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(
        f"DATE_EXTRACT({_render(date_part)}, {_render(date)})"
    )


def date_format(
    date: ExpressionType,
    date_format: ExpressionType = None,
) -> InstrumentedExpression:
    """Returns a string representation of a date, in the provided format.

    :param date_format: Date format (optional).  If no format is specified, the
                        `yyyy-MM-dd'T'HH:mm:ss.SSSZ` format is used. If `null`, the
                        function returns `null`.
    :param date: Date expression. If `null`, the function returns `null`.
    """
    if date_format is not None:
        return InstrumentedExpression(
            f"DATE_FORMAT({_render(date_format)}, {_render(date)})"
        )
    else:
        return InstrumentedExpression(f"DATE_FORMAT({_render(date)})")


def date_parse(
    date_pattern: ExpressionType, date_string: ExpressionType
) -> InstrumentedExpression:
    """Returns a date by parsing the second argument using the format specified
    in the first argument.

    :param date_pattern: The date format. If `null`, the function returns `null`.
    :param date_string: Date expression as a string. If `null` or an empty
                        string, the function returns `null`.
    """
    return InstrumentedExpression(
        f"DATE_PARSE({_render(date_pattern)}, {_render(date_string)})"
    )


def date_trunc(
    interval: ExpressionType, date: ExpressionType
) -> InstrumentedExpression:
    """Rounds down a date to the closest interval since epoch, which starts at `0001-01-01T00:00:00Z`.

    :param interval: Interval; expressed using the timespan literal syntax.
    :param date: Date expression
    """
    return InstrumentedExpression(f"DATE_TRUNC({_render(interval)}, {_render(date)})")


def e() -> InstrumentedExpression:
    """Returns Euler’s number)."""
    return InstrumentedExpression("E()")


def ends_with(str: ExpressionType, suffix: ExpressionType) -> InstrumentedExpression:
    """Returns a boolean that indicates whether a keyword string ends with
    another string.

    :param str: String expression. If `null`, the function returns `null`.
    :param suffix: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ENDS_WITH({_render(str)}, {_render(suffix)})")


def exp(number: ExpressionType) -> InstrumentedExpression:
    """Returns the value of e raised to the power of the given number.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"EXP({_render(number)})")


def first_over_time(field: ExpressionType) -> InstrumentedExpression:
    """The earliest value of a field, where recency determined by the
    `@timestamp` field.

    :param field:
    """
    return InstrumentedExpression(f"FIRST_OVER_TIME({_render(field)})")


def floor(number: ExpressionType) -> InstrumentedExpression:
    """Round a number down to the nearest integer.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"FLOOR({_render(number)})")


def from_base64(string: ExpressionType) -> InstrumentedExpression:
    """Decode a base64 string.

    :param string: A base64 string.
    """
    return InstrumentedExpression(f"FROM_BASE64({_render(string)})")


def greatest(first: ExpressionType, rest: ExpressionType) -> InstrumentedExpression:
    """Returns the maximum value from multiple columns. This is similar to
    `MV_MAX` except it is intended to run on multiple columns at once.

    :param first: First of the columns to evaluate.
    :param rest: The rest of the columns to evaluate.
    """
    return InstrumentedExpression(f"GREATEST({_render(first)}, {_render(rest)})")


def hash(algorithm: ExpressionType, input: ExpressionType) -> InstrumentedExpression:
    """Computes the hash of the input using various algorithms such as MD5,
    SHA, SHA-224, SHA-256, SHA-384, SHA-512.

    :param algorithm: Hash algorithm to use.
    :param input: Input to hash.
    """
    return InstrumentedExpression(f"HASH({_render(algorithm)}, {_render(input)})")


def hypot(number1: ExpressionType, number2: ExpressionType) -> InstrumentedExpression:
    """Returns the hypotenuse of two numbers. The input can be any numeric
    values, the return value is always a double. Hypotenuses of infinities are null.

    :param number1: Numeric expression. If `null`, the function returns `null`.
    :param number2: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"HYPOT({number1}, {number2})")


def ip_prefix(
    ip: ExpressionType,
    prefix_length_v4: ExpressionType,
    prefix_length_v6: ExpressionType,
) -> InstrumentedExpression:
    """Truncates an IP to a given prefix length.

    :param ip: IP address of type `ip` (both IPv4 and IPv6 are supported).
    :param prefix_length_v4: Prefix length for IPv4 addresses.
    :param prefix_length_v6: Prefix length for IPv6 addresses.
    """
    return InstrumentedExpression(
        f"IP_PREFIX({_render(ip)}, {prefix_length_v4}, {prefix_length_v6})"
    )


def knn(
    field: ExpressionType, query: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Finds the k nearest vectors to a query vector, as measured by a
    similarity metric. knn function finds nearest vectors through approximate
    search on indexed dense_vectors.

    :param field: Field that the query will target.
    :param query: Vector value to find top nearest neighbours for.
    :param options: (Optional) kNN additional options as function named parameters.
    """
    if options is not None:
        return InstrumentedExpression(
            f"KNN({_render(field)}, {_render(query)}, {_render(options)})"
        )
    else:
        return InstrumentedExpression(f"KNN({_render(field)}, {_render(query)})")


def kql(query: ExpressionType) -> InstrumentedExpression:
    """Performs a KQL query. Returns true if the provided KQL query string
    matches the row.

    :param query: Query string in KQL query string format.
    """
    return InstrumentedExpression(f"KQL({_render(query)})")


def last_over_time(field: ExpressionType) -> InstrumentedExpression:
    """The latest value of a field, where recency determined by the
    `@timestamp` field.

    :param field:
    """
    return InstrumentedExpression(f"LAST_OVER_TIME({_render(field)})")


def least(first: ExpressionType, rest: ExpressionType) -> InstrumentedExpression:
    """Returns the minimum value from multiple columns. This is similar to
    `MV_MIN` except it is intended to run on multiple columns at once.

    :param first: First of the columns to evaluate.
    :param rest: The rest of the columns to evaluate.
    """
    return InstrumentedExpression(f"LEAST({_render(first)}, {_render(rest)})")


def left(string: ExpressionType, length: ExpressionType) -> InstrumentedExpression:
    """Returns the substring that extracts *length* chars from *string*
    starting from the left.

    :param string: The string from which to return a substring.
    :param length: The number of characters to return.
    """
    return InstrumentedExpression(f"LEFT({_render(string)}, {_render(length)})")


def length(string: ExpressionType) -> InstrumentedExpression:
    """Returns the character length of a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"LENGTH({_render(string)})")


def locate(
    string: ExpressionType, substring: ExpressionType, start: ExpressionType
) -> InstrumentedExpression:
    """Returns an integer that indicates the position of a keyword substring
    within another string. Returns `0` if the substring cannot be found. Note
    that string positions start from `1`.

    :param string: An input string
    :param substring: A substring to locate in the input string
    :param start: The start index
    """
    return InstrumentedExpression(
        f"LOCATE({_render(string)}, {_render(substring)}, {_render(start)})"
    )


def log(base: ExpressionType, number: ExpressionType) -> InstrumentedExpression:
    """Returns the logarithm of a value to a base. The input can be any numeric
    value, the return value is always a double.  Logs of zero, negative
    numbers, and base of one return `null` as well as a warning.

    :param base: Base of logarithm. If `null`, the function returns `null`. If
                 not provided, this function returns the natural logarithm (base e) of a value.
    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"LOG({_render(base)}, {_render(number)})")


def log10(number: ExpressionType) -> InstrumentedExpression:
    """Returns the logarithm of a value to base 10. The input can be any
    numeric value, the return value is always a double.  Logs of 0 and negative
    numbers return `null` as well as a warning.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"LOG10({_render(number)})")


def ltrim(string: ExpressionType) -> InstrumentedExpression:
    """Removes leading whitespaces from a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"LTRIM({_render(string)})")


def match(
    field: ExpressionType, query: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Use `MATCH` to perform a match query on the specified field. Using
    `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.

    :param field: Field that the query will target.
    :param query: Value to find in the provided field.
    :param options: (Optional) Match additional options as function named parameters.
    """
    if options is not None:
        return InstrumentedExpression(
            f"MATCH({_render(field)}, {_render(query)}, {_render(options)})"
        )
    else:
        return InstrumentedExpression(f"MATCH({_render(field)}, {_render(query)})")


def match_phrase(
    field: ExpressionType, query: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Use `MATCH_PHRASE` to perform a `match_phrase` on the specified field.
    Using `MATCH_PHRASE` is equivalent to using the `match_phrase` query in the
    Elasticsearch Query DSL.

    :param field: Field that the query will target.
    :param query: Value to find in the provided field.
    :param options: (Optional) MatchPhrase additional options as function named parameters.
    """
    if options is not None:
        return InstrumentedExpression(
            f"MATCH_PHRASE({_render(field)}, {_render(query)}, {_render(options)})"
        )
    else:
        return InstrumentedExpression(
            f"MATCH_PHRASE({_render(field)}, {_render(query)})"
        )


def max(field: ExpressionType) -> InstrumentedExpression:
    """The maximum value of a field.

    :param field:
    """
    return InstrumentedExpression(f"MAX({_render(field)})")


def max_over_time(field: ExpressionType) -> InstrumentedExpression:
    """The maximum over time value of a field.

    :param field:
    """
    return InstrumentedExpression(f"MAX_OVER_TIME({_render(field)})")


def md5(input: ExpressionType) -> InstrumentedExpression:
    """Computes the MD5 hash of the input.

    :param input: Input to hash.
    """
    return InstrumentedExpression(f"MD5({_render(input)})")


def median(number: ExpressionType) -> InstrumentedExpression:
    """The value that is greater than half of all values and less than half of
    all values, also known as the 50% `PERCENTILE`.

    :param number: Expression that outputs values to calculate the median of.
    """
    return InstrumentedExpression(f"MEDIAN({_render(number)})")


def median_absolute_deviation(number: ExpressionType) -> InstrumentedExpression:
    """Returns the median absolute deviation, a measure of variability. It is a
    robust statistic, meaning that it is useful for describing data that may
    have outliers, or may not be normally distributed. For such data it can be
    more descriptive than standard deviation.  It is calculated as the median
    of each data point’s deviation from the median of the entire sample. That
    is, for a random variable `X`, the median absolute deviation is
    `median(|median(X) - X|)`.

    :param number:
    """
    return InstrumentedExpression(f"MEDIAN_ABSOLUTE_DEVIATION({_render(number)})")


def min(field: ExpressionType) -> InstrumentedExpression:
    """The minimum value of a field.

    :param field:
    """
    return InstrumentedExpression(f"MIN({_render(field)})")


def min_over_time(field: ExpressionType) -> InstrumentedExpression:
    """The minimum over time value of a field.

    :param field:
    """
    return InstrumentedExpression(f"MIN_OVER_TIME({_render(field)})")


def multi_match(
    query: ExpressionType, *fields: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Use `MULTI_MATCH` to perform a multi-match query on the specified field.
    The multi_match query builds on the match query to allow multi-field queries.

    :param query: Value to find in the provided fields.
    :param fields: Fields to use for matching
    :param options: (Optional) Additional options for MultiMatch, passed as function
                    named parameters
    """
    if options is not None:
        return InstrumentedExpression(
            f'MULTI_MATCH({_render(query)}, {", ".join([_render(c) for c in fields])}, {_render(options)})'
        )
    else:
        return InstrumentedExpression(
            f'MULTI_MATCH({_render(query)}, {", ".join([_render(c) for c in fields])})'
        )


def mv_append(field1: ExpressionType, field2: ExpressionType) -> InstrumentedExpression:
    """Concatenates values of two multi-value fields.

    :param field1:
    :param field2:
    """
    return InstrumentedExpression(f"MV_APPEND({field1}, {field2})")


def mv_avg(number: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued field into a single valued field containing the
    average of all of the values.

    :param number: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_AVG({_render(number)})")


def mv_concat(string: ExpressionType, delim: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued string expression into a single valued column
    containing the concatenation of all values separated by a delimiter.

    :param string: Multivalue expression.
    :param delim: Delimiter.
    """
    return InstrumentedExpression(f"MV_CONCAT({_render(string)}, {_render(delim)})")


def mv_count(field: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued expression into a single valued column containing
    a count of the number of values.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_COUNT({_render(field)})")


def mv_dedupe(field: ExpressionType) -> InstrumentedExpression:
    """Remove duplicate values from a multivalued field.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_DEDUPE({_render(field)})")


def mv_first(field: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued expression into a single valued column containing
    the first value. This is most useful when reading from a function that
    emits multivalued columns in a known order like `SPLIT`.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_FIRST({_render(field)})")


def mv_last(field: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalue expression into a single valued column containing
    the last value. This is most useful when reading from a function that emits
    multivalued columns in a known order like `SPLIT`.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_LAST({_render(field)})")


def mv_max(field: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued expression into a single valued column containing
    the maximum value.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_MAX({_render(field)})")


def mv_median(number: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued field into a single valued field containing the
    median value.

    :param number: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_MEDIAN({_render(number)})")


def mv_median_absolute_deviation(number: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued field into a single valued field containing the
    median absolute deviation.  It is calculated as the median of each data
    point’s deviation from the median of the entire sample. That is, for a
    random variable `X`, the median absolute deviation is `median(|median(X) - X|)`.

    :param number: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_MEDIAN_ABSOLUTE_DEVIATION({_render(number)})")


def mv_min(field: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued expression into a single valued column containing
    the minimum value.

    :param field: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_MIN({_render(field)})")


def mv_percentile(
    number: ExpressionType, percentile: ExpressionType
) -> InstrumentedExpression:
    """Converts a multivalued field into a single valued field containing the
    value at which a certain percentage of observed values occur.

    :param number: Multivalue expression.
    :param percentile: The percentile to calculate. Must be a number between 0
                       and 100. Numbers out of range will return a null instead.
    """
    return InstrumentedExpression(
        f"MV_PERCENTILE({_render(number)}, {_render(percentile)})"
    )


def mv_pseries_weighted_sum(
    number: ExpressionType, p: ExpressionType
) -> InstrumentedExpression:
    """Converts a multivalued expression into a single-valued column by
    multiplying every element on the input list by its corresponding term in
    P-Series and computing the sum.

    :param number: Multivalue expression.
    :param p: It is a constant number that represents the *p* parameter in the
              P-Series. It impacts every element’s contribution to the weighted sum.
    """
    return InstrumentedExpression(
        f"MV_PSERIES_WEIGHTED_SUM({_render(number)}, {_render(p)})"
    )


def mv_slice(
    field: ExpressionType, start: ExpressionType, end: ExpressionType = None
) -> InstrumentedExpression:
    """Returns a subset of the multivalued field using the start and end index
    values. This is most useful when reading from a function that emits
    multivalued columns in a known order like `SPLIT` or `MV_SORT`.

    :param field: Multivalue expression. If `null`, the function returns `null`.
    :param start: Start position. If `null`, the function returns `null`. The
                  start argument can be negative. An index of -1 is used to specify
                  the last value in the list.
    :param end: End position(included). Optional; if omitted, the position at
                `start` is returned. The end argument can be negative. An index of -1
                is used to specify the last value in the list.
    """
    if end is not None:
        return InstrumentedExpression(
            f"MV_SLICE({_render(field)}, {_render(start)}, {_render(end)})"
        )
    else:
        return InstrumentedExpression(f"MV_SLICE({_render(field)}, {_render(start)})")


def mv_sort(field: ExpressionType, order: ExpressionType) -> InstrumentedExpression:
    """Sorts a multivalued field in lexicographical order.

    :param field: Multivalue expression. If `null`, the function returns `null`.
    :param order: Sort order. The valid options are ASC and DESC, the default is ASC.
    """
    return InstrumentedExpression(f"MV_SORT({_render(field)}, {_render(order)})")


def mv_sum(number: ExpressionType) -> InstrumentedExpression:
    """Converts a multivalued field into a single valued field containing the
    sum of all of the values.

    :param number: Multivalue expression.
    """
    return InstrumentedExpression(f"MV_SUM({_render(number)})")


def mv_zip(
    string1: ExpressionType, string2: ExpressionType, delim: ExpressionType = None
) -> InstrumentedExpression:
    """Combines the values from two multivalued fields with a delimiter that
    joins them together.

    :param string1: Multivalue expression.
    :param string2: Multivalue expression.
    :param delim: Delimiter. Optional; if omitted, `,` is used as a default delimiter.
    """
    if delim is not None:
        return InstrumentedExpression(f"MV_ZIP({string1}, {string2}, {_render(delim)})")
    else:
        return InstrumentedExpression(f"MV_ZIP({string1}, {string2})")


def now() -> InstrumentedExpression:
    """Returns current date and time."""
    return InstrumentedExpression("NOW()")


def percentile(
    number: ExpressionType, percentile: ExpressionType
) -> InstrumentedExpression:
    """Returns the value at which a certain percentage of observed values
    occur. For example, the 95th percentile is the value which is greater than
    95% of the observed values and the 50th percentile is the `MEDIAN`.

    :param number:
    :param percentile:
    """
    return InstrumentedExpression(
        f"PERCENTILE({_render(number)}, {_render(percentile)})"
    )


def pi() -> InstrumentedExpression:
    """Returns Pi, the ratio of a circle’s circumference to its diameter."""
    return InstrumentedExpression("PI()")


def pow(base: ExpressionType, exponent: ExpressionType) -> InstrumentedExpression:
    """Returns the value of `base` raised to the power of `exponent`.

    :param base: Numeric expression for the base. If `null`, the function returns `null`.
    :param exponent: Numeric expression for the exponent. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"POW({_render(base)}, {_render(exponent)})")


def qstr(
    query: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Performs a query string query. Returns true if the provided query string
    matches the row.

    :param query: Query string in Lucene query string format.
    :param options: (Optional) Additional options for Query String as function named
                    parameters.
    """
    if options is not None:
        return InstrumentedExpression(f"QSTR({_render(query)}, {_render(options)})")
    else:
        return InstrumentedExpression(f"QSTR({_render(query)})")


def rate(field: ExpressionType) -> InstrumentedExpression:
    """The rate of a counter field.

    :param field:
    """
    return InstrumentedExpression(f"RATE({_render(field)})")


def repeat(string: ExpressionType, number: ExpressionType) -> InstrumentedExpression:
    """Returns a string constructed by concatenating `string` with itself the
    specified `number` of times.

    :param string: String expression.
    :param number: Number times to repeat.
    """
    return InstrumentedExpression(f"REPEAT({_render(string)}, {_render(number)})")


def replace(
    string: ExpressionType, regex: ExpressionType, new_string: ExpressionType
) -> InstrumentedExpression:
    """The function substitutes in the string `str` any match of the regular
    expression `regex` with the replacement string `newStr`.

    :param string: String expression.
    :param regex: Regular expression.
    :param new_string: Replacement string.
    """
    return InstrumentedExpression(
        f"REPLACE({_render(string)}, {_render(regex)}, {_render(new_string)})"
    )


def reverse(str: ExpressionType) -> InstrumentedExpression:
    """Returns a new string representing the input string in reverse order.

    :param str: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"REVERSE({_render(str)})")


def right(string: ExpressionType, length: ExpressionType) -> InstrumentedExpression:
    """Return the substring that extracts *length* chars from *str* starting
    from the right.

    :param string: The string from which to returns a substring.
    :param length: The number of characters to return.
    """
    return InstrumentedExpression(f"RIGHT({_render(string)}, {_render(length)})")


def round(
    number: ExpressionType, decimals: ExpressionType = None
) -> InstrumentedExpression:
    """Rounds a number to the specified number of decimal places. Defaults to
    0, which returns the nearest integer. If the precision is a negative
    number, rounds to the number of digits left of the decimal point.

    :param number: The numeric value to round. If `null`, the function returns `null`.
    :param decimals: The number of decimal places to round to. Defaults to 0. If
                     `null`, the function returns `null`.
    """
    if decimals is not None:
        return InstrumentedExpression(f"ROUND({_render(number)}, {_render(decimals)})")
    else:
        return InstrumentedExpression(f"ROUND({_render(number)})")


def round_to(field: ExpressionType, points: ExpressionType) -> InstrumentedExpression:
    """Rounds down to one of a list of fixed points.

    :param field: The numeric value to round. If `null`, the function returns `null`.
    :param points: Remaining rounding points. Must be constants.
    """
    return InstrumentedExpression(f"ROUND_TO({_render(field)}, {_render(points)})")


def rtrim(string: ExpressionType) -> InstrumentedExpression:
    """Removes trailing whitespaces from a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"RTRIM({_render(string)})")


def sample(field: ExpressionType, limit: ExpressionType) -> InstrumentedExpression:
    """Collects sample values for a field.

    :param field: The field to collect sample values for.
    :param limit: The maximum number of values to collect.
    """
    return InstrumentedExpression(f"SAMPLE({_render(field)}, {_render(limit)})")


def scalb(d: ExpressionType, scale_factor: ExpressionType) -> InstrumentedExpression:
    """Returns the result of `d * 2 ^ scaleFactor`, Similar to Java's `scalb`
    function. Result is rounded as if performed by a single correctly rounded
    floating-point multiply to a member of the double value set.

    :param d: Numeric expression for the multiplier. If `null`, the function
              returns `null`.
    :param scale_factor: Numeric expression for the scale factor. If `null`, the
                         function returns `null`.
    """
    return InstrumentedExpression(f"SCALB({_render(d)}, {_render(scale_factor)})")


def sha1(input: ExpressionType) -> InstrumentedExpression:
    """Computes the SHA1 hash of the input.

    :param input: Input to hash.
    """
    return InstrumentedExpression(f"SHA1({_render(input)})")


def sha256(input: ExpressionType) -> InstrumentedExpression:
    """Computes the SHA256 hash of the input.

    :param input: Input to hash.
    """
    return InstrumentedExpression(f"SHA256({_render(input)})")


def signum(number: ExpressionType) -> InstrumentedExpression:
    """Returns the sign of the given number. It returns `-1` for negative
    numbers, `0` for `0` and `1` for positive numbers.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"SIGNUM({_render(number)})")


def sin(angle: ExpressionType) -> InstrumentedExpression:
    """Returns the sine of an angle.

    :param angle: An angle, in radians. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"SIN({_render(angle)})")


def sinh(number: ExpressionType) -> InstrumentedExpression:
    """Returns the hyperbolic sine of a number.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"SINH({_render(number)})")


def space(number: ExpressionType) -> InstrumentedExpression:
    """Returns a string made of `number` spaces.

    :param number: Number of spaces in result.
    """
    return InstrumentedExpression(f"SPACE({_render(number)})")


def split(string: ExpressionType, delim: ExpressionType) -> InstrumentedExpression:
    """Split a single valued string into multiple strings.

    :param string: String expression. If `null`, the function returns `null`.
    :param delim: Delimiter. Only single byte delimiters are currently supported.
    """
    return InstrumentedExpression(f"SPLIT({_render(string)}, {_render(delim)})")


def sqrt(number: ExpressionType) -> InstrumentedExpression:
    """Returns the square root of a number. The input can be any numeric value,
    the return value is always a double. Square roots of negative numbers and
    infinities are null.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"SQRT({_render(number)})")


def starts_with(str: ExpressionType, prefix: ExpressionType) -> InstrumentedExpression:
    """Returns a boolean that indicates whether a keyword string starts with
    another string.

    :param str: String expression. If `null`, the function returns `null`.
    :param prefix: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"STARTS_WITH({_render(str)}, {_render(prefix)})")


def std_dev(number: ExpressionType) -> InstrumentedExpression:
    """The population standard deviation of a numeric field.

    :param number:
    """
    return InstrumentedExpression(f"STD_DEV({_render(number)})")


def st_centroid_agg(field: ExpressionType) -> InstrumentedExpression:
    """Calculate the spatial centroid over a field with spatial point geometry type.

    :param field:
    """
    return InstrumentedExpression(f"ST_CENTROID_AGG({_render(field)})")


def st_contains(
    geom_a: ExpressionType, geom_b: ExpressionType
) -> InstrumentedExpression:
    """Returns whether the first geometry contains the second geometry. This is
    the inverse of the ST_WITHIN function.

    :param geom_a: Expression of type `geo_point`, `cartesian_point`,
                   `geo_shape` or `cartesian_shape`. If `null`, the function returns
                   `null`.
    :param geom_b: Expression of type `geo_point`, `cartesian_point`, `geo_shape`
                   or `cartesian_shape`. If `null`, the function returns `null`. The
                   second parameter must also have the same coordinate system as the
                   first. This means it is not possible to combine `geo_*` and
                   `cartesian_*` parameters.
    """
    return InstrumentedExpression(f"ST_CONTAINS({_render(geom_a)}, {_render(geom_b)})")


def st_disjoint(
    geom_a: ExpressionType, geom_b: ExpressionType
) -> InstrumentedExpression:
    """Returns whether the two geometries or geometry columns are disjoint.
    This is the inverse of the ST_INTERSECTS function. In mathematical terms:
    ST_Disjoint(A, B) ⇔ A ⋂ B = ∅

    :param geom_a: Expression of type `geo_point`, `cartesian_point`,
                   `geo_shape` or `cartesian_shape`. If `null`, the function returns
                   `null`.
    :param geom_b: Expression of type `geo_point`, `cartesian_point`, `geo_shape`
                   or `cartesian_shape`. If `null`, the function returns `null`. The
                   second parameter must also have the same coordinate system as the
                   first. This means it is not possible to combine `geo_*` and
                   `cartesian_*` parameters.
    """
    return InstrumentedExpression(f"ST_DISJOINT({_render(geom_a)}, {_render(geom_b)})")


def st_distance(
    geom_a: ExpressionType, geom_b: ExpressionType
) -> InstrumentedExpression:
    """Computes the distance between two points. For cartesian geometries, this
    is the pythagorean distance in the same units as the original coordinates.
    For geographic geometries, this is the circular distance along the great
    circle in meters.

    :param geom_a: Expression of type `geo_point` or `cartesian_point`. If
                   `null`, the function returns `null`.
    :param geom_b: Expression of type `geo_point` or `cartesian_point`. If
                   `null`, the function returns `null`. The second parameter must
                   also have the same coordinate system as the first. This means it
                   is not possible to combine `geo_point` and `cartesian_point` parameters.
    """
    return InstrumentedExpression(f"ST_DISTANCE({_render(geom_a)}, {_render(geom_b)})")


def st_envelope(geometry: ExpressionType) -> InstrumentedExpression:
    """Determines the minimum bounding box of the supplied geometry.

    :param geometry: Expression of type `geo_point`, `geo_shape`,
                     `cartesian_point` or `cartesian_shape`. If `null`, the function
                     returns `null`.
    """
    return InstrumentedExpression(f"ST_ENVELOPE({_render(geometry)})")


def st_extent_agg(field: ExpressionType) -> InstrumentedExpression:
    """Calculate the spatial extent over a field with geometry type. Returns a
    bounding box for all values of the field.

    :param field:
    """
    return InstrumentedExpression(f"ST_EXTENT_AGG({_render(field)})")


def st_geohash(
    geometry: ExpressionType, precision: ExpressionType, bounds: ExpressionType = None
) -> InstrumentedExpression:
    """Calculates the `geohash` of the supplied geo_point at the specified
    precision. The result is long encoded. Use ST_GEOHASH_TO_STRING to convert
    the result to a string.  These functions are related to the `geo_grid`
    query and the `geohash_grid` aggregation.

    :param geometry: Expression of type `geo_point`. If `null`, the function
                     returns `null`.
    :param precision: Expression of type `integer`. If `null`, the function
                      returns `null`. Valid values are between 1 and 12.
    :param bounds: Optional bounds to filter the grid tiles, a `geo_shape` of
                   type `BBOX`. Use `ST_ENVELOPE` if the `geo_shape` is of any
                   other type.
    """
    if bounds is not None:
        return InstrumentedExpression(
            f"ST_GEOHASH({_render(geometry)}, {_render(precision)}, {_render(bounds)})"
        )
    else:
        return InstrumentedExpression(
            f"ST_GEOHASH({_render(geometry)}, {_render(precision)})"
        )


def st_geohash_to_long(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a geohash grid-ID in string format
    into a long.

    :param grid_id: Input geohash grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOHASH_TO_LONG({_render(grid_id)})")


def st_geohash_to_string(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a geohash grid-ID in long format
    into a string.

    :param grid_id: Input geohash grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOHASH_TO_STRING({_render(grid_id)})")


def st_geohex(
    geometry: ExpressionType, precision: ExpressionType, bounds: ExpressionType = None
) -> InstrumentedExpression:
    """Calculates the `geohex`, the H3 cell-id, of the supplied geo_point at
    the specified precision. The result is long encoded. Use
    ST_GEOHEX_TO_STRING to convert the result to a string.  These functions are
    related to the `geo_grid` query and the `geohex_grid` aggregation.

    :param geometry: Expression of type `geo_point`. If `null`, the function
                     returns `null`.
    :param precision: Expression of type `integer`. If `null`, the function
                      returns `null`. Valid values are between 0 and 15.
    :param bounds: Optional bounds to filter the grid tiles, a `geo_shape` of
                   type `BBOX`. Use `ST_ENVELOPE` if the `geo_shape`
                   is of any other type.
    """
    if bounds is not None:
        return InstrumentedExpression(
            f"ST_GEOHEX({_render(geometry)}, {_render(precision)}, {_render(bounds)})"
        )
    else:
        return InstrumentedExpression(
            f"ST_GEOHEX({_render(geometry)}, {_render(precision)})"
        )


def st_geohex_to_long(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a geohex grid-ID in string format
    into a long.

    :param grid_id: Input geohex grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOHEX_TO_LONG({_render(grid_id)})")


def st_geohex_to_string(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a Geohex grid-ID in long format
    into a string.

    :param grid_id: Input Geohex grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOHEX_TO_STRING({_render(grid_id)})")


def st_geotile(
    geometry: ExpressionType, precision: ExpressionType, bounds: ExpressionType = None
) -> InstrumentedExpression:
    """Calculates the `geotile` of the supplied geo_point at the specified
    precision. The result is long encoded. Use ST_GEOTILE_TO_STRING to convert
    the result to a string.  These functions are related to the `geo_grid`
    query and the `geotile_grid` aggregation.

    :param geometry: Expression of type `geo_point`. If `null`, the function
                     returns `null`.
    :param precision: Expression of type `integer`. If `null`, the function
                      returns `null`. Valid values are between 0 and 29.
    :param bounds: Optional bounds to filter the grid tiles, a `geo_shape` of
                   type `BBOX`. Use `ST_ENVELOPE` if the `geo_shape` is of any
                   other type.
    """
    if bounds is not None:
        return InstrumentedExpression(
            f"ST_GEOTILE({_render(geometry)}, {_render(precision)}, {_render(bounds)})"
        )
    else:
        return InstrumentedExpression(
            f"ST_GEOTILE({_render(geometry)}, {_render(precision)})"
        )


def st_geotile_to_long(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a geotile grid-ID in string format
    into a long.

    :param grid_id: Input geotile grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOTILE_TO_LONG({_render(grid_id)})")


def st_geotile_to_string(grid_id: ExpressionType) -> InstrumentedExpression:
    """Converts an input value representing a geotile grid-ID in long format
    into a string.

    :param grid_id: Input geotile grid-id. The input can be a single- or
                    multi-valued column or an expression.
    """
    return InstrumentedExpression(f"ST_GEOTILE_TO_STRING({_render(grid_id)})")


def st_intersects(
    geom_a: ExpressionType, geom_b: ExpressionType
) -> InstrumentedExpression:
    """Returns true if two geometries intersect. They intersect if they have
    any point in common, including their interior points (points along lines or
    within polygons). This is the inverse of the ST_DISJOINT function. In
    mathematical terms: ST_Intersects(A, B) ⇔ A ⋂ B ≠ ∅

    :param geom_a: Expression of type `geo_point`, `cartesian_point`,
                   `geo_shape` or `cartesian_shape`. If `null`, the function returns
                   `null`.
    :param geom_b: Expression of type `geo_point`, `cartesian_point`, `geo_shape`
                   or `cartesian_shape`. If `null`, the function returns `null`. The
                   second parameter must also have the same coordinate system as the
                   first. This means it is not possible to combine `geo_*` and
                   `cartesian_*` parameters.
    """
    return InstrumentedExpression(
        f"ST_INTERSECTS({_render(geom_a)}, {_render(geom_b)})"
    )


def st_within(geom_a: ExpressionType, geom_b: ExpressionType) -> InstrumentedExpression:
    """Returns whether the first geometry is within the second geometry. This
    is the inverse of the ST_CONTAINS function.

    :param geom_a: Expression of type `geo_point`, `cartesian_point`,
                   `geo_shape` or `cartesian_shape`. If `null`, the function returns
                   `null`.
    :param geom_b: Expression of type `geo_point`, `cartesian_point`, `geo_shape`
                   or `cartesian_shape`. If `null`, the function returns `null`. The
                   second parameter must also have the same coordinate system as the
                   first. This means it is not possible to combine `geo_*` and
                   `cartesian_*` parameters.
    """
    return InstrumentedExpression(f"ST_WITHIN({_render(geom_a)}, {_render(geom_b)})")


def st_x(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the `x` coordinate from the supplied point. If the points is of
    type `geo_point` this is equivalent to extracting the `longitude` value.

    :param point: Expression of type `geo_point` or `cartesian_point`. If
                  `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_X({_render(point)})")


def st_xmax(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the maximum value of the `x` coordinates from the supplied
    geometry. If the geometry is of type `geo_point` or `geo_shape` this is
    equivalent to extracting the maximum `longitude` value.

    :param point: Expression of type `geo_point`, `geo_shape`, `cartesian_point`
                  or `cartesian_shape`. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_XMAX({_render(point)})")


def st_xmin(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the minimum value of the `x` coordinates from the supplied
    geometry. If the geometry is of type `geo_point` or `geo_shape` this is
    equivalent to extracting the minimum `longitude` value.

    :param point: Expression of type `geo_point`, `geo_shape`, `cartesian_point`
                  or `cartesian_shape`. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_XMIN({_render(point)})")


def st_y(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the `y` coordinate from the supplied point. If the points is of
    type `geo_point` this is equivalent to extracting the `latitude` value.

    :param point: Expression of type `geo_point` or `cartesian_point`. If
                  `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_Y({_render(point)})")


def st_ymax(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the maximum value of the `y` coordinates from the supplied
    geometry. If the geometry is of type `geo_point` or `geo_shape` this is
    equivalent to extracting the maximum `latitude` value.

    :param point: Expression of type `geo_point`, `geo_shape`, `cartesian_point`
                  or `cartesian_shape`. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_YMAX({_render(point)})")


def st_ymin(point: ExpressionType) -> InstrumentedExpression:
    """Extracts the minimum value of the `y` coordinates from the supplied
    geometry. If the geometry is of type `geo_point` or `geo_shape` this is
    equivalent to extracting the minimum `latitude` value.

    :param point: Expression of type `geo_point`, `geo_shape`, `cartesian_point`
                  or `cartesian_shape`. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"ST_YMIN({_render(point)})")


def substring(
    string: ExpressionType, start: ExpressionType, length: ExpressionType = None
) -> InstrumentedExpression:
    """Returns a substring of a string, specified by a start position and an
    optional length.

    :param string: String expression. If `null`, the function returns `null`.
    :param start: Start position.
    :param length: Length of the substring from the start position. Optional; if
                   omitted, all positions after `start` are returned.
    """
    if length is not None:
        return InstrumentedExpression(
            f"SUBSTRING({_render(string)}, {_render(start)}, {_render(length)})"
        )
    else:
        return InstrumentedExpression(f"SUBSTRING({_render(string)}, {_render(start)})")


def sum(number: ExpressionType) -> InstrumentedExpression:
    """The sum of a numeric expression.

    :param number:
    """
    return InstrumentedExpression(f"SUM({_render(number)})")


def tan(angle: ExpressionType) -> InstrumentedExpression:
    """Returns the tangent of an angle.

    :param angle: An angle, in radians. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"TAN({_render(angle)})")


def tanh(number: ExpressionType) -> InstrumentedExpression:
    """Returns the hyperbolic tangent of a number.

    :param number: Numeric expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"TANH({_render(number)})")


def tau() -> InstrumentedExpression:
    """Returns the ratio of a circle’s circumference to its radius."""
    return InstrumentedExpression("TAU()")


def term(field: ExpressionType, query: ExpressionType) -> InstrumentedExpression:
    """Performs a Term query on the specified field. Returns true if the
    provided term matches the row.

    :param field: Field that the query will target.
    :param query: Term you wish to find in the provided field.
    """
    return InstrumentedExpression(f"TERM({_render(field)}, {_render(query)})")


def top(
    field: ExpressionType, limit: ExpressionType, order: ExpressionType
) -> InstrumentedExpression:
    """Collects the top values for a field. Includes repeated values.

    :param field: The field to collect the top values for.
    :param limit: The maximum number of values to collect.
    :param order: The order to calculate the top values. Either `asc` or `desc`.
    """
    return InstrumentedExpression(
        f"TOP({_render(field)}, {_render(limit)}, {_render(order)})"
    )


def to_aggregate_metric_double(number: ExpressionType) -> InstrumentedExpression:
    """Encode a numeric to an aggregate_metric_double.

    :param number: Input value. The input can be a single- or multi-valued
                   column or an expression.
    """
    return InstrumentedExpression(f"TO_AGGREGATE_METRIC_DOUBLE({_render(number)})")


def to_base64(string: ExpressionType) -> InstrumentedExpression:
    """Encode a string to a base64 string.

    :param string: A string.
    """
    return InstrumentedExpression(f"TO_BASE64({_render(string)})")


def to_boolean(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a boolean value. A string value of `true`
    will be case-insensitive converted to the Boolean `true`. For anything
    else, including the empty string, the function will return `false`. The
    numerical value of `0` will be converted to `false`, anything else will be
    converted to `true`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_BOOLEAN({_render(field)})")


def to_cartesianpoint(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a `cartesian_point` value. A string will only
    be successfully converted if it respects the WKT Point format.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_CARTESIANPOINT({_render(field)})")


def to_cartesianshape(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a `cartesian_shape` value. A string will only
    be successfully converted if it respects the WKT format.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_CARTESIANSHAPE({_render(field)})")


def to_dateperiod(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value into a `date_period` value.

    :param field: Input value. The input is a valid constant date period expression.
    """
    return InstrumentedExpression(f"TO_DATEPERIOD({_render(field)})")


def to_datetime(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a date value. A string will only be
    successfully converted if it’s respecting the format
    `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`. To convert dates in other formats, use `DATE_PARSE`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_DATETIME({_render(field)})")


def to_date_nanos(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input to a nanosecond-resolution date value (aka date_nanos).

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_DATE_NANOS({_render(field)})")


def to_degrees(number: ExpressionType) -> InstrumentedExpression:
    """Converts a number in radians to degrees).

    :param number: Input value. The input can be a single- or multi-valued
                   column or an expression.
    """
    return InstrumentedExpression(f"TO_DEGREES({_render(number)})")


def to_double(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a double value. If the input parameter is of
    a date type, its value will be interpreted as milliseconds since the Unix
    epoch, converted to double. Boolean `true` will be converted to double
    `1.0`, `false` to `0.0`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_DOUBLE({_render(field)})")


def to_geopoint(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a `geo_point` value. A string will only be
    successfully converted if it respects the WKT Point format.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_GEOPOINT({_render(field)})")


def to_geoshape(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a `geo_shape` value. A string will only be
    successfully converted if it respects the WKT format.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_GEOSHAPE({_render(field)})")


def to_integer(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to an integer value. If the input parameter is
    of a date type, its value will be interpreted as milliseconds since the
    Unix epoch, converted to integer. Boolean `true` will be converted to
    integer `1`, `false` to `0`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_INTEGER({_render(field)})")


def to_ip(
    field: ExpressionType, options: ExpressionType = None
) -> InstrumentedExpression:
    """Converts an input string to an IP value.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    :param options: (Optional) Additional options.
    """
    if options is not None:
        return InstrumentedExpression(f"TO_IP({_render(field)}, {_render(options)})")
    else:
        return InstrumentedExpression(f"TO_IP({_render(field)})")


def to_long(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to a long value. If the input parameter is of a
    date type, its value will be interpreted as milliseconds since the Unix
    epoch, converted to long. Boolean `true` will be converted to long `1`,
    `false` to `0`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_LONG({_render(field)})")


def to_lower(str: ExpressionType) -> InstrumentedExpression:
    """Returns a new string representing the input string converted to lower case.

    :param str: String expression. If `null`, the function returns `null`. The
                input can be a single-valued column or expression, or a multi-valued
                column or expression.
    """
    return InstrumentedExpression(f"TO_LOWER({_render(str)})")


def to_radians(number: ExpressionType) -> InstrumentedExpression:
    """Converts a number in degrees) to radians.

    :param number: Input value. The input can be a single- or multi-valued
                   column or an expression.
    """
    return InstrumentedExpression(f"TO_RADIANS({_render(number)})")


def to_string(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value into a string.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_STRING({_render(field)})")


def to_timeduration(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value into a `time_duration` value.

    :param field: Input value. The input is a valid constant time duration expression.
    """
    return InstrumentedExpression(f"TO_TIMEDURATION({_render(field)})")


def to_unsigned_long(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input value to an unsigned long value. If the input
    parameter is of a date type, its value will be interpreted as milliseconds
    since the Unix epoch, converted to unsigned long. Boolean `true` will be
    converted to unsigned long `1`, `false` to `0`.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_UNSIGNED_LONG({_render(field)})")


def to_upper(str: ExpressionType) -> InstrumentedExpression:
    """Returns a new string representing the input string converted to upper case.

    :param str: String expression. If `null`, the function returns `null`. The
                input can be a single-valued column or expression, or a multi-valued
                column or expression.
    """
    return InstrumentedExpression(f"TO_UPPER({_render(str)})")


def to_version(field: ExpressionType) -> InstrumentedExpression:
    """Converts an input string to a version value.

    :param field: Input value. The input can be a single- or multi-valued column
                  or an expression.
    """
    return InstrumentedExpression(f"TO_VERSION({_render(field)})")


def trim(string: ExpressionType) -> InstrumentedExpression:
    """Removes leading and trailing whitespaces from a string.

    :param string: String expression. If `null`, the function returns `null`.
    """
    return InstrumentedExpression(f"TRIM({_render(string)})")


def values(field: ExpressionType) -> InstrumentedExpression:
    """Returns unique values as a multivalued field. The order of the returned
    values isn’t guaranteed. If you need the values returned in order use `MV_SORT`.

    :param field:
    """
    return InstrumentedExpression(f"VALUES({_render(field)})")


def weighted_avg(
    number: ExpressionType, weight: ExpressionType
) -> InstrumentedExpression:
    """The weighted average of a numeric expression.

    :param number: A numeric value.
    :param weight: A numeric weight.
    """
    return InstrumentedExpression(f"WEIGHTED_AVG({_render(number)}, {_render(weight)})")
