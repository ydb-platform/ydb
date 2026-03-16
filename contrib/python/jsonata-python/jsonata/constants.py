#
# Copyright Robert Yokota
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
# Derived from the following code:
#
#   Project name: jsonata-java
#   Copyright Dashjoin GmbH. https://dashjoin.com
#   Licensed under the Apache License, Version 2.0 (the "License")
#

#
# Constants required by DateTimeUtils
# 
class Constants:
    ERR_MSG_SEQUENCE_UNSUPPORTED = "Formatting or parsing an integer as a sequence starting with %s is not supported by this implementation"
    ERR_MSG_DIFF_DECIMAL_GROUP = "In a decimal digit pattern, all digits must be from the same decimal group"
    ERR_MSG_NO_CLOSING_BRACKET = "No matching closing bracket ']' in date/time picture string"
    ERR_MSG_UNKNOWN_COMPONENT_SPECIFIER = "Unknown component specifier %s in date/time picture string"
    ERR_MSG_INVALID_NAME_MODIFIER = "The 'name' modifier can only be applied to months and days in the date/time picture string, not %s"
    ERR_MSG_TIMEZONE_FORMAT = "The timezone integer format specifier cannot have more than four digits"
    ERR_MSG_MISSING_FORMAT = "The date/time picture string is missing specifiers required to parse the timestamp"
    ERR_MSG_INVALID_OPTIONS_SINGLE_CHAR = "Argument 3 of function %s is invalid. The value of the %s property must be a single character"
    ERR_MSG_INVALID_OPTIONS_STRING = "Argument 3 of function %s is invalid. The value of the %s property must be a string"

    #
    # Collection of decimal format strings that defined by xsl:decimal-format.
    # 
    # <pre>
    #     &lt;!ELEMENT xsl:decimal-format EMPTY&gt
    #     &lt;!ATTLIST xsl:decimal-format
    #       name %qname; #IMPLIED
    #       decimal-separator %char; "."
    #       grouping-separator %char; ","
    #       infinity CDATA "Infinity"
    #       minus-sign %char; "-"
    #       NaN CDATA "NaN"
    #       percent %char; "%"
    #       per-mille %char; "&#x2030;"
    #       zero-digit %char; "0"
    #       digit %char; "#"
    #       pattern-separator %char; ";"&GT
    # </pre>
    # 
    # http://www.w3.org/TR/xslt#format-number} to explain format-number in XSLT
    #      Specification xsl.usage advanced
    #    
    SYMBOL_DECIMAL_SEPARATOR = "decimal-separator"
    SYMBOL_GROUPING_SEPARATOR = "grouping-separator"
    SYMBOL_INFINITY = "infinity"
    SYMBOL_MINUS_SIGN = "minus-sign"
    SYMBOL_NAN = "NaN"
    SYMBOL_PERCENT = "percent"
    SYMBOL_PER_MILLE = "per-mille"
    SYMBOL_ZERO_DIGIT = "zero-digit"
    SYMBOL_DIGIT = "digit"
    SYMBOL_PATTERN_SEPARATOR = "pattern-separator"
