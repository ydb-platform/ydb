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

import re

from typing import Any, Optional


class JException(RuntimeError):
    error: str
    location: int
    current: Optional[Any]
    expected: Optional[Any]

    type: Optional[str]

    # remaining: Sequence[tokenizer.Tokenizer.Token] | None

    def __init__(self, error, location=0, current_token=None, expected=None):
        super().__init__(JException.msg(error, location, current_token, expected))
        self.error = error
        self.location = location
        self.current = current_token
        self.expected = expected

        self.type = None
        self.remaining = None

    #
    # Returns the error code, i.e. S0201
    # @return
    #     
    def get_error(self) -> str:
        return self.error

    #
    # Returns the error location (in characters)
    # @return
    #     
    def get_location(self) -> int:
        return self.location

    #
    # Returns the current token
    # @return
    #     
    def get_current(self) -> Optional[Any]:
        return self.current

    #
    # Returns the expected token
    # @return
    #     
    def get_expected(self) -> Optional[Any]:
        return self.expected

    #
    # Returns the error message with error details in the text.
    # Example: Syntax error: ")" {code=S0201 position=3}
    # @return
    #     
    def get_detailed_error_message(self) -> str:
        return JException.msg(self.error, self.location, self.current, self.expected, True)

    #
    # Generate error message from given error code
    # Codes are defined in Jsonata.errorCodes
    # 
    # Fallback: if error code does not exist, return a generic message
    # 
    # @param error
    # @param location
    # @param arg1
    # @param arg2
    # @param details True = add error details as text, false = don't add details (use getters to retrieve details)
    # @return
    #     
    @staticmethod
    def msg(error: str, location: int, arg1: Optional[Any], arg2: Optional[Any], details: bool = False) -> str:
        message = JException.error_codes.get(error)

        if message is None:
            # unknown error code
            return "JSonataException " + str(error) + (
                " {code=unknown position=" + str(location) + " arg1=" + arg1 + " arg2=" + arg2 + "}" if details else "")

        formatted = message

        # Replace any {{var}} with format "{}"
        formatted = re.sub("\\{\\{\\w+\\}\\}", "{}", formatted)

        formatted = formatted.format(arg1, arg2)

        if details:
            formatted = formatted + " {code=" + error
            if location >= 0:
                formatted += " position=" + str(location)
            formatted += "}"
        return formatted

    #
    # Error codes
    #
    # Sxxxx    - Static errors (compile time)
    # Txxxx    - Type errors
    # Dxxxx    - Dynamic errors (evaluate time)
    #  01xx    - tokenizer
    #  02xx    - parser
    #  03xx    - regex parser
    #  04xx    - Object signature parser/evaluator
    #  10xx    - evaluator
    #  20xx    - operators
    #  3xxx    - functions (blocks of 10 for each function)
    #
    error_codes = {
        "S0101": "String literal must be terminated by a matching quote",
        "S0102": "Number out of range: {{token}}",
        "S0103": "Unsupported escape sequence: \\{{token}}",
        "S0104": "The escape sequence \\u must be followed by 4 hex digits",
        "S0105": "Quoted property name must be terminated with a backquote (`)",
        "S0106": "Comment has no closing tag",
        "S0201": "Syntax error: {{token}}",
        "S0202": "Expected {{value}}, got {{token}}",
        "S0203": "Expected {{value}} before end of expression",
        "S0204": "Unknown operator: {{token}}",
        "S0205": "Unexpected token: {{token}}",
        "S0206": "Unknown expression type: {{token}}",
        "S0207": "Unexpected end of expression",
        "S0208": "Parameter {{value}} of Object definition must be a variable name (start with $)",
        "S0209": "A predicate cannot follow a grouping expression in a step",
        "S0210": "Each step can only have one grouping expression",
        "S0211": "The symbol {{token}} cannot be used as a unary operator",
        "S0212": "The left side of := must be a variable name (start with $)",
        "S0213": "The literal value {{value}} cannot be used as a step within a path expression",
        "S0214": "The right side of {{token}} must be a variable name (start with $)",
        "S0215": "A context variable binding must precede any predicates on a step",
        "S0216": "A context variable binding must precede the \"order-by\" clause on a step",
        "S0217": "The object representing the \"parent\" cannot be derived from this expression",
        "S0301": "Empty regular expressions are not allowed",
        "S0302": "No terminating / in regular expression",
        "S0402": "Choice groups containing parameterized types are not supported",
        "S0401": "Type parameters can only be applied to functions and arrays",
        "S0500": "Attempted to evaluate an expression containing syntax error(s)",
        "T0410": "Argument {{index}} of Object {{token}} does not match Object signature",
        "T0411": "Context value is not a compatible type with argument {{index}} of Object {{token}}",
        "T0412": "Argument {{index}} of Object {{token}} must be an array of {{type}}",
        "D1001": "Number out of range: {{value}}",
        "D1002": "Cannot negate a non-numeric value: {{value}}",
        "T1003": "Key in object structure must evaluate to a string; got: {{value}}",
        "D1004": "Regular expression matches zero length string",
        "T1005": "Attempted to invoke a non-function. Did you mean ${{{token}}}?",
        "T1006": "Attempted to invoke a non-function",
        "T1007": "Attempted to partially apply a non-function. Did you mean ${{{token}}}?",
        "T1008": "Attempted to partially apply a non-function",
        "D1009": "Multiple key definitions evaluate to same key: {{value}}",
        "T1010": "The matcher Object argument passed to Object {{token}} does not return the correct object structure",
        "T2001": "The left side of the {{token}} operator must evaluate to a number",
        "T2002": "The right side of the {{token}} operator must evaluate to a number",
        "T2003": "The left side of the range operator (..) must evaluate to an integer",
        "T2004": "The right side of the range operator (..) must evaluate to an integer",
        "D2005": "The left side of := must be a variable name (start with $)",
        # defunct - replaced by S0212 parser error
        "T2006": "The right side of the Object application operator ~> must be a function",
        "T2007": "Type mismatch when comparing values {{value}} and {{value2}} in order-by clause",
        "T2008": "The expressions within an order-by clause must evaluate to numeric or string values",
        "T2009": "The values {{value}} and {{value2}} either side of operator {{token}} must be of the same data type",
        "T2010": "The expressions either side of operator {{token}} must evaluate to numeric or string values",
        "T2011": "The insert/update clause of the transform expression must evaluate to an object: {{value}}",
        "T2012": "The delete clause of the transform expression must evaluate to a string or array of strings: {{value}}",
        "T2013": "The transform expression clones the input object using the $clone() function.  This has been overridden in the current scope by a non-function.",
        "D2014": "The size of the sequence allocated by the range operator (..) must not exceed 1e6.  Attempted to allocate {{value}}.",
        "D3001": "Attempting to invoke string Object on Infinity or NaN",
        "D3010": "Second argument of replace Object cannot be an empty string",
        "D3011": "Fourth argument of replace Object must evaluate to a positive number",
        "D3012": "Attempted to replace a matched string with a non-string value",
        "D3020": "Third argument of split Object must evaluate to a positive number",
        "D3030": "Unable to cast value to a number: {{value}}",
        "D3040": "Third argument of match Object must evaluate to a positive number",
        "D3050": "The second argument of reduce Object must be a Object with at least two arguments",
        "D3060": "The sqrt Object cannot be applied to a negative number: {{value}}",
        "D3061": "The power Object has resulted in a value that cannot be represented as a JSON number: base={{value}}, exponent={{exp}}",
        "D3070": "The single argument form of the sort Object can only be applied to an array of strings or an array of numbers.  Use the second argument to specify a comparison function",
        "D3080": "The picture string must only contain a maximum of two sub-pictures",
        "D3081": "The sub-picture must not contain more than one instance of the \"decimal-separator\" character",
        "D3082": "The sub-picture must not contain more than one instance of the \"percent\" character",
        "D3083": "The sub-picture must not contain more than one instance of the \"per-mille\" character",
        "D3084": "The sub-picture must not contain both a \"percent\" and a \"per-mille\" character",
        "D3085": "The mantissa part of a sub-picture must contain at least one character that is either an \"optional digit character\" or a member of the \"decimal digit family\"",
        "D3086": "The sub-picture must not contain a passive character that is preceded by an active character and that is followed by another active character",
        "D3087": "The sub-picture must not contain a \"grouping-separator\" character that appears adjacent to a \"decimal-separator\" character",
        "D3088": "The sub-picture must not contain a \"grouping-separator\" at the end of the integer part",
        "D3089": "The sub-picture must not contain two adjacent instances of the \"grouping-separator\" character",
        "D3090": "The integer part of the sub-picture must not contain a member of the \"decimal digit family\" that is followed by an instance of the \"optional digit character\"",
        "D3091": "The fractional part of the sub-picture must not contain an instance of the \"optional digit character\" that is followed by a member of the \"decimal digit family\"",
        "D3092": "A sub-picture that contains a \"percent\" or \"per-mille\" character must not contain a character treated as an \"exponent-separator\"",
        "D3093": "The exponent part of the sub-picture must comprise only of one or more characters that are members of the \"decimal digit family\"",
        "D3100": "The radix of the formatBase Object must be between 2 and 36.  It was given {{value}}",
        "D3110": "The argument of the toMillis Object must be an ISO 8601 formatted timestamp. Given {{value}}",
        "D3120": "Syntax error in expression passed to Object eval: {{value}}",
        "D3121": "Dynamic error evaluating the expression passed to Object eval: {{value}}",
        "D3130": "Formatting or parsing an integer as a sequence starting with {{value}} is not supported by this implementation",
        "D3131": "In a decimal digit pattern, all digits must be from the same decimal group",
        "D3132": "Unknown component specifier {{value}} in date/time picture string",
        "D3133": "The \"name\" modifier can only be applied to months and days in the date/time picture string, not {{value}}",
        "D3134": "The timezone integer format specifier cannot have more than four digits",
        "D3135": "No matching closing bracket \"]\" in date/time picture string",
        "D3136": "The date/time picture string is missing specifiers required to parse the timestamp",
        "D3137": "{{{message}}}",
        "D3138": "The $single() Object expected exactly 1 matching result.  Instead it matched more.",
        "D3139": "The $single() Object expected exactly 1 matching result.  Instead it matched 0.",
        "D3140": "Malformed URL passed to ${{{function_name}}}(): {{value}}",
        "D3141": "{{{message}}}"
    }
