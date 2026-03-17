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
#   Project name: elementpath
#   Copyright (c), 2018-2021, SISSA (Scuola Internazionale Superiore di Studi Avanzati)
#   This project is licensed under the MIT License, see LICENSE

import base64
import datetime
import decimal
import functools
import inspect
import json
import math
import random
import re
import sys
import unicodedata
import urllib.parse
from dataclasses import dataclass
from typing import Any, AnyStr, Mapping, NoReturn, Optional, Sequence, Callable, Type, Union

from jsonata import datetimeutils, jexception, parser, utils


class Functions:

    #
    # Sum function
    # @param {Object} args - Arguments
    # @returns {number} Total value of arguments
    #     
    @staticmethod
    def sum(args: Optional[Sequence[float]]) -> Optional[float]:
        # undefined inputs always return undefined
        if args is None:
            return None

        return sum(args)

    #
    # Count function
    # @param {Object} args - Arguments
    # @returns {number} Number of elements in the array
    #     
    @staticmethod
    def count(args: Optional[Sequence[Any]]) -> float:
        # undefined inputs always return undefined
        if args is None:
            return 0

        return len(args)

    #
    # Max function
    # @param {Object} args - Arguments
    # @returns {number} Max element in the array
    #     
    @staticmethod
    def max(args: Optional[Sequence[float]]) -> Optional[float]:
        # undefined inputs always return undefined
        if args is None or not args:
            return None

        return max(args)

    #
    # Min function
    # @param {Object} args - Arguments
    # @returns {number} Min element in the array
    #     
    @staticmethod
    def min(args: Optional[Sequence[float]]) -> Optional[float]:
        # undefined inputs always return undefined
        if args is None or not args:
            return None

        return min(args)

    #
    # Average function
    # @param {Object} args - Arguments
    # @returns {number} Average element in the array
    #     
    @staticmethod
    def average(args: Optional[Sequence[float]]) -> Optional[float]:
        # undefined inputs always return undefined
        if args is None or not args:
            return None

        return sum(args) / len(args)

    #
    # Stringify arguments
    # @param {Object} arg - Arguments
    # @param {boolean} [prettify] - Pretty print the result
    # @returns {String} String from arguments
    #     
    @staticmethod
    def string(arg: Optional[Any], prettify: Optional[bool]) -> Optional[str]:

        if isinstance(arg, utils.Utils.JList):
            if arg.outer_wrapper:
                arg = arg[0]

        if arg is None:
            return None

        # see https://docs.jsonata.org/string-functions#string: Strings are unchanged
        if isinstance(arg, str):
            return str(arg)

        return Functions._string(arg, bool(prettify))

    @staticmethod
    def _string(arg: Any, prettify: bool) -> str:
        from jsonata import jsonata

        if isinstance(arg, (jsonata.Jsonata.JFunction, parser.Parser.Symbol)):
            return ""

        if prettify:
            return json.dumps(arg, cls=Functions.Encoder, indent="  ")
        else:
            return json.dumps(arg, cls=Functions.Encoder, separators=(',', ':'))

    class Encoder(json.JSONEncoder):
        def encode(self, arg):
            if not isinstance(arg, bool) and isinstance(arg, (int, float)):
                d = decimal.Decimal(arg)
                res = Functions.remove_exponent(d, decimal.Context(prec=15))
                return str(res).lower()

            return super().encode(arg)

        def default(self, arg):
            from jsonata import jsonata

            if arg is utils.Utils.NULL_VALUE:
                return None

            if isinstance(arg, (jsonata.Jsonata.JFunction, parser.Parser.Symbol)):
                return ""

            return super().default(arg)

    @staticmethod
    def remove_exponent(d: decimal.Decimal, ctx: decimal.Context) -> decimal.Decimal:
        # Adapted from https://docs.python.org/3/library/decimal.html#decimal-faq
        if d == d.to_integral():
            try:
                return d.quantize(decimal.Decimal(1), context=ctx)
            except decimal.InvalidOperation:
                pass
        return d.normalize(ctx)

    #
    # Validate input data types.
    # This will make sure that all input data can be processed.
    # 
    # @param arg
    # @return
    #     
    @staticmethod
    def validate_input(arg: Optional[Any]) -> None:
        from jsonata import jsonata

        if arg is None or arg is utils.Utils.NULL_VALUE:
            return

        if isinstance(arg, (jsonata.Jsonata.JFunction, parser.Parser.Symbol)):
            return

        if isinstance(arg, bool):
            return

        if isinstance(arg, (int, float)):
            return

        if isinstance(arg, str):
            return

        if isinstance(arg, dict):
            for k, v in arg.items():
                Functions.validate_input(k)
                Functions.validate_input(v)
            return

        if isinstance(arg, list):
            for v in arg:
                Functions.validate_input(v)
            return

        # Throw error for unknown types
        raise ValueError(
            "Only JSON types (values, Map, List) are allowed as input. Unsupported type: " + str(type(arg)))

    #
    # Create substring based on character number and length
    # @param {String} str - String to evaluate
    # @param {Integer} start - Character number to start substring
    # @param {Integer} [length] - Number of characters in substring
    # @returns {string|*} Substring
    #     
    @staticmethod
    def substring(string: Optional[str], start: Optional[float], length: Optional[float]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        start = int(start) if start is not None else None
        length = int(length) if length is not None else None

        # not used: var strArray = stringToArray(string)
        str_length = len(string)

        if str_length + start < 0:
            start = 0

        if length is not None:
            if length <= 0:
                return ""
            return Functions.substr(string, start, length)

        return Functions.substr(string, start, str_length)

    #
    # Source = Jsonata4Java JSONataUtils.substr
    # @param str
    # @param start  Location at which to begin extracting characters. If a negative
    #               number is given, it is treated as strLength - start where
    #               strLength is the length of the string. For example,
    #               str.substr(-3) is treated as str.substr(str.length - 3)
    # @param length The number of characters to extract. If this argument is null,
    #               all the characters from start to the end of the string are
    #               extracted.
    # @return A new string containing the extracted section of the given string. If
    #         length is 0 or a negative number, an empty string is returned.
    #     
    @staticmethod
    def substr(string: str, start: int, length: int) -> str:

        # below has to convert start and length for emojis and unicode
        orig_len = len(string)

        str_data = string
        str_len = len(str_data)
        if start >= str_len:
            return ""
        # If start is negative, substr() uses it as a character index from the
        # end of the string; the index of the last character is -1.
        start = start if start >= 0 else (0 if (str_len + start) < 0 else str_len + start)
        if start < 0:
            start = 0  # If start is negative and abs(start) is larger than the length of the
        # string, substr() uses 0 as the start index.
        # If length is omitted, substr() extracts characters to the end of the
        # string.
        if length is None:
            length = len(str_data)
        elif length < 0:
            # If length is 0 or negative, substr() returns an empty string.
            return ""
        elif length > len(str_data):
            length = len(str_data)

        if start >= 0:
            # If start is positive and is greater than or equal to the length of
            # the string, substr() returns an empty string.
            if start >= orig_len:
                return ""

        # collect length characters (unless it reaches the end of the string
        # first, in which case it will return fewer)
        end = start + length
        if end > orig_len:
            end = orig_len

        return str_data[start:end]

    #
    # Create substring up until a character
    # @param {String} str - String to evaluate
    # @param {String} chars - Character to define substring boundary
    # @returns {*} Substring
    #     
    @staticmethod
    def substring_before(string: Optional[str], chars: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if chars is None:
            return string

        pos = string.find(chars)
        if pos > -1:
            return string[0:pos]
        else:
            return string

    #
    # Create substring after a character
    # @param {String} str - String to evaluate
    # @param {String} chars - Character to define substring boundary
    # @returns {*} Substring
    #     
    @staticmethod
    def substring_after(string: Optional[str], chars: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        pos = string.find(chars)
        if pos > -1:
            return string[pos + len(chars):]
        else:
            return string

    #
    # Lowercase a string
    # @param {String} str - String to evaluate
    # @returns {string} Lowercase string
    #     
    @staticmethod
    def lowercase(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        return string.casefold()

    #
    # Uppercase a string
    # @param {String} str - String to evaluate
    # @returns {string} Uppercase string
    #     
    @staticmethod
    def uppercase(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        return string.upper()

    #
    # length of a string
    # @param {String} str - string
    # @returns {Number} The number of characters in the string
    #     
    @staticmethod
    def length(string: Optional[str]) -> Optional[int]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        return len(string)

    #
    # Normalize and trim whitespace within a string
    # @param {string} str - string to be trimmed
    # @returns {string} - trimmed string
    #     
    @staticmethod
    def trim(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if not string:
            return ""

        # normalize whitespace
        result = re.sub("[ \t\n\r]+", " ", string)
        if result[0] == ' ':
            # strip leading space
            result = result[1:]

        if result == "":
            return ""

        if result[len(result) - 1] == ' ':
            # strip trailing space
            result = result[0:len(result) - 1]
        return result

    #
    # Pad a string to a minimum width by adding characters to the start or end
    # @param {string} str - string to be padded
    # @param {number} width - the minimum width; +ve pads to the right, -ve pads to the left
    # @param {string} [char] - the pad character(s); defaults to ' '
    # @returns {string} - padded string
    #     
    @staticmethod
    def pad(string: Optional[str], width: Optional[int], char: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if char is None or not char:
            char = " "

        # match JS: truncate width to integer
        if width is not None:
            try:
                width = int(width)
            except Exception:
                width = 0

        if width < 0:
            result = Functions.left_pad(string, -width, char)
        else:
            result = Functions.right_pad(string, width, char)
        return result

    # Source: Jsonata4Java PadFunction
    @staticmethod
    def left_pad(string: Optional[str], size: Optional[int], pad_str: Optional[str]) -> Optional[str]:
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if pad_str is None:
            pad_str = " "

        str_data = string
        str_len = len(str_data)

        if not pad_str:
            pad_str = " "
        pads = size - str_len
        if pads <= 0:
            return string
        padding = ""
        i = 0
        while i < pads + 1:
            padding += pad_str
            i += 1
        return Functions.substr(padding, 0, pads) + string

    # Source: Jsonata4Java PadFunction
    @staticmethod
    def right_pad(string: Optional[str], size: Optional[int], pad_str: Optional[str]) -> Optional[str]:
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if pad_str is None:
            pad_str = " "

        str_data = string
        str_len = len(str_data)

        if not pad_str:
            pad_str = " "
        pads = size - str_len
        if pads <= 0:
            return string
        padding = ""
        i = 0
        while i < pads + 1:
            padding += pad_str
            i += 1
        return string + Functions.substr(padding, 0, pads)

    @dataclass
    class RegexpMatch:
        match: str
        index: int
        groups: Sequence[AnyStr]

    #
    # Evaluate the matcher function against the str arg
    #
    # @param {*} matcher - matching function (native or lambda)
    # @param {string} str - the string to match against
    # @returns {object} - structure that represents the match(es)
    #     
    @staticmethod
    def evaluate_matcher(matcher: re.Pattern, string: Optional[str]) -> list[RegexpMatch]:
        res = []
        matches = matcher.finditer(string)
        for m in matches:
            groups = []
            # Collect the groups
            g = 1
            while g <= len(m.groups()):
                groups.append(m.group(g))
                g += 1

            rm = Functions.RegexpMatch(m.group(), m.start(), groups)
            rm.groups = groups
            res.append(rm)
        return res

    #
    # Tests if the str contains the token
    # @param {String} str - string to test
    # @param {String} token - substring or regex to find
    # @returns {Boolean} - true if str contains token
    #     
    @staticmethod
    def contains(string: Optional[str], token: Union[None, str, re.Pattern]) -> Optional[bool]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            return None

        result = False

        if isinstance(token, str):
            result = (string.find(str(token)) != - 1)
        elif isinstance(token, re.Pattern):
            matches = Functions.evaluate_matcher(token, string)
            # if (dbg) System.out.println("match = "+matches)
            # result = (typeof matches !== 'undefined')
            # throw new Error("regexp not impl"); //result = false
            result = bool(matches)
        else:
            raise RuntimeError("unknown type to match: " + str(token))

        return result

    #
    # Match a string with a regex returning an array of object containing details of each match
    # @param {String} str - string
    # @param {String} regex - the regex applied to the string
    # @param {Integer} [limit] - max number of matches to return
    # @returns {Array} The array of match objects
    #     
    @staticmethod
    def match_(string: Optional[str], regex: Optional[re.Pattern], limit: Optional[int]) -> Optional[list[dict[str, Any]]]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        # limit, if specified, must be a non-negative number
        if limit is not None and limit < 0:
            raise jexception.JException("D3040", -1, limit)

        result = utils.Utils.create_sequence()
        matches = Functions.evaluate_matcher(regex, string)
        max = sys.maxsize
        if limit is not None:
            max = limit

        for i, rm in enumerate(matches):
            m = {"match": rm.match, "index": rm.index, "groups": rm.groups}
            # Convert to JSON map:
            result.append(m)
            if i >= max:
                break
        return result

    #
    # Join an array of strings
    # @param {Array} strs - array of string
    # @param {String} [separator] - the token that splits the string
    # @returns {String} The concatenated string
    #     
    @staticmethod
    def join(strs: Optional[Sequence[str]], separator: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if strs is None:
            return None

        # if separator is not specified, default to empty string
        if separator is None:
            separator = ""

        return separator.join(strs)

    @staticmethod
    def safe_replacement(in_: str) -> str:
        result = in_

        # Replace "$<num>" with "\<num>" for Python regex
        result = re.sub(r"\$(\d+)", r"\\g<\g<1>>", result)

        # Replace "$$" with "$"
        result = re.sub("\\$\\$", "$", result)

        return result

    #
    # Safe replaceAll
    # 
    # In Java, non-existing groups cause an exception.
    # Ignore these non-existing groups (replace with "")
    # 
    # @param s
    # @param pattern
    # @param replacement
    # @return
    #     
    @staticmethod
    def safe_replace_all(s: str, pattern: re.Pattern, replacement: Optional[Any]) -> Optional[str]:

        if not (isinstance(replacement, str)):
            return Functions.safe_replace_all_fn(s, pattern, replacement)

        replacement = str(replacement)

        replacement = Functions.safe_replacement(replacement)
        r = None
        for i in range(0, 10):
            try:
                r = re.sub(pattern, replacement, s)
                break
            except Exception as e:
                msg = str(e)

                # Message we understand needs to be:
                # invalid group reference <g> at position <p>
                m = re.match(r"invalid group reference (\d+) at position (\d+)", msg)

                if m is None:
                    raise e

                g = m.group(1)
                suffix = g[-1]
                prefix = g[:-1]
                # Try capturing a smaller numbered group, e.g. "\g<1>2" instead of "\g<12>"
                replace = "" if not prefix else r"\g<" + prefix + ">" + suffix

                # Adjust replacement to remove the non-existing group
                replacement = replacement.replace(r"\g<" + g + ">", replace)
        return r

    #
    # Converts Java MatchResult to the Jsonata object format
    # @param mr
    # @return
    #     
    @staticmethod
    def to_jsonata_match(mr: re.Match[str]) -> dict[str, list[str]]:
        obj = {"match": mr.group()}

        groups = []
        i = 0
        while i <= len(mr.groups()):
            groups.append(mr.group(i))
            i += 1

        obj["groups"] = groups

        return obj

    #
    # Regexp Replace with replacer function
    # @param s
    # @param pattern
    # @param fn
    # @return
    #     
    @staticmethod
    def safe_replace_all_fn(s: str, pattern: re.Pattern, fn: Optional[Any]) -> str:
        def replace_fn(t):
            res = Functions.func_apply(fn, [Functions.to_jsonata_match(t)])
            if isinstance(res, str):
                return res
            else:
                raise jexception.JException("D3012", -1)

        r = re.sub(pattern, replace_fn, s)
        return r

    #
    # Safe replaceFirst
    # 
    # @param s
    # @param pattern
    # @param replacement
    # @return
    #     
    @staticmethod
    def safe_replace_first(s: str, pattern: re.Pattern, replacement: str) -> Optional[str]:
        replacement = Functions.safe_replacement(replacement)
        r = None
        for i in range(0, 10):
            try:
                r = re.sub(pattern, replacement, s, count=1)
                break
            except Exception as e:
                msg = str(e)

                # Message we understand needs to be:
                # invalid group reference <g> at position <p>
                m = re.match(r"invalid group reference (\d+) at position (\d+)", msg)

                if m is None:
                    raise e

                g = m.group(1)
                suffix = g[-1]
                prefix = g[:-1]
                # Try capturing a smaller numbered group, e.g. "\g<1>2" instead of "\g<12>"
                replace = "" if not prefix else r"\g<" + prefix + ">" + suffix

                # Adjust replacement to remove the non-existing group
                replacement = replacement.replace(r"\g<" + g + ">", replace)
        return r

    @staticmethod
    def replace(string: Optional[str], pattern: Union[str, re.Pattern], replacement: Optional[Any], limit: Optional[int]) -> Optional[str]:
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if isinstance(pattern, str):
            if not pattern:
                raise jexception.JException("Second argument of replace function cannot be an empty string", 0)
        
        if limit is not None and limit < 0:
            raise jexception.JException("Fourth argument of replace function must evaluate to a positive number", 0)

        def string_replacer(match):
            result = ''
            position = 0
            repl = str(replacement)
            while position < len(repl):
                index = repl.find('$', position)
                if index == -1:
                    result += repl[position:]
                    break
                result += repl[position:index]
                position = index + 1
                if position < len(repl):
                    dollar_val = repl[position]
                    if dollar_val == '$':
                        result += '$'
                        position += 1
                    elif dollar_val == '0':
                        result += match.group(0)
                        position += 1
                    else:
                        max_digits = len(str(len(match.groups())))
                        group_num = repl[position:position+max_digits]
                        if group_num.isdigit():
                            group_index = int(group_num)
                            if 0 < group_index <= len(match.groups()):
                                result += match.group(group_index) or ''
                                position += len(group_num)
                            else:
                                result += '$'
                        else:
                            result += '$'
                else:
                    result += '$'
            return result

        if callable(replacement):
            replacer = lambda m: replacement(m.groupdict())
        elif isinstance(replacement, str):
            replacer = string_replacer
        else:
            replacer = lambda m: str(replacement)

        if isinstance(pattern, str):
            # Use string methods for literal string patterns
            result = ''
            position = 0
            count = 0
            while True:
                if limit is not None and count >= limit:
                    result += string[position:]
                    break
                index = string.find(pattern, position)
                if index == -1:
                    result += string[position:]
                    break
                result += string[position:index]
                match = re.match(re.escape(pattern), string[index:])
                result += replacer(match)
                position = index + len(pattern)
                count += 1
            return result
        else:
            # Use regex for pattern objects
            if limit is None:
                return Functions.safe_replace_all(string, pattern, replacement)
            else:
                count = 0
                result = string
                while count < limit:
                    result = Functions.safe_replace_first(result, pattern, str(replacement))
                    count += 1
                return result

    #
    # Base64 encode a string
    # @param {String} str - string
    # @returns {String} Base 64 encoding of the binary data
    #     
    @staticmethod
    def base64encode(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        try:
            return base64.b64encode(string.encode("utf-8")).decode("utf-8")
        except Exception as e:
            return None

    #
    # Base64 decode a string
    # @param {String} str - string
    # @returns {String} Base 64 encoding of the binary data
    #     
    @staticmethod
    def base64decode(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        try:
            return base64.b64decode(string.encode("utf-8")).decode("utf-8")
        except Exception as e:
            return None

    #
    # Encode a string into a component for a url
    # @param {String} str - String to encode
    # @returns {string} Encoded string
    #     
    @staticmethod
    def encode_url_component(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        # See https://stackoverflow.com/questions/946170/equivalent-javascript-functions-for-pythons-urllib-parse-quote-and-urllib-par
        return urllib.parse.quote(string, safe="~()*!.'")

    #
    # Encode a string into a url
    # @param {String} str - String to encode
    # @returns {string} Encoded string
    #     
    @staticmethod
    def encode_url(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        # See https://stackoverflow.com/questions/946170/equivalent-javascript-functions-for-pythons-urllib-parse-quote-and-urllib-par
        return urllib.parse.quote(string, safe="~@#$&()*!+=:;,.?/'")

    #
    # Decode a string from a component for a url
    # @param {String} str - String to decode
    # @returns {string} Decoded string
    #     
    @staticmethod
    def decode_url_component(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        # See https://stackoverflow.com/questions/946170/equivalent-javascript-functions-for-pythons-urllib-parse-quote-and-urllib-par
        return urllib.parse.unquote(string, errors="strict")

    #
    # Decode a string from a url
    # @param {String} str - String to decode
    # @returns {string} Decoded string
    #     
    @staticmethod
    def decode_url(string: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        # See https://stackoverflow.com/questions/946170/equivalent-javascript-functions-for-pythons-urllib-parse-quote-and-urllib-par
        return urllib.parse.unquote(string, errors="strict")

    @staticmethod
    def split(string: Optional[str], pattern: Union[str, Optional[re.Pattern]], limit: Optional[float]) -> Optional[list[str]]:
        if string is None:
            return None

        if string is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if limit is not None and int(limit) < 0:
            raise jexception.JException("D3020", -1, string)

        result = []
        if limit is not None and int(limit) == 0:
            return result

        if isinstance(pattern, str):
            sep = str(pattern)
            if not sep:
                # $split("str", ""): Split string into characters
                lim = int(limit) if limit is not None else sys.maxsize
                i = 0
                while i < len(string) and i < lim:
                    result.append(string[i])
                    i += 1
            else:
                # Quote separator string + preserve trailing empty strings (-1)
                result = string.split(sep, -1)
        else:
            result = pattern.split(string)
        if limit is not None and int(limit) < len(result):
            result = result[0:int(limit)]
        return result

    EXPONENT_PIC = re.compile(r'\d[eE]\d')

    #
    # Formats a number into a decimal string representation using XPath 3.1 F&O fn:format-number spec
    # @param {number} value - number to format
    # @param {String} picture - picture string definition
    # @param {Object} [options] - override locale defaults
    # @returns {String} The formatted string
    #     
    # Adapted from https://github.com/sissaschool/elementpath
    @staticmethod
    def format_number(value: Optional[float], picture: Optional[str], decimal_format: Optional[Mapping[str, str]]) -> Optional[str]:
        if decimal_format is None:
            decimal_format = {}
        pattern_separator = decimal_format.get('pattern-separator', ';')
        sub_pictures = picture.split(pattern_separator)
        if len(sub_pictures) > 2:
            raise jexception.JException('D3080', -1)

        decimal_separator = decimal_format.get('decimal-separator', '.')
        if any(p.count(decimal_separator) > 1 for p in sub_pictures):
            raise jexception.JException('D3081', -1)

        percent_sign = decimal_format.get('percent', '%')
        if any(p.count(percent_sign) > 1 for p in sub_pictures):
            raise jexception.JException('D3082', -1)

        per_mille_sign = decimal_format.get('per-mille', '‰')
        if any(p.count(per_mille_sign) > 1 for p in sub_pictures):
            raise jexception.JException('D3083', -1)
        if any(p.count(percent_sign) + p.count(per_mille_sign) > 1 for p in sub_pictures):
            raise jexception.JException('D3084')

        zero_digit = decimal_format.get('zero-digit', '0')
        optional_digit = decimal_format.get('digit', '#')
        digits_family = ''.join(chr(cp + ord(zero_digit)) for cp in range(10))
        if any(optional_digit not in p and all(x not in p for x in digits_family)
               for p in sub_pictures):
            raise jexception.JException('D3085', -1)

        grouping_separator = decimal_format.get('grouping-separator', ',')
        adjacent_pattern = re.compile(r'[\\%s\\%s]{2}' % (grouping_separator, decimal_separator))
        if any(adjacent_pattern.search(p) for p in sub_pictures):
            raise jexception.JException('D3087', -1)

        if any(x.endswith(grouping_separator)
               for s in sub_pictures for x in s.split(decimal_separator)):
            raise jexception.JException('D3088', -1)

        active_characters = digits_family + ''.join([
            decimal_separator, grouping_separator, pattern_separator, optional_digit
        ])

        exponent_pattern = None

        # Check optional exponent spec correctness in each sub-picture
        exponent_separator = decimal_format.get('exponent-separator', 'e')
        pattern = re.compile(r'(?<=[{0}]){1}[{0}]'.format(
            re.escape(active_characters), exponent_separator
        ))
        for p in sub_pictures:
            for match in pattern.finditer(p):
                if percent_sign in p or per_mille_sign in p:
                    raise jexception.JException('D3092', -1)
                elif any(c not in digits_family for c in p[match.span()[1] - 1:]):
                    # detailed check to consider suffix
                    has_suffix = False
                    for ch in p[match.span()[1] - 1:]:
                        if ch in digits_family:
                            if has_suffix:
                                raise jexception.JException('D3093', -1)
                        elif ch in active_characters:
                            raise jexception.JException('D3086', -1)
                        else:
                            has_suffix = True

                exponent_pattern = pattern

        if value is None:
            return None
        elif math.isnan(value):
            return decimal_format.get('NaN', 'NaN')
        elif isinstance(value, float):
            value = decimal.Decimal.from_float(value)
        elif not isinstance(value, decimal.Decimal):
            value = decimal.Decimal(value)

        minus_sign = decimal_format.get('minus-sign', '-')

        prefix = ''
        if value >= 0:
            subpic = sub_pictures[0]
        else:
            subpic = sub_pictures[-1]
            if len(sub_pictures) == 1:
                prefix = minus_sign

        for k, ch in enumerate(subpic):
            if ch in active_characters:
                prefix += subpic[:k]
                subpic = subpic[k:]
                break
        else:
            prefix += subpic
            subpic = ''

        if not subpic:
            suffix = ''
        elif subpic.endswith(percent_sign):
            suffix = percent_sign
            subpic = subpic[:-len(percent_sign)]

            if value.as_tuple().exponent < 0:
                value *= 100
            else:
                value = decimal.Decimal(int(value) * 100)

        elif subpic.endswith(per_mille_sign):
            suffix = per_mille_sign
            subpic = subpic[:-len(per_mille_sign)]

            if value.as_tuple().exponent < 0:
                value *= 1000
            else:
                value = decimal.Decimal(int(value) * 1000)

        else:
            for k, ch in enumerate(reversed(subpic)):
                if ch in active_characters:
                    idx = len(subpic) - k
                    suffix = subpic[idx:]
                    subpic = subpic[:idx]
                    break
            else:
                suffix = subpic
                subpic = ''

        exp_fmt = None
        if exponent_pattern is not None:
            exp_match = exponent_pattern.search(subpic)
            if exp_match is not None:
                exp_fmt = subpic[exp_match.span()[0] + 1:]
                subpic = subpic[:exp_match.span()[0]]

        fmt_tokens = subpic.split(decimal_separator)
        if all(not fmt for fmt in fmt_tokens):
            raise jexception.JException('both integer and fractional parts are empty', -1)

        if math.isinf(value):
            return prefix + decimal_format.get('infinity', '∞') + suffix

        # Calculate the exponent value if it's in the sub-picture
        exp_value = 0
        if exp_fmt and value:
            num_digits = 0
            for ch in fmt_tokens[0]:
                if ch in digits_family:
                    num_digits += 1

            if abs(value) > 1:
                v = abs(value)
                while v > 10 ** num_digits:
                    exp_value += 1
                    v /= 10

                # modify empty fractional part to store a digit
                if not num_digits:
                    if len(fmt_tokens) == 1:
                        fmt_tokens.append(zero_digit)
                    elif not fmt_tokens[-1]:
                        fmt_tokens[-1] = zero_digit

            elif len(fmt_tokens) > 1 and fmt_tokens[-1] and value >= 0:
                v = abs(value) * 10
                while v < 10 ** num_digits:
                    exp_value -= 1
                    v *= 10
            else:
                v = abs(value) * 10
                while v < 10:
                    exp_value -= 1
                    v *= 10

            if exp_value:
                value = value * decimal.Decimal(10) ** -exp_value

        # round the value by fractional part
        if len(fmt_tokens) == 1 or not fmt_tokens[-1]:
            exp = decimal.Decimal('1')
        else:
            k = -1
            for ch in fmt_tokens[-1]:
                if ch in digits_family or ch == optional_digit:
                    k += 1
            exp = decimal.Decimal('.' + '0' * k + '1')

        try:
            if value > 0:
                value = value.quantize(exp, rounding='ROUND_HALF_UP')
            else:
                value = value.quantize(exp, rounding='ROUND_HALF_DOWN')
        except decimal.InvalidOperation:
            pass  # number too large, don't round ...

        chunks = Functions.decimal_to_string(value).lstrip('-').split('.')
        kwargs = {
            'digits_family': digits_family,
            'optional_digit': optional_digit,
            'grouping_separator': grouping_separator,
        }
        result = Functions.format_digits(chunks[0], fmt_tokens[0], **kwargs)

        if len(fmt_tokens) > 1 and fmt_tokens[0]:
            has_decimal_digit = False
            for ch in fmt_tokens[0]:
                if ch in digits_family:
                    has_decimal_digit = True
                elif ch == optional_digit and has_decimal_digit:
                    raise jexception.JException('D3090', -1)

        if len(fmt_tokens) > 1 and fmt_tokens[-1]:
            has_optional_digit = False
            for ch in fmt_tokens[-1]:
                if ch == optional_digit:
                    has_optional_digit = True
                elif ch in digits_family and has_optional_digit:
                    raise jexception.JException('D3091', -1)

            if len(chunks) == 1:
                chunks.append(zero_digit)

            decimal_part = Functions.format_digits(chunks[1], fmt_tokens[-1], **kwargs)

            for ch in reversed(fmt_tokens[-1]):
                if ch == optional_digit:
                    if decimal_part and decimal_part[-1] == zero_digit:
                        decimal_part = decimal_part[:-1]
                else:
                    if not decimal_part:
                        decimal_part = zero_digit
                    break

            if decimal_part:
                result += decimal_separator + decimal_part

                if not fmt_tokens[0] and result.startswith(zero_digit):
                    result = result.lstrip(zero_digit)

        if exp_fmt:
            exp_digits = Functions.format_digits(str(abs(exp_value)), exp_fmt, **kwargs)
            if exp_value >= 0:
                result += f'{exponent_separator}{exp_digits}'
            else:
                result += f'{exponent_separator}-{exp_digits}'

        return prefix + result + suffix

    @staticmethod
    def decimal_to_string(value: decimal.Decimal) -> str:
        """
        Convert a Decimal value to a string representation
        that not includes exponent and with its decimals.
        """
        sign, digits, exponent = value.as_tuple()

        if not exponent:
            result = ''.join(str(x) for x in digits)
        elif exponent > 0:
            result = ''.join(str(x) for x in digits) + '0' * exponent
        else:
            result = ''.join(str(x) for x in digits[:exponent])
            if not result:
                result = '0'
            result += '.'
            if len(digits) >= -exponent:
                result += ''.join(str(x) for x in digits[exponent:])
            else:
                result += '0' * (-exponent - len(digits))
                result += ''.join(str(x) for x in digits)

        return '-' + result if sign else result

    @staticmethod
    def format_digits(digits: str,
                      fmt: str,
                      digits_family: str = '0123456789',
                      optional_digit: str = '#',
                      grouping_separator: Optional[str] = None) -> str:
        result = []
        iter_num_digits = reversed(digits)
        num_digit = next(iter_num_digits)

        for fmt_char in reversed(fmt):
            if fmt_char in digits_family or fmt_char == optional_digit:
                if num_digit:
                    result.append(digits_family[ord(num_digit) - 48])
                    num_digit = next(iter_num_digits, '')
                elif fmt_char != optional_digit:
                    result.append(digits_family[0])
            elif not result or not result[-1] in digits_family and grouping_separator \
                    and result[-1] != grouping_separator:
                raise jexception.JException("invalid grouping in picture argument", -1)
            else:
                result.append(fmt_char)

        if num_digit:
            separator = ''
            sep = {x for x in fmt if x not in digits_family and x != optional_digit}
            if len(sep) != 1:
                repeat = None
            else:
                separator = sep.pop()
                chunks = fmt.split(separator)

                if len(chunks[0]) > len(chunks[-1]):
                    repeat = None
                elif all(len(item) == len(chunks[-1]) for item in chunks[1:-1]):
                    repeat = len(chunks[-1]) + 1
                else:
                    repeat = None

            if repeat is None:
                while num_digit:
                    result.append(digits_family[ord(num_digit) - 48])
                    num_digit = next(iter_num_digits, '')
            else:
                while num_digit:
                    if ((len(result) + 1) % repeat) == 0:
                        result.append(separator)
                    result.append(digits_family[ord(num_digit) - 48])
                    num_digit = next(iter_num_digits, '')

        if grouping_separator:
            return ''.join(reversed(result)).lstrip(grouping_separator)
        while result and \
                unicodedata.category(result[-1]) not in ('Nd', 'Nl', 'No', 'Lu', 'Ll', 'Lt', 'Lm', 'Lo'):
            result.pop()
        return ''.join(reversed(result))

    #
    # Converts a number to a string using a specified number base
    # @param {number} value - the number to convert
    # @param {number} [radix] - the number base; must be between 2 and 36. Defaults to 10
    # @returns {string} - the converted string
    #     
    @staticmethod
    def format_base(value: Optional[float], radix: Optional[float]) -> Optional[str]:
        # undefined inputs always return undefined
        if value is None:
            return None

        value = Functions.round(value, 0)

        if radix is None:
            radix = 10
        else:
            radix = int(radix)

        if radix < 2 or radix > 36:
            raise jexception.JException("D3100", radix)

        result = Functions.base_repr(int(value), radix).lower()

        return result

    @staticmethod
    def base_repr(number: int, base: int = 2, padding: int = 0) -> str:
        """
        Return a string representation of a number in the given base system.

        Parameters
        ----------
        number : int
            The value to convert. Positive and negative values are handled.
        base : int, optional
            Convert `number` to the `base` number system. The valid range is 2-36,
            the default value is 2.
        padding : int, optional
            Number of zeros padded on the left. Default is 0 (no padding).

        Returns
        -------
        out : str
            String representation of `number` in `base` system.

        """
        digits = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        if base > len(digits):
            raise ValueError("Bases greater than 36 not handled in base_repr.")
        elif base < 2:
            raise ValueError("Bases less than 2 not handled in base_repr.")

        num = abs(number)
        res = []
        while num:
            res.append(digits[num % base])
            num //= base
        if padding:
            res.append('0' * padding)
        if number < 0:
            res.append('-')
        return ''.join(reversed(res or '0'))

    #
    # Cast argument to number
    # @param {Object} arg - Argument
    # @throws NumberFormatException
    # @returns {Number} numeric value of argument
    #     
    @staticmethod
    def number(arg: Optional[Any]) -> Optional[float]:
        result = None

        # undefined inputs always return undefined
        if arg is None:
            return None

        if arg is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if isinstance(arg, bool):
            result = 1 if (bool(arg)) else 0
        elif isinstance(arg, (int, float)):
            result = arg
        elif isinstance(arg, str):
            s = str(arg)
            if s.startswith("0x"):
                result = int(s[2:], 16)
            elif s.startswith("0B"):
                result = int(s[2:], 2)
            elif s.startswith("0O"):
                result = int(s[2:], 8)
            else:
                result = float(str(arg))
        return result

    #
    # Absolute value of a number
    # @param {Number} arg - Argument
    # @returns {Number} absolute value of argument
    #     
    @staticmethod
    def abs(arg: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        return abs(float(arg)) if isinstance(arg, float) else abs(int(arg))

    #
    # Rounds a number down to integer
    # @param {Number} arg - Argument
    # @returns {Number} rounded integer
    #     
    @staticmethod
    def floor(arg: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        return math.floor(float(arg))

    #
    # Rounds a number up to integer
    # @param {Number} arg - Argument
    # @returns {Number} rounded integer
    #     
    @staticmethod
    def ceil(arg: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        return math.ceil(float(arg))

    #
    # Round to half even
    # @param {Number} arg - Argument
    # @param {Number} [precision] - number of decimal places
    # @returns {Number} rounded integer
    #     
    @staticmethod
    def round(arg: Optional[float], precision: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        d = decimal.Decimal(str(arg))
        return float(round(d, precision))

    #
    # Square root of number
    # @param {Number} arg - Argument
    # @returns {Number} square root
    #     
    @staticmethod
    def sqrt(arg: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        if float(arg) < 0:
            raise jexception.JException("D3060", 1, arg)

        return math.sqrt(float(arg))

    #
    # Raises number to the power of the second number
    # @param {Number} arg - the base
    # @param {Number} exp - the exponent
    # @returns {Number} rounded integer
    #     
    @staticmethod
    def power(arg: Optional[float], exp: Optional[float]) -> Optional[float]:

        # undefined inputs always return undefined
        if arg is None:
            return None

        result = float(arg) ** float(exp)

        if not math.isfinite(result):
            raise jexception.JException("D3061", 1, arg, exp)

        return result

    #
    # Returns a random number 0 <= n < 1
    # @returns {number} random number
    #     
    @staticmethod
    def random() -> float:
        return random.random()

    #
    # Evaluate an input and return a boolean
    # @param {*} arg - Arguments
    # @returns {boolean} Boolean
    #     
    @staticmethod
    def to_boolean(arg: Optional[Any]) -> Optional[bool]:
        from jsonata import jsonata
        # cast arg to its effective boolean value
        # boolean: unchanged
        # string: zero-length -> false; otherwise -> true
        # number: 0 -> false; otherwise -> true
        # null -> false
        # array: empty -> false; length > 1 -> true
        # object: empty -> false; non-empty -> true
        # function -> false

        # undefined inputs always return undefined
        if arg is None:
            return None  # Uli: Null would need to be handled as false anyway

        result = False
        if isinstance(arg, list):
            el = arg
            if len(el) == 1:
                result = Functions.to_boolean(el[0])
            elif len(el) > 1:
                result = any(jsonata.Jsonata.boolize(e) for e in el)
        elif isinstance(arg, str):
            s = str(arg)
            if s:
                result = True
        elif isinstance(arg, bool):
            result = bool(arg)
        elif isinstance(arg, (int, float)):
            if float(arg) != 0:
                result = True
        elif isinstance(arg, dict):
            if arg:
                result = True
        return result

    #
    # returns the Boolean NOT of the arg
    # @param {*} arg - argument
    # @returns {boolean} - NOT arg
    #     
    @staticmethod
    def not_(arg: Optional[Any]) -> Optional[bool]:
        # undefined inputs always return undefined
        if arg is None:
            return None

        return not Functions.to_boolean(arg)

    @staticmethod
    def get_function_arity(func: Any) -> int:
        from jsonata import jsonata
        if isinstance(func, jsonata.Jsonata.JFunction):
            return func.signature.get_min_number_of_args()
        elif isinstance(func, jsonata.Jsonata.JLambda):
            from inspect import signature

            return len(signature(func.function).parameters)
        else:
            return len(func.arguments)

    #
    # Helper function to build the arguments to be supplied to the function arg of the
    # HOFs map, filter, each, sift and single
    # @param {function} func - the function to be invoked
    # @param {*} arg1 - the first (required) arg - the value
    # @param {*} arg2 - the second (optional) arg - the position (index or key)
    # @param {*} arg3 - the third (optional) arg - the whole structure (array or object)
    # @returns {*[]} the argument list
    #     
    @staticmethod
    def hof_func_args(func: Any, arg1: Optional[Any], arg2: Optional[Any], arg3: Optional[Any]) -> list:
        func_args = [arg1]
        # the other two are optional - only supply it if the function can take it
        length = Functions.get_function_arity(func)
        if length >= 2:
            func_args.append(arg2)
        if length >= 3:
            func_args.append(arg3)
        return func_args

    #
    # Call helper for Java
    # 
    # @param func
    # @param funcArgs
    # @return
    # @throws Throwable
    #     
    @staticmethod
    def func_apply(func: Any, func_args: Optional[Sequence]) -> Optional[Any]:
        from jsonata import jsonata
        res = None
        if Functions.is_lambda(func):
            res = jsonata.Jsonata.CURRENT.jsonata.apply(func, func_args, None,
                                                        jsonata.Jsonata.CURRENT.jsonata.environment)
        else:
            res = func.call(None, func_args)
        return res

    #
    # Create a map from an array of arguments
    # @param {Array} [arr] - array to map over
    # @param {Function} func - function to apply
    # @returns {Array} Map array
    #     
    @staticmethod
    def map(arr: Optional[Sequence], func: Any) -> Optional[list]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        result = (res for res in (Functions.func_apply(func, Functions.hof_func_args(func, arg, i, arr))
                                  for i, arg in enumerate(arr)) if res is not None)
        return utils.Utils.create_sequence_from_iter(result)

    #
    # Create a map from an array of arguments
    # @param {Array} [arr] - array to filter
    # @param {Function} func - predicate function
    # @returns {Array} Map array
    #     
    @staticmethod
    def filter(arr: Optional[Sequence], func: Any) -> Optional[list]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        result = (arg for i, arg in enumerate(arr) if Functions.to_boolean(
            Functions.func_apply(func, Functions.hof_func_args(func, arg, i, arr))))
        return utils.Utils.create_sequence_from_iter(result)

    #
    # Given an array, find the single element matching a specified condition
    # Throws an exception if the number of matching elements is not exactly one
    # @param {Array} [arr] - array to filter
    # @param {Function} [func] - predicate function
    # @returns {*} Matching element
    #     
    @staticmethod
    def single(arr: Optional[Sequence], func: Any) -> Optional[Any]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        has_found_match = False
        result = None

        for i, entry in enumerate(arr):
            positive_result = True
            if func is not None:
                func_args = Functions.hof_func_args(func, entry, i, arr)
                # invoke func
                res = Functions.func_apply(func, func_args)
                positive_result = Functions.to_boolean(res)
            if positive_result:
                if not has_found_match:
                    result = entry
                    has_found_match = True
                else:
                    raise jexception.JException("D3138", i)

        if not has_found_match:
            raise jexception.JException("D3139", -1)

        return result

    #
    # Convolves (zips) each value from a set of arrays
    # @param {Array} [args] - arrays to zip
    # @returns {Array} Zipped array
    #     
    @staticmethod
    def zip(*args: Sequence) -> list:
        if any(a is None for a in args):
            return []

        return [list(a) for a in zip(*args)]

    #
    # Fold left function
    # @param {Array} sequence - Sequence
    # @param {Function} func - Function
    # @param {Object} init - Initial value
    # @returns {*} Result
    #     
    @staticmethod
    def fold_left(sequence: Optional[Sequence], func: Any, init: Optional[Any]) -> Optional[Any]:
        # undefined inputs always return undefined
        if sequence is None:
            return None
        result = None

        arity = Functions.get_function_arity(func)
        if arity < 2:
            raise jexception.JException("D3050", 1)

        index = 0
        if init is None and sequence:
            result = sequence[0]
            index = 1
        else:
            result = init
            index = 0

        while index < len(sequence):
            args = [result, sequence[index]]
            if arity >= 3:
                args.append(index)
            if arity >= 4:
                args.append(sequence)
            result = Functions.func_apply(func, args)
            index += 1

        return result

    #
    # Return keys for an object
    # @param {Object} arg - Object
    # @returns {Array} Array of keys
    #     
    @staticmethod
    def keys(arg: Union[Sequence, Mapping, None]) -> list:
        if isinstance(arg, list):
            # merge the keys of all of the items in the array
            keys = {k: '' for el in arg for k in Functions.keys(el)}
            result = utils.Utils.create_sequence_from_iter(keys.keys())
        elif isinstance(arg, dict):
            result = utils.Utils.create_sequence_from_iter(arg.keys())
        else:
            result = utils.Utils.create_sequence()

        return result

    # here: append, lookup

    #
    # Determines if the argument is undefined
    # @param {*} arg - argument
    # @returns {boolean} False if argument undefined, otherwise true
    #     
    @staticmethod
    def exists(arg: Optional[Any]) -> bool:
        return arg is not None

    #
    # Splits an object into an array of object with one property each
    # @param {*} arg - the object to split
    # @returns {*} - the array
    #     
    @staticmethod
    def spread(arg: Optional[Any]) -> Optional[Any]:
        result = utils.Utils.create_sequence()

        if isinstance(arg, list):
            # spread all of the items in the array
            for item in arg:
                result = Functions.append(result, Functions.spread(item))
        elif isinstance(arg, dict):
            for k, v in arg.items():
                obj = {k: v}
                result.append(obj)
        else:
            return arg  # result = arg;
        return result

    #
    # Merges an array of objects into a single object.  Duplicate properties are
    # overridden by entries later in the array
    # @param {*} arg - the objects to merge
    # @returns {*} - the object
    #     
    @staticmethod
    def merge(arg: Optional[Sequence[Mapping]]) -> Optional[dict]:
        # undefined inputs always return undefined
        if arg is None:
            return None

        return {k: v for obj in arg for k, v in obj.items()}

    #
    # Reverses the order of items in an array
    # @param {Array} arr - the array to reverse
    # @returns {Array} - the reversed array
    #     
    @staticmethod
    def reverse(arr: Optional[Sequence]) -> Optional[Sequence]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        if len(arr) <= 1:
            return arr

        result = list(arr)
        result.reverse()
        return result

    #
    #
    # @param {*} obj - the input object to iterate over
    # @param {*} func - the function to apply to each key/value pair
    # @throws Throwable
    # @returns {Array} - the resultant array
    #     
    @staticmethod
    def each(obj: Optional[Mapping], func: Any) -> Optional[list]:
        if obj is None:
            return None

        result = (res for res in (Functions.func_apply(func, Functions.hof_func_args(func, value, key, obj))
                                  for key, value in obj.items()) if res is not None)
        return utils.Utils.create_sequence_from_iter(result)

    #
    #
    # @param {string} [message] - the message to attach to the error
    # @throws custom error with code 'D3137'
    #     
    @staticmethod
    def error(message: Optional[str]) -> NoReturn:
        raise jexception.JException("D3137", -1, message if message is not None else "$error() function evaluated")

    #
    #
    # @param {boolean} condition - the condition to evaluate
    # @param {string} [message] - the message to attach to the error
    # @throws custom error with code 'D3137'
    # @returns {undefined}
    #     
    @staticmethod
    def assert_fn(condition: Optional[bool], message: Optional[str]) -> None:
        if condition is utils.Utils.NULL_VALUE:
            raise jexception.JException("T0410", -1)

        if not condition:
            raise jexception.JException("D3141", -1, "$assert() statement failed")
            #                message: message || "$assert() statement failed"

    #
    #
    # @param {*} [value] - the input to which the type will be checked
    # @returns {string} - the type of the input
    #     
    @staticmethod
    def type(value: Optional[Any]) -> Optional[str]:
        if value is None:
            return None

        if value is utils.Utils.NULL_VALUE:
            return "null"

        if isinstance(value, bool):
            return "boolean"

        if isinstance(value, (int, float)):
            return "number"

        if isinstance(value, str):
            return "string"

        if isinstance(value, list):
            return "array"

        if utils.Utils.is_function(value) or Functions.is_lambda(value):
            return "function"

        return "object"

    #
    # Implements the merge sort (stable) with optional comparator function
    #
    # @param {Array} arr - the array to sort
    # @param {*} comparator - comparator function
    # @returns {Array} - sorted array
    #     
    @staticmethod
    def sort(arr: Optional[Sequence], comparator: Optional[Any]) -> Optional[Sequence]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        if len(arr) <= 1:
            return arr

        result = list(arr)

        if comparator is not None:
            comp = Functions.Comparator(comparator).compare
            result = sorted(result, key=functools.cmp_to_key(comp))
        else:
            result = sorted(result)

        return result

    class Comparator:
        _comparator: Optional[Any]

        def __init__(self, comparator):
            from jsonata import jsonata
            if isinstance(comparator, Callable):
                self._comparator = jsonata.Jsonata.JLambda(comparator)
            else:
                self._comparator = comparator

        def compare(self, o1, o2):
            res = Functions.func_apply(self._comparator, [o1, o2])
            if isinstance(res, bool):
                return 1 if res else -1
            return int(res)

    #
    # Randomly shuffles the contents of an array
    # @param {Array} arr - the input array
    # @returns {Array} the shuffled array
    #     
    @staticmethod
    def shuffle(arr: Optional[Sequence]) -> Optional[Sequence]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        if len(arr) <= 1:
            return arr

        result = list(arr)
        random.shuffle(result)
        return result

    #
    # Returns the values that appear in a sequence, with duplicates eliminated.
    # @param {Array} arr - An array or sequence of values
    # @returns {Array} - sequence of distinct values
    #     
    @staticmethod
    def distinct(arr: Optional[Any]) -> Optional[Any]:
        # undefined inputs always return undefined
        if arr is None:
            return None

        if not (isinstance(arr, list)) or len(arr) <= 1:
            return arr

        results = utils.Utils.create_sequence() if (isinstance(arr, utils.Utils.JList)) else []

        for el in arr:
            if el not in results:
                results.append(el)

        return results

    #
    # Applies a predicate function to each key/value pair in an object, and returns an object containing
    # only the key/value pairs that passed the predicate
    #
    # @param {object} arg - the object to be sifted
    # @param {object} func - the predicate function (lambda or native)
    # @throws Throwable
    # @returns {object} - sifted object
    #     
    @staticmethod
    def sift(arg: Optional[Mapping], func: Any) -> Optional[dict]:
        from jsonata import jsonata
        if arg is None:
            return None

        result = {item: entry for item, entry in arg.items() if jsonata.Jsonata.boolize(
            Functions.func_apply(func, Functions.hof_func_args(func, entry, item, arg)))}

        # empty objects should be changed to undefined
        if not result:
            result = None

        return result

    #
    # Append second argument to first
    # @param {Array|Object} arg1 - First argument
    # @param {Array|Object} arg2 - Second argument
    # @returns {*} Appended arguments
    #     
    @staticmethod
    def append(arg1: Optional[Any], arg2: Optional[Any]) -> Optional[Any]:
        # disregard undefined args
        if arg1 is None:
            return arg2
        if arg2 is None:
            return arg1

        # if either argument is not an array, make it so
        if not (isinstance(arg1, list)):
            arg1 = utils.Utils.create_sequence(arg1)
        if not (isinstance(arg2, list)):
            arg2 = utils.Utils.JList([arg2])

        arg1 = utils.Utils.JList(arg1)  # create a new copy!
        arg1.extend(arg2)
        return arg1

    @staticmethod
    def is_lambda(result: Optional[Any]) -> bool:
        return isinstance(result, parser.Parser.Symbol) and result._jsonata_lambda

    #
    # Return value from an object for a given key
    # @param {Object} input - Object/Array
    # @param {String} key - Key in object
    # @returns {*} Value of key in object
    #     
    @staticmethod
    def lookup(input: Union[Mapping, Optional[Sequence]], key: Optional[str]) -> Optional[Any]:
        # lookup the 'name' item in the input
        result = None
        if isinstance(input, list):
            result = utils.Utils.create_sequence()
            for inp in input:
                res = Functions.lookup(inp, key)
                if res is not None:
                    if isinstance(res, list):
                        result.extend(res)
                    else:
                        result.append(res)
        elif isinstance(input, dict):
            result = input.get(key, utils.Utils.NONE)
            # Detect the case where the value is null:
            if result is None and key in input:
                result = utils.Utils.NULL_VALUE
            elif result is utils.Utils.NONE:
                result = None
        return result

    @staticmethod
    def test(a: Optional[str], b: Optional[str]) -> str:
        return a + b

    @staticmethod
    def get_function(clz: Optional[Type], name: Optional[str]) -> Optional[Any]:
        if name is None:
            return None
        return getattr(clz, name)

    @staticmethod
    def call(clz: Optional[Type], name: Optional[str], args: Optional[Sequence]) -> Optional[Any]:
        m = Functions.get_function(clz, name)
        nargs = len(inspect.signature(m).parameters)
        return Functions._call(m, nargs, args)

    @staticmethod
    def _call(m: Callable, nargs: int, args: Optional[Sequence]) -> Optional[Any]:
        call_args = list(args)
        while len(call_args) < nargs:
            # Add default arg null if not enough args were provided
            call_args.append(None)

        res = m(*call_args)
        if utils.Utils.is_numeric(res):
            res = utils.Utils.convert_number(res)
        return res

    #
    # DateTime
    #

    #
    # Converts an ISO 8601 timestamp to milliseconds since the epoch
    #
    # @param {string} timestamp - the timestamp to be converted
    # @param {string} [picture] - the picture string defining the format of the timestamp (defaults to ISO 8601)
    # @throws ParseException 
    # @returns {Number} - milliseconds since the epoch
    #     
    @staticmethod
    def datetime_to_millis(timestamp: Optional[str], picture: Optional[str]) -> Optional[int]:
        # undefined inputs always return undefined
        if timestamp is None:
            return None

        if picture is None:
            if Functions.is_numeric(timestamp):
                dt = datetime.datetime.strptime(timestamp, "%Y")
            else:
                dt = datetime.datetime.fromisoformat(timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
            # try:
            #     size = len(timestamp)
            #     if size > 5:
            #         if timestamp[size - 5] == '+' or timestamp[size - 5] == '-':
            #             if (timestamp[size - 4]).isdigit() and (timestamp[size - 3]).isdigit() and (
            #             timestamp[size - 2]).isdigit() and (timestamp[size - 1]).isdigit():
            #                 timestamp = timestamp[0:size - 2] + ':' + timestamp[size - 2:size]
            #     return java.time.OffsetDateTime.parse(timestamp).toInstant().toEpochMilli()
            # except RuntimeError as e:
            #     ldt = java.time.LocalDate.parse(timestamp, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
            #     return ldt.atStartOfDay().atZone(java.time.ZoneId.of("UTC")).toInstant().toEpochMilli()
        else:
            return datetimeutils.DateTimeUtils.parse_datetime(timestamp, picture)

    # Adapted from: org.apache.commons.lang3.StringUtils
    @staticmethod
    def is_numeric(cs: Optional[str]) -> bool:
        if cs is None or not cs:
            return False
        for c in cs:
            if not c.isdigit():
                return False
        return True

    #
    # Converts milliseconds since the epoch to an ISO 8601 timestamp
    # @param {Number} millis - milliseconds since the epoch to be converted
    # @param {string} [picture] - the picture string defining the format of the timestamp (defaults to ISO 8601)
    # @param {string} [timezone] - the timezone to format the timestamp in (defaults to UTC)
    # @returns {String} - the formatted timestamp
    #     
    @staticmethod
    def datetime_from_millis(millis: Optional[float], picture: Optional[str], timezone: Optional[str]) -> Optional[str]:
        # undefined inputs always return undefined
        if millis is None:
            return None

        return datetimeutils.DateTimeUtils.format_datetime(int(millis), picture, timezone)

    #
    # Formats an integer as specified by the XPath fn:format-integer function
    # See https://www.w3.org/TR/xpath-functions-31/#func-format-integer
    # @param {number} value - the number to be formatted
    # @param {string} picture - the picture string that specifies the format
    # @returns {string} - the formatted number
    #     
    @staticmethod
    def format_integer(value: Optional[float], picture: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return datetimeutils.DateTimeUtils.format_integer(int(value), picture)

    #
    # parse a string containing an integer as specified by the picture string
    # @param {string} value - the string to parse
    # @param {string} picture - the picture string
    # @throws ParseException
    # @returns {number} - the parsed number
    #     
    @staticmethod
    def parse_integer(value: Optional[str], picture: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        return datetimeutils.DateTimeUtils.parse_integer(value, picture)

    #
    # Clones an object
    # @param {Object} arg - object to clone (deep copy)
    # @returns {*} - the cloned object
    #     
    @staticmethod
    def function_clone(arg: Optional[Any]) -> Optional[Any]:
        # undefined inputs always return undefined
        if arg is None:
            return None

        res = json.loads(Functions.string(arg, False))
        return res

    #
    # parses and evaluates the supplied expression
    # @param {string} expr - expression to evaluate
    # @returns {*} - result of evaluating the expression
    #     
    @staticmethod
    def function_eval(expr: Optional[str], focus: Optional[Any]) -> Optional[Any]:
        from jsonata import jsonata
        # undefined inputs always return undefined
        if expr is None:
            return None
        input = jsonata.Jsonata.CURRENT.jsonata.input  # =  this.input;
        if focus is not None:
            input = focus
            # if the input is a JSON array, then wrap it in a singleton sequence so it gets treated as a single input
            if (isinstance(input, list)) and not utils.Utils.is_sequence(input):
                input = utils.Utils.create_sequence(input)
                input.outer_wrapper = True

        ast = None
        try:
            ast = jsonata.Jsonata(expr)
        except Exception as err:
            # error parsing the expression passed to $eval
            # populateMessage(err)
            raise jexception.JException("D3120", -1)
        result = None
        try:
            result = ast.evaluate(input, jsonata.Jsonata.CURRENT.jsonata.environment)
        except Exception as err:
            # error evaluating the expression passed to $eval
            # populateMessage(err)
            raise jexception.JException("D3121", -1)

        return result

    #  environment.bind("now", defineFunction(function(picture, timezone) {
    #      return datetime.fromMillis(timestamp.getTime(), picture, timezone)
    #  }, "<s?s?:s>"))
    @staticmethod
    def now(picture: Optional[str], timezone: Optional[str]) -> Optional[str]:
        from jsonata import jsonata
        t = jsonata.Jsonata.CURRENT.jsonata.timestamp
        return Functions.datetime_from_millis(t, picture, timezone)

    #  environment.bind("millis", defineFunction(function() {
    #      return timestamp.getTime()
    #  }, "<:n>"))
    @staticmethod
    def millis() -> int:
        from jsonata import jsonata
        t = jsonata.Jsonata.CURRENT.jsonata.timestamp
        return t
