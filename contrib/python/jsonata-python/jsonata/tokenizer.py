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
#
# Derived from the following code:
#
#   Project name: jsonata-java
#   Copyright Dashjoin GmbH. https://dashjoin.com
#   Licensed under the Apache License, Version 2.0 (the "License")
#
#   Project name: JSONata
# © Copyright IBM Corp. 2016, 2018 All Rights Reserved
#   This project is licensed under the MIT License, see LICENSE
#

import math
import re
from dataclasses import dataclass
from typing import Any, Optional

from jsonata import jexception, utils


class Tokenizer:
    operators = {
        '.': 75,
        '[': 80,
        ']': 0,
        '{': 70,
        '}': 0,
        '(': 80,
        ')': 0,
        ',': 0,
        '@': 80,
        '#': 80,
        ';': 80,
        ':': 80,
        '?': 20,
        '+': 50,
        '-': 50,
        '*': 60,
        '/': 60,
        '%': 60,
        '|': 20,
        '=': 40,
        '<': 40,
        '>': 40,
        '^': 40,
        '**': 60,
        '..': 20,
        ':=': 10,
        '!=': 40,
        '<=': 40,
        '>=': 40,
        '~>': 40,
        '?:': 40,
        '??': 40,
        'and': 30,
        'or': 25,
        'in': 40,
        '&': 50,
        '!': 0,  # not an operator, but needed as a stop character for name tokens
        '~': 0  # not an operator, but needed as a stop character for name tokens
    }

    escapes = {
        '"': '"',
        '\\': '\\',
        '/': '/',
        'b': '\b',
        'f': '\f',
        'n': '\n',
        'r': '\r',
        't': '\t'
    }

    # Tokenizer (lexer) - invoked by the parser to return one token at a time

    position: int
    depth: int
    path: str
    length: int

    def __init__(self, path):
        self.position = 0
        self.depth = 0

        self.path = path
        self.length = len(path)

    @dataclass
    class Token:
        type: Optional[str]
        value: Optional[Any]
        position: int
        id: Optional[Any] = None

    def create(self, type: Optional[str], value: Optional[Any]) -> Token:
        return Tokenizer.Token(type, value, self.position)

    def is_closing_slash(self, position: int) -> bool:
        if self.path[position] == '/' and self.depth == 0:
            backslash_count = 0
            while self.path[position - (backslash_count + 1)] == '\\':
                backslash_count += 1
            if int(math.fmod(backslash_count, 2)) == 0:
                return True
        return False

    def scan_regex(self) -> re.Pattern:
        # the prefix '/' will have been previously scanned. Find the end of the regex.
        # search for closing '/' ignoring any that are escaped, or within brackets
        start = self.position
        # int depth = 0
        pattern = None
        flags = None

        while self.position < self.length:
            current_char = self.path[self.position]
            if self.is_closing_slash(self.position):
                # end of regex found
                pattern = self.path[start:self.position]
                if pattern == "":
                    raise jexception.JException("S0301", self.position)
                self.position += 1
                current_char = self.path[self.position]
                # flags
                start = self.position
                while current_char == 'i' or current_char == 'm':
                    self.position += 1
                    if self.position < self.length:
                        current_char = self.path[self.position]
                    else:
                        current_char = None
                flags = self.path[start:self.position] + 'g'

                # Convert flags to Java Pattern flags
                _flags = 0
                if "i" in flags:
                    _flags |= re.I
                if "m" in flags:
                    _flags |= re.M
                return re.compile(pattern, _flags)  # Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
            if (current_char == '(' or current_char == '[' or current_char == '{') and self.path[self.position - 1] != '\\':
                self.depth += 1
            if (current_char == ')' or current_char == ']' or current_char == '}') and self.path[self.position - 1] != '\\':
                self.depth -= 1
            self.position += 1
        raise jexception.JException("S0302", self.position)

    def next(self, prefix: bool) -> Optional[Token]:
        if self.position >= self.length:
            return None
        current_char = self.path[self.position]
        # skip whitespace
        while self.position < self.length and " \t\n\r".find(current_char) > -1:
            self.position += 1
            if self.position >= self.length:
                return None  # Uli: JS relies on charAt returns null
            current_char = self.path[self.position]
        # skip comments
        if current_char == '/' and self.path[self.position + 1] == '*':
            comment_start = self.position
            self.position += 2
            current_char = self.path[self.position]
            while not (current_char == '*' and self.path[self.position + 1] == '/'):
                self.position += 1
                current_char = self.path[self.position]
                if self.position >= self.length:
                    # no closing tag
                    raise jexception.JException("S0106", comment_start)
            self.position += 2
            current_char = self.path[self.position]
            return self.next(prefix)  # need this to swallow any following whitespace
        # test for regex
        if not prefix and current_char == '/':
            self.position += 1
            return self.create("regex", self.scan_regex())
        # handle double-char operators
        have_more = self.position < len(self.path) - 1  # Java: position+1 is valid
        if current_char == '.' and have_more and self.path[self.position + 1] == '.':
            # double-dot .. range operator
            self.position += 2
            return self.create("operator", "..")
        if current_char == ':' and have_more and self.path[self.position + 1] == '=':
            # := assignment
            self.position += 2
            return self.create("operator", ":=")
        if current_char == '!' and have_more and self.path[self.position + 1] == '=':
            # !=
            self.position += 2
            return self.create("operator", "!=")
        if current_char == '>' and have_more and self.path[self.position + 1] == '=':
            # >=
            self.position += 2
            return self.create("operator", ">=")
        if current_char == '<' and have_more and self.path[self.position + 1] == '=':
            # <=
            self.position += 2
            return self.create("operator", "<=")
        if current_char == '*' and have_more and self.path[self.position + 1] == '*':
            # **  descendant wildcard
            self.position += 2
            return self.create("operator", "**")
        if current_char == '~' and have_more and self.path[self.position + 1] == '>':
            # ~>  chain function
            self.position += 2
            return self.create("operator", "~>")
        if current_char == '?' and have_more and self.path[self.position + 1] == ':':
            # ?: default / elvis operator
            self.position += 2
            return self.create("operator", "?:")
        if current_char == '?' and have_more and self.path[self.position + 1] == '?':
            # ?? coalescing operator
            self.position += 2
            return self.create("operator", "??")
        # test for single char operators
        if Tokenizer.operators.get(str(current_char)) is not None:
            self.position += 1
            return self.create("operator", current_char)
        # test for string literals
        if current_char == '"' or current_char == '\'':
            quote_type = current_char
            # double quoted string literal - find end of string
            self.position += 1
            qstr = ""
            while self.position < self.length:
                current_char = self.path[self.position]
                if current_char == '\\':
                    self.position += 1
                    current_char = self.path[self.position]
                    if Tokenizer.escapes.get(str(current_char)) is not None:
                        qstr += Tokenizer.escapes[str(current_char)]
                    elif current_char == 'u':
                        #  u should be followed by 4 hex digits
                        octets = self.path[self.position + 1:(self.position + 1) + 4]
                        if re.match("^[0-9a-fA-F]+$", octets):
                            codepoint = int(octets, 16)
                            qstr += chr(codepoint)
                            self.position += 4
                        else:
                            raise jexception.JException("S0104", self.position)
                    else:
                        # illegal escape sequence
                        raise jexception.JException("S0301", self.position, current_char)

                elif current_char == quote_type:
                    self.position += 1
                    return self.create("string", qstr)
                else:
                    qstr += current_char
                self.position += 1
            raise jexception.JException("S0101", self.position)
        # test for numbers
        numregex = re.compile("^-?(0|([1-9][0-9]*))(\\.[0-9]+)?([Ee][-+]?[0-9]+)?")
        match_ = numregex.search(self.path[self.position:])
        if match_ is not None:
            num = float(match_.group(0))
            if not math.isnan(num) and math.isfinite(num):
                self.position += len(match_.group(0))
                # If the number is integral, use long as type
                return self.create("number", utils.Utils.convert_number(num))
            else:
                raise jexception.JException("S0102", self.position)  # , match.group[0]);

        # test for quoted names (backticks)
        name = None
        if current_char == '`':
            # scan for closing quote
            self.position += 1
            end = self.path.find('`', self.position)
            if end != -1:
                name = self.path[self.position:end]
                self.position = end + 1
                return self.create("name", name)
            self.position = self.length
            raise jexception.JException("S0105", self.position)
        # test for names
        i = self.position
        while True:
            # if (i>=length) return null; // Uli: JS relies on charAt returns null

            ch = self.path[i] if i < self.length else chr(0)
            if i == self.length or " \t\n\r".find(ch) > -1 or str(ch) in Tokenizer.operators:
                if self.path[self.position] == '$':
                    # variable reference
                    name = self.path[self.position + 1:i]
                    self.position = i
                    return self.create("variable", name)
                else:
                    name = self.path[self.position:i]
                    self.position = i
                    if name == "or" or name == "in" or name == "and":
                        return self.create("operator", name)
                    elif name == "true":
                        return self.create("value", True)
                    elif name == "false":
                        return self.create("value", False)
                    elif name == "null":
                        return self.create("value", None)
                    else:
                        if self.position == self.length and name == "":
                            # whitespace at end of input
                            return None
                        return self.create("name", name)
            else:
                i += 1
