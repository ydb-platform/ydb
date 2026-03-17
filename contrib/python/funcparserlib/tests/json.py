# -*- coding: utf-8 -*-

# Copyright © 2009/2021 Andrey Vlasovskikh
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
# OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""A JSON parser using funcparserlib.

The parser is based on [the JSON grammar][1].

  [1]: https://tools.ietf.org/html/rfc4627
"""

from __future__ import print_function, unicode_literals

import re
import sys
from pprint import pformat
from re import VERBOSE
from typing import (
    List,
    Sequence,
    Optional,
    Tuple,
    Any,
    Dict,
    Match,
    TypeVar,
    Callable,
    Text,
    Union,
)

import six

from funcparserlib.lexer import TokenSpec, make_tokenizer, Token, LexerError
from funcparserlib.parser import (
    maybe,
    many,
    finished,
    forward_decl,
    NoParseError,
    Parser,
    tok,
)

ENCODING = "UTF-8"
# noinspection SpellCheckingInspection
regexps = {
    "escaped": r"""
        \\                                  # Escape
          ((?P<standard>["\\/bfnrt])        # Standard escapes
        | (u(?P<unicode>[0-9A-Fa-f]{4})))   # uXXXX
        """,
    "unescaped": r"""
        [^"\\]                              # Unescaped: avoid ["\\]
        """,
}
re_esc = re.compile(regexps["escaped"], VERBOSE)
T = TypeVar("T")  # noqa
JsonValue = Union[None, bool, dict, list, int, float, str]
JsonMember = Tuple[str, JsonValue]


def tokenize(s):
    # type: (Text) -> List[Token]
    specs = [
        TokenSpec("space", r"[ \t\r\n]+"),
        TokenSpec("string", r'"(%(unescaped)s | %(escaped)s)*"' % regexps, VERBOSE),
        TokenSpec(
            "number",
            r"""
            -?                  # Minus
            (0|([1-9][0-9]*))   # Int
            (\.[0-9]+)?         # Frac
            ([Ee][+-]?[0-9]+)?   # Exp
            """,
            VERBOSE,
        ),
        TokenSpec("op", r"[{}\[\]\-,:]"),
        TokenSpec("name", r"[A-Za-z_][A-Za-z_0-9]*"),
    ]
    useless = ["space"]
    t = make_tokenizer(specs)
    return [x for x in t(s) if x.type not in useless]


def parse(tokens):
    # type: (Sequence[Token]) -> object

    def const(x):
        # type: (T) -> Callable[[Any], T]
        return lambda _: x

    def op(s):
        # type: (Text) -> Parser[Token, Text]
        return tok("op", s)

    def n(s):
        # type: (Text) -> Parser[Token, Text]
        return tok("name", s)

    def make_array(values):
        # type: (Optional[Tuple[JsonValue, List[JsonValue]]]) -> List[Any]
        if values is None:
            return []
        else:
            return [values[0]] + values[1]

    def make_object(values):
        # type: (Optional[Tuple[JsonMember, List[JsonMember]]]) -> Dict[str, Any]
        if values is None:
            return {}
        else:
            first, rest = values
            k, v = first
            d = {k: v}
            d.update(rest)
            return d

    def make_number(s):
        # type: (Text) -> float
        try:
            return int(s)
        except ValueError:
            return float(s)

    def unescape(s):
        # type: (Text) -> Text
        std = {
            '"': '"',
            "\\": "\\",
            "/": "/",
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
        }

        def sub(m):
            # type: (Match[Text]) -> Text
            if m.group("standard") is not None:
                return std[m.group("standard")]
            else:
                return six.unichr(int(m.group("unicode"), 16))

        return re_esc.sub(sub, s)

    def make_string(s):
        # type: (Text) -> Text
        return unescape(s[1:-1])

    def make_member(values):
        # type: (JsonMember) -> JsonMember
        k, v = values
        return k, v

    null = n("null") >> const(None)
    true = n("true") >> const(True)
    false = n("false") >> const(False)
    number = tok("number") >> make_number
    string = tok("string") >> make_string
    value = forward_decl().named("json_value")  # type: Parser[Token, JsonValue]
    member = string + -op(":") + value >> make_member
    json_object = (
        (-op("{") + maybe(member + many(-op(",") + member)) + -op("}")) >> make_object
    ).named("json_object")
    json_array = (
        (-op("[") + maybe(value + many(-op(",") + value)) + -op("]")) >> make_array
    ).named("json_array")
    value.define(null | true | false | json_object | json_array | number | string)
    json_text = value + -finished

    return json_text.parse(tokens)


def loads(s):
    # type: (Text) -> Any
    return parse(tokenize(s))


def main():
    # type: () -> None
    try:
        text = sys.stdin.read()
        tree = loads(text)
        print(pformat(tree))
    except (NoParseError, LexerError) as e:
        print("syntax error: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
