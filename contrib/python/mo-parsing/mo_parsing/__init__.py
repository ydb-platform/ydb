# encoding: utf-8

# ORIGINALLY COPIED FROM pyparsing UNDER THE MIT LICENCE

# module pyparsing.py
#
# Copyright (c) 2003-2019  Paul T. McGuire
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
from mo_parsing import whitespaces
from mo_parsing.core import ParserElement, _PendingSkip, set_parser_names
from mo_parsing.enhancement import *
from mo_parsing.exceptions import ParseException, ParseException, ParseSyntaxException, RecursiveGrammarException
from mo_parsing.expressions import And, MatchAll, MatchFirst, Or, ParseExpression
from mo_parsing.whitespaces import Whitespace

NO_WHITESPACE = whitespaces.NO_WHITESPACE = Whitespace("").use()
STANDARD_WHITESPACE = whitespaces.STANDARD_WHITESPACE = Whitespace().use()

from mo_parsing.infix import LEFT_ASSOC, RIGHT_ASSOC, infix_notation, delimited_list, one_of
from mo_parsing.regex import Regex
from mo_parsing.tokens import *

__all__ = [
    "And",
    "AnyChar",
    "CaselessKeyword",
    "CaselessLiteral",
    "Char",
    "CharsNotIn",
    "CloseMatch",
    "Combine",
    "delimited_list",
    "delimited_list",
    "Dict",
    "Empty",
    "FollowedBy",
    "Forward",
    "Group",
    "infix_notation",
    "infix_notation",
    "Keyword",
    "LEFT_ASSOC",
    "LEFT_ASSOC",
    "LineEnd",
    "LineStart",
    "Literal",
    "LookAhead",
    "LookBehind",
    "Many",
    "MatchAll",
    "MatchFirst",
    "NO_WHITESPACE",
    "NoMatch",
    "NotAny",
    "one_of",
    "OneOrMore",
    "Optional",
    "Or",
    "ParseEnhancement",
    "ParseException",
    "ParseException",
    "ParseExpression",
    "ParserElement",
    "ParseResults",
    "ParseSyntaxException",
    "PrecededBy",
    "RecursiveGrammarException",
    "Regex",
    "RIGHT_ASSOC",
    "RIGHT_ASSOC",
    "set_parser_names",
    "SkipTo",
    "StringEnd",
    "StringStart",
    "Suppress",
    "Token",
    "TokenConverter",
    "White",
    "Whitespace",
    "Word",
    "WordEnd",
    "WordStart",
    "ZeroOrMore",
]
