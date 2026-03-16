# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import re
from collections import OrderedDict

from efc.rpn_builder.lexer import tokens
from efc.rpn_builder.lexer.errors import CheckSumError
from efc.utils import Array

__all__ = ('Lexer',)

TOKENS_PRIORITY = (
    tokens.FloatToken,
    tokens.IntToken,
    tokens.BoolToken,
    tokens.StringToken,
    tokens.FunctionToken,
    tokens.CellsRangeToken,
    tokens.SingleCellToken,
    tokens.NamedRangeToken,
    tokens.AddToken,
    tokens.SubtractToken,
    tokens.DivideToken,
    tokens.MultiplyToken,
    tokens.ConcatToken,
    tokens.ExponentToken,
    tokens.CompareNotEqToken,
    tokens.CompareGTEToken,
    tokens.CompareLTEToken,
    tokens.CompareGTToken,
    tokens.CompareLTToken,
    tokens.CompareEqToken,
    tokens.LeftBracketToken,
    tokens.RightBracketToken,
    tokens.SpaceToken,
    tokens.Separator,
)


class TokensLine(Array):
    def __init__(self, line):
        self.src_line = line
        super(TokensLine, self).__init__()


class Lexer(object):
    def __init__(self):
        self.lexer_tokens = OrderedDict()
        self.regexp = None

        self.prepare_regexp()

    def prepare_regexp(self):
        regexp_list = []
        for c in TOKENS_PRIORITY:
            self.lexer_tokens[c.__name__] = c
            regexp_list.append(c.get_group_pattern())
        self.regexp = r'|'.join(regexp_list)

    def parse(self, line):
        lexer_tokens = self.lexer_tokens
        parsed_line = []

        tokens_line = TokensLine(line)
        for match in re.finditer(self.regexp, line, flags=re.UNICODE):
            token = lexer_tokens[match.lastgroup](match)
            if not isinstance(token, tokens.SpaceToken):
                tokens_line.append(token)
            parsed_line.append(token.src_value)

        parsed_line = ''.join(parsed_line)
        if parsed_line != line:
            raise CheckSumError(line, parsed_line)
        return tokens_line
