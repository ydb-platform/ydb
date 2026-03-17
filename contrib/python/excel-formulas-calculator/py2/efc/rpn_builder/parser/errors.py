# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from efc.base.errors import BaseEFCException


class ParserError(BaseEFCException):
    def __init__(self, ws_name, formula):
        self.ws_name = ws_name
        self.formula = formula


class InconsistentParentheses(ParserError):
    code = 200
    msg = 'Formula on ws "{ws_name}" has inconsistent parentheses'


class SeparatorBlockError(ParserError):
    code = 201
    msg = 'Separator without brackets on ws "{ws_name}. Should be like "(op1, op2)"'
