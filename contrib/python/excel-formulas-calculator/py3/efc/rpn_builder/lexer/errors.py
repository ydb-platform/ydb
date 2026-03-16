# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from efc.base.errors import BaseEFCException


class LexerError(BaseEFCException):
    pass


class CheckSumError(LexerError):
    code = 100
    msg = 'Some symbols from line are lost. Src line: {src_line}. Parsed line: {parsed_line}.'

    def __init__(self, src_line, parsed_line):
        self.src_line = src_line
        self.parsed_line = parsed_line
