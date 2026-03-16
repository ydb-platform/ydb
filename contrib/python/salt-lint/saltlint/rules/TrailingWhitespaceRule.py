# -*- coding: utf-8 -*-
# Copyright (c) 2013-2014 Will Thames <will@thames.id.au>
# Modified work Copyright (c) 2020 Warpnet B.V.

from saltlint.linter.rule import Rule


class TrailingWhitespaceRule(Rule):
    id = '201'
    shortdesc = 'Trailing whitespace'
    description = 'There should not be any trailing whitespace'
    severity = 'INFO'
    tags = ['formatting']
    version_added = 'v0.0.1'

    def match(self, file, line):
        line = line.replace("\r", "")
        return line.rstrip() != line
