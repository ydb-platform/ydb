# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

import re
from saltlint.linter.rule import DeprecationRule
from saltlint.utils import LANGUAGE_SLS


class CmdWaitRecommendRule(DeprecationRule):
    id = '213'
    shortdesc = 'SaltStack recommends using cmd.run together with onchanges, rather than cmd.wait'
    description = 'SaltStack recommends using cmd.run together with onchanges, rather than cmd.wait'

    severity = 'LOW'
    languages = [LANGUAGE_SLS]
    tags = ['formatting']
    version_added = 'v0.5.0'

    regex = re.compile(r"^\s{2}cmd\.wait:(\s+)?$")

    def match(self, file, line):
        return self.regex.search(line)
