# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

import re
from saltlint.linter.rule import JinjaRule


class JinjaStatementHasSpacesRule(JinjaRule):
    id = '202'
    shortdesc = "Jinja statement should have spaces before and after: '{% statement %}'"
    description = "Jinja statement should have spaces before and after: '{% statement %}'"
    severity = 'LOW'
    version_added = 'v0.0.2'

    bracket_regex = re.compile(r"{%[^ \-\+]|{%[\-\+][^ ]|[^ \-\+]%}|[^ ][\-\+]%}")

    def match(self, file, line):
        return self.bracket_regex.search(line)
