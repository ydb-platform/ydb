# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

import re
from saltlint.linter.rule import JinjaRule


class JinjaCommentHasSpacesRule(JinjaRule):
    id = '209'
    shortdesc = "Jinja comment should have spaces before and after: '{# comment #}'"
    description = "Jinja comment should have spaces before and after: '{# comment #}'"
    severity = 'LOW'
    version_added = 'v0.0.5'

    bracket_regex = re.compile(r"{#[^ \-\+]|{#[\-\+][^ ]|[^ \-\+]#}|[^ ][\-\+]#}")

    def match(self, file, line):
        return self.bracket_regex.search(line)
