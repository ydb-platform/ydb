# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2021 Warpnet B.V.
# Modified work Copyright (c) 2021 Yury Bushmelev

import re
from saltlint.linter.rule import TypographicalErrorRule


class TypoOnchangesRule(TypographicalErrorRule):
    id = '216'
    shortdesc = '"onchange" looks like a typo. Did you mean "onchanges"?'
    description = '"onchange" looks like a typo. Did you mean "onchanges"?'
    version_added = 'v0.6.0'
    regex = re.compile(r"^\s+- (on_?change(|_in|_any)|on_changes(|_in|_any)):")
