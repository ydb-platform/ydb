# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2021 Warpnet B.V.
# Modified work Copyright (c) 2021 Yury Bushmelev

import re
from saltlint.linter.rule import TypographicalErrorRule


class TypoRequireRule(TypographicalErrorRule):
    id = '217'
    shortdesc = '"requires" looks like a typo. Did you mean "require"?'
    description = '"requires" looks like a typo. Did you mean "require"?'
    version_added = 'v0.6.0'
    regex = re.compile(r"^\s+- requires(|_in|_any):")
