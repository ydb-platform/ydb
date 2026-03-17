# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020 Warpnet B.V.

import re
from saltlint.linter.rule import Rule
from saltlint.utils import LANGUAGE_SLS


class YamlHasOctalValueRule(Rule):
    id = '210'
    shortdesc = "Numbers that start with '0' should always be encapsulated in quotation marks"
    description = "Numbers that start with '0' should always be encapsulated in quotation marks"
    severity = 'HIGH'
    languages = [LANGUAGE_SLS]
    tags = ['formatting']
    version_added = 'v0.0.6'

    bracket_regex = re.compile(r"^[^:]+:\s{0,}0[0-9]{1,}\s{0,}((?={#)|(?=#)|(?=$))")

    def match(self, file, line):
        return self.bracket_regex.search(line)
