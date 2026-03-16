# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

import re
from saltlint.linter.rule import JinjaRule


class JinjaPillarGrainsGetFormatRule(JinjaRule):
    id = '211'
    shortdesc = 'pillar.get or grains.get should be formatted differently'
    description = "pillar.get and grains.get should always be formatted " \
                  "like salt['pillar.get']('item'), grains['item1'] or " \
                  " pillar.get('item')"
    severity = 'HIGH'
    version_added = 'v0.0.10'

    bracket_regex = re.compile(r"{{( |\-|\+)?.(pillar|grains).get\[.+}}")

    def match(self, file, line):
        return self.bracket_regex.search(line)
