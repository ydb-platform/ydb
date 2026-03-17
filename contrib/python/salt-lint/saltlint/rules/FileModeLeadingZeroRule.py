# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020-2021 Warpnet B.V.

import re
from saltlint.linter.rule import Rule
from saltlint.utils import LANGUAGE_SLS


class FileModeLeadingZeroRule(Rule):
    id = '208'
    shortdesc = 'File modes should always contain a leading zero'
    description = 'File modes should always contain a leading zero'
    severity = 'LOW'
    languages = [LANGUAGE_SLS]
    tags = ['formatting']
    version_added = 'v0.0.3'

    regex = re.compile(
        r"""^\s+             # line starting with whitespace
        -\                   # whitespace escaped due to re.VERBOSE
        (?:dir_|file_)?mode  # file_mode, dir_mode or mode
        :\                   # whitespace escaped due to re.VERBOSE
        ['"]?                # optional quotation character
        ([0-9]{3})           # three number digit
        (?:['"\s]|$)         # followed by whitespace, quoation or EOL
        """,
        re.VERBOSE
    )

    def match(self, file, line):
        # TODO: when salt-lint becomes state aware it should ignore the mode
        # argument in the network.managed state.
        return self.regex.search(line)
