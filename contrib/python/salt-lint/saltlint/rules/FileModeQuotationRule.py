# -*- coding: utf-8 -*-
# Copyright (c) 2016 Will Thames and contributors
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020 Warpnet B.V.

import re
from saltlint.linter.rule import Rule
from saltlint.utils import LANGUAGE_SLS


class FileModeQuotationRule(Rule):
    id = '207'
    shortdesc = 'File modes should always be encapsulated in quotation marks'
    description = 'File modes should always be encapsulated in quotation marks'
    severity = 'HIGH'
    languages = [LANGUAGE_SLS]
    tags = ['formatting']
    version_added = 'v0.0.3'

    regex = re.compile(
        r"""^\s+
        -\                   # whitespace escaped due to re.VERBOSE
        (?:dir_|file_)?mode  # file_mode, dir_mode or mode
        :\                   # whitespace escaped duo to re.VERBOSE
        (?:
            (\d{3,4})        # mode without quotation
            (?:
                ['"]         # followed by a quotation character
              |
                \s           # followed by a whitespace
              |
                $            # followed by EOL
            )
          |
            (['"]\d{3,4}     # mode prefixed with quotation
              (?:[^\d'"]|$)  # not followed by digit, quoation or EOL
            )
        )
        """,
        re.VERBOSE
    )

    def match(self, file, line):
        # TODO: when salt-lint becomes state aware it should ignore the mode
        # argument in the network.managed state.
        return self.regex.search(line)
