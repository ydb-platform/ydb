# -*- coding: utf-8 -*-
# opyright (c) 2020 Warpnet B.V.

import os
from saltlint.linter.rule import Rule
from saltlint.utils import LANGUAGE_SLS


class SLSFileNameRule(Rule):
    id = '214'
    shortdesc = ('SLS file with a period in the name (besides the suffix period) can not be '
                 'referenced')
    description = ('SLS file with a period in the name (besides the suffix period) can not be '
                   'referenced')
    severity = 'HIGH'
    languages = [LANGUAGE_SLS]
    tags = ['formatting']
    version_added = 'v0.5.0'

    def matchtext(self, file, text):
        results = []
        path = file['path']
        basename = os.path.basename(path)
        if len(basename.split('.')) > 2:
            line_no = 1
            lines = text.splitlines()
            line = lines[0] if len(lines) > 0 else ''
            results.append((line_no, line, self.shortdesc))
        return results
