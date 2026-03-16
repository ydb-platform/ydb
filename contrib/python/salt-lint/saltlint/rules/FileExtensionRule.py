# -*- coding: utf-8 -*-
# Copyright (c) 2016 Tsukinowa Inc. <info@tsukinowa.jp>
# Copyright (c) 2018 Ansible Project
# Modified work Copyright (c) 2020 Warpnet B.V.

import os
from saltlint.linter.rule import Rule


class FileExtensionRule(Rule):
    id = '205'
    shortdesc = 'Use ".sls" as a Salt State file extension'
    description = 'Salt State files should have the ".sls" extension'
    severity = 'MEDIUM'
    tags = ['formatting']
    version_added = 'v0.0.1'

    def matchtext(self, file, text):
        results = []
        path = file['path']
        ext = os.path.splitext(path)
        if ext[1] not in ['.sls']:
            line_no = 1
            lines = text.splitlines()
            line = lines[0] if len(lines) > 0 else ''
            results.append((line_no, line, self.shortdesc))
        return results
