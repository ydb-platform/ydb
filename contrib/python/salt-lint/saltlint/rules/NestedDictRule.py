# -*- coding: utf-8 -*-
# Copyright (c) 2020 Warpnet B.V.

import re
from saltlint.linter.rule import Rule
from saltlint.utils import get_rule_skips_from_text
from saltlint.utils import LANGUAGE_SLS


class NestedDictRule(Rule):
    id = "219"
    shortdesc = "Nested dictionaries (in context or defaults) should be over-indented"
    description = "Nested dictionaries (in context or defaults) should be over-indented"

    severity = "HIGH"
    languages = [LANGUAGE_SLS]
    tags = ["formatting"]
    version_added = "develop"

    regex = re.compile(
        r"^(\s+)-\s+(context|defaults):[^{[\n]*\n^\1\s{0,3}[^-{[\s]*:\s.+",
        re.MULTILINE,
    )

    def matchtext(self, file, text):
        results = []

        for match in re.finditer(self.regex, text):
            # Get the location of the regex match
            start = match.start()
            end = match.end()

            # Get the line number of the last character
            lines = text[:end].splitlines()
            line_no = len(lines)

            # Skip result if noqa for this rule ID is found in section
            section = text[start:end]
            if self.id in get_rule_skips_from_text(section):
                continue

            # Append the match to the results
            results.append((line_no, lines[-1], self.shortdesc))

        return results
