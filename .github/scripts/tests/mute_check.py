#!/usr/bin/env python3
import re
from mute_utils import pattern_to_re


class YaMuteCheck:
    def __init__(self):
        self.regexps = []

    def load(self, fn):
        with open(fn, "r") as fp:
            for line in fp:
                line = line.strip()
                try:
                    testsuite, testcase = line.split(" ", maxsplit=1)
                except ValueError:
                    print(f"SKIP INVALID MUTE CONFIG LINE: {line!r}", file=__import__('sys').stderr)
                    continue
                self.populate(testsuite, testcase)

    def populate(self, testsuite, testcase):
        check = []

        for p in (pattern_to_re(testsuite), pattern_to_re(testcase)):
            try:
                check.append(re.compile(p))
            except re.error:
                print(f"Unable to compile regex {p!r}", file=__import__('sys').stderr)
                return

        self.regexps.append(tuple(check))

    def __call__(self, suite_name, test_name):
        for ps, pt in self.regexps:
            if ps.match(suite_name) and pt.match(test_name):
                return True
        return False



