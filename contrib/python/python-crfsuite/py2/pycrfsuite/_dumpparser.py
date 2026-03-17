# -*- coding: utf-8 -*-
from __future__ import absolute_import
import re


class ParsedDump(object):
    """
    CRFsuite model parameters. Objects of this type are returned by
    :meth:`pycrfsuite.Tagger.info()` method.

    Attributes
    ----------

    transitions : dict
        ``{(from_label, to_label): weight}`` dict with learned transition weights

    state_features : dict
        ``{(attribute, label): weight}`` dict with learned ``(attribute, label)`` weights

    header : dict
        Metadata from the file header

    labels : dict
        ``{name: internal_id}`` dict with model labels

    attributes : dict
        ``{name: internal_id}`` dict with known attributes

    """
    def __init__(self):
        self.header = {}
        self.labels = {}
        self.attributes = {}
        self.transitions = {}
        self.state_features = {}


class CRFsuiteDumpParser(object):
    """
    A hack: parser for `crfsuite dump` results.

    Obtaining coefficients "the proper way" is quite hard otherwise
    because in CRFsuite they are hidden in private structures.
    """

    def __init__(self):
        self.state = None
        self.result = ParsedDump()

    def feed(self, line):
        # Strip initial ws and line terminator, but allow for ws at the end of feature names.
        line = line.lstrip().rstrip('\r\n')
        if not line:
            return

        m = re.match(r"(FILEHEADER|LABELS|ATTRIBUTES|TRANSITIONS|STATE_FEATURES) = {", line)
        if m:
            self.state = m.group(1)
        elif line == '}':
            self.state = None
        else:
            getattr(self, 'parse_%s' % self.state)(line)

    def parse_FILEHEADER(self, line):
        m = re.match(r"(\w+): (.*)", line)
        self.result.header[m.group(1)] = m.group(2)

    def parse_LABELS(self, line):
        m = re.match(r"(\d+): (.*)", line)
        self.result.labels[m.group(2)] = m.group(1)

    def parse_ATTRIBUTES(self, line):
        m = re.match(r"(\d+): (.*)", line)
        self.result.attributes[m.group(2)] = m.group(1)

    def parse_TRANSITIONS(self, line):
        m = re.match(r"\(\d+\) (.+) --> (.+): ([+-]?\d+\.\d+)", line)
        from_, to_ = m.group(1), m.group(2)
        assert from_ in self.result.labels
        assert to_ in self.result.labels
        self.result.transitions[(from_, to_)] = float(m.group(3))

    def parse_STATE_FEATURES(self, line):
        m = re.match(r"\(\d+\) (.+) --> (.+): ([+-]?\d+\.\d+)", line)
        attr, label = m.group(1), m.group(2)
        assert attr in self.result.attributes
        assert label in self.result.labels
        self.result.state_features[(attr, label)] = float(m.group(3))
