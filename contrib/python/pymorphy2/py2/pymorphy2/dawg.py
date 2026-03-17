# -*- coding: utf-8 -*-
from __future__ import absolute_import, division

try:
    from dawg import DAWG, RecordDAWG, IntCompletionDAWG
    EXTENSION_AVAILABLE = True

except ImportError:
    from dawg_python import DAWG, RecordDAWG, IntCompletionDAWG
    EXTENSION_AVAILABLE = False


def assert_can_create():
    if not EXTENSION_AVAILABLE:
        msg = ("Creating of DAWGs with DAWG-Python is "
               "not supported; install 'dawg' package.")
        raise NotImplementedError(msg)


class WordsDawg(RecordDAWG):
    """
    DAWG for storing words.
    """

    # We are storing 2 unsigned short ints as values:
    # the paradigm ID and the form index (inside paradigm).
    # Byte order is big-endian (this makes word forms properly sorted).
    DATA_FORMAT = str(">HH")

    def __init__(self, data=None):
        if data is None:
            super(WordsDawg, self).__init__(self.DATA_FORMAT)
        else:
            assert_can_create()
            super(WordsDawg, self).__init__(self.DATA_FORMAT, data)


class PredictionSuffixesDAWG(WordsDawg):
    """
    DAWG for storing prediction data.
    """

    # We are storing 3 unsigned short ints as values:
    # count, the paradigm ID and the form index (inside paradigm).
    # Byte order is big-endian (this makes word forms properly sorted).
    DATA_FORMAT = str(">HHH")


class ConditionalProbDistDAWG(IntCompletionDAWG):

    MULTIPLIER = 1000000

    def __init__(self, data=None):
        if data is None:
            super(ConditionalProbDistDAWG, self).__init__()
        else:
            assert_can_create()
            dawg_data = (
                ("%s:%s" % (word, tag), int(prob*self.MULTIPLIER))
                for (word, tag), prob in data
            )
            super(ConditionalProbDistDAWG, self).__init__(dawg_data)

    def prob(self, word, tag):
        dawg_key = "%s:%s" % (word, tag)
        return self.get(dawg_key, 0) / self.MULTIPLIER


class DawgPrefixMatcher(DAWG):
    def is_prefixed(self, word):
        return bool(self.prefixes(word))


class PythonPrefixMatcher(object):
    def __init__(self, prefixes):
        self._prefixes = tuple(prefixes)

    def prefixes(self, word):
        if not self.is_prefixed(word):  # fail-fast path
            return []
        return [pref for pref in self._prefixes if word.startswith(pref)]

    def is_prefixed(self, word):
        return word.startswith(self._prefixes)


PrefixMatcher = DawgPrefixMatcher if EXTENSION_AVAILABLE else PythonPrefixMatcher
