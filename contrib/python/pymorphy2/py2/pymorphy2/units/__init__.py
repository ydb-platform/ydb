# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .by_lookup import DictionaryAnalyzer
from .by_analogy import (
    KnownPrefixAnalyzer,
    KnownSuffixAnalyzer,
    UnknownPrefixAnalyzer
)
from .by_hyphen import (
    HyphenatedWordsAnalyzer,
    HyphenAdverbAnalyzer,
    HyphenSeparatedParticleAnalyzer
)
from .by_shape import (
    LatinAnalyzer,
    PunctuationAnalyzer,
    NumberAnalyzer,
    RomanNumberAnalyzer
)
from .abbreviations import (
    AbbreviatedFirstNameAnalyzer,
    AbbreviatedPatronymicAnalyzer
)
from .unkn import UnknAnalyzer
