from .abbreviations import (
    AbbreviatedFirstNameAnalyzer,
    AbbreviatedPatronymicAnalyzer
)
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
from .by_lookup import DictionaryAnalyzer
from .by_shape import (
    LatinAnalyzer,
    PunctuationAnalyzer,
    NumberAnalyzer,
    RomanNumberAnalyzer
)
from .unkn import UnknAnalyzer
