# -*- coding: utf-8 -*-
"""
Constants and configuration for Ukrainian language.
"""
from __future__ import absolute_import, unicode_literals
from pymorphy2 import units
from ._prefixes import KNOWN_PREFIXES

# paradigm prefixes used for dictionary compilation
PARADIGM_PREFIXES = ["", "най", "якнай", "щонай"]

# letters initials can start with
INITIAL_LETTERS = 'АБВГҐДЕЄЖЗІЇЙКЛМНОПРСТУФХЦЧШЩЮЯ'

# a list of particles which can be attached to a word using a hyphen
PARTICLES_AFTER_HYPHEN = ["-но", "-таки", "-бо", "-от"]

# "ґ" is sometimes written as "г", but not the other way around
CHAR_SUBSTITUTES = {'г': 'ґ'}

# default analyzer units
DEFAULT_UNITS = [
    [
        units.DictionaryAnalyzer(),
        units.AbbreviatedFirstNameAnalyzer(INITIAL_LETTERS),
        units.AbbreviatedPatronymicAnalyzer(INITIAL_LETTERS),

        # "I" can be a Roman number or an English word
        units.RomanNumberAnalyzer(),
        units.LatinAnalyzer()
    ],

    units.NumberAnalyzer(),
    units.PunctuationAnalyzer(),

    units.HyphenSeparatedParticleAnalyzer(PARTICLES_AFTER_HYPHEN),
    units.HyphenatedWordsAnalyzer(skip_prefixes=KNOWN_PREFIXES),
    units.KnownPrefixAnalyzer(known_prefixes=KNOWN_PREFIXES),
    [
        units.UnknownPrefixAnalyzer(),
        units.KnownSuffixAnalyzer()
    ],
    units.UnknAnalyzer(),
]
