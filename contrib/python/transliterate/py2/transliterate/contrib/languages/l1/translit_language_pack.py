# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.l1 import data

__title__ = 'transliterate.contrib.languages.l1.translit_language_pack'
__author__ = 'Marco Pattaro'
__copyright__ = '2016-2017 Marco Pattaro'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('Latin1SupplementLanguagePack',)


class Latin1SupplementLanguagePack(TranslitLanguagePack):
    """Language pack for Latin1 Supplement.

    Though not exactly a language, it's a set of commonly found unicode
    characters. See
    `http://en.wikipedia.org/wiki/Latin-1_Supplement_%28Unicode_block%29` for
    details.
    """
    language_code = "l1"
    language_name = "Latin1 Supplement"
    character_ranges = ((0x00C0, 0x00D6), (0x00D8, 0x00F6), (0x00F8, 0x00FF))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    reversed_specific_pre_processor_mapping = \
        data.reversed_specific_pre_processor_mapping
    detectable = True


registry.register(Latin1SupplementLanguagePack)
