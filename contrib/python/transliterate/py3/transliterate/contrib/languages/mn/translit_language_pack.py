# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.mn import data

__title__ = 'transliterate.contrib.languages.mn.translit_language_pack'
__author__ = 'Enkhbold Bataa'
__copyright__ = '2016 Enkhbold Bataa'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('MongolianLanguagePack',)


class MongolianLanguagePack(TranslitLanguagePack):
    """Language pack for Mongolian language.

    See `https://en.wikipedia.org/wiki/Mongolian_Cyrillic_alphabet` for
    details.
    """
    language_code = "mn"
    language_name = "Mongolian"
    character_ranges = ((0x0400, 0x04FF), (0x0500, 0x052F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = False


registry.register(MongolianLanguagePack)
