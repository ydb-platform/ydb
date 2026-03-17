# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.mk import data

__title__ = 'transliterate.contrib.languages.mk.translit_language_pack'
__author__ = 'Igor Stamatovski'
__copyright__ = '2016 Igor Stamatovski'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('MacedonianLanguagePack',)


class MacedonianLanguagePack(TranslitLanguagePack):
    """Language pack for Macedonian language.

    See http://en.wikipedia.org/wiki/Romanization_of_Macedonian for details.
    """
    language_code = "mk"
    language_name = "Macedonian"
    character_ranges = ((0x0400, 0x04FF), (0x0500, 0x052F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = False


registry.register(MacedonianLanguagePack)
