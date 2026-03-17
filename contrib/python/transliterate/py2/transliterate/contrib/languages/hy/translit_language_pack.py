# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.hy import data

__title__ = 'transliterate.contrib.languages.hy.translit_language_pack'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('ArmenianLanguagePack',)


class ArmenianLanguagePack(TranslitLanguagePack):
    """Language pack for Armenian language.

    See `https://en.wikipedia.org/wiki/Armenian_alphabet` for details.
    """
    language_code = "hy"
    language_name = "Armenian"
    character_ranges = ((0x0530, 0x058F), (0xFB10, 0xFB1F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    reversed_specific_pre_processor_mapping = \
        data.reversed_specific_pre_processor_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = True


registry.register(ArmenianLanguagePack)
