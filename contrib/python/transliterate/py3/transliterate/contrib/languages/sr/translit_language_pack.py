# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.sr import data

__title__ = 'transliterate.contrib.languages.sr.translit_language_pack'
__author__ = 'Saša Kelečević'
__copyright__ = '2017 Saša Kelečević'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('SerbianLanguagePack',)


class SerbianLanguagePack(TranslitLanguagePack):
    """Language pack for Serbian language.

    See https://en.wikipedia.org/wiki/Romanization_of_Serbian for details.
    """
    language_code = "sr"
    language_name = "Serbian"
    character_ranges = ((0x0408, 0x04F0), (0x0000, 0x017F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = False


registry.register(SerbianLanguagePack)
