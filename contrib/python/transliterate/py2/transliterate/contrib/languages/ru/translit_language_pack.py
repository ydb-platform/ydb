# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.ru import data

__title__ = 'transliterate.contrib.languages.ru.translit_language_pack'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2015 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('RussianLanguagePack',)


class RussianLanguagePack(TranslitLanguagePack):
    """Language pack for Russian language.

    See `http://en.wikipedia.org/wiki/Russian_alphabet` for details.
    """
    language_code = "ru"
    language_name = "Russian"
    character_ranges = ((0x0400, 0x04FF), (0x0500, 0x052F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = True


registry.register(RussianLanguagePack)
