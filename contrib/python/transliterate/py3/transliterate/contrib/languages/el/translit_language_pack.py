# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.el import data

__title__ = 'transliterate.contrib.languages.el.translit_language_pack'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('GreekLanguagePack',)


class GreekLanguagePack(TranslitLanguagePack):
    """Language pack for Greek language.

    See `http://en.wikipedia.org/wiki/Greek_alphabet` and
    `https://en.wikipedia.org/wiki/Romanization_of_Greek#Modern_Greek`
    for details.
    """
    language_code = "el"
    language_name = "Greek"
    character_ranges = ((0x0370, 0x03FF), (0x1F00, 0x1FFF))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = True


registry.register(GreekLanguagePack)
