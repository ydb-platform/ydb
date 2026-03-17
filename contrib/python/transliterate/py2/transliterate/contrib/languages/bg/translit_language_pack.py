# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.bg import data

__title__ = 'transliterate.contrib.languages.bg.translit_language_pack'
__author__ = 'Petar Chakarov'
__copyright__ = '2014-2015 Petar Chakarov'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('BulgarianLanguagePack',)


class BulgarianLanguagePack(TranslitLanguagePack):
    """Language pack for Bulgarian language.

    See http://en.wikipedia.org/wiki/Romanization_of_Bulgarian for details.
    """
    language_code = "bg"
    language_name = "Bulgarian"
    character_ranges = ((0x0400, 0x04FF), (0x0500, 0x052F))
    mapping = data.mapping
    reversed_specific_mapping = data.reversed_specific_mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = False


registry.register(BulgarianLanguagePack)
