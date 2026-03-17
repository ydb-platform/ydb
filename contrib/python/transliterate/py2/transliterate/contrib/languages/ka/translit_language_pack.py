# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry
from transliterate.contrib.languages.ka import data

__title__ = 'transliterate.contrib.languages.ka.translit_language_pack'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2015 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('GeorgianLanguagePack',)


class GeorgianLanguagePack(TranslitLanguagePack):
    """Language pack for Georgian language.

    See `http://en.wikipedia.org/wiki/Georgian_alphabet for details.
    """
    language_code = "ka"
    language_name = "Georgian"
    character_ranges = ((0x10A0, 0x10C5), (0x10D0, 0x10FC), (0x2D00, 0x2D25))
    mapping = data.mapping
    pre_processor_mapping = data.pre_processor_mapping
    detectable = True

    def translit(self,
                 value,
                 reversed=False,
                 strict=False,
                 fail_silently=True):

        # Georgian language knows no capitals. Therefore, we convert
        # everything to lowercase.
        value = value.lower()

        # Continue the standard way
        return super(GeorgianLanguagePack, self).translit(
            value=value,
            reversed=reversed,
            strict=strict,
            fail_silently=fail_silently
        )


registry.register(GeorgianLanguagePack)
