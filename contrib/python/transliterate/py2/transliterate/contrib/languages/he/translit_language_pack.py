# -*- coding: utf-8 -*-

from transliterate.base import TranslitLanguagePack, registry

__title__ = 'transliterate.contrib.languages.he.translit_language_pack'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('HebrewLanguagePack',)


class HebrewLanguagePack(TranslitLanguagePack):
    """Language pack for Hebrew language.

    See http://en.wikipedia.org/wiki/Hebrew_alphabet for details. See the
    http://en.wikipedia.org/wiki/Romanization_of_Hebrew#When_to_transliterate
    for transliteration details. Note, that this language pack implements the
    new standards (2006) of Hebrew Academy.

    Confirmed
        a אּ
        v ב
        b בּּּ
        ּg ג
        gg ג
        ּd ד
        dd דּ
        h ה
        h הּ
        v ו
        vv וּ
        z ז
        zz זּ
    """
    language_code = "he"
    language_name = "Hebrew"
    character_ranges = ((0x0530, 0x058F), (0xFB10, 0xFB1F))
    mapping = (
        u"abgdvzhilmnsfckrt",
        # trkcfsnmlihzvdgbа
        u"אבגדוזחילמנספצקרת",
    )
    reversed_specific_mapping = (
        u"פ",
        # p
        u"p"
    )
    pre_processor_mapping = {
        # lowercase
        u"ha'": u"ה",
        u"tt": u"ט",
        u"ka": u"כ",
        u"aa": u"ע",
        u"sh": u"ש",
        u"fs": u"ף",
        u"cs": u"ץ",
        u"ms": u"ם",
        u"ns": u"ן",
        u"ks": u"ך",
    }
    detectable = True


# registry.register(HebrewLanguagePack)
