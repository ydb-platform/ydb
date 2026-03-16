# -*- coding: utf-8 -*-

__title__ = 'transliterate.contrib.languages.hy.data.default'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'mapping',
    'reversed_specific_mapping',
    'reversed_specific_pre_processor_mapping',
    'pre_processor_mapping',
)

mapping = (
    u"abgdezilxkhmjnpsvtrcq&ofABGDEZILXKHMJNPSVTRCQOF",
    u"աբգդեզիլխկհմյնպսվտրցքևօֆԱԲԳԴԵԶԻԼԽԿՀՄՅՆՊՍՎՏՐՑՔՕՖ",
)
reversed_specific_mapping = (
    u"ռՌ",
    u"rR"
)
reversed_specific_pre_processor_mapping = {
    u"ու": u"u",
    u"Ու": u"U"
}
pre_processor_mapping = {
    # lowercase
    u"e'": u"է",
    u"y": u"ը",
    u"th": u"թ",
    u"jh": u"ժ",
    u"ts": u"ծ",
    u"dz": u"ձ",
    u"gh": u"ղ",
    u"tch": u"ճ",
    u"sh": u"շ",
    u"vo": u"ո",
    u"ch": u"չ",
    u"dj": u"ջ",
    u"ph": u"փ",
    u"u": u"ու",

    # uppercase
    u"E'": u"Է",
    u"Y": u"Ը",
    u"Th": u"Թ",
    u"Jh": u"Ժ",
    u"Ts": u"Ծ",
    u"Dz": u"Ձ",
    u"Gh": u"Ղ",
    u"Tch": u"Ճ",
    u"Sh": u"Շ",
    u"Vo": u"Ո",
    u"Ch": u"Չ",
    u"Dj": u"Ջ",
    u"Ph": u"Փ",
    u"U": u"Ու"
}
