# -*- coding: utf-8 -*-

mapping = (
    u"abvgdjziklmnoprstuufhewABVGDJZIKLMNOPRSTUUFHEW",
    u"абвгджзиклмнопрстуүфхэвАБВГДЖЗИКЛМНОПРСТУҮФХЭВ",
)
reversed_specific_mapping = (
    u"ъьЪЬйЙөӨуУүҮ",
    u"iiIIiIoOuUuU"
)
pre_processor_mapping = {
    u"ii": u"ы",
    u"II": u"Ы",
    u"ye": u"е",
    u"YE": u"Е",
    u"yo": u"ё",
    u"Yo": u"Ё",
    u"YO": u"Ё",
    u"ts": u"ц",
    u"TS": u"Ц",
    u"ch": u"ч",
    u"CH": u"Ч",
    u"sh": u"ш",
    u"SH": u"Ш",
    u"yu": u"ю",
    u"Yu": u"Юу",
    u"YU": u"Ю",
    u"ya": u"я",
    u"YA": u"Я",
    u"ai": u"ай",
    u"Ai": u"Ай",
    u"AI": u"АЙ",
    u"ei": u"эй",
    u"Ei": u"Эй",
    u"EI": u"ЭЙ",
    u"ii": u"ий",
    u"Ii": u"Ий",
    u"II": u"ИЙ",
    u"oi": u"ой",
    u"Oi": u"Ой",
    u"OI": u"ОЙ",
    u"ui": u"уй",
    u"Ui": u"Уй",
    u"UI": u"УЙ",
    u"KH": u"Х",
    u"kh": u"х"
}
reversed_specific_pre_processor_mapping = {
    u"щ": u"sh",
    u"Щ": u"SH",
    u"ю": u"yu",
    u"Ю": u"Yu",
    u"Ч": u"Ch",
    u"x": u"h"
}
