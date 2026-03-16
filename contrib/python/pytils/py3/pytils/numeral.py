# -*- test-case-name: pytils.test.test_numeral -*-
"""
Plural forms and in-word representation for numerals.
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast

from pytils.utils import check_length, check_positive, split_values

FRACTIONS = (
    ("десятая", "десятых", "десятых"),
    ("сотая", "сотых", "сотых"),
    ("тысячная", "тысячных", "тысячных"),
    ("десятитысячная", "десятитысячных", "десятитысячных"),
    ("стотысячная", "стотысячных", "стотысячных"),
    ("миллионная", "милллионных", "милллионных"),
    ("десятимиллионная", "десятимилллионных", "десятимиллионных"),
    ("стомиллионная", "стомилллионных", "стомиллионных"),
    ("миллиардная", "миллиардных", "миллиардных"),
)  #: Forms (1, 2, 5) for fractions

ONES = {
    0: ("", "", ""),
    1: ("один", "одна", "одно"),
    2: ("два", "две", "два"),
    3: ("три", "три", "три"),
    4: ("четыре", "четыре", "четыре"),
    5: ("пять", "пять", "пять"),
    6: ("шесть", "шесть", "шесть"),
    7: ("семь", "семь", "семь"),
    8: ("восемь", "восемь", "восемь"),
    9: ("девять", "девять", "девять"),
}  #: Forms (MALE, FEMALE, NEUTER) for ones

TENS = {
    0: "",
    # 1 - особый случай
    10: "десять",
    11: "одиннадцать",
    12: "двенадцать",
    13: "тринадцать",
    14: "четырнадцать",
    15: "пятнадцать",
    16: "шестнадцать",
    17: "семнадцать",
    18: "восемнадцать",
    19: "девятнадцать",
    2: "двадцать",
    3: "тридцать",
    4: "сорок",
    5: "пятьдесят",
    6: "шестьдесят",
    7: "семьдесят",
    8: "восемьдесят",
    9: "девяносто",
}  #: Tens

HUNDREDS = {
    0: "",
    1: "сто",
    2: "двести",
    3: "триста",
    4: "четыреста",
    5: "пятьсот",
    6: "шестьсот",
    7: "семьсот",
    8: "восемьсот",
    9: "девятьсот",
}  #: Hundreds

MALE = 1  #: sex - male
FEMALE = 2  #: sex - female
NEUTER = 3  #: sex - neuter

FORMS_COUNT = 3


def _get_float_remainder(fvalue: int | float | Decimal, signs: int = 9) -> str:
    """
    Get remainder of float, i.e. 2.05 -> '05'

    @param fvalue: input value
    @type fvalue: C{integer types}, C{float} or C{Decimal}

    @param signs: maximum number of signs
    @type signs: C{integer types}

    @return: remainder
    @rtype: C{str}

    @raise ValueError: fvalue is negative
    @raise ValueError: signs overflow
    """
    check_positive(fvalue)
    if isinstance(fvalue, int):
        return "0"
    if isinstance(fvalue, Decimal) and fvalue.as_tuple()[2] == 0:
        # Decimal.as_tuple() -> (sign, digit_tuple, exponent)
        # если экспонента "0" -- значит дробной части нет
        return "0"

    signs = min(signs, len(FRACTIONS))

    # нужно remainder в строке, потому что дробные X.0Y
    # будут "ломаться" до X.Y
    remainder = str(fvalue).split(".")[1]
    iremainder = int(remainder)
    orig_remainder = remainder
    factor = len(str(remainder)) - signs

    if factor > 0:
        # после запятой цифр больше чем signs, округляем
        iremainder = int(round(iremainder / (10.0**factor)))
    format = "%%0%dd" % min(len(remainder), signs)

    remainder = format % iremainder

    if len(remainder) > signs:
        # при округлении цифр вида 0.998 ругаться
        raise ValueError(
            "Signs overflow: I can't round only fractional part \
                          of %s to fit %s in %d signs"
            % (str(fvalue), orig_remainder, signs)
        )

    return remainder


def choose_plural(amount: int, variants: str | tuple[str, ...]) -> str:
    """
    Choose proper case depending on amount

    @param amount: amount of objects
    @type amount: C{integer types}

    @param variants: variants (forms) of object in such form:
        (1 object, 2 objects, 5 objects).
    @type variants: 3-element C{sequence} of C{unicode}
        or C{unicode} (three variants with delimeter ',')

    @return: proper variant
    @rtype: C{str}

    @raise ValueError: variants' length lesser than 3
    """

    if isinstance(variants, str):
        variants = split_values(variants)
    check_length(variants, FORMS_COUNT)

    amount = abs(amount)

    if amount % 10 == 1 and amount % 100 != 11:
        variant = 0
    elif (
        amount % 10 >= 2
        and amount % 10 <= 4
        and (amount % 100 < 10 or amount % 100 >= 20)
    ):
        variant = 1
    else:
        variant = 2

    return variants[variant]


def get_plural(
    amount: int, variants: str | tuple[str, ...], absence: str | None = None
) -> str:
    """
    Get proper case with value

    @param amount: amount of objects
    @type amount: C{integer types}

    @param variants: variants (forms) of object in such form:
        (1 object, 2 objects, 5 objects).
    @type variants: 3-element C{sequence} of C{str}
        or C{str} (three variants with delimeter ',')

    @param absence: if amount is zero will return it
    @type absence: C{str}

    @return: amount with proper variant
    @rtype: C{str}
    """
    if amount or absence is None:
        return "%d %s" % (amount, choose_plural(amount, variants))
    else:
        return absence


def _get_plural_legacy(amount, extra_variants):
    """
    Get proper case with value (legacy variant, without absence)

    @param amount: amount of objects
    @type amount: C{integer types}

    @param variants: variants (forms) of object in such form:
        (1 object, 2 objects, 5 objects, 0-object variant).
        0-object variant is similar to C{absence} in C{get_plural}
    @type variants: 3-element C{sequence} of C{str}
        or C{str} (three variants with delimeter ',')

    @return: amount with proper variant
    @rtype: C{str}
    """
    absence = None
    if isinstance(extra_variants, str):
        extra_variants = split_values(extra_variants)
    if len(extra_variants) == 4:
        variants = extra_variants[:3]
        absence = extra_variants[3]
    else:
        variants = extra_variants
    return get_plural(amount, variants, absence)


def rubles(amount: int | float | Decimal, zero_for_kopeck: bool = False) -> str:
    """
    Get string for money

    @param amount: amount of money
    @type amount: C{integer types}, C{float} or C{Decimal}

    @param zero_for_kopeck: If false, then zero kopecks ignored
    @type zero_for_kopeck: C{bool}

    @return: in-words representation of money's amount
    @rtype: C{str}

    @raise ValueError: amount is negative
    """
    check_positive(amount)

    pts = []
    amount = round(amount, 2)
    pts.append(sum_string(int(amount), MALE, ("рубль", "рубля", "рублей")))
    remainder = _get_float_remainder(amount, 2)
    iremainder = int(remainder)

    if iremainder != 0 or zero_for_kopeck:
        # если 3.1, то это 10 копеек, а не одна
        if iremainder < 10 and len(remainder) == 1:
            iremainder *= 10
        pts.append(sum_string(iremainder, FEMALE, ("копейка", "копейки", "копеек")))

    return " ".join(pts)


def in_words_int(amount: int, gender: int = MALE) -> str:
    """
    Integer in words

    @param amount: numeral
    @type amount: C{integer types}

    @param gender: gender (MALE, FEMALE or NEUTER)
    @type gender: C{int}

    @return: in-words reprsentation of numeral
    @rtype: C{str}

    @raise ValueError: amount is negative
    """
    check_positive(amount)

    return sum_string(amount, gender)


def in_words_float(amount: float | Decimal) -> str:
    """
    Float in words

    @param amount: float numeral
    @type amount: C{float} or C{Decimal}

    @return: in-words reprsentation of float numeral
    @rtype: C{str}

    @raise ValueError: when ammount is negative
    """
    check_positive(amount)

    pts = []
    # преобразуем целую часть
    pts.append(sum_string(int(amount), FEMALE, ("целая", "целых", "целых")))
    # теперь то, что после запятой
    remainder = _get_float_remainder(amount)
    signs = len(str(remainder)) - 1
    pts.append(sum_string(int(remainder), FEMALE, FRACTIONS[signs]))

    return " ".join(pts)


def in_words(amount: int | float | Decimal, gender: int | None = None) -> str:
    """
    Numeral in words

    @param amount: numeral
    @type amount: C{integer types}, C{float} or C{Decimal}

    @param gender: gender (MALE, FEMALE or NEUTER)
    @type gender: C{int}

    @return: in-words reprsentation of numeral
    @rtype: C{str}

    raise ValueError: when amount is negative
    """
    check_positive(amount)
    if isinstance(amount, Decimal) and amount.as_tuple()[2] == 0:
        # если целое,
        # т.е. Decimal.as_tuple -> (sign, digits tuple, exponent), exponent=0
        # то как целое
        amount = int(amount)
    if gender is None:
        args = (amount,)
    else:
        args = (amount, gender)
    # если целое
    if isinstance(amount, int):
        return in_words_int(*args)  # ty: ignore[invalid-argument-type]
    # если дробное
    elif isinstance(amount, (float, Decimal)):
        return in_words_float(amount)
    # ни float, ни int, ни Decimal
    else:
        # до сюда не должно дойти
        raise TypeError(
            "amount should be number type (int, long, float, Decimal), got %s"
            % type(amount)
        )


def sum_string(
    amount: int, gender: int, items: str | tuple[str, ...] | None = None
) -> str:
    """
    Get sum in words

    @param amount: amount of objects
    @type amount: C{integer types}

    @param gender: gender of object (MALE, FEMALE or NEUTER)
    @type gender: C{int}

    @param items: variants of object in three forms:
        for one object, for two objects and for five objects
    @type items: 3-element C{sequence} of C{str} or
        just C{str} (three variants with delimeter ',')

    @return: in-words representation objects' amount
    @rtype: C{str}

    @raise ValueError: items isn't 3-element C{sequence} or C{unicode}
    @raise ValueError: amount bigger than 10**11
    @raise ValueError: amount is negative
    """
    if isinstance(items, str):
        items_tuple = split_values(items)
    elif items is None:
        items_tuple = ("", "", "")
    else:
        items_tuple = items

    check_positive(amount)
    check_length(items_tuple, FORMS_COUNT)
    items_tuple = cast(tuple[str, str, str], items_tuple)

    _, _, five_items = items_tuple

    if amount == 0:
        if five_items:
            return "ноль %s" % five_items
        else:
            return "ноль"

    into = ""
    tmp_val = amount

    # единицы
    into, tmp_val = _sum_string_fn(into, tmp_val, gender, items_tuple)
    # тысячи
    into, tmp_val = _sum_string_fn(into, tmp_val, FEMALE, ("тысяча", "тысячи", "тысяч"))
    # миллионы
    into, tmp_val = _sum_string_fn(
        into, tmp_val, MALE, ("миллион", "миллиона", "миллионов")
    )
    # миллиарды
    into, tmp_val = _sum_string_fn(
        into, tmp_val, MALE, ("миллиард", "миллиарда", "миллиардов")
    )
    if tmp_val == 0:
        return into
    else:
        raise ValueError("Cannot operand with numbers bigger than 10**11")


def _sum_string_fn(
    into: str, tmp_val: int, gender: int, items: tuple[str, str, str]
) -> tuple[str, int]:
    """
    Make in-words representation of single order

    @param into: in-words representation of lower orders
    @type into: C{str}

    @param tmp_val: temporary value without lower orders
    @type tmp_val: C{integer types}

    @param gender: gender (MALE, FEMALE or NEUTER)
    @type gender: C{int}

    @param items: variants of objects
    @type items: 3-element C{sequence} of C{str}

    @return: new into and tmp_val
    @rtype: C{tuple}

    @raise ValueError: tmp_val is negative
    """
    _, _, five_items = items

    check_positive(tmp_val)

    if tmp_val == 0:
        return into, tmp_val

    words = []

    rest = tmp_val % 1000
    tmp_val = tmp_val // 1000
    if rest == 0:
        # последние три знака нулевые
        if into == "":
            into = "%s " % five_items
        return into, tmp_val

    # начинаем подсчет с rest
    end_word = five_items

    # сотни
    words.append(HUNDREDS[rest // 100])

    # десятки
    rest = rest % 100
    rest1 = rest // 10
    # особый случай -- tens=1
    tens = rest1 == 1 and TENS[rest] or TENS[rest1]
    words.append(tens)

    # единицы
    if rest1 < 1 or rest1 > 1:
        amount = rest % 10
        end_word = choose_plural(amount, items)
        words.append(ONES[amount][gender - 1])
    words.append(end_word)

    # добавляем то, что уже было
    words.append(into)

    # убираем пустые подстроки
    words = filter(lambda x: len(x) > 0, words)

    # склеиваем и отдаем
    return " ".join(words).strip(), tmp_val
