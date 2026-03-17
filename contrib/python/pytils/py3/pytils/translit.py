# -*- test-case-name: pytils.test.test_translit -*-
"""
Simple transliteration
"""

from __future__ import annotations

import re

TRANSTABLE = (
    ("'", "'"),
    ('"', '"'),
    ("‘", "'"),
    ("’", "'"),
    ("«", '"'),
    ("»", '"'),
    ("“", '"'),
    ("”", '"'),
    ("–", "-"),  # en dash
    ("—", "-"),  # em dash
    ("‒", "-"),  # figure dash
    ("−", "-"),  # minus
    ("…", "..."),
    ("№", "#"),
    ## upper
    # three-symbols replacements
    ("Щ", "Sch"),
    # on russian->english translation only first replacement will be done
    # i.e. Sch
    # but on english->russian translation both variants (Sch and SCH) will play
    ("Щ", "SCH"),
    # two-symbol replacements
    ("Ё", "Yo"),
    ("Ё", "YO"),
    ("Ж", "Zh"),
    ("Ж", "ZH"),
    ("Ц", "Ts"),
    ("Ц", "TS"),
    ("Ч", "Ch"),
    ("Ч", "CH"),
    ("Ш", "Sh"),
    ("Ш", "SH"),
    ("Ы", "Yi"),
    ("Ы", "YI"),
    ("Ю", "YU"),
    ("Ю", "Yu"),
    ("Я", "Ya"),
    ("Я", "YA"),
    # one-symbol replacements
    ("А", "A"),
    ("Б", "B"),
    ("В", "V"),
    ("Г", "G"),
    ("Д", "D"),
    ("Е", "E"),
    ("З", "Z"),
    ("И", "I"),
    ("Й", "J"),
    ("К", "K"),
    ("Л", "L"),
    ("М", "M"),
    ("Н", "N"),
    ("О", "O"),
    ("П", "P"),
    ("Р", "R"),
    ("С", "S"),
    ("Т", "T"),
    ("У", "U"),
    ("Ф", "F"),
    ("Х", "H"),
    ("Э", "E"),
    ("Ъ", "`"),
    ("Ь", "'"),
    ## lower
    # three-symbols replacements
    ("щ", "sch"),
    # two-symbols replacements
    ("ё", "yo"),
    ("ж", "zh"),
    ("ц", "ts"),
    ("ч", "ch"),
    ("ш", "sh"),
    ("ы", "yi"),
    ("ю", "yu"),
    ("я", "ya"),
    # one-symbol replacements
    ("а", "a"),
    ("б", "b"),
    ("в", "v"),
    ("г", "g"),
    ("д", "d"),
    ("е", "e"),
    ("з", "z"),
    ("и", "i"),
    ("й", "j"),
    ("к", "k"),
    ("л", "l"),
    ("м", "m"),
    ("н", "n"),
    ("о", "o"),
    ("п", "p"),
    ("р", "r"),
    ("с", "s"),
    ("т", "t"),
    ("у", "u"),
    ("ф", "f"),
    ("х", "h"),
    ("э", "e"),
    ("ъ", "`"),
    ("ь", "'"),
    # Make english alphabet full: append english-english pairs
    # for symbols which is not used in russian-english
    # translations. Used in slugify.
    ("c", "c"),
    ("q", "q"),
    ("y", "y"),
    ("x", "x"),
    ("w", "w"),
    ("1", "1"),
    ("2", "2"),
    ("3", "3"),
    ("4", "4"),
    ("5", "5"),
    ("6", "6"),
    ("7", "7"),
    ("8", "8"),
    ("9", "9"),
    ("0", "0"),
)  #: Translation table

RU_ALPHABET = [x[0] for x in TRANSTABLE]  #: Russian alphabet that we can translate
EN_ALPHABET = [
    x[1] for x in TRANSTABLE
]  #: English alphabet that we can detransliterate
ALPHABET = RU_ALPHABET + EN_ALPHABET  #: Alphabet that we can (de)transliterate


def translify(in_string: str, strict: bool = True) -> str:
    """
    Translify russian text

    @param in_string: input string
    @type in_string: C{str}

    @param strict: raise error if transliteration is incomplete.
        (True by default)
    @type strict: C{bool}

    @return: transliterated string
    @rtype: C{str}

    @raise ValueError: when string doesn't transliterate completely.
        Raised only if strict=True
    """
    translit = in_string
    for symb_in, symb_out in TRANSTABLE:
        translit = translit.replace(symb_in, symb_out)

    if strict and any(ord(symb) > 128 for symb in translit):
        raise ValueError(
            "Unicode string doesn't transliterate completely, " + "is it russian?"
        )

    return translit


def detranslify(in_string: str) -> str:
    """
    Detranslify

    @param in_string: input string
    @type in_string: C{basestring}

    @return: detransliterated string
    @rtype: C{str}

    @raise ValueError: if in_string is C{str}, but it isn't ascii
    """
    try:
        russian = str(in_string)
    except UnicodeDecodeError:
        raise ValueError(
            "We expects if in_string is 8-bit string,"
            + "then it consists only ASCII chars, but now it doesn't. "
            + "Use unicode in this case."
        )

    for symb_out, symb_in in TRANSTABLE:
        russian = russian.replace(symb_in, symb_out)

    # TODO: выбрать правильный регистр для ь и ъ
    # твердый и мягкий знак в dentranslify всегда будут в верхнем регистре
    # потому что ` и ' не несут информацию о регистре
    return russian


def slugify(in_string: str) -> str:
    """
    Prepare string for slug (i.e. URL or file/dir name)

    @param in_string: input string
    @type in_string: C{basestring}

    @return: slug-string
    @rtype: C{str}

    @raise ValueError: if in_string is C{str}, but it isn't ascii
    """
    try:
        u_in_string = str(in_string).lower()
    except UnicodeDecodeError:
        raise ValueError(
            "We expects when in_string is str type,"
            + "it is an ascii, but now it isn't. Use unicode "
            + "in this case."
        )
    # convert & to "and"
    u_in_string = re.sub(r"\&amp\;|\&", " and ", u_in_string)
    # replace spaces by hyphen
    u_in_string = re.sub(r"[-\s]+", "-", u_in_string)
    # remove symbols that not in alphabet
    u_in_string = "".join([symb for symb in u_in_string if symb in ALPHABET])
    # translify it
    out_string = translify(u_in_string)
    # remove non-alpha
    return re.sub(r"[^\w\s-]", "", out_string).strip().lower()


def dirify(in_string: str) -> None:
    """
    Alias for L{slugify}
    """
    slugify(in_string)
