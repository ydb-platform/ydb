from normality.util import Categories

# https://en.wikipedia.org/wiki/Cyrillic_script_in_Unicode
# Cyrillic: U+0400–U+04FF, 256 characters
# Cyrillic Supplement: U+0500–U+052F, 48 characters
# Cyrillic Extended-A: U+2DE0–U+2DFF, 32 characters
# Cyrillic Extended-B: U+A640–U+A69F, 96 characters
# Cyrillic Extended-C: U+1C80–U+1C8F, 9 characters
# Phonetic Extensions: U+1D2B, U+1D78, 2 Cyrillic characters
# Combining Half Marks: U+FE2E–U+FE2F, 2 Cyrillic characters


WS: str = " "

# Unicode character classes, see:
# http://www.fileformat.info/info/unicode/category/index.htm
# https://en.wikipedia.org/wiki/Unicode_character_property
# http://www.unicode.org/charts/beta/script/
UNICODE_CATEGORIES: Categories = {
    "Cc": WS,
    "Cf": None,
    "Cs": None,
    "Co": None,
    "Cn": None,
    "Lm": None,
    "Mn": None,
    "Mc": WS,
    "Me": None,
    "No": None,
    "Zs": WS,
    "Zl": WS,
    "Zp": WS,
    "Pc": WS,  # TODO: figure out if this wants to be None
    "Pd": WS,
    "Ps": WS,
    "Pe": WS,
    "Pi": WS,
    "Pf": WS,
    "Po": WS,
    "Sm": WS,
    "Sc": None,
    "Sk": None,
    "So": WS,
}

SLUG_CATEGORIES: Categories = {
    "Cc": None,
    "Cf": None,
    "Cs": None,
    "Co": None,
    "Cn": None,
    # "Lm": None,
    # "Mn": None,
    "Mc": WS,
    "Me": None,
    "No": None,
    "Zs": WS,
    "Zl": WS,
    "Zp": WS,
    "Pc": WS,
    "Pd": WS,
    "Ps": WS,
    "Pe": WS,
    "Pi": WS,
    "Pf": WS,
    "Po": WS,
    "Sm": WS,
    "Sc": None,
    "Sk": None,
    "So": WS,
}


CONTROL_CODES: Categories = {"Cc": WS, "Cf": WS, "Cs": WS, "Co": WS, "Cn": WS, "Zl": WS}
