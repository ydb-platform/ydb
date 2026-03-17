#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Translation maps for XPath 3.0+ format functions. Add languages with pull-requests.
"""
from string import ascii_lowercase

ALPHABET_CHARACTERS = {
    None: ascii_lowercase,
    'en': ascii_lowercase,
    'it': 'abcdefghilmnopqrstuvz',
    'el': 'αβγδεζηθικλμνξοπρςστυφχψω',
}

OTHER_NUMBERS = (
    '\u2070\u00B9\u00B2\u00B3' +
    ''.join(chr(x) for x in range(0x2074, 0x207A)),  # superscript digits (0-9)
    ''.join(chr(x) for x in range(0x2080, 0x208A)),  # subscript digits (0-9)
    ''.join(chr(x) for x in range(0x2460, 0x2474)),  # circled numbers (1-20)
    ''.join(chr(x) for x in range(0x2474, 0x2488)),  # parenthesized numbers (1-20)
    ''.join(chr(x) for x in range(0x2488, 0x249C)),  # full stop numbers (1-20)
)

ROMAN_NUMERALS_MAP = {
    1000: 'M',
    900: 'CM',
    500: 'D',
    400: 'CD',
    100: 'C',
    90: 'XC',
    50: 'L',
    40: 'XL',
    10: 'X',
    9: 'IX',
    5: 'V',
    4: 'IV',
    1: 'I',
}

NUM_TO_MONTH_MAPS = {
    'en': {
        1: 'january',
        2: 'february',
        3: 'march',
        4: 'april',
        5: 'may',
        6: 'june',
        7: 'july',
        8: 'august',
        9: 'september',
        10: 'october',
        11: 'november',
        12: 'december',
    },
    'it': {
        1: 'gennaio',
        2: 'febbraio',
        3: 'marzo',
        4: 'aprile',
        5: 'maggio',
        6: 'giugno',
        7: 'luglio',
        8: 'agosto',
        9: 'settembre',
        10: 'ottobre',
        11: 'novembre',
        12: 'dicembre',
    },
}

NUM_TO_WEEKDAY_MAPS = {
    'en': {
        1: 'monday',
        2: 'tuesday',
        3: 'wednesday',
        4: 'thursday',
        5: 'friday',
        6: 'saturday',
        7: 'sunday',
    },
    'it': {
        1: 'lunedì',
        2: 'martedì',
        3: 'mercoledì',
        4: 'giovedì',
        5: 'venerdì',
        6: 'sabato',
        7: 'domenica',
    },
}


NUM_TO_WORD_MAPS = {
    'en': {
        10 ** 9: 'billion',
        10 ** 6: 'million',
        1000: 'thousand',
        100: 'hundred',
        90: 'ninety',
        80: 'eighty',
        70: 'seventy',
        60: 'sixty',
        50: 'fifty',
        40: 'forty',
        30: 'thirty',
        20: 'twenty',
        19: 'nineteen',
        18: 'eighteen',
        17: 'seventeen',
        16: 'sixteen',
        15: 'fifteen',
        14: 'fourteen',
        13: 'thirteen',
        12: 'twelve',
        11: 'eleven',
        10: 'ten',
        9: 'nine',
        8: 'eight',
        7: 'seven',
        6: 'six',
        5: 'five',
        4: 'four',
        3: 'three',
        2: 'two',
        1: 'one',
        0: 'zero',
    },
    'it': {
        10 ** 9: 'miliardo',
        10 ** 6: 'milione',
        1000: 'mille',
        100: 'cento',
        90: 'novanta',
        80: 'ottanta',
        70: 'settanta',
        60: 'sessanta',
        50: 'cinquanta',
        40: 'quaranta',
        30: 'trenta',
        20: 'venti',
        19: 'diciannove',
        18: 'diciotto',
        17: 'diciassette',
        16: 'sedici',
        15: 'quindici',
        14: 'quattordici',
        13: 'tredici',
        12: 'dodici',
        11: 'undici',
        10: 'dieci',
        9: 'nove',
        8: 'otto',
        7: 'sette',
        6: 'sei',
        5: 'cinque',
        4: 'quattro',
        3: 'tre',
        2: 'due',
        1: 'uno',
        0: 'zero',
    }
}

MILITARY_TIME_ZONES = {
    '+01': 'A',
    '+02': 'B',
    '+03': 'C',
    '+04': 'D',
    '+05': 'E',
    '+06': 'F',
    '+07': 'G',
    '+08': 'H',
    '+09': 'I',
    None: 'J',
    '+10': 'K',
    '+11': 'L',
    '+12': 'M',
    '-01': 'N',
    '-02': 'O',
    '-03': 'P',
    '-04': 'Q',
    '-05': 'R',
    '-06': 'S',
    '-07': 'T',
    '-08': 'U',
    '-09': 'V',
    '-10': 'W',
    '-11': 'X',
    '-12': 'Y',
    '+00': 'Z',
}
