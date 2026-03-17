#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import calendar
import datetime
import decimal
import re
from collections.abc import Iterator
from typing import Any, Optional, Union
from unicodedata import category

from elementpath.exceptions import xpath_error
from elementpath.regex import translate_pattern

from ._translation_maps import ALPHABET_CHARACTERS, OTHER_NUMBERS, ROMAN_NUMERALS_MAP, \
    NUM_TO_MONTH_MAPS, NUM_TO_WEEKDAY_MAPS, NUM_TO_WORD_MAPS, MILITARY_TIME_ZONES

PRESENTATION_FORMATS = {'i', 'I', 'w', 'W', 'Ww', 'a', 'A', 'n', 'N', 'Nn', 'Z'}
PICTURE_PATTERN = re.compile(r'\[(?!\[)[^]]+]')
UNICODE_DIGIT_PATTERN = re.compile(r'\d')
DECIMAL_DIGIT_PATTERN = re.compile(translate_pattern(r'^((\p{Nd}|#|[^\p{N}\p{L}])+?)$'))
FMT_MODIFIER_PATTERN = re.compile(r'([co](\(.+\))?)?[at]?$')
WIDTH_PATTERN = re.compile(r'^([0-9]+|\*)(-([0-9]+|\*))?$')
MODIFIER_PATTERN = re.compile(r'^([co](\(.+\))?)?[at]?$')


def decimal_to_string(value: decimal.Decimal) -> str:
    """
    Convert a Decimal value to a string representation
    that not includes exponent and with its decimals.
    """
    exponent: Any
    sign, digits, exponent = value.as_tuple()

    if not exponent:
        result = ''.join(str(x) for x in digits)
    elif exponent > 0:
        result = ''.join(str(x) for x in digits) + '0' * exponent
    else:
        result = ''.join(str(x) for x in digits[:exponent])
        if not result:
            result = '0'
        result += '.'
        if len(digits) >= -exponent:
            result += ''.join(str(x) for x in digits[exponent:])
        else:
            result += '0' * (-exponent - len(digits))
            result += ''.join(str(x) for x in digits)

    return '-' + result if sign else result


def int_to_roman(num: int) -> str:
    """
    Convert an integer to Roman ordinal.
    """
    def roman_num(value: int) -> Iterator[str]:
        if not value:
            yield '0'
            return
        elif value < 0:
            yield '-'
            value = abs(value)

        for base, roman in ROMAN_NUMERALS_MAP.items():
            if value:
                yield roman * (value // base)
                value %= base

    return ''.join(x for x in roman_num(num))


def int_to_alphabetic(num: int, reference: Optional[str] = None) -> str:
    if not reference or len(reference) > 1:
        try:
            alphabet = ALPHABET_CHARACTERS[reference]
        except KeyError:
            msg = "formatting for language {!r} is not supported"
            raise NotImplementedError(msg.format(reference))

    elif reference.isdigit():
        for alphabet in OTHER_NUMBERS:
            if reference in alphabet:
                break
        else:
            alphabet = '1234567890'
    else:
        for alphabet in ALPHABET_CHARACTERS.values():
            if reference.lower() in alphabet:
                break
        else:
            alphabet = '1234567890'

    base = len(alphabet)

    if not num:
        return '0'

    chars = []
    negative = num < 0

    num = abs(num) - 1
    while num >= 0:
        chars.append(alphabet[num % base])
        num = (num // base) - 1

    if negative:
        chars.append('-')
    return ''.join(reversed(chars))


def int_to_month(num: int, lang: Optional[str] = None) -> str:
    if lang is None:
        lang = 'en'

    try:
        months_map = NUM_TO_MONTH_MAPS[lang]
    except KeyError:
        months_map = NUM_TO_MONTH_MAPS['en']

    return months_map[num]


def int_to_weekday(num: int, lang: Optional[str] = None) -> str:
    if lang is None:
        lang = 'en'

    try:
        weekday_map = NUM_TO_WEEKDAY_MAPS[lang]
    except KeyError:
        weekday_map = NUM_TO_WEEKDAY_MAPS['en']

    return weekday_map[num]


def week_in_month(dt: datetime.datetime) -> int:
    month_cal = calendar.monthcalendar(dt.year, dt.month)
    for k, week_cal in enumerate(month_cal, start=1):
        if dt.day in week_cal:
            if month_cal[0][3]:
                return k
            elif k > 1:
                return k - 1

            if dt.month > 1:
                prev_month_cal = calendar.monthcalendar(dt.year, dt.month - 1)
            else:
                prev_month_cal = calendar.monthcalendar(dt.year - 1, 12)

            if prev_month_cal[0][3]:
                return len(prev_month_cal)
            else:
                return len(prev_month_cal) - 1
    else:
        raise ValueError(f'{dt.day} does not match related calendar')


def format_digits(digits: str,
                  fmt: str,
                  digits_family: str = '0123456789',
                  optional_digit: str = '#',
                  grouping_separator: Optional[str] = None) -> str:
    result = []
    iter_num_digits = reversed(digits)
    num_digit = next(iter_num_digits)

    for fmt_char in reversed(fmt):
        if fmt_char.isdigit() or fmt_char == optional_digit:
            if num_digit:
                result.append(digits_family[ord(num_digit) - 48])
                num_digit = next(iter_num_digits, '')
            elif fmt_char != optional_digit:
                result.append(digits_family[0])
        elif not result or not result[-1].isdigit() and grouping_separator \
                and result[-1] != grouping_separator:
            raise xpath_error('FODF1310', "invalid grouping in picture argument")
        else:
            result.append(fmt_char)

    if num_digit:
        separator = ''
        _separator = {x for x in fmt if not x.isdigit() and x != optional_digit}
        if len(_separator) != 1:
            repeat = None
        else:
            separator = _separator.pop()
            chunks = fmt.split(separator)

            if len(chunks[0]) > len(chunks[-1]):
                repeat = None
            elif all(len(item) == len(chunks[-1]) for item in chunks[1:-1]):
                repeat = len(chunks[-1]) + 1
            else:
                repeat = None

        if repeat is None:
            while num_digit:
                result.append(digits_family[ord(num_digit) - 48])
                num_digit = next(iter_num_digits, '')
        else:
            while num_digit:
                if ((len(result) + 1) % repeat) == 0:
                    result.append(separator)
                result.append(digits_family[ord(num_digit) - 48])
                num_digit = next(iter_num_digits, '')

    if grouping_separator:
        return ''.join(reversed(result)).lstrip(grouping_separator)
    while result and \
            category(result[-1]) not in ('Nd', 'Nl', 'No', 'Lu', 'Ll', 'Lt', 'Lm', 'Lo'):
        result.pop()
    return ''.join(reversed(result))


def ordinal_suffix(value: int) -> str:
    value = abs(value) % 100
    if 3 < value < 20:
        return 'th'

    value %= 10
    if value == 1:
        return 'st'
    elif value == 2:
        return 'nd'
    elif value == 3:
        return 'rd'
    else:
        return 'th'


def to_ordinal_en(num_as_words: str) -> str:
    if num_as_words.endswith('one'):
        return num_as_words[:-3] + 'first'
    elif num_as_words.endswith('two'):
        return num_as_words[:-3] + 'second'
    elif num_as_words.endswith('three'):
        return num_as_words[:-5] + 'third'
    elif num_as_words.endswith('eight'):
        return num_as_words + 'h'
    elif num_as_words.endswith('nine'):
        return num_as_words[:-1] + 'th'
    elif num_as_words.endswith('y'):
        return num_as_words[:-1] + 'ieth'
    elif num_as_words.endswith('e'):
        return num_as_words[:-2] + 'fth'
    else:
        return num_as_words + 'th'


def to_ordinal_it(num_as_words: str, fmt_modifier: str = '') -> str:
    if '%spellout-ordinal-feminine' in fmt_modifier:
        suffix = 'a'
    elif fmt_modifier.startswith('o(-'):
        suffix = fmt_modifier[3:-1]
    else:
        suffix = ''

    ordinal_map = {
        'zero': '',
        'uno': 'primo',
        'due': 'secondo',
        'tre': 'terzo',
        'quattro': 'quarto',
        'cinque': 'quinto',
        'sei': 'sesto',
        'sette': 'settimo',
        'otto': 'ottavo',
        'nove': 'nono',
        'dieci': 'decimo',
    }

    try:
        value = ordinal_map[num_as_words]
    except KeyError:
        if num_as_words[-1] in 'eo' or num_as_words[-2:] == 'ci':
            value = num_as_words[:-1] + 'esimo'
        else:
            value = num_as_words + 'esimo'

    if value and suffix:
        return value[:-1] + suffix
    return value


def int_to_words(num: int, lang: Optional[str] = None, fmt_modifier: str = '') -> str:

    def word_num(value: int) -> Iterator[str]:
        if not value:
            yield num_map[value]

        for base, word in num_map.items():
            if base >= 1:
                floor = value // base
                if not floor:
                    continue
                elif base >= 100:
                    yield from word_num(floor)
                    yield ' '

                yield word
                value %= base
                if not value:
                    break
                elif base < 100:
                    yield '-'
                elif base == 100:
                    if lang == 'en':
                        yield ' and '
                else:
                    yield ' '

    try:
        num_map = NUM_TO_WORD_MAPS[lang]  # type: ignore[index]
    except KeyError:
        lang = 'en'
        num_map = NUM_TO_WORD_MAPS[lang]

    if num < 0:
        result = '-' + ''.join(x for x in word_num(abs(num)))
    else:
        result = ''.join(x for x in word_num(num))

    if not fmt_modifier.startswith('o'):
        return result

    if lang == 'en':
        return to_ordinal_en(result)
    elif lang == 'it':
        return to_ordinal_it(result, fmt_modifier)
    else:
        return result


def parse_datetime_picture(picture: str) -> tuple[list[str], list[str]]:
    """
    Analyze a picture argument of XPath 3.0+ formatting functions.

    :param picture: the picture string.
    :return: a couple of lists containing the literal parts and markers.
    """
    min_value: Union[int, str]
    max_value: Union[None, int, str]

    literals = []
    for lit in PICTURE_PATTERN.split(picture):
        if '[' in lit.replace('[[', ''):
            raise xpath_error('FOFD1340', "Invalid character '[' in picture literal")
        elif ']' in lit.replace(']]', ''):
            raise xpath_error('FOFD1340', "Invalid character ']' in picture literal")
        else:
            literals.append(lit.replace('[[', '[').replace(']]', ']'))

    markers = [x.group().replace(' ', '').replace('\n', '').replace('\t', '')
               for x in PICTURE_PATTERN.finditer(picture)]
    assert len(markers) == (len(literals) - 1)

    msg_tmpl = 'Invalid formatting component {!r}'
    for value in markers:
        if value[1] not in 'YMDdFWwHhPmsfZzCE':
            raise xpath_error('FOFD1340', msg_tmpl.format(value))

        if ',' not in value:
            presentation = value[2:-1]
        else:
            presentation, width = value[2:-1].rsplit(',', maxsplit=1)

            if WIDTH_PATTERN.match(width) is None:
                raise xpath_error('FOFD1340', f'Invalid width modifier {value!r}')
            elif '-' not in width:
                if '*' not in width and not int(width):
                    raise xpath_error('FOFD1340', f'Invalid width modifier {value!r}')
            elif '*' not in width:
                min_value, max_value = map(int, width.split('-'))
                if min_value < 1 or max_value < min_value:
                    raise xpath_error('FOFD1340', msg_tmpl.format(value))
            else:
                min_value, max_value = width.split('-')
                if min_value != '*' and not int(min_value):
                    raise xpath_error('FOFD1340', f'Invalid width modifier {value!r}')
                if max_value != '*' and not int(max_value):
                    raise xpath_error('FOFD1340', f'Invalid width modifier {value!r}')

        if len(presentation) > 1 and presentation[-1] in 'atco':
            presentation = presentation[:-1]

        if not presentation or presentation in PRESENTATION_FORMATS:
            pass
        elif DECIMAL_DIGIT_PATTERN.match(presentation) is None:
            raise xpath_error('FOFD1340', msg_tmpl.format(value))
        else:
            if value[1] == 'f':
                if presentation[0] == '#' and any(ch.isdigit() for ch in presentation):
                    msg = 'picture argument has an invalid primary format token'
                    raise xpath_error('FOFD1340', msg)
            elif presentation[0].isdigit() and '#' in presentation:
                msg = 'picture argument has an invalid primary format token'
                raise xpath_error('FOFD1340', msg)

            # Check digits set uniformity
            cp = None
            for ch in reversed(presentation):
                if not ch.isdigit():
                    continue
                elif cp is None:
                    cp = ord(ch)
                elif abs(ord(ch) - cp) > 10:
                    raise xpath_error('FOFD1340', msg_tmpl.format(value))

    return literals, markers


def parse_datetime_marker(marker: str, dt: datetime.datetime, lang: Optional[str] = None) -> str:
    min_width: int
    max_width: Optional[int]

    component = marker[1]
    fmt_token = marker[2:-1]

    if ',' not in fmt_token:
        presentation, width = fmt_token, ''
    else:
        presentation, width = fmt_token.rsplit(',', maxsplit=1)

    if not presentation:
        fmt_modifier = ''
        if component in 'Hhf':
            presentation = '1'
        elif component in 'ms':
            presentation = '01'
        elif component in 'Zz':
            presentation = '01:01'
        else:
            presentation = 'n'
    elif presentation == 'a':
        fmt_modifier = ''
    else:
        _match = FMT_MODIFIER_PATTERN.search(presentation)
        if _match is None:
            fmt_modifier = ''
        else:
            fmt_modifier = _match.group(0)
            if fmt_modifier:
                presentation = presentation[:-len(fmt_modifier)]

        if presentation.startswith('#') and presentation.endswith('#'):
            msg_tmpl = 'Invalid formatting component {!r}'
            raise xpath_error('FOFD1340', msg_tmpl.format(component))

    for pch in presentation:
        if pch.isdigit():
            zero_cp = ord(pch) - int(pch)
            zero_ch = chr(zero_cp)
            break
    else:
        zero_cp, zero_ch = ord('0'), '0'

    digits = sum(c.isdigit() for c in presentation)
    opt_digits = presentation.count('#')
    if not width or width == '*':
        if digits > 1:
            min_width, max_width = digits, digits + opt_digits
        else:
            min_width, max_width = 0, None
    else:
        min_width, max_width = parse_width(width)
        if digits > 1:
            min_width = max(min_width, digits)
            if max_width:
                max_width = max(max_width, digits + opt_digits)

    if component == 'Y':
        value = str(abs(dt.year))
    elif component == 'M':
        if presentation.lower().startswith('n') and lang is not None:
            value = int_to_month(dt.month, lang)
        else:
            value = str(dt.month)
    elif component == 'D':
        value = str(dt.day)
    elif component == 'H':
        value = str(dt.hour)
    elif component == 'h':
        if dt.hour == 0:
            value = '12'
        elif dt.hour > 12:
            value = str(dt.hour % 12)
        else:
            value = str(dt.hour)
    elif component == 'P':
        value = 'a.m.' if dt.hour < 12 else 'p.m.'
    elif component == 'm':
        value = str(dt.minute)
    elif component == 's':
        value = str(dt.second)
    elif component == 'f':
        value = str('{:06}'.format(dt.microsecond))
    elif component == 'z' or component == 'Z':
        if presentation == 'N':
            value = dt.tzname() or ''
        elif dt.tzinfo is None:
            value = '+00:00'
        else:
            value = str(dt)
            if value.endswith('Z'):
                value = '+00:00'
            else:
                value = value[-6:]
    elif component == 'W':
        value = str(dt.isocalendar()[1])
    elif component == 'w':
        value = str(week_in_month(dt))
    elif component == 'F':
        if presentation.lower().startswith('n') and lang is not None:
            value = int_to_weekday(dt.isocalendar()[2], lang)
        else:
            value = str(dt.isocalendar()[2])
    elif component == 'E':
        if dt.year < 0:
            value = 'BC'
        else:
            value = 'AD'
    elif component == 'd':
        delta = dt - type(dt)(dt.year, 1, 1)
        value = str(1 + delta.seconds // 86400)
    else:
        msg_tmpl = 'Invalid formatting component {!r}'
        raise xpath_error('FOFD1340', msg_tmpl.format(component))

    sign = ''
    left_to_right = component != 'Y'

    if presentation == 'n':
        fmt_chunk = value.lower()
    elif presentation == 'N':
        fmt_chunk = value.upper()
    elif presentation == 'Nn':
        fmt_chunk = value.title()
    elif presentation == 'I' or presentation == 'i':
        fmt_chunk = value
    elif presentation == 'Z' and component == 'Z':
        if dt.tzinfo is None:
            fmt_chunk = MILITARY_TIME_ZONES[None]
        elif value.endswith(':00'):
            fmt_chunk = MILITARY_TIME_ZONES.get(value[:3], value)
        else:
            fmt_chunk = value

    elif presentation == 'w':
        fmt_chunk = int_to_words(int(value), lang, fmt_modifier)
    elif presentation == 'W':
        fmt_chunk = int_to_words(int(value), lang, fmt_modifier).upper()
    elif presentation == 'Ww':
        fmt_chunk = int_to_words(int(value), lang, fmt_modifier).title()
    elif presentation == 'a':
        fmt_chunk = int_to_alphabetic(int(value), lang)
    elif presentation == 'A':
        fmt_chunk = int_to_alphabetic(int(value), lang).upper()
    else:
        left_to_right = False
        k = 0
        pch = ''
        chars = []

        # Extract the sign
        if value.startswith('-') or value.startswith('+'):
            sign = value[0]
            value = value[1:]

        if component in 'zZ':
            if presentation.isdigit():
                if len(presentation) <= 2:
                    if value.endswith(':00'):
                        value = value[:-3]
                        left_to_right = True
                    elif len(presentation) == 1:
                        presentation = '#0:01'
                        min_width, max_width = 3, 4
                    else:
                        presentation = '01:01'
                        min_width = max_width = 4
                elif len(presentation) == 3:
                    presentation = '#001'
                    min_width, max_width = 3, 4
            elif presentation.replace(':', '', 1).isdigit():
                if len(presentation) == 4:
                    presentation = '#0:01'
                    min_width, max_width = 3, 4

        if component != 'f':
            presentation = ''.join(reversed(presentation))
            value = ''.join(reversed(value))

        for ch in value:
            try:
                pch = presentation[k]
            except IndexError:
                if ch == '0' and not pch.isdigit():
                    break
            else:
                k += 1

            while pch != '#' and not pch.isdigit():
                chars.append(pch)
                min_width += 1
                if max_width is not None:
                    max_width += 1

                try:
                    pch = presentation[k]
                except IndexError:
                    break
                else:
                    k += 1
            else:
                if ch.isdigit():
                    chars.append(ch)

        if component != 'f':
            fmt_chunk = ''.join(reversed(chars))
        else:
            fmt_chunk = ''.join(chars)

        if 'o' in fmt_modifier:
            try:
                fmt_chunk += ordinal_suffix(int(fmt_chunk))
            except ValueError:
                pass
            else:
                min_width += 2
                if max_width is not None:
                    max_width += 2

    if len(fmt_chunk) < min_width and component not in 'PzZ':
        if component in 'f':
            fmt_chunk += zero_ch * (min_width - len(fmt_chunk))
        else:
            fmt_chunk = zero_ch * (min_width - len(fmt_chunk)) + fmt_chunk

    if max_width:
        if left_to_right or component in 'f':
            fmt_chunk = fmt_chunk[:max_width]
        else:
            fmt_chunk = fmt_chunk[max(0, len(fmt_chunk)-max_width):]

    if component in 'zZ':
        if not min_width:
            fmt_chunk = fmt_chunk.lstrip('0')
            if not fmt_chunk:
                return 'Z' if component == 'Z' else 'GMT' + sign + '0'
        else:
            try:
                nz_first = min(k for k in range(len(fmt_chunk)) if fmt_chunk[k] != zero_ch)
            except ValueError:
                fmt_chunk = fmt_chunk[max(0, len(fmt_chunk) - min_width):]
            else:
                fmt_chunk = fmt_chunk[max(0, min(nz_first, len(fmt_chunk) - min_width)):]

    elif min_width == 3 and component == 'F':
        fmt_chunk = fmt_chunk[:3]
    elif min_width or component == 'f':
        try:
            nz_last = max(k for k in range(len(fmt_chunk)) if fmt_chunk[k] != zero_ch)
        except ValueError:
            nz_last = 0

        fmt_chunk = fmt_chunk[:max(min_width, nz_last + 1)]

    if zero_ch != '0':
        fmt_chunk = ''.join(chr(zero_cp + int(ch)) if ch.isdigit() else ch for ch in fmt_chunk)

    if component == 'z':
        return 'GMT' + sign + fmt_chunk

    if presentation == 'I':
        return sign + int_to_roman(int(fmt_chunk))
    elif presentation == 'i':
        return sign + int_to_roman(int(fmt_chunk)).lower()

    return sign + fmt_chunk


def parse_width(width: str) -> tuple[int, Optional[int]]:
    min_width: Union[str, int]
    max_width: Union[str, int, None]

    if WIDTH_PATTERN.match(width) is None:
        raise xpath_error('FOFD1340', f'Invalid width modifier {width!r}')
    elif '-' not in width:
        if width == '*':
            return 0, None
        min_width = int(width)
        if not min_width:
            raise xpath_error('FOFD1340', f'Invalid width modifier {width!r}')
        return min_width, None
    elif '*' not in width:
        min_width, max_width = map(int, width.split('-'))
        if not min_width or max_width < min_width:
            raise xpath_error('FOFD1340', f'Invalid width modifier {width!r}')
        return min_width, max_width
    else:
        min_width, max_width = width.split('-')
        if min_width == '*':
            min_width = 0
        else:
            min_width = int(min_width)
            if not min_width:
                raise xpath_error('FOFD1340', f'Invalid width modifier {width!r}')

        if max_width == '*':
            return min_width, None
        else:
            max_width = int(max_width)
            if not max_width:
                raise xpath_error('FOFD1340', f'Invalid width modifier {width!r}')
            return min_width, max_width
