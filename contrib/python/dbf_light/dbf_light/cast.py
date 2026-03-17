# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

from datetime import datetime
from decimal import Decimal


def parse_string(field, val):
    return val.decode(field.encoding).strip()


def parse_date(field, val):
    val = parse_string(field, val)

    if not val:
        return None

    return datetime.strptime(val, '%Y%m%d').date()


def parse_numeric(field, val):

    val = parse_string(field, val)

    if not val:
        return None

    if field.data['decimal_count']:
        val = Decimal(val)
    else:
        val = int(val)

    return val


def parse_float(field, val):
    val = parse_string(field, val)

    if not val:
        return None

    return float(val)


def parse_bool(field, val):
    val = parse_string(field, val).lower()

    if not val or val == '?':
        return None

    return val in {'t', 'y'}


def parse_memo(field, val):
    val = parse_string(field, val).lower()

    if not val:
        return None

    return int(val)


CAST_MAP = {
    b'C': parse_string,
    b'D': parse_date,
    b'N': parse_numeric,
    b'F': parse_float,
    b'L': parse_bool,
    b'M': parse_memo,
}
