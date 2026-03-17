# -*- coding: utf-8 -*-
"""Define the line_protocol handler."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from numbers import Integral

from pytz import UTC
from dateutil.parser import parse
from six import binary_type, text_type, integer_types, PY2

EPOCH = UTC.localize(datetime.utcfromtimestamp(0))


def _to_nanos(timestamp):
    delta = timestamp - EPOCH
    nanos_in_days = delta.days * 86400 * 10 ** 9
    nanos_in_seconds = delta.seconds * 10 ** 9
    nanos_in_micros = delta.microseconds * 10 ** 3
    return nanos_in_days + nanos_in_seconds + nanos_in_micros


def _convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(_get_unicode(timestamp), text_type):
        timestamp = parse(timestamp)

    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)

        ns = _to_nanos(timestamp)
        if precision is None or precision == 'n':
            return ns

        if precision == 'u':
            return ns / 10**3

        if precision == 'ms':
            return ns / 10**6

        if precision == 's':
            return ns / 10**9

        if precision == 'm':
            return ns / 10**9 / 60

        if precision == 'h':
            return ns / 10**9 / 3600

    raise ValueError(timestamp)


def _escape_tag(tag):
    tag = _get_unicode(tag, force=True)
    return tag.replace(
        "\\", "\\\\"
    ).replace(
        " ", "\\ "
    ).replace(
        ",", "\\,"
    ).replace(
        "=", "\\="
    ).replace(
        "\n", "\\n"
    )


def _escape_tag_value(value):
    ret = _escape_tag(value)
    if ret.endswith('\\'):
        ret += ' '
    return ret


def quote_ident(value):
    """Indent the quotes."""
    return "\"{}\"".format(value
                           .replace("\\", "\\\\")
                           .replace("\"", "\\\"")
                           .replace("\n", "\\n"))


def quote_literal(value):
    """Quote provided literal."""
    return "'{}'".format(value
                         .replace("\\", "\\\\")
                         .replace("'", "\\'"))


def _is_float(value):
    try:
        float(value)
    except (TypeError, ValueError):
        return False

    return True


def _escape_value(value):
    if value is None:
        return ''

    value = _get_unicode(value)
    if isinstance(value, text_type):
        return quote_ident(value)

    if isinstance(value, integer_types) and not isinstance(value, bool):
        return str(value) + 'i'

    if isinstance(value, bool):
        return str(value)

    if _is_float(value):
        return repr(float(value))

    return str(value)


def _get_unicode(data, force=False):
    """Try to return a text aka unicode object from the given data."""
    if isinstance(data, binary_type):
        return data.decode('utf-8')

    if data is None:
        return ''

    if force:
        if PY2:
            return unicode(data)
        return str(data)

    return data


def make_line(measurement, tags=None, fields=None, time=None, precision=None):
    """Extract the actual point from a given measurement line."""
    tags = tags or {}
    fields = fields or {}

    line = _escape_tag(_get_unicode(measurement))

    # tags should be sorted client-side to take load off server
    tag_list = []
    for tag_key in sorted(tags.keys()):
        key = _escape_tag(tag_key)
        value = _escape_tag(tags[tag_key])

        if key != '' and value != '':
            tag_list.append(
                "{key}={value}".format(key=key, value=value)
            )

    if tag_list:
        line += ',' + ','.join(tag_list)

    field_list = []
    for field_key in sorted(fields.keys()):
        key = _escape_tag(field_key)
        value = _escape_value(fields[field_key])

        if key != '' and value != '':
            field_list.append("{key}={value}".format(
                key=key,
                value=value
            ))

    if field_list:
        line += ' ' + ','.join(field_list)

    if time is not None:
        timestamp = _get_unicode(str(int(
            _convert_timestamp(time, precision)
        )))
        line += ' ' + timestamp

    return line


def make_lines(data, precision=None):
    """Extract points from given dict.

    Extracts the points from the given dict and returns a Unicode string
    matching the line protocol introduced in InfluxDB 0.9.0.
    """
    lines = []
    static_tags = data.get('tags')
    for point in data['points']:
        if static_tags:
            tags = dict(static_tags)  # make a copy, since we'll modify
            tags.update(point.get('tags') or {})
        else:
            tags = point.get('tags') or {}

        line = make_line(
            point.get('measurement', data.get('measurement')),
            tags=tags,
            fields=point.get('fields'),
            precision=precision,
            time=point.get('time')
        )
        lines.append(line)

    return '\n'.join(lines) + '\n'
