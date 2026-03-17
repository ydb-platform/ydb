"""Tools to format messages to different formats."""

import datetime
import itertools

ENTRIES_SEP = '\t'
KV_SEP = '='


def dict_to_tskv(dct, tskv_format='', add_header=True):
    """Convert dictionary to string in TSKV format."""
    return items_to_tskv(
        dct.items(),
        tskv_format=tskv_format,
        add_header=add_header,
    )


def items_to_tskv(items, tskv_format='', add_header=True):
    """Return items to string in TSKV format."""
    data_segments = map(_format_pair, items)

    segments: map | itertools.chain
    if add_header:
        segments = itertools.chain(
            ('tskv', _format_pair(('tskv_format', tskv_format))),
            data_segments,
        )
    else:
        segments = data_segments

    return ENTRIES_SEP.join(segments)


def join_tskv(*escaped_strings):
    """Join strings in TSKV format."""
    return ENTRIES_SEP.join(escaped_strings)


def _escape(item):
    if isinstance(item, datetime.datetime):
        return _escape_datetime(item)
    return _escape_string(str(item))


def _escape_datetime(stamp):
    return stamp.isoformat()


def _escape_key(string):
    return _escape_string(string).replace('=', '\\=')


def _escape_string(string):
    if not isinstance(string, str):
        string = str(string)
    return (
        string.replace('\\', '\\\\')
        .replace('\t', '\\t')
        .replace('\r', '\\r')
        .replace('\n', '\\n')
        .replace('\0', '\\0')
    )


def _format_pair(value):
    return f'{_escape_key(value[0])}={_escape(value[1])}'
