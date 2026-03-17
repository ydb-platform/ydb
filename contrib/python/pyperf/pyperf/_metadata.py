import collections

from pyperf._formatter import (format_number, format_seconds, format_filesize,
                               UNIT_FORMATTERS)


METADATA_VALUE_TYPES = (int, str, float)
NUMBER_TYPES = (int, float)


def _common_metadata(metadatas):
    if not metadatas:
        return {}

    metadata = dict(metadatas[0])
    for run_metadata in metadatas[1:]:
        for key in set(metadata) - set(run_metadata):
            del metadata[key]
        for key in set(run_metadata) & set(metadata):
            if run_metadata[key] != metadata[key]:
                del metadata[key]
    return metadata


def format_generic(value):
    if not isinstance(value, str):
        return str(value)

    return value


def format_system_load(load):
    # Format system load read from /proc/loadavg on Linux (ex: 0.12)
    return '%.2f' % load


def is_strictly_positive(value):
    return (value >= 1)


def is_positive(value):
    return (value >= 0)


def is_tags(value):
    if not isinstance(value, list):
        return False
    return all(isinstance(x, str) and x not in ('all', '') for x in value)


def parse_load_avg(value):
    if isinstance(value, NUMBER_TYPES):
        return value
    else:
        # special case for load_avg_1min on pyperf < 0.7.2
        return float(value)


def format_noop(value):
    return value


# types: accepted types
_MetadataInfo = collections.namedtuple('_MetadataInfo', 'formatter types check_value unit')

BYTES = _MetadataInfo(format_filesize, (int,), is_strictly_positive, 'byte')
DATETIME = _MetadataInfo(format_noop, (str,), None, None)
LOOPS = _MetadataInfo(format_number, (int,), is_strictly_positive, 'integer')
WARMUPS = _MetadataInfo(format_number, (int,), is_positive, 'integer')
SECONDS = _MetadataInfo(format_seconds, NUMBER_TYPES, is_positive, 'second')
TAGS = _MetadataInfo(format_generic, (list,), is_tags, 'tag')

# Registry of metadata keys
METADATA = {
    'loops': LOOPS,
    'inner_loops': LOOPS,

    'duration': SECONDS,
    'uptime': SECONDS,
    'load_avg_1min': _MetadataInfo(format_system_load, NUMBER_TYPES, is_positive, None),

    'mem_max_rss': BYTES,
    'mem_peak_pagefile_usage': BYTES,
    'command_max_rss': BYTES,

    'unit': _MetadataInfo(format_noop, (str,), UNIT_FORMATTERS.__contains__, None),
    'date': DATETIME,
    'boot_time': DATETIME,

    'calibrate_loops': LOOPS,
    'recalibrate_loops': LOOPS,
    'calibrate_warmups': WARMUPS,
    'recalibrate_warmups': WARMUPS,
    'tags': TAGS,
}

DEFAULT_METADATA_INFO = _MetadataInfo(format_generic, METADATA_VALUE_TYPES, None, None)


def get_metadata_info(name):
    return METADATA.get(name, DEFAULT_METADATA_INFO)


def check_metadata(name, value):
    info = get_metadata_info(name)

    if not isinstance(name, str):
        raise TypeError("metadata name must be a string, got %s"
                        % type(name).__name__)

    if not isinstance(value, info.types):
        raise ValueError("invalid metadata %r value type: got %r"
                         % (name, type(value).__name__))

    if info.check_value is not None and not info.check_value(value):
        raise ValueError("invalid metadata %r value: %r"
                         % (name, value))


def parse_metadata(metadata):
    result = {}
    for name, value in metadata.items():
        if isinstance(value, str):
            value = value.strip()
            if '\n' in value or '\r' in value:
                raise ValueError("newline characters are not allowed "
                                 "in metadata values: %r" % value)
            if not value:
                raise ValueError("metadata %r value is empty" % name)
        check_metadata(name, value)
        result[name] = value
    return result


def format_metadata(name, value):
    info = get_metadata_info(name)
    return info.formatter(value)


class Metadata:
    def __init__(self, name, value):
        self._name = name
        self._value = value

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    def __str__(self):
        info = get_metadata_info(self._name)
        return info.formatter(self._value)

    def __eq__(self, other):
        if not isinstance(other, Metadata):
            return False
        return (self._name == other._name and self._value == other._value)

    def __repr__(self):
        return ('<pyperf.Metadata name=%r value=%r>'
                % (self._name, self._value))


def _exclude_common_metadata(metadata, common_metadata):
    if common_metadata:
        metadata = {key: value for key, value in metadata.items()
                    if key not in common_metadata}
    return metadata
