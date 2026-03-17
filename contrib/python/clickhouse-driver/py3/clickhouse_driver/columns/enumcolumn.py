from enum import Enum
from collections import OrderedDict

from .. import errors
from .intcolumn import IntColumn

invalid_names_for_python_enum = frozenset(['mro', ''])


class EnumColumn(IntColumn):
    py_types = (Enum, int, str)

    def __init__(self, name_by_value, value_by_name, **kwargs):
        self.name_by_value = name_by_value
        self.value_by_name = value_by_name
        super(EnumColumn, self).__init__(**kwargs)

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value
        name_by_value = self.name_by_value
        value_by_name = self.value_by_name

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            source_value = item.name if isinstance(item, Enum) else item

            # Check real enum value
            try:
                if isinstance(source_value, str):
                    items[i] = value_by_name[source_value]
                else:
                    items[i] = value_by_name[name_by_value[source_value]]
            except (ValueError, KeyError):
                choices = ', '.join(
                    "'{}' = {}".format(name.replace("'", r"\'"), value)
                    for name, value in value_by_name.items()
                )
                enum_str = '{}({})'.format(self.ch_type, choices)

                raise errors.LogicalError(
                    "Unknown element '{}' for type {}"
                    .format(source_value, enum_str)
                )

    def after_read_items(self, items, nulls_map=None):
        name_by_value = self.name_by_value

        if nulls_map is None:
            return tuple(name_by_value[item] for item in items)
        else:
            return tuple(
                (None if is_null else name_by_value[items[i]])
                for i, is_null in enumerate(nulls_map)
            )


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = 'b'
    int_size = 1


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = 'h'
    int_size = 2


def create_enum_column(spec, column_options):
    if spec.startswith('Enum8'):
        params = spec[6:-1]
        cls = Enum8Column
    else:
        params = spec[7:-1]
        cls = Enum16Column

    name_by_value, value_by_name = _parse_options(params)

    return cls(name_by_value, value_by_name, **column_options)


def _parse_options(option_string):
    name_by_value, value_by_name = {}, OrderedDict()
    after_name = False
    escaped = False
    quote_character = None
    name = ''
    value = ''

    for ch in option_string:
        if escaped:
            name += ch
            escaped = False  # accepting escaped character

        elif after_name:
            if ch in (' ', '='):
                pass
            elif ch == ',':
                value = int(value)
                name_by_value[value] = name
                value_by_name[name] = value
                after_name = False
                name = ''
                value = ''  # reset before collecting new option
            else:
                value += ch

        elif quote_character:
            if ch == '\\':
                escaped = True
            elif ch == quote_character:
                quote_character = None
                after_name = True  # start collecting option value
            else:
                name += ch

        else:
            if ch == "'":
                quote_character = ch

    if after_name:
        value = int(value)
        name_by_value[value] = name
        value_by_name[name] = value

    return name_by_value, value_by_name
