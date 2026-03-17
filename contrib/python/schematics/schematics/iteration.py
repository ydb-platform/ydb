"""
Core loop over the data structures according to a defined schema.
"""

from __future__ import unicode_literals, absolute_import
from collections import namedtuple

from .compat import iteritems
from .undefined import Undefined

try:
    # optional type checking
    import typing
    if typing.TYPE_CHECKING:
        from typing import Mapping, Tuple, Callable, Optional, Any, Iterable
        from .schema import Schema
except ImportError:
    pass

Atom = namedtuple('Atom', ('name', 'field', 'value'))
Atom.__new__.__defaults__ = (None,) * len(Atom._fields)


def atoms(schema, mapping, keys=tuple(Atom._fields), filter=None):
    # type: (Schema, Mapping, Tuple[str, str, str], Optional[Callable[[Atom], bool]]) -> Iterable[Atom]
    """
    Iterator for the atomic components of a model definition and relevant
    data that creates a 3-tuple of the field's name, its type instance and
    its value.

    :type schema: schematics.schema.Schema
    :param schema:
        The Schema definition.
    :type mapping: Mapping
    :param mapping:
        The structure where fields from schema are mapped to values. The only
        expectation for this structure is that it implements a ``Mapping``
        interface.
    :type keys: Tuple[str, str, str]
    :param keys:
        Tuple specifying the output of the iterator. Valid keys are:
            `name`: the field name
            `field`: the field descriptor object
            `value`: the current value set on the field
        Specifying invalid keys will raise an exception.
    :type filter: Optional[Callable[[Atom], bool]]
    :param filter:
        Function to filter out atoms from the iteration.

    :rtype: Iterable[Atom]
    """
    if not set(keys).issubset(Atom._fields):
        raise TypeError('invalid key specified')

    has_name = 'name' in keys
    has_field = 'field' in keys
    has_value = (mapping is not None) and ('value' in keys)

    for field_name, field in iteritems(schema.fields):
        value = Undefined

        if has_value:
            try:
                value = mapping[field_name]
            except Exception:
                value = Undefined

        atom_tuple = Atom(
            name=field_name if has_name else None,
            field=field if has_field else None,
            value=value)
        if filter is None:
            yield atom_tuple
        elif filter(atom_tuple):
            yield atom_tuple


class atom_filter:
    """Group for the default filter functions."""

    @staticmethod
    def has_setter(atom):
        return getattr(atom.field, 'fset', None) is not None

    @staticmethod
    def not_setter(atom):
        return not atom_filter.has_setter(atom)
