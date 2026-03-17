# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.

from __future__ import unicode_literals

import collections

from . import compat


def import_object(module_name, attribute_name):
    """Import an object from its absolute path.

    Example:
        >>> import_object('datetime', 'datetime')
        <type 'datetime.datetime'>
    """
    # Py2 compatibility: force str (i.e bytes) when importing.
    module = __import__(str(module_name), {}, {}, [str(attribute_name)], 0)
    return getattr(module, str(attribute_name))


def _safe_repr(obj):
    try:
        return log_repr(obj)
    except Exception:
        return '<bad_repr object at %s>' % id(obj)


class log_pprint(object):
    """Helper for properly printing args / kwargs passed to an object.

    Since it is only used with factory.debug(), the computation is
    performed lazily.
    """
    __slots__ = ['args', 'kwargs']

    def __init__(self, args=(), kwargs=None):
        self.args = args
        self.kwargs = kwargs or {}

    def __repr__(self):
        return repr(str(self))

    def __str__(self):
        return ', '.join(
            [
                _safe_repr(arg) for arg in self.args
            ] + [
                '%s=%s' % (key, _safe_repr(value))
                for key, value in self.kwargs.items()
            ]
        )


def log_repr(obj):
    """Generate a text-compatible repr of an object.

    Some projects have a tendency to generate bytes-style repr in Python2.
    """
    return compat.force_text(repr(obj))


class ResetableIterator(object):
    """An iterator wrapper that can be 'reset()' to its start."""
    def __init__(self, iterator, **kwargs):
        super(ResetableIterator, self).__init__(**kwargs)
        self.iterator = iter(iterator)
        self.past_elements = collections.deque()
        self.next_elements = collections.deque()

    def __iter__(self):
        while True:
            if self.next_elements:
                yield self.next_elements.popleft()
            else:
                try:
                    value = next(self.iterator)
                except StopIteration:
                    break
                else:
                    self.past_elements.append(value)
                    yield value

    def reset(self):
        self.next_elements.clear()
        self.next_elements.extend(self.past_elements)


class OrderedBase(object):
    """Marks a class as being ordered.

    Each instance (even from subclasses) will share a global creation counter.
    """

    CREATION_COUNTER_FIELD = '_creation_counter'

    def __init__(self, **kwargs):
        super(OrderedBase, self).__init__(**kwargs)
        if type(self) is not OrderedBase:
            bases = type(self).__mro__
            root = bases[bases.index(OrderedBase) - 1]
            if not hasattr(root, self.CREATION_COUNTER_FIELD):
                setattr(root, self.CREATION_COUNTER_FIELD, 0)
            next_counter = getattr(self, self.CREATION_COUNTER_FIELD)
            setattr(self, self.CREATION_COUNTER_FIELD, next_counter)
            setattr(root, self.CREATION_COUNTER_FIELD, next_counter + 1)


def sort_ordered_objects(items, getter=lambda x: x):
    """Sort an iterable of OrderedBase instances.

    Args:
        items (iterable): the objects to sort
        getter (callable or None): a function to extract the OrderedBase instance from an object.

    Examples:
        >>> sort_ordered_objects([x, y, z])
        >>> sort_ordered_objects(v.items(), getter=lambda e: e[1])
    """
    return sorted(items, key=lambda x: getattr(getter(x), OrderedBase.CREATION_COUNTER_FIELD, -1))
