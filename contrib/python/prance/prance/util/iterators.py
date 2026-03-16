"""This submodule contains specialty iterators over specs."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


def item_iterator(value, path=()):
    """
    Return item iterator over the a nested dict- or list-like object.

    Returns each item value as the second item to unpack, and a tuple path to the
    item as the first value - in that, it behaves much like viewitems(). For list
    like values, the path is made up of numeric indices.

    Given a spec such as this::

      spec = {
        'foo': 42,
        'bar': {
          'some': 'dict',
        },
        'baz': [
          { 1: 2 },
          { 3: 4 },
        ]
      }

    Here, (parts of) the yielded values would be:

      ======== =============
      item     path
      ======== =============
      [...]    ('baz',)
      { 1: 2 } ('baz', 0)
      2        ('baz', 0, 1)
      ======== =============

    :param dict/list value: The specifications to iterate over.
    :return: An iterator over all items in the value.
    :rtype: iterator
    """
    # Yield the top-level object, always
    yield path, value

    from collections.abc import Mapping, Sequence

    # For dict and list like objects, we also need to yield each item
    # recursively.
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from item_iterator(item, path + (key,))
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for idx, item in enumerate(value):
            yield from item_iterator(item, path + (idx,))


def reference_iterator(specs, path=()):
    """
    Iterate through the given specs, returning only references.

    The iterator returns three values:
      - The key, mimicking the behaviour of other iterators, although
        it will always equal '$ref'
      - The value
      - The path to the item. This is a tuple of all the item's ancestors,
        in sequence, so that you can reasonably easily find the containing
        item. It does not include the final '$ref' key.

    :param dict specs: The specifications to iterate over.
    :return: An iterator over all references in the specs.
    :rtype: iterator
    """
    # We need to iterate through the nested specification dict, so let's
    # start with an appropriate iterator. We can immediately optimize it by
    # only returning '$ref' items.
    for item_path, item in item_iterator(specs, path):
        if len(item_path) <= 0:
            continue
        key = item_path[-1]
        if key == "$ref":
            yield key, item, item_path[:-1]
