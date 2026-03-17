class Symbol:
    """
    Symbol Usage Explanation:

    $add: Indicates keys or indices where new elements have been added.
    $discard: Indicates elements that have been removed from a set.
    $delete: Indicates keys or indices where elements have been deleted.
    $insert: Used in lists to specify new elements inserted at specific indices.
    $update: Used to indicate that the value of an existing key has changed.
    $replace: Used to completely replace the value at a given location.

    These symbols are used within the diff structures returned by methods of JsonDiffer classes to represent different
    types of changes between two JSON structures. For example:

    - In a dictionary, $add might be used to show new keys added, $delete to show keys that were removed, and $update
      for keys whose values have changed.
    - In a list, $insert could indicate new items added at specific positions, and $delete could show items removed
      from specific positions.
    - The $replace symbol is generally used when an entire section of the JSON (be it a list, dict, or value)
      is replaced with another.

    These symbols help in succinctly representing changes in a structured way, making it easier to apply or revert
    changes programmatically.
    """
    def __init__(self, label):
        self._label = label

    @property
    def label(self):
        return self._label

    def __repr__(self):
        return self.label

    def __str__(self):
        return "$" + self.label

    def __eq__(self, other):
        if not isinstance(other, Symbol):
            return False
        return self.label == other.label

    def __hash__(self) -> int:
        return hash(self.label)


missing = Symbol('missing')
identical = Symbol('identical')
delete = Symbol('delete')
insert = Symbol('insert')
update = Symbol('update')
add = Symbol('add')
discard = Symbol('discard')
replace = Symbol('replace')
left = Symbol('left')
right = Symbol('right')

_all_symbols_ = [
    missing,
    identical,
    delete,
    insert,
    update,
    add,
    discard,
    replace,
    left,
    right
]

__all__ = [
    'missing',
    'identical',
    'delete',
    'insert',
    'update',
    'add',
    'discard',
    'replace',
    'left',
    'right',
    '_all_symbols_'
]
