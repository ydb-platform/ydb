from collections.abc import Callable, Sequence
from operator import itemgetter

from magic_filter import MagicFilter

ItemsGetter = Callable[[dict], Sequence]
ItemsGetterVariant = str | ItemsGetter | MagicFilter | Sequence


def _get_identity(items: Sequence) -> ItemsGetter:
    def identity(data) -> Sequence:
        return items

    return identity


def _get_magic_getter(f: MagicFilter) -> ItemsGetter:
    def items_magic(data: dict) -> Sequence:
        items = f.resolve(data)
        if isinstance(items, Sequence):
            return items
        else:
            return []

    return items_magic


def get_items_getter(attr_val: ItemsGetterVariant) -> ItemsGetter:
    if isinstance(attr_val, str):
        return itemgetter(attr_val)
    elif isinstance(attr_val, MagicFilter):
        return _get_magic_getter(attr_val)
    elif isinstance(attr_val, Callable):
        return attr_val
    else:
        return _get_identity(attr_val)
