import typing as tp

import operator


# ---

if tp.TYPE_CHECKING:
    def mapping_keys[K, V](mapping: tp.Mapping[K, V]) -> tp.Iterator[K]: ...
    def mapping_values[K, V](mapping: tp.Mapping[K, V]) -> tp.Iterator[V]: ...
    def mapping_items[K, V](mapping: tp.Mapping[K, V]) -> tp.Iterator[tp.Tuple[K, V]]: ...
else:
    mapping_keys = operator.methodcaller('keys')
    mapping_values = operator.methodcaller('values')
    mapping_items = operator.methodcaller('items')
