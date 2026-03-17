# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from functools import reduce
from operator import xor as xor_operator

from . import _typing as t
from ._codec.hydration import BrokenHydrationObject
from ._conf import iter_items
from .exceptions import BrokenRecordError
from .graph import (
    Node,
    Path,
    Relationship,
)
from .spatial import Point
from .time import (
    Date,
    DateTime,
    Duration,
    Time,
)


_T = t.TypeVar("_T")
_K: t.TypeAlias = int | str


class Record(tuple, t.Mapping):
    """
    Immutable, ordered collection of key-value pairs.

    It is generally closer to a :func:`collections.namedtuple` than to a
    :class:`OrderedDict` in as much as iteration of the collection will yield
    values rather than keys.
    """

    __keys: tuple[str]

    def __new__(cls, iterable=()):
        keys = []
        values = []
        for key, value in iter_items(iterable):
            keys.append(key)
            values.append(value)
        inst = tuple.__new__(cls, values)
        inst.__keys = tuple(keys)
        return inst

    def _broken_record_error(self, index):
        return BrokenRecordError(
            f"Record contains broken data at {index} ('{self.__keys[index]}')"
        )

    def _super_getitem_single(self, index):
        value = super().__getitem__(index)
        if isinstance(value, BrokenHydrationObject):
            raise self._broken_record_error(index) from value.error
        return value

    def __repr__(self) -> str:
        fields = " ".join(
            f"{field}={value!r}"
            for field, value in zip(
                self.__keys, super().__iter__(), strict=True
            )
        )
        return f"<{self.__class__.__name__} {fields}>"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        """
        Compare this record with another object for equality.

        In order to be flexible regarding comparison, the equality rules
        for a record permit comparison with any other Sequence or Mapping.

        :param other:
        :returns:
        """
        compare_as_sequence = isinstance(other, t.Sequence)
        compare_as_mapping = isinstance(other, t.Mapping)
        if compare_as_sequence and compare_as_mapping:
            other = t.cast(t.Mapping, other)
            return list(self) == list(other) and dict(self) == dict(other)
        elif compare_as_sequence:
            other = t.cast(t.Sequence, other)
            return list(self) == list(other)
        elif compare_as_mapping:
            other = t.cast(t.Mapping, other)
            return dict(self) == dict(other)
        else:
            return NotImplemented

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __iter__(self) -> t.Iterator[t.Any]:
        for i, v in enumerate(super().__iter__()):
            if isinstance(v, BrokenHydrationObject):
                raise self._broken_record_error(i) from v.error
            yield v

    def __getitem__(  # type: ignore[override]
        self, key: _K | slice
    ) -> t.Any:
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super().__getitem__(key)
            return self.__class__(zip(keys, values, strict=True))
        try:
            index = self.index(key)
        except IndexError:
            return None
        else:
            return self._super_getitem_single(index)

    def get(self, key: str, default: object = None) -> t.Any:
        """
        Obtain a value from the record by key.

        The ``default`` is returned if the key does not exist.

        :param key: a key
        :param default: default value

        :returns: a value
        """
        try:
            index = self.__keys.index(str(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return self._super_getitem_single(index)
        else:
            return default

    def index(self, key: _K) -> int:  # type: ignore[override]
        """
        Return the index of the given item.

        :param key: a key

        :returns: index
        """
        if isinstance(key, int):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, str):
            try:
                return self.__keys.index(key)
            except ValueError as exc:
                raise KeyError(key) from exc
        else:
            raise TypeError(key)

    def value(self, key: _K = 0, default: object = None) -> t.Any:
        """
        Obtain a single value from the record by index or key.

        If no index or key is specified, the first value is returned.
        If the specified item does not exist, the default value is returned.

        :param key: an index or key
        :param default: default value

        :returns: a single value
        """
        try:
            index = self.index(key)
        except (IndexError, KeyError):
            return default
        else:
            return self[index]

    def keys(self) -> list[str]:  # type: ignore[override]
        """
        Return the keys of the record.

        :returns: list of key names
        """
        return list(self.__keys)

    def values(self, *keys: _K) -> list[t.Any]:  # type: ignore[override]
        """
        Return the values of the record.

        The values returned can optionally be filtered to include only certain
        values by index or key.

        :param keys: indexes or keys of the items to include; if none
            are provided, all values will be included

        :returns: list of values
        """
        if keys:
            d: list[t.Any] = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """
        Return the fields of the record as a list of key and value tuples.

        :returns: a list of value tuples
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return [
            (self.__keys[i], self._super_getitem_single(i))
            for i in range(len(self))
        ]

    def data(self, *keys: _K) -> dict[str, t.Any]:
        """
        Return the record as a dictionary.

        Return the keys and values of this record as a dictionary, optionally
        including only certain values by index or key.
        Keys provided in the items that are not in the record will be inserted
        with a value of :data:`None`; indexes provided that are out of bounds
        will trigger an :exc:`IndexError`.

        This function provides a convenient but opinionated way to transform
        the record into a mostly JSON serializable format. It is mainly useful
        for interactive sessions and rapid prototyping.

        The transformation works as follows:

         * Nodes are transformed into dictionaries of their
           properties.

           * No indication of their original type remains.
           * Not all information is serialized (e.g., labels and element_id are
             absent).

         * Relationships are transformed to a tuple of
           ``(start_node, type, end_node)``, where the nodes are transformed
           as described above, and type is the relationship type name
           (:class:`str`).

           * No indication of their original type remains.
           * No other information (properties, element_id, start_node,
             end_node, ...) is serialized.

         * Paths are transformed into lists of nodes and relationships. No
           indication of the original type remains.
         * :class:`list` and :class:`dict` values are recursively transformed.
         * Every other type remains unchanged.

           * Spatial types and durations inherit from :class:`tuple`. Hence,
             they are JSON serializable, but, like graph types, type
             information will be lost in the process.
           * The remaining temporal types are not JSON serializable.

        You will have to implement a custom serializer should you need more
        control over the output format.

        :param keys: Indexes or keys of the items to include. If none are
            provided, all values will be included.

        :returns: dictionary of values, keyed by field name

        :raises: :exc:`IndexError` if an out-of-bounds index is specified.
        """
        return RecordExporter().transform(dict(self.items(*keys)))


class DataTransformer(ABC):
    """Abstract base class for transforming data from one form into another."""

    @abstractmethod
    def transform(self, x):
        """
        Transform a value, or collection of values.

        :param x: input value
        :returns: output value
        """


class RecordExporter(DataTransformer):
    """Transformer class used by the :meth:`.Record.data` method."""

    def transform(self, x):
        if isinstance(x, Node):
            return self.transform(dict(x))
        elif isinstance(x, Relationship):
            return (
                self.transform(dict(x.start_node)),
                x.__class__.__name__,
                self.transform(dict(x.end_node)),
            )
        elif isinstance(x, Path):
            path = [self.transform(x.start_node)]
            for i, relationship in enumerate(x.relationships):
                path.append(self.transform(relationship.__class__.__name__))
                path.append(self.transform(x.nodes[i + 1]))
            return path
        elif isinstance(x, (str, Point, Date, Time, DateTime, Duration)):
            return x
        elif isinstance(x, (t.Sequence, t.Set)):
            typ = type(x)
            return typ(map(self.transform, x))
        elif isinstance(x, t.Mapping):
            typ = type(x)
            return typ((k, self.transform(v)) for k, v in x.items())
        else:
            return x


class RecordTableRowExporter(DataTransformer):
    """Transformer class used by the :meth:`.Result.to_df` method."""

    @staticmethod
    def _escape_map_key(key: str) -> str:
        return key.replace("\\", "\\\\").replace(".", "\\.")

    def transform(self, x):
        assert isinstance(x, t.Mapping)
        typ = type(x)
        return typ(
            item
            for k, v in x.items()
            for item in self._transform(
                v, prefix=self._escape_map_key(k)
            ).items()
        )

    def _transform(self, x, prefix):
        if isinstance(x, Node):
            res = {
                f"{prefix}().element_id": x.element_id,
                f"{prefix}().labels": x.labels,
            }
            res.update((f"{prefix}().prop.{k}", v) for k, v in x.items())
            return res
        elif isinstance(x, Relationship):
            res = {
                f"{prefix}->.element_id": x.element_id,
                f"{prefix}->.start.element_id": x.start_node.element_id,
                f"{prefix}->.end.element_id": x.end_node.element_id,
                f"{prefix}->.type": x.__class__.__name__,
            }
            res.update((f"{prefix}->.prop.{k}", v) for k, v in x.items())
            return res
        elif isinstance(x, (Path, str)):
            return {prefix: x}
        elif isinstance(x, t.Sequence):
            return dict(
                item
                for i, v in enumerate(x)
                for item in self._transform(
                    v, prefix=f"{prefix}[].{i}"
                ).items()
            )
        elif isinstance(x, t.Mapping):
            typ = type(x)
            return typ(
                item
                for k, v in x.items()
                for item in self._transform(
                    v, prefix=f"{prefix}{{}}.{self._escape_map_key(k)}"
                ).items()
            )
        else:
            return {prefix: x}
