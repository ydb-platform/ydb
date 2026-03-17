from collections.abc import Mapping
from typing import Any, Callable, Dict, Iterator, List, Tuple, Union

# Optional cython extension:
try:
    from aiochclient._types import empty_convertor, what_py_converter
except ImportError:
    from aiochclient.types import empty_convertor, what_py_converter

__all__ = ["RecordsFabric", "Record", "FromJsonFabric"]


class Record(Mapping):
    """Lightweight, memory efficient objects with full mapping interface, where
    you can get fields by names or by indexes.

    Usage:

    .. code-block:: python

        row = await client.fetchrow("SELECT a, b FROM t WHERE a=1")

        assert row["a"] == 1
        assert row[0] == 1
        assert row[:] == (1, (dt.date(2018, 9, 8), 3.14))
        assert list(row.keys()) == ["a", "b"]
        assert list(row.values()) == [1, (dt.date(2018, 9, 8), 3.14)]

    """

    __slots__ = ("_converters", "_decoded", "_names", "_row")

    def __init__(self, row: bytes, names: Dict[str, Any], converters: List[Callable]):
        self._row: Union[bytes, Tuple[Any]] = row
        if not self._row:
            # in case of empty row
            self._decoded = True
            self._converters = []
            self._names = {}
        else:
            self._decoded = False
            self._converters = converters
            self._names = names

    def __getitem__(self, key: Union[str, int, slice]) -> Any:
        self._decode()
        return self._getitem(key)

    def _getitem(self, key: Union[str, int, slice]) -> Any:
        if type(key) == str:
            try:
                return self._row[self._names[key]]
            except KeyError:
                if not self._row:
                    raise KeyError(
                        "Empty row. May be it is result of 'WITH TOTALS' query."
                    )
                raise KeyError(f"No fields with name '{key}'")
        try:
            return self._row[key]
        except IndexError:
            if not self._row:
                raise IndexError(
                    "Empty row. May be it is result of 'WITH TOTALS' query."
                )
            raise IndexError(f"No fields with index '{key}'")

    def __iter__(self) -> Iterator:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)

    def _decode(self):
        if self._decoded:
            return None
        self._row = tuple(
            converter(val)
            for converter, val in zip(self._converters, self._row.split(b"\t"))
        )
        self._decoded = True


class RecordsFabric:
    __slots__ = ("converters", "names")

    def __init__(self, tps: bytes, names: bytes, convert: bool = True):
        names = names.decode().strip().split("\t")
        self.names = {key: index for (index, key) in enumerate(names)}
        if convert:
            self.converters = [
                what_py_converter(tp) for tp in tps.decode().strip().split("\t")
            ]
        else:
            self.converters = [
                empty_convertor for _ in tps.decode().strip().split("\t")
            ]

    def new(self, row: bytes) -> Record:
        return Record(
            row=row[:-1],  # because of delimiter
            names=self.names,
            converters=self.converters,
        )


class FromJsonFabric:
    def __init__(self, loads):
        self.loads = loads

    def new(self, row: bytes) -> Any:
        return self.loads(row)
