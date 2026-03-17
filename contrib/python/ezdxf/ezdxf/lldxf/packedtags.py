# Copyright (c) 2018-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, MutableSequence, Sequence, Iterator, Optional
from typing_extensions import overload
from array import array
import numpy as np

from ezdxf.lldxf.tagwriter import AbstractTagWriter
from ezdxf.math import Matrix44
from ezdxf.tools.indexing import Index

from .tags import Tags
from .types import DXFTag


class TagList:
    """Store data in a standard Python ``list``."""

    __slots__ = ("values",)

    def __init__(self, data: Optional[Iterable] = None):
        self.values: MutableSequence = list(data or [])

    def clone(self) -> TagList:
        """Returns a deep copy."""
        return self.__class__(data=self.values)

    @classmethod
    def from_tags(cls, tags: Tags, code: int) -> TagList:
        """
        Setup list from iterable tags.

        Args:
            tags: tag collection as :class:`~ezdxf.lldxf.tags.Tags`
            code: group code to collect

        """
        return cls(data=(tag.value for tag in tags if tag.code == code))

    def clear(self) -> None:
        """Delete all data values."""
        del self.values[:]


class TagArray(TagList):
    """Store data in an :class:`array.array`. Array type is defined by class
    variable ``DTYPE``.
    """

    __slots__ = ("values",)
    # Defines the data type of array.array()
    DTYPE = "i"

    def __init__(self, data: Optional[Iterable] = None):
        self.values: array = array(self.DTYPE, data or [])

    def set_values(self, values: Iterable) -> None:
        """Replace data by `values`."""
        self.values[:] = array(self.DTYPE, values)


class VertexArray:
    """Store vertices in a ``numpy.ndarray``. Vertex size is defined by class variable
    ``VERTEX_SIZE``.
    """

    VERTEX_SIZE = 3
    __slots__ = ("values",)

    def __init__(self, data: Iterable[Sequence[float]] | None = None):
        size = self.VERTEX_SIZE
        if data:
            values = np.array(data, dtype=np.float64)
            if values.shape[1] != size:
                raise TypeError(
                    f"invalid data shape, expected (n, {size}), got {values.shape}"
                )
        else:
            values = np.ndarray((0, size), dtype=np.float64)
        self.values = values

    def __len__(self) -> int:
        """Count of vertices."""
        return len(self.values)

    @overload
    def __getitem__(self, index: int) -> Sequence[float]: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[Sequence[float]]: ...

    def __getitem__(self, index: int | slice):
        """Get vertex at `index`, extended slicing supported."""
        return self.values[index]

    def __setitem__(self, index: int, point: Sequence[float]) -> None:
        """Set vertex `point` at `index`, extended slicing not supported."""
        if isinstance(index, slice):
            raise TypeError("slicing not supported")
        self._set_point(self._index(index), point)

    def __delitem__(self, index: int | slice) -> None:
        """Delete vertex at `index`, extended slicing supported."""
        if isinstance(index, slice):
            self._del_points(self._slicing(index))
        else:
            self._del_points((index,))

    def __str__(self) -> str:
        """String representation."""
        return str(self.values)

    def __iter__(self) -> Iterator[Sequence[float]]:
        """Returns iterable of vertices."""
        return iter(self.values)

    def insert(self, pos: int, point: Sequence[float]):
        """Insert `point` in front of vertex at index `pos`.

        Args:
            pos: insert position
            point: point as tuple

        """
        size = self.VERTEX_SIZE
        if len(point) != size:
            raise ValueError(f"point requires exact {size} components.")

        values = self.values
        if len(values) == 0:
            self.extend((point,))
        ins_point = np.array((point,), dtype=np.float64)
        self.values = np.concatenate((values[0:pos], ins_point, values[pos:]))

    def clone(self) -> VertexArray:
        """Returns a deep copy."""
        return self.__class__(data=self.values)

    @classmethod
    def from_tags(cls, tags: Iterable[DXFTag], code: int = 10) -> VertexArray:
        """Setup point array from iterable tags.

        Args:
            tags: iterable of :class:`~ezdxf.lldxf.types.DXFVertex`
            code: group code to collect

        """
        vertices = [tag.value for tag in tags if tag.code == code]
        return cls(data=vertices)

    def _index(self, item) -> int:
        return Index(self).index(item, error=IndexError)

    def _slicing(self, index) -> Iterable[int]:
        return Index(self).slicing(index)

    def _set_point(self, index: int, point: Sequence[float]):
        size = self.VERTEX_SIZE
        if len(point) != size:
            raise ValueError(f"point requires exact {size} components.")
        self.values[index] = point  # type: ignore

    def _del_points(self, indices: Iterable[int]) -> None:
        del_flags = set(indices)
        survivors = np.array(
            [v for i, v in enumerate(self.values) if i not in del_flags], np.float64
        )
        self.values = survivors

    def export_dxf(self, tagwriter: AbstractTagWriter, code=10):
        for vertex in self.values:
            tagwriter.write_tag2(code, vertex[0])
            tagwriter.write_tag2(code + 10, vertex[1])
            if len(vertex) > 2:
                tagwriter.write_tag2(code + 20, vertex[2])

    def append(self, point: Sequence[float]) -> None:
        """Append `point`."""
        if len(point) != self.VERTEX_SIZE:
            raise ValueError(f"point requires exact {self.VERTEX_SIZE} components.")
        self.extend((point,))

    def extend(self, points: Iterable[Sequence[float]]) -> None:
        """Extend array by `points`."""
        vertices = np.array(points, np.float64)
        if vertices.shape[1] != self.VERTEX_SIZE:
            raise ValueError(f"points require exact {self.VERTEX_SIZE} components.")
        if len(self.values) == 0:
            self.values = vertices
        else:
            self.values = np.concatenate((self.values, vertices))

    def clear(self) -> None:
        """Delete all vertices."""
        self.values = np.ndarray((0, self.VERTEX_SIZE), dtype=np.float64)

    def set(self, points: Iterable[Sequence[float]]) -> None:
        """Replace all vertices by `points`."""
        vertices = np.array(points, np.float64)
        if vertices.shape[1] != self.VERTEX_SIZE:
            raise ValueError(f"points require exact {self.VERTEX_SIZE} components.")
        self.values = vertices

    def transform(self, m: Matrix44) -> None:
        """Transform vertices inplace by transformation matrix `m`."""
        if self.VERTEX_SIZE in (2, 3):
            m.transform_array_inplace(self.values, self.VERTEX_SIZE)
