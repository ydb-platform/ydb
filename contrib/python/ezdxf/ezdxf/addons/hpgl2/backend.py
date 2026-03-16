#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Sequence, NamedTuple, Any, Iterator
from typing_extensions import Self
import abc
import copy
import enum
import math
from .deps import (
    Vec2,
    Path,
    colors,
    Matrix44,
    BoundingBox2d,
)
from .properties import Properties, Pen
from ezdxf.npshapes import NumpyPath2d, NumpyPoints2d

# Page coordinates are always plot units:
# 1 plot unit (plu) = 0.025mm
# 40 plu = 1mm
# 1016 plu = 1 inch
# 3.39 plu = 1 dot @300 dpi
# positive x-axis is horizontal from left to right
# positive y-axis is vertical from bottom to top


class Backend(abc.ABC):
    """Abstract base class for implementing a low level output backends."""

    @abc.abstractmethod
    def draw_polyline(self, properties: Properties, points: Sequence[Vec2]) -> None:
        """Draws a polyline from a sequence `points`. The input coordinates are page
        coordinates in plot units. The `points` sequence can contain 0 or more
        points!

        Args:
            properties: display :class:`Properties` for the polyline
            points: sequence of :class:`ezdxf.math.Vec2` instances

        """
        ...

    @abc.abstractmethod
    def draw_paths(
        self, properties: Properties, paths: Sequence[Path], filled: bool
    ) -> None:
        """Draws filled or outline paths from the sequence of `paths`. The input coordinates
        are page coordinates in plot units. The `paths` sequence can contain 0 or more
        single :class:`~ezdxf.path.Path` instances. Draws outline paths if
        Properties.FillType is NONE and filled paths otherwise.

        Args:
            properties: display :class:`Properties` for the filled polygon
            paths: sequence of single :class:`ezdxf.path.Path` instances
            filled: draw filled paths if ``True`` otherwise outline paths

        """
        ...


class RecordType(enum.Enum):
    POLYLINE = enum.auto()
    FILLED_PATHS = enum.auto()
    OUTLINE_PATHS = enum.auto()


class DataRecord(NamedTuple):
    type: RecordType
    property_hash: int
    data: Any


class Recorder(Backend):
    """The :class:`Recorder` class records the output of the :class:`Plotter` class.

    All input coordinates are page coordinates:

    - 1 plot unit (plu) = 0.025mm
    - 40 plu = 1 mm
    - 1016 plu = 1 inch

    """

    def __init__(self) -> None:
        self._records: list[DataRecord] = []
        self._properties: dict[int, Properties] = {}
        self._pens: Sequence[Pen] = []

    def player(self) -> Player:
        """Returns a :class:`Player` instance with the original recordings. Make a copy
        of this player to protect the original recordings from being modified::

            safe_player = recorder.player().copy()

        """
        return Player(self._records, self._properties)

    def draw_polyline(self, properties: Properties, points: Sequence[Vec2]) -> None:
        self.store(RecordType.POLYLINE, properties, NumpyPoints2d(points))

    def draw_paths(
        self, properties: Properties, paths: Sequence[Path], filled: bool
    ) -> None:
        data = tuple(map(NumpyPath2d, paths))
        record_type = RecordType.FILLED_PATHS if filled else RecordType.OUTLINE_PATHS
        self.store(record_type, properties, data)

    def store(self, record_type: RecordType, properties: Properties, args) -> None:
        prop_hash = properties.hash()
        if prop_hash not in self._properties:
            self._properties[prop_hash] = properties.copy()
        self._records.append(DataRecord(record_type, prop_hash, args))
        if len(self._pens) != len(properties.pen_table):
            self._pens = list(properties.pen_table.values())


class Player:
    """This class replays the recordings of the :class:`Recorder` class on another
    backend. The class can modify the recorded output.
    """

    def __init__(self, records: list[DataRecord], properties: dict[int, Properties]):
        self._records: list[DataRecord] = records
        self._properties: dict[int, Properties] = properties
        self._bbox = BoundingBox2d()

    def __copy__(self) -> Self:
        """Returns a new :class:`Player` instance with a copy of recordings."""
        records = copy.deepcopy(self._records)
        player = self.__class__(records, self._properties)
        player._bbox = self._bbox.copy()
        return player

    copy = __copy__

    def recordings(self) -> Iterator[tuple[RecordType, Properties, Any]]:
        """Yields all recordings as `(RecordType, Properties, Data)` tuples.

        The content of the `Data` field is determined by the enum :class:`RecordType`:

        - :attr:`RecordType.POLYLINE` returns a :class:`NumpyPoints2d` instance
        - :attr:`RecordType.FILLED_POLYGON` returns a tuple of :class:`NumpyPath2d` instances

        """
        props = self._properties
        for record in self._records:
            yield record.type, props[record.property_hash], record.data

    def bbox(self) -> BoundingBox2d:
        """Returns the bounding box of all recorded polylines and polygons as
        :class:`~ezdxf.math.BoundingBox2d`.
        """
        if not self._bbox.has_data:
            self.update_bbox()
        return self._bbox

    def update_bbox(self) -> None:
        points: list[Vec2] = []
        for record in self._records:
            if record.type == RecordType.POLYLINE:
                points.extend(record.data.extents())
            else:
                for path in record.data:
                    points.extend(path.extents())
        self._bbox = BoundingBox2d(points)

    def replay(self, backend: Backend) -> None:
        """Replay the recording on another backend."""
        current_props = Properties()
        props = self._properties
        for record in self._records:
            current_props = props.get(record.property_hash, current_props)
            if record.type == RecordType.POLYLINE:
                backend.draw_polyline(current_props, record.data.vertices())
            else:
                paths = [p.to_path2d() for p in record.data]
                backend.draw_paths(
                    current_props, paths, filled=record.type == RecordType.FILLED_PATHS
                )

    def transform(self, m: Matrix44) -> None:
        """Transforms the recordings by a transformation matrix `m` of type
        :class:`~ezdxf.math.Matrix44`.
        """
        for record in self._records:
            if record.type == RecordType.POLYLINE:
                record.data.transform_inplace(m)
            else:
                for path in record.data:
                    path.transform_inplace(m)

        if self._bbox.has_data:
            # fast, but maybe inaccurate update
            self._bbox = BoundingBox2d(m.fast_2d_transform(self._bbox.rect_vertices()))

    def sort_filled_paths(self) -> None:
        """Sort filled paths by descending luminance (from light to dark).

        This also changes the plot order in the way that all filled paths are plotted
        before polylines and outline paths.
        """
        fillings = []
        outlines = []
        current = Properties()
        props = self._properties
        for record in self._records:
            if record.type == RecordType.FILLED_PATHS:
                current = props.get(record.property_hash, current)
                key = colors.luminance(current.resolve_fill_color())
                fillings.append((key, record))
            else:
                outlines.append(record)

        fillings.sort(key=lambda r: r[0], reverse=True)
        records = [sort_rec[1] for sort_rec in fillings]
        records.extend(outlines)
        self._records = records


def placement_matrix(
    bbox: BoundingBox2d, sx: float = 1.0, sy: float = 1.0, rotation: float = 0.0
) -> Matrix44:
    """Returns a matrix to place the bbox in the first quadrant of the coordinate
    system (+x, +y).
    """
    if abs(sx) < 1e-9:
        sx = 1.0
    if abs(sy) < 1e-9:
        sy = 1.0
    m = Matrix44.scale(sx, sy, 1.0)
    if rotation:
        m @= Matrix44.z_rotate(math.radians(rotation))
    corners = m.fast_2d_transform(bbox.rect_vertices())
    tx, ty = BoundingBox2d(corners).extmin
    return m @ Matrix44.translate(-tx, -ty, 0)
