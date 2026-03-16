#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Optional, Iterator, Sequence
from typing_extensions import Self, TypeAlias
import abc

import numpy as np
import numpy.typing as npt

from ezdxf.math import (
    Matrix44,
    UVec,
    Vec2,
    Vec3,
    has_clockwise_orientation,
    Bezier3P,
    Bezier4P,
    BoundingBox2d,
    BoundingBox,
)
from ezdxf.path import (
    Path,
    Command,
    PathElement,
    LineTo,
    MoveTo,
    Curve3To,
    Curve4To,
    nesting,
)

try:
    from ezdxf.acc import np_support
except ImportError:
    np_support = None

__all__ = [
    "NumpyPath2d",
    "NumpyPoints2d",
    "NumpyPoints3d",
    "NumpyShapesException",
    "EmptyShapeError",
    "to_qpainter_path",
    "to_matplotlib_path",
    "single_paths",
    "orient_paths",
]

# comparing Command.<attrib> to ints is very slow
CMD_MOVE_TO = int(Command.MOVE_TO)
CMD_LINE_TO = int(Command.LINE_TO)
CMD_CURVE3_TO = int(Command.CURVE3_TO)
CMD_CURVE4_TO = int(Command.CURVE4_TO)


class NumpyShapesException(Exception):
    pass


class EmptyShapeError(NumpyShapesException):
    pass


CommandNumpyType: TypeAlias = np.int8
VertexNumpyType: TypeAlias = np.float64
EMPTY_SHAPE = np.array([], dtype=VertexNumpyType)
NO_COMMANDS = np.array([], dtype=CommandNumpyType)


class NumpyShape2d(abc.ABC):
    """This is an optimization to store many 2D paths and polylines in a compact way
    without sacrificing basic functions like transformation and bounding box calculation.
    """

    _vertices: npt.NDArray[VertexNumpyType] = EMPTY_SHAPE

    def extents(self) -> tuple[Vec2, Vec2]:
        """Returns the extents of the bounding box as tuple (extmin, extmax)."""
        v = self._vertices
        if len(v) > 0:
            return Vec2(v.min(0)), Vec2(v.max(0))
        else:
            raise EmptyShapeError("empty shape has no extends")

    @abc.abstractmethod
    def clone(self) -> Self:
        ...

    def np_vertices(self) -> npt.NDArray[VertexNumpyType]:
        return self._vertices

    def transform_inplace(self, m: Matrix44) -> None:
        """Transforms the vertices of the shape inplace."""
        v = self._vertices
        if len(v) == 0:
            return
        m.transform_array_inplace(v, 2)

    def vertices(self) -> list[Vec2]:
        """Returns the shape vertices as list of :class:`Vec2` 
        e.g. [Vec2(1, 2), Vec2(3, 4), ...] 
        """
        return [Vec2(v) for v in self._vertices]

    def to_tuples(self) -> list[tuple[float, float]]:
        """Returns the shape vertices as list of 2-tuples 
        e.g. [(1, 2), (3, 4), ...]
        """
        return [tuple(v) for v in self._vertices]
    
    def to_list(self) -> list[list[float]]:
        """Returns the shape vertices as list of lists 
        e.g. [[1, 2], [3, 4], ...]
        """
        return self._vertices.tolist()  # type: ignore
    
    def bbox(self) -> BoundingBox2d:
        """Returns the bounding box of all vertices."""
        return BoundingBox2d(self.extents())


class NumpyPoints2d(NumpyShape2d):
    """Represents an array of 2D points stored as a ndarray."""

    def __init__(self, points: Optional[Iterable[Vec2 | Vec3]]) -> None:
        if points:
            self._vertices = np.array(
                [(v.x, v.y) for v in points], dtype=VertexNumpyType
            )

    def clone(self) -> Self:
        clone = self.__class__(None)
        clone._vertices = self._vertices.copy()
        return clone

    __copy__ = clone

    def __len__(self) -> int:
        return len(self._vertices)


class NumpyPath2d(NumpyShape2d):
    """Represents a 2D path, the path control vertices and commands are stored as ndarray.

    This class cannot build paths from scratch and is therefore not a drop-in replacement
    for the :class:`ezdxf.path.Path` class. Operations like transform and reverse are
    done inplace to utilize the `numpy` capabilities. This behavior is different from the
    :class:`ezdxf.path.Path` class!!!

    Construct new paths by the :class:`Path` class and convert them to
    :class:`NumpyPath2d` instances::

        path = Path((0, 0))
        path.line_to((50, 70))
        ...
        path2d = NumpyPath2d(path)

    """

    _commands: npt.NDArray[CommandNumpyType]

    def __init__(self, path: Optional[Path]) -> None:
        if path is None:
            self._vertices = EMPTY_SHAPE
            self._commands = NO_COMMANDS
            return
        # (v.x, v.y) is 4x faster than Vec2(v), see profiling/numpy_array_setup.py
        vertices = [(v.x, v.y) for v in path.control_vertices()]
        if len(vertices) == 0:
            try:  # control_vertices() does not return the start point of empty paths
                vertices = [Vec2(path.start)]
            except IndexError:
                vertices = []
        self._vertices = np.array(vertices, dtype=VertexNumpyType)
        self._commands = np.array(path.command_codes(), dtype=CommandNumpyType)

    def __len__(self) -> int:
        return len(self._commands)

    @property
    def start(self) -> Vec2:
        """Returns the start point as :class:`~ezdxf.math.Vec2` instance."""
        return Vec2(self._vertices[0])

    @property
    def end(self) -> Vec2:
        """Returns the end point as :class:`~ezdxf.math.Vec2` instance."""
        return Vec2(self._vertices[-1])

    def control_vertices(self) -> list[Vec2]:
        return [Vec2(v) for v in self._vertices]

    def clone(self) -> Self:
        clone = self.__class__(None)
        clone._commands = self._commands.copy()
        clone._vertices = self._vertices.copy()
        return clone

    __copy__ = clone

    def command_codes(self) -> list[int]:
        """Internal API."""
        return list(self._commands)

    def commands(self) -> Iterator[PathElement]:
        vertices = self.vertices()
        index = 1
        for cmd in self._commands:
            if cmd == CMD_LINE_TO:
                yield LineTo(vertices[index])
                index += 1
            elif cmd == CMD_CURVE3_TO:
                yield Curve3To(vertices[index + 1], vertices[index])
                index += 2
            elif cmd == CMD_CURVE4_TO:
                yield Curve4To(
                    vertices[index + 2], vertices[index], vertices[index + 1]
                )
                index += 3
            elif cmd == CMD_MOVE_TO:
                yield MoveTo(vertices[index])
                index += 1

    def to_path(self) -> Path:
        """Returns a new :class:`ezdxf.path.Path` instance."""
        vertices = [Vec3(v) for v in self._vertices]
        commands = [Command(c) for c in self._commands]
        return Path.from_vertices_and_commands(vertices, commands)

    @classmethod
    def from_vertices(
        cls, vertices: Iterable[Vec2 | Vec3], close: bool = False
    ) -> Self:
        new_path = cls(None)
        vertices = list(vertices)
        if len(vertices) == 0:
            return new_path
        if close and not vertices[0].isclose(vertices[-1]):
            vertices.append(vertices[0])
        # (v.x, v.y) is 4x faster than Vec2(v), see profiling/numpy_array_setup.py
        points = [(v.x, v.y) for v in vertices]
        new_path._vertices = np.array(points, dtype=VertexNumpyType)
        new_path._commands = np.full(
            len(points) - 1, fill_value=CMD_LINE_TO, dtype=CommandNumpyType
        )
        return new_path

    @property
    def has_sub_paths(self) -> bool:
        """Returns ``True`` if the path is a :term:`Multi-Path` object that
        contains multiple sub-paths.

        """
        return CMD_MOVE_TO in self._commands

    @property
    def is_closed(self) -> bool:
        """Returns ``True`` if the start point is close to the end point."""
        if len(self._vertices) > 1:
            return self.start.isclose(self.end)
        return False

    @property
    def has_lines(self) -> bool:
        """Returns ``True`` if the path has any line segments."""
        return CMD_LINE_TO in self._commands

    @property
    def has_curves(self) -> bool:
        """Returns ``True`` if the path has any curve segments."""
        return CMD_CURVE3_TO in self._commands or CMD_CURVE4_TO in self._commands

    def sub_paths(self) -> list[Self]:
        """Yield all sub-paths as :term:`Single-Path` objects.

        It's safe to call :meth:`sub_paths` on any path-type:
        :term:`Single-Path`, :term:`Multi-Path` and :term:`Empty-Path`.

        """

        def append_sub_path() -> None:
            s: Self = self.__class__(None)
            s._vertices = vertices[vtx_start_index : vtx_index + 1]  # .copy() ?
            s._commands = commands[cmd_start_index:cmd_index]  # .copy() ?
            sub_paths.append(s)

        commands = self._commands
        if len(commands) == 0:
            return []
        if CMD_MOVE_TO not in commands:
            return [self]

        sub_paths: list[Self] = []
        vertices = self._vertices
        vtx_start_index = 0
        vtx_index = 0
        cmd_start_index = 0
        cmd_index = 0
        for cmd in commands:
            if cmd == CMD_LINE_TO:
                vtx_index += 1
            elif cmd == CMD_CURVE3_TO:
                vtx_index += 2
            elif cmd == CMD_CURVE4_TO:
                vtx_index += 3
            elif cmd == CMD_MOVE_TO:
                append_sub_path()
                # MOVE_TO target vertex is the start vertex of the following path.
                vtx_index += 1
                vtx_start_index = vtx_index
                cmd_start_index = cmd_index + 1
            cmd_index += 1

        if commands[-1] != CMD_MOVE_TO:
            append_sub_path()
        return sub_paths

    def has_clockwise_orientation(self) -> bool:
        """Returns ``True`` if 2D path has clockwise orientation.

        Raises:
            TypeError: can't detect orientation of a :term:`Multi-Path` object

        """
        if self.has_sub_paths:
            raise TypeError("can't detect orientation of a multi-path object")
        if np_support is None:
            return has_clockwise_orientation(self.vertices())
        else:
            return np_support.has_clockwise_orientation(self._vertices)

    def reverse(self) -> Self:
        """Reverse path orientation inplace."""
        commands = self._commands
        if not len(self._commands):
            return self
        if commands[-1] == CMD_MOVE_TO:
            # The last move_to will become the first move_to.
            # A move_to as first command just moves the start point and can be
            # removed!
            # There are never two consecutive MOVE_TO commands in a Path!
            self._commands = np.flip(commands[:-1]).copy()
            self._vertices = np.flip(self._vertices[:-1, ...], axis=0).copy()
        else:
            self._commands = np.flip(commands).copy()
            self._vertices = np.flip(self._vertices, axis=0).copy()
        return self

    def clockwise(self) -> Self:
        """Apply clockwise orientation inplace.

        Raises:
            TypeError: can't detect orientation of a :term:`Multi-Path` object

        """
        if not self.has_clockwise_orientation():
            self.reverse()
        return self

    def counter_clockwise(self) -> Self:
        """Apply counter-clockwise orientation inplace.

        Raises:
            TypeError: can't detect orientation of a :term:`Multi-Path` object

        """

        if self.has_clockwise_orientation():
            self.reverse()
        return self

    def flattening(self, distance: float, segments: int = 4) -> Iterator[Vec2]:
        """Flatten path to vertices as :class:`Vec2` instances."""
        if not len(self._commands):
            return

        vertices = self.vertices()
        start = vertices[0]
        yield start
        index = 1
        for cmd in self._commands:
            if cmd == CMD_LINE_TO or cmd == CMD_MOVE_TO:
                end_location = vertices[index]
                index += 1
                yield end_location
            elif cmd == CMD_CURVE3_TO:
                ctrl, end_location = vertices[index : index + 2]
                index += 2
                pts = Vec2.generate(
                    Bezier3P((start, ctrl, end_location)).flattening(distance, segments)
                )
                next(pts)  # skip first vertex
                yield from pts
            elif cmd == CMD_CURVE4_TO:
                ctrl1, ctrl2, end_location = vertices[index : index + 3]
                index += 3
                pts = Vec2.generate(
                    Bezier4P((start, ctrl1, ctrl2, end_location)).flattening(
                        distance, segments
                    )
                )
                next(pts)  # skip first vertex
                yield from pts
            else:
                raise ValueError(f"Invalid command: {cmd}")
            start = end_location

    # Appending single commands (line_to, move_to, curve3_to, curve4_to) is not
    # efficient, because numpy arrays do not grow dynamically, they are reallocated for
    # every single command!
    # Construct paths as ezdxf.path.Path and convert them to NumpyPath2d.
    # Concatenation of NumpyPath2d objects is faster than extending Path objects

    def extend(self, paths: Sequence[NumpyPath2d]) -> None:
        """Extend an existing path by appending additional paths. The paths are
        connected by MOVE_TO commands if the end- and start point of sequential paths
        are not coincident (multi-path).
        """
        if not len(paths):
            return
        if not len(self._commands):
            first = paths[0]
            paths = paths[1:]
        else:
            first = self

        vertices: list[np.ndarray] = [first._vertices]
        commands: list[np.ndarray] = [first._commands]
        end: Vec2 = first.end

        for next_path in paths:
            if len(next_path._commands) == 0:
                continue
            if not end.isclose(next_path.start):
                commands.append(np.array((CMD_MOVE_TO,), dtype=CommandNumpyType))
                vertices.append(next_path._vertices)
            else:
                vertices.append(next_path._vertices[1:])
            end = next_path.end
            commands.append(next_path._commands)
        self._vertices = np.concatenate(vertices, axis=0)
        self._commands = np.concatenate(commands)

    @staticmethod
    def concatenate(paths: Sequence[NumpyPath2d]) -> NumpyPath2d:
        """Returns a new path of concatenated paths. The paths are connected by
        MOVE_TO commands if the end- and start point of sequential paths are not
        coincident (multi-path).
        """

        if not paths:
            return NumpyPath2d(None)
        first = paths[0].clone()
        first.extend(paths[1:])
        return first


def to_qpainter_path(paths: Iterable[NumpyPath2d]):
    """Convert the given `paths` into a single :class:`QPainterPath`."""
    from ezdxf.addons.xqt import QPainterPath, QPointF

    paths = list(paths)
    if len(paths) == 0:
        raise ValueError("one or more paths required")

    qpath = QPainterPath()
    for path in paths:
        points = [QPointF(v.x, v.y) for v in path.vertices()]
        qpath.moveTo(points[0])
        index = 1
        for cmd in path.command_codes():
            # using Command.<attr> slows down this function by a factor of 4!!!
            if cmd == CMD_LINE_TO:
                qpath.lineTo(points[index])
                index += 1
            elif cmd == CMD_CURVE3_TO:
                qpath.quadTo(points[index], points[index + 1])
                index += 2
            elif cmd == CMD_CURVE4_TO:
                qpath.cubicTo(points[index], points[index + 1], points[index + 2])
                index += 3
            elif cmd == CMD_MOVE_TO:
                qpath.moveTo(points[index])
                index += 1
    return qpath


MPL_MOVETO = 1
MPL_LINETO = 2
MPL_CURVE3 = 3
MPL_CURVE4 = 4

MPL_CODES = [
    (0,),  # dummy
    (MPL_LINETO,),
    (MPL_CURVE3, MPL_CURVE3),
    (MPL_CURVE4, MPL_CURVE4, MPL_CURVE4),
    (MPL_MOVETO,),
]


def to_matplotlib_path(paths: Iterable[NumpyPath2d], *, detect_holes=False):
    """Convert the given `paths` into a single :class:`matplotlib.path.Path`.

    Matplotlib requires counter-clockwise oriented outside paths and clockwise oriented
    holes. Set the `detect_holes` argument to ``True`` if this path orientation is not
    yet satisfied.
    """
    from matplotlib.path import Path

    paths = list(paths)
    if len(paths) == 0:
        raise ValueError("one or more paths required")

    if detect_holes:
        # path orientation for holes is important, see #939
        paths = orient_paths(paths)

    vertices: list[np.ndarray] = []
    codes: list[int] = []
    for path in paths:
        vertices.append(path.np_vertices())
        codes.append(MPL_MOVETO)
        for cmd in path.command_codes():
            codes.extend(MPL_CODES[cmd])
    points = np.concatenate(vertices)
    try:
        return Path(points, codes)
    except Exception as e:
        raise ValueError(f"matplotlib.path.Path({str(points)}, {str(codes)}): {str(e)}")


def single_paths(paths: Iterable[NumpyPath2d]) -> list[NumpyPath2d]:
    single_paths_: list[NumpyPath2d] = []
    for p in paths:
        sub_paths = p.sub_paths()
        if sub_paths:
            single_paths_.extend(sub_paths)
    return single_paths_


def orient_paths(paths: list[NumpyPath2d]) -> list[NumpyPath2d]:
    """Returns a new list of paths, with outer paths oriented counter-clockwise and
    holes oriented clockwise.
    """
    sub_paths: list[NumpyPath2d] = single_paths(paths)
    if len(sub_paths) < 2:
        return paths

    polygons = nesting.make_polygon_structure(sub_paths)
    outer_paths: list[NumpyPath2d]
    holes: list[NumpyPath2d]
    outer_paths, holes = nesting.winding_deconstruction(polygons)

    path: NumpyPath2d
    for path in outer_paths:
        path.counter_clockwise()
    for path in holes:
        path.clockwise()
    return outer_paths + holes


class NumpyShape3d(abc.ABC):
    """This is an optimization to store many 3D paths and polylines in a compact way
    without sacrificing basic functions like transformation and bounding box calculation.
    """

    _vertices: npt.NDArray[VertexNumpyType] = EMPTY_SHAPE

    def extents(self) -> tuple[Vec3, Vec3]:
        """Returns the extents of the bounding box as tuple (extmin, extmax)."""
        v = self._vertices
        if len(v) > 0:
            return Vec3(v.min(0)), Vec3(v.max(0))
        else:
            raise EmptyShapeError("empty shape has no extends")

    @abc.abstractmethod
    def clone(self) -> Self:
        ...

    def np_vertices(self) -> npt.NDArray[VertexNumpyType]:
        return self._vertices

    def transform_inplace(self, m: Matrix44) -> None:
        """Transforms the vertices of the shape inplace."""
        v = self._vertices
        if len(v) == 0:
            return
        m.transform_array_inplace(v, 3)

    def vertices(self) -> list[Vec3]:
        """Returns the shape vertices as list of :class:`Vec3`."""
        return [Vec3(v) for v in self._vertices]

    def bbox(self) -> BoundingBox:
        """Returns the bounding box of all vertices."""
        return BoundingBox(self.extents())


class NumpyPoints3d(NumpyShape3d):
    """Represents an array of 3D points stored as a ndarray."""

    def __init__(self, points: Optional[Iterable[UVec]]) -> None:
        if points:
            self._vertices = np.array(
                [Vec3(v).xyz for v in points], dtype=VertexNumpyType
            )

    def clone(self) -> Self:
        clone = self.__class__(None)
        clone._vertices = self._vertices.copy()
        return clone

    __copy__ = clone

    def __len__(self) -> int:
        return len(self._vertices)
