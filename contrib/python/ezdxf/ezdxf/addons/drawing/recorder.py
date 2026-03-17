#  Copyright (c) 2023-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Sequence,
    Callable,
    Optional,
    NamedTuple,
)
from typing_extensions import Self, TypeAlias
import copy
import abc

from ezdxf.math import BoundingBox2d, Matrix44, Vec2, UVec
from ezdxf.npshapes import NumpyPath2d, NumpyPoints2d, EmptyShapeError
from ezdxf.tools import take2
from ezdxf.tools.clipping_portal import ClippingRect

from .backend import BackendInterface, ImageData
from .config import Configuration
from .properties import BackendProperties
from .type_hints import Color


class DataRecord(abc.ABC):
    def __init__(self) -> None:
        self.property_hash: int = 0
        self.handle: str = ""

    @abc.abstractmethod
    def bbox(self) -> BoundingBox2d:
        ...

    @abc.abstractmethod
    def transform_inplace(self, m: Matrix44) -> None:
        ...


class PointsRecord(DataRecord):
    # n=1 point; n=2 line; n>2 filled polygon
    def __init__(self, points: NumpyPoints2d) -> None:
        super().__init__()
        self.points: NumpyPoints2d = points

    def bbox(self) -> BoundingBox2d:
        try:
            return self.points.bbox()
        except EmptyShapeError:
            pass
        return BoundingBox2d()

    def transform_inplace(self, m: Matrix44) -> None:
        self.points.transform_inplace(m)


class SolidLinesRecord(DataRecord):
    def __init__(self, lines: NumpyPoints2d) -> None:
        super().__init__()
        self.lines: NumpyPoints2d = lines

    def bbox(self) -> BoundingBox2d:
        try:
            return self.lines.bbox()
        except EmptyShapeError:
            pass
        return BoundingBox2d()

    def transform_inplace(self, m: Matrix44) -> None:
        self.lines.transform_inplace(m)


class PathRecord(DataRecord):
    def __init__(self, path: NumpyPath2d) -> None:
        super().__init__()
        self.path: NumpyPath2d = path

    def bbox(self) -> BoundingBox2d:
        try:
            return self.path.bbox()
        except EmptyShapeError:
            pass
        return BoundingBox2d()

    def transform_inplace(self, m: Matrix44) -> None:
        self.path.transform_inplace(m)


class FilledPathsRecord(DataRecord):
    def __init__(self, paths: Sequence[NumpyPath2d]) -> None:
        super().__init__()
        self.paths: Sequence[NumpyPath2d] = paths

    def bbox(self) -> BoundingBox2d:
        bbox = BoundingBox2d()
        for path in self.paths:
            if len(path):
                bbox.extend(path.extents())
        return bbox

    def transform_inplace(self, m: Matrix44) -> None:
        for path in self.paths:
            path.transform_inplace(m)


class ImageRecord(DataRecord):
    def __init__(self, boundary: NumpyPoints2d, image_data: ImageData) -> None:
        super().__init__()
        self.boundary: NumpyPoints2d = boundary
        self.image_data: ImageData = image_data

    def bbox(self) -> BoundingBox2d:
        try:
            return self.boundary.bbox()
        except EmptyShapeError:
            pass
        return BoundingBox2d()

    def transform_inplace(self, m: Matrix44) -> None:
        self.boundary.transform_inplace(m)
        self.image_data.transform @= m


class Recorder(BackendInterface):
    """Records the output of the Frontend class."""

    def __init__(self) -> None:
        self.config = Configuration()
        self.background: Color = "#000000"
        self.records: list[DataRecord] = []
        self.properties: dict[int, BackendProperties] = dict()

    def player(self) -> Player:
        """Returns a :class:`Player` instance with the original recordings! Make a copy
        of this player to protect the original recordings from being modified::

            safe_player = recorder.player().copy()

        """
        player = Player()
        player.config = self.config
        player.background = self.background
        player.records = self.records
        player.properties = self.properties
        player.has_shared_recordings = True
        return player

    def configure(self, config: Configuration) -> None:
        self.config = config

    def set_background(self, color: Color) -> None:
        self.background = color

    def store(self, record: DataRecord, properties: BackendProperties) -> None:
        # exclude top-level entity handle to reduce the variance:
        # color, lineweight, layer, pen
        prop_hash = hash(properties[:4])
        record.property_hash = prop_hash
        record.handle = properties.handle
        self.records.append(record)
        self.properties[prop_hash] = properties

    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.store(PointsRecord(NumpyPoints2d((pos,))), properties)

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        self.store(PointsRecord(NumpyPoints2d((start, end))), properties)

    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        def flatten() -> Iterator[Vec2]:
            for s, e in lines:
                yield s
                yield e

        self.store(SolidLinesRecord(NumpyPoints2d(flatten())), properties)

    def draw_path(self, path: NumpyPath2d, properties: BackendProperties) -> None:
        assert isinstance(path, NumpyPath2d)
        self.store(PathRecord(path), properties)

    def draw_filled_polygon(
        self, points: NumpyPoints2d, properties: BackendProperties
    ) -> None:
        assert isinstance(points, NumpyPoints2d)
        self.store(PointsRecord(points), properties)

    def draw_filled_paths(
        self, paths: Iterable[NumpyPath2d], properties: BackendProperties
    ) -> None:
        paths = tuple(paths)
        if len(paths) == 0:
            return

        assert isinstance(paths[0], NumpyPath2d)
        self.store(FilledPathsRecord(paths), properties)

    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        # preserve the boundary in image_data in pixel coordinates
        boundary = copy.deepcopy(image_data.pixel_boundary_path)
        boundary.transform_inplace(image_data.transform)
        self.store(ImageRecord(boundary, image_data), properties)

    def enter_entity(self, entity, properties) -> None:
        pass

    def exit_entity(self, entity) -> None:
        pass

    def clear(self) -> None:
        raise NotImplementedError()

    def finalize(self) -> None:
        pass


class Override(NamedTuple):
    """Represents the override state for a data record.

    Attributes:
        properties: original or modified :class:`BackendProperties`
        is_visible: override visibility e.g. switch layers on/off

    """

    properties: BackendProperties
    is_visible: bool = True


OverrideFunc: TypeAlias = Callable[[BackendProperties], Override]


class Player:
    """Plays the recordings of the :class:`Recorder` backend on another backend."""

    def __init__(self) -> None:
        self.config = Configuration()
        self.background: Color = "#000000"
        self.records: list[DataRecord] = []
        self.properties: dict[int, BackendProperties] = dict()
        self._bbox = BoundingBox2d()
        self.has_shared_recordings: bool = False

    def __copy__(self) -> Self:
        """Returns a copy of the player with non-shared recordings."""
        player = self.__class__()
        # config is a frozen dataclass:
        player.config = self.config
        player.background = self.background
        # recordings are mutable: transform and crop inplace
        player.records = copy.deepcopy(self.records)
        # the properties dict may grow, but entries will never be removed:
        player.properties = self.properties
        player.has_shared_recordings = False
        return player

    copy = __copy__

    def recordings(self) -> Iterator[tuple[DataRecord, BackendProperties]]:
        """Yields all recordings as `(DataRecord, BackendProperties)` tuples."""
        props = self.properties
        for record in self.records:
            properties = BackendProperties(
                *props[record.property_hash][:4], record.handle
            )
            yield record, properties

    def replay(
        self, backend: BackendInterface, override: Optional[OverrideFunc] = None
    ) -> None:
        """Replay the recording on another backend that implements the
        :class:`BackendInterface`. The optional `override` function can be used to
        override the properties and state of data records, it gets the :class:`BackendProperties`
        as input and must return an :class:`Override` instance.
        """

        backend.configure(self.config)
        backend.set_background(self.background)
        for record, properties in self.recordings():
            if override:
                state = override(properties)
                if not state.is_visible:
                    continue
                properties = state.properties
            if isinstance(record, PointsRecord):
                count = len(record.points)
                if count == 0:
                    continue
                if count > 2:
                    backend.draw_filled_polygon(record.points, properties)
                    continue
                vertices = record.points.vertices()
                if len(vertices) == 1:
                    backend.draw_point(vertices[0], properties)
                else:
                    backend.draw_line(vertices[0], vertices[1], properties)
            elif isinstance(record, SolidLinesRecord):
                backend.draw_solid_lines(take2(record.lines.vertices()), properties)
            elif isinstance(record, PathRecord):
                backend.draw_path(record.path, properties)
            elif isinstance(record, FilledPathsRecord):
                backend.draw_filled_paths(record.paths, properties)
            elif isinstance(record, ImageRecord):
                backend.draw_image(record.image_data, properties)
        backend.finalize()

    def transform(self, m: Matrix44) -> None:
        """Transforms the recordings inplace by a transformation matrix `m` of type
        :class:`~ezdxf.math.Matrix44`.
        """
        for record in self.records:
            record.transform_inplace(m)

        if self._bbox.has_data:
            # works for 90-, 180- and 270-degree rotation
            self._bbox = BoundingBox2d(m.fast_2d_transform(self._bbox.rect_vertices()))

    def bbox(self) -> BoundingBox2d:
        """Returns the bounding box of all records as :class:`~ezdxf.math.BoundingBox2d`."""
        if not self._bbox.has_data:
            self.update_bbox()
        return self._bbox

    def update_bbox(self) -> None:
        bbox = BoundingBox2d()
        for record in self.records:
            bbox.extend(record.bbox())
        self._bbox = bbox

    def crop_rect(self, p1: UVec, p2: UVec, distance: float) -> None:
        """Crop recorded shapes inplace by a rectangle defined by two points.

        The argument `distance` defines the approximation precision for paths which have
        to be approximated as polylines for cropping but only paths which are really get
        cropped are approximated, paths that are fully inside the crop box will not be
        approximated.

        Args:
            p1: first corner of the clipping rectangle
            p2: second corner of the clipping rectangle
            distance: maximum distance from the center of the curve to the
                center of the line segment between two approximation points to
                determine if a segment should be subdivided.

        """
        crop_rect = BoundingBox2d([Vec2(p1), Vec2(p2)])
        self.records = crop_records_rect(self.records, crop_rect, distance)
        self._bbox = BoundingBox2d()  # determine new bounding box on demand


def crop_records_rect(
    records: list[DataRecord], crop_rect: BoundingBox2d, distance: float
) -> list[DataRecord]:
    """Crop recorded shapes inplace by a rectangle."""

    def sort_paths(np_paths: Sequence[NumpyPath2d]):
        _inside: list[NumpyPath2d] = []
        _crop: list[NumpyPath2d] = []

        for np_path in np_paths:
            bbox = BoundingBox2d(np_path.extents())
            if not crop_rect.has_intersection(bbox):
                # path is complete outside the cropping rectangle
                pass
            elif crop_rect.inside(bbox.extmin) and crop_rect.inside(bbox.extmax):
                # path is complete inside the cropping rectangle
                _inside.append(np_path)
            else:
                _crop.append(np_path)

        return _crop, _inside

    def crop_paths(
        np_paths: Sequence[NumpyPath2d],
    ) -> list[NumpyPath2d]:
        return list(clipper.clip_filled_paths(np_paths, distance))

    # an undefined crop box crops nothing:
    if not crop_rect.has_data:
        return records
    cropped_records: list[DataRecord] = []
    size = crop_rect.size
    # a crop box size of zero in any dimension crops everything:
    if size.x < 1e-12 or size.y < 1e-12:
        return cropped_records

    clipper = ClippingRect(crop_rect.rect_vertices())
    for record in records:
        record_box = record.bbox()
        if not crop_rect.has_intersection(record_box):
            # record is complete outside the cropping rectangle
            continue
        if crop_rect.inside(record_box.extmin) and crop_rect.inside(record_box.extmax):
            # record is complete inside the cropping rectangle
            cropped_records.append(record)
            continue

        if isinstance(record, FilledPathsRecord):
            paths_to_crop, inside = sort_paths(record.paths)
            cropped_paths = crop_paths(paths_to_crop) + inside
            if cropped_paths:
                record.paths = tuple(cropped_paths)
                cropped_records.append(record)
        elif isinstance(record, PathRecord):
            # could be split into multiple parts
            for p in clipper.clip_paths([record.path], distance):
                path_record = PathRecord(p)
                path_record.property_hash = record.property_hash
                path_record.handle = record.handle
                cropped_records.append(path_record)
        elif isinstance(record, PointsRecord):
            count = len(record.points)
            if count == 1:
                # record is inside the clipping shape!
                cropped_records.append(record)
            elif count == 2:
                s, e = record.points.vertices()
                for segment in clipper.clip_line(s, e):
                    if not segment:
                        continue
                    _record = copy.copy(record)  # shallow copy
                    _record.points = NumpyPoints2d(segment)
                    cropped_records.append(_record)
            else:
                for polygon in clipper.clip_polygon(record.points):
                    if not polygon:
                        continue
                    _record = copy.copy(record)  # shallow copy!
                    _record.points = polygon
                    cropped_records.append(_record)
        elif isinstance(record, SolidLinesRecord):
            points: list[Vec2] = []
            for s, e in take2(record.lines.vertices()):
                for segment in clipper.clip_line(s, e):
                    points.extend(segment)
            record.lines = NumpyPoints2d(points)
            cropped_records.append(record)
        elif isinstance(record, ImageRecord):
            pass
            # TODO: Image cropping not supported
            #   Crop image boundary and apply transparency to cropped
            #   parts of the image? -> Image boundary is now a polygon!
        else:
            raise ValueError("invalid record type")
    return cropped_records
