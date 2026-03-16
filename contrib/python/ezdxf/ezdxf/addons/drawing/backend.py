# Copyright (c) 2020-2023, Matthew Broadway
# License: MIT License
from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from typing import Optional, Iterable

import numpy as np
from typing_extensions import TypeAlias
import dataclasses

from ezdxf.addons.drawing.config import Configuration
from ezdxf.addons.drawing.properties import Properties, BackendProperties
from ezdxf.addons.drawing.type_hints import Color
from ezdxf.entities import DXFGraphic
from ezdxf.math import Vec2, Matrix44
from ezdxf.npshapes import NumpyPath2d, NumpyPoints2d, single_paths

BkPath2d: TypeAlias = NumpyPath2d
BkPoints2d: TypeAlias = NumpyPoints2d

# fmt: off
_IMAGE_FLIP_MATRIX = [
    1.0, 0.0, 0.0, 0.0, 
    0.0, -1.0, 0.0, 0.0, 
    0.0, 0.0, 1.0, 0.0, 
    0.0, 999, 0.0, 1.0  # index 13: 999 = image height
]
# fmt: on


@dataclasses.dataclass
class ImageData:
    """Image data.

    Attributes:
        image: an array of RGBA pixels
        transform: the transformation to apply to the image when drawing
            (the transform from pixel coordinates to wcs)
        pixel_boundary_path: boundary path vertices in pixel coordinates, the image
            coordinate system has an inverted y-axis and the top-left corner is (0, 0)
        remove_outside: remove image outside the clipping boundary if ``True`` otherwise
            remove image inside the clipping boundary

    """

    image: np.ndarray
    transform: Matrix44
    pixel_boundary_path: NumpyPoints2d
    use_clipping_boundary: bool = False
    remove_outside: bool = True

    def image_size(self) -> tuple[int, int]:
        """Returns the image size as tuple (width, height)."""
        image_height, image_width, *_ = self.image.shape
        return image_width, image_height

    def flip_matrix(self) -> Matrix44:
        """Returns the transformation matrix to align the image coordinate system with
        the WCS.
        """
        _, image_height = self.image_size()
        _IMAGE_FLIP_MATRIX[13] = image_height
        return Matrix44(_IMAGE_FLIP_MATRIX)


class BackendInterface(ABC):
    """Public interface for 2D rendering backends."""

    @abstractmethod
    def configure(self, config: Configuration) -> None:
        raise NotImplementedError

    @abstractmethod
    def enter_entity(self, entity: DXFGraphic, properties: Properties) -> None:
        # gets the full DXF properties information
        raise NotImplementedError

    @abstractmethod
    def exit_entity(self, entity: DXFGraphic) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_background(self, color: Color) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def finalize(self) -> None:
        raise NotImplementedError


class Backend(BackendInterface, metaclass=ABCMeta):
    def __init__(self) -> None:
        self.entity_stack: list[tuple[DXFGraphic, Properties]] = []
        self.config: Configuration = Configuration()

    def configure(self, config: Configuration) -> None:
        self.config = config

    def enter_entity(self, entity: DXFGraphic, properties: Properties) -> None:
        # gets the full DXF properties information
        self.entity_stack.append((entity, properties))

    def exit_entity(self, entity: DXFGraphic) -> None:
        e, p = self.entity_stack.pop()
        assert e is entity, "entity stack mismatch"

    @property
    def current_entity(self) -> Optional[DXFGraphic]:
        """Obtain the current entity being drawn"""
        return self.entity_stack[-1][0] if self.entity_stack else None

    @abstractmethod
    def set_background(self, color: Color) -> None:
        raise NotImplementedError

    @abstractmethod
    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        """Draw a real dimensionless point, because not all backends support
        zero-length lines!
        """
        raise NotImplementedError

    @abstractmethod
    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        raise NotImplementedError

    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        """Fast method to draw a bunch of solid lines with the same properties."""
        # Must be overridden by the backend to gain a performance benefit.
        # This is the default implementation to ensure compatibility with
        # existing backends.
        for s, e in lines:
            if e.isclose(s):
                self.draw_point(s, properties)
            else:
                self.draw_line(s, e, properties)

    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        """Draw an outline path (connected string of line segments and Bezier
        curves).

        The :meth:`draw_path` implementation is a fall-back implementation
        which approximates Bezier curves by flattening as line segments.
        Backends can override this method if better path drawing functionality
        is available for that backend.

        """
        if len(path):
            vertices = iter(
                path.flattening(distance=self.config.max_flattening_distance)
            )
            prev = next(vertices)
            for vertex in vertices:
                self.draw_line(prev, vertex, properties)
                prev = vertex

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        """Draw multiple filled paths (connected string of line segments and
        Bezier curves).

        The current implementation passes these paths to the backend, all backends
        included in ezdxf handle holes by the even-odd method.  If a backend requires
        oriented paths (exterior paths in counter-clockwise and holes in clockwise
        orientation) use the function :func:`oriented_paths` to separate and orient the
        input paths.

        The default implementation draws all paths as filled polygons.

        Args:
            paths: sequence of paths
            properties: HATCH properties

        """
        for path in paths:
            self.draw_filled_polygon(
                BkPoints2d(
                    path.flattening(distance=self.config.max_flattening_distance)
                ),
                properties,
            )

    @abstractmethod
    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        """Fill a polygon whose outline is defined by the given points.
        Used to draw entities with simple outlines where :meth:`draw_path` may
        be an inefficient way to draw such a polygon.
        """
        raise NotImplementedError

    @abstractmethod
    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        """Draw an image with the given pixels."""
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        """Clear the canvas. Does not reset the internal state of the backend.
        Make sure that the previous drawing is finished before clearing.

        """
        raise NotImplementedError

    def finalize(self) -> None:
        pass


def oriented_paths(paths: Iterable[BkPath2d]) -> tuple[list[BkPath2d], list[BkPath2d]]:
    """Separate paths into exterior paths and holes. Exterior paths are oriented
    counter-clockwise, holes are oriented clockwise.
    """
    from ezdxf.path import winding_deconstruction, make_polygon_structure

    polygons = make_polygon_structure(single_paths(paths))
    external_paths: list[BkPath2d]
    holes: list[BkPath2d]
    external_paths, holes = winding_deconstruction(polygons)
    for p in external_paths:
        p.counter_clockwise()
    for p in holes:
        p.clockwise()
    return external_paths, holes
