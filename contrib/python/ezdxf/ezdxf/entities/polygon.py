# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Optional, Union, TYPE_CHECKING, Iterator
from typing_extensions import Self
import abc
import copy

from ezdxf.audit import Auditor, AuditError
from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags
from ezdxf import colors
from ezdxf.tools import pattern
from ezdxf.math import Vec3, Matrix44
from ezdxf.math.transformtools import OCSTransform
from .boundary_paths import BoundaryPaths
from .dxfns import SubclassProcessor, DXFNamespace
from .dxfgfx import DXFGraphic
from .gradient import Gradient
from .pattern import Pattern, PatternLine
from .dxfentity import DXFEntity
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf import xref

RGB = colors.RGB

__all__ = ["DXFPolygon"]

PATH_CODES = {
    10,
    11,
    12,
    13,
    40,
    42,
    50,
    51,
    42,
    72,
    73,
    74,
    92,
    93,
    94,
    95,
    96,
    97,
    330,
}
PATTERN_DEFINITION_LINE_CODES = {53, 43, 44, 45, 46, 79, 49}


class DXFPolygon(DXFGraphic):
    """Base class for the HATCH and the MPOLYGON entity."""

    LOAD_GROUP_CODES: dict[int, Union[str, list[str]]] = {}

    def __init__(self) -> None:
        super().__init__()
        self.paths = BoundaryPaths()
        self.pattern: Optional[Pattern] = None
        self.gradient: Optional[Gradient] = None
        self.seeds: list[tuple[float, float]] = []  # not supported/exported by MPOLYGON

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy paths, pattern, gradient, seeds."""
        assert isinstance(entity, DXFPolygon)
        entity.paths = copy.deepcopy(self.paths)
        entity.pattern = copy.deepcopy(self.pattern)
        entity.gradient = copy.deepcopy(self.gradient)
        entity.seeds = copy.deepcopy(self.seeds)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            # Copy without subclass marker:
            tags = Tags(processor.subclasses[2][1:])
            # Removes boundary path data from tags:
            tags = self.load_paths(tags)
            # Removes gradient data from tags:
            tags = self.load_gradient(tags)
            # Removes pattern from tags:
            tags = self.load_pattern(tags)
            # Removes seeds from tags:
            tags = self.load_seeds(tags)

            # Load HATCH DXF attributes from remaining tags:
            processor.fast_load_dxfattribs(
                dxf, self.LOAD_GROUP_CODES, subclass=tags, recover=True
            )
        return dxf

    def load_paths(self, tags: Tags) -> Tags:
        # Find first group code 91 = count of loops, Spline data also contains
        # group code 91!
        try:
            start_index = tags.tag_index(91)
        except const.DXFValueError:
            raise const.DXFStructureError(
                f"{self.dxftype()}: Missing required DXF tag 'Number of "
                f"boundary paths (loops)' (code=91)."
            )

        path_tags = tags.collect_consecutive_tags(PATH_CODES, start=start_index + 1)
        if len(path_tags):
            self.paths = BoundaryPaths.load_tags(path_tags)
        end_index = start_index + len(path_tags) + 1
        del tags[start_index:end_index]
        return tags

    def load_pattern(self, tags: Tags) -> Tags:
        try:
            # Group code 78 = Number of pattern definition lines
            index = tags.tag_index(78)
        except const.DXFValueError:
            # No pattern definition lines found.
            return tags

        pattern_tags = tags.collect_consecutive_tags(
            PATTERN_DEFINITION_LINE_CODES, start=index + 1
        )
        self.pattern = Pattern.load_tags(pattern_tags)

        # Delete pattern data including length tag 78
        del tags[index : index + len(pattern_tags) + 1]
        return tags

    def load_gradient(self, tags: Tags) -> Tags:
        try:
            index = tags.tag_index(450)
        except const.DXFValueError:
            # No gradient data present
            return tags

        # Gradient data is always at the end of the AcDbHatch subclass.
        self.gradient = Gradient.load_tags(tags[index:])  # type: ignore
        # Remove gradient data from tags
        del tags[index:]
        return tags

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, DXFPolygon)
        assert clone.doc is not None

        super().map_resources(clone, mapping)
        db = clone.doc.entitydb
        for path in clone.paths:
            handles = [mapping.get_handle(h) for h in path.source_boundary_objects]
            path.source_boundary_objects = [h for h in handles if h in db]

    def load_seeds(self, tags: Tags) -> Tags:
        return tags

    @property
    def has_solid_fill(self) -> bool:
        """``True`` if entity has a solid fill. (read only)"""
        return bool(self.dxf.solid_fill)

    @property
    def has_pattern_fill(self) -> bool:
        """``True`` if entity has a pattern fill. (read only)"""
        return not bool(self.dxf.solid_fill)

    @property
    def has_gradient_data(self) -> bool:
        """``True`` if entity has a gradient fill. A hatch with gradient fill
        has also a solid fill. (read only)
        """
        return bool(self.gradient)

    @property
    def bgcolor(self) -> Optional[RGB]:
        """
        Set pattern fill background color as (r, g, b)-tuple, rgb values
        in the range [0, 255] (read/write/del)

        usage::

            r, g, b = entity.bgcolor  # get pattern fill background color
            entity.bgcolor = (10, 20, 30)  # set pattern fill background color
            del entity.bgcolor  # delete pattern fill background color

        """
        try:
            xdata_bgcolor = self.get_xdata("HATCHBACKGROUNDCOLOR")
        except const.DXFValueError:
            return None
        color = xdata_bgcolor.get_first_value(1071, 0)
        try:
            return colors.int2rgb(int(color))
        except ValueError:  # invalid data type
            return RGB(0, 0, 0)

    @bgcolor.setter
    def bgcolor(self, rgb: RGB) -> None:
        color_value = (
            colors.rgb2int(rgb) | -0b111110000000000000000000000000
        )  # it's magic

        self.discard_xdata("HATCHBACKGROUNDCOLOR")
        self.set_xdata("HATCHBACKGROUNDCOLOR", [(1071, color_value)])

    @bgcolor.deleter
    def bgcolor(self) -> None:
        self.discard_xdata("HATCHBACKGROUNDCOLOR")

    def set_gradient(
        self,
        color1: RGB = RGB(0, 0, 0),
        color2: RGB = RGB(255, 255, 255),
        rotation: float = 0.0,
        centered: float = 0.0,
        one_color: int = 0,
        tint: float = 0.0,
        name: str = "LINEAR",
    ) -> None:
        """Sets the gradient fill mode and removes all pattern fill related data, requires
        DXF R2004 or newer.  A gradient filled hatch is also a solid filled hatch.

        Valid gradient type names are:

            - "LINEAR"
            - "CYLINDER"
            - "INVCYLINDER"
            - "SPHERICAL"
            - "INVSPHERICAL"
            - "HEMISPHERICAL"
            - "INVHEMISPHERICAL"
            - "CURVED"
            - "INVCURVED"

        Args:
            color1: (r, g, b)-tuple for first color, rgb values as int in
                the range [0, 255]
            color2: (r, g, b)-tuple for second color, rgb values as int in
                the range [0, 255]
            rotation: rotation angle in degrees
            centered: determines whether the gradient is centered or not
            one_color: 1 for gradient from `color1` to tinted `color1`
            tint: determines the tinted target `color1` for a one color
                gradient. (valid range 0.0 to 1.0)
            name: name of gradient type, default "LINEAR"

        """
        if self.doc is not None and self.doc.dxfversion < const.DXF2004:
            raise const.DXFVersionError("Gradient support requires DXF R2004")
        if name and name not in const.GRADIENT_TYPES:
            raise const.DXFValueError(f"Invalid gradient type name: {name}")

        self.pattern = None
        self.dxf.solid_fill = 1
        self.dxf.pattern_name = "SOLID"
        self.dxf.pattern_type = const.HATCH_TYPE_PREDEFINED

        gradient = Gradient()
        gradient.color1 = color1
        gradient.color2 = color2
        gradient.one_color = one_color
        gradient.rotation = rotation
        gradient.centered = centered
        gradient.tint = tint
        gradient.name = name
        self.gradient = gradient

    def set_pattern_fill(
        self,
        name: str,
        color: int = 7,
        angle: float = 0.0,
        scale: float = 1.0,
        double: int = 0,
        style: int = 1,
        pattern_type: int = 1,
        definition=None,
    ) -> None:
        """Sets the pattern fill mode and removes all gradient related data.

        The pattern definition should be designed for a scale factor 1 and a rotation
        angle of 0 degrees.  The predefined hatch pattern like "ANSI33" are scaled
        according to the HEADER variable $MEASUREMENT for ISO measurement (m, cm, ... ),
        or imperial units (in, ft, ...), this replicates the behavior of BricsCAD.

        Args:
            name: pattern name as string
            color: pattern color as :ref:`ACI`
            angle: pattern rotation angle in degrees
            scale: pattern scale factor
            double: double size flag
            style: hatch style (0 = normal; 1 = outer; 2 = ignore)
            pattern_type: pattern type (0 = user-defined;
                1 = predefined; 2 = custom)
            definition: list of definition lines and a definition line is a
                4-tuple [angle, base_point, offset, dash_length_items],
                see :meth:`set_pattern_definition`

        """
        self.gradient = None
        self.dxf.solid_fill = 0
        self.dxf.pattern_name = name
        self.dxf.color = color
        self.dxf.pattern_scale = float(scale)
        self.dxf.pattern_angle = float(angle)
        self.dxf.pattern_double = int(double)
        self.dxf.hatch_style = style
        self.dxf.pattern_type = pattern_type

        if definition is None:
            measurement = 1
            if self.doc:
                measurement = self.doc.header.get("$MEASUREMENT", measurement)
            predefined_pattern = (
                pattern.ISO_PATTERN if measurement else pattern.IMPERIAL_PATTERN
            )
            definition = predefined_pattern.get(name, predefined_pattern["ANSI31"])
        self.set_pattern_definition(
            definition,
            factor=self.dxf.pattern_scale,
            angle=self.dxf.pattern_angle,
        )

    def set_pattern_definition(
        self, lines: Sequence, factor: float = 1, angle: float = 0
    ) -> None:
        """Setup pattern definition by a list of definition lines and the
        definition line is a 4-tuple (angle, base_point, offset, dash_length_items).
        The pattern definition should be designed for a pattern scale factor of 1 and
        a pattern rotation angle of 0.

            - angle: line angle in degrees
            - base-point: (x, y) tuple
            - offset: (dx, dy) tuple
            - dash_length_items: list of dash items (item > 0 is a line,
              item < 0 is a gap and item == 0.0 is a point)

        Args:
            lines: list of definition lines
            factor: pattern scale factor
            angle: rotation angle in degrees

        """
        if factor != 1 or angle:
            lines = pattern.scale_pattern(lines, factor=factor, angle=angle)
        self.pattern = Pattern(
            [PatternLine(line[0], line[1], line[2], line[3]) for line in lines]
        )

    def set_pattern_scale(self, scale: float) -> None:
        """Sets the pattern scale factor and scales the pattern definition.

        The method always starts from the original base scale, the
        :code:`set_pattern_scale(1)` call resets the pattern scale to the original
        appearance as defined by the pattern designer, but only if the pattern attribute
        :attr:`dxf.pattern_scale` represents the actual scale, it cannot
        restore the original pattern scale from the pattern definition itself.

        Args:
            scale: pattern scale factor

        """
        if not self.has_pattern_fill:
            return
        dxf = self.dxf
        self.pattern.scale(factor=1.0 / dxf.pattern_scale * scale)  # type: ignore
        dxf.pattern_scale = scale

    def set_pattern_angle(self, angle: float) -> None:
        """Sets the pattern rotation angle and rotates the pattern definition.

        The method always starts from the original base rotation of 0, the
        :code:`set_pattern_angle(0)` call resets the pattern rotation angle to the
        original appearance as defined by the pattern designer, but only if the
        pattern attribute :attr:`dxf.pattern_angle` represents the actual pattern
        rotation, it cannot restore the original rotation angle from the
        pattern definition itself.

        Args:
            angle: pattern rotation angle in degrees

        """
        if not self.has_pattern_fill:
            return
        dxf = self.dxf
        self.pattern.scale(angle=angle - dxf.pattern_angle)  # type: ignore
        dxf.pattern_angle = angle % 360.0

    def transform(self, m: Matrix44) -> DXFPolygon:
        """Transform entity by transformation matrix `m` inplace."""
        dxf = self.dxf
        ocs = OCSTransform(dxf.extrusion, m)

        elevation = Vec3(dxf.elevation).z
        self.paths.transform(ocs, elevation=elevation)
        dxf.elevation = ocs.transform_vertex(Vec3(0, 0, elevation)).replace(
            x=0.0, y=0.0
        )
        dxf.extrusion = ocs.new_extrusion
        if self.pattern:
            # todo: non-uniform scaling
            # take the scaling factor of the x-axis
            factor = ocs.transform_length((self.dxf.pattern_scale, 0, 0))
            angle = ocs.transform_deg_angle(self.dxf.pattern_angle)
            # todo: non-uniform pattern scaling is not supported
            self.pattern.scale(factor, angle)
            self.dxf.pattern_scale = factor
            self.dxf.pattern_angle = angle
        self.post_transform(m)
        return self

    def triangulate(self, max_sagitta, min_segments=16) -> Iterator[Sequence[Vec3]]:
        """Triangulate the HATCH/MPOLYGON in OCS coordinates, Elevation and offset is
        applied to all vertices.

        Args:
            max_sagitta: maximum distance from the center of the curve to the
                center of the line segment between two approximation points to determine
                if a segment should be subdivided.
            min_segments: minimum segment count per Bézier curve

        .. versionadded:: 1.1

        """
        from ezdxf import path

        elevation = Vec3(self.dxf.elevation)
        if self.dxf.hasattr("offset"):  # MPOLYGON
            elevation += Vec3(self.dxf.offset)  # offset in OCS?
        boundary_paths = [path.from_hatch_boundary_path(p) for p in self.paths]
        for vertices in path.triangulate(boundary_paths, max_sagitta, min_segments):
            yield tuple(elevation + v for v in vertices)

    def render_pattern_lines(self) -> Iterator[tuple[Vec3, Vec3]]:
        """Yields the pattern lines in WCS coordinates.

        .. versionadded:: 1.1

        """
        from ezdxf.render import hatching

        if self.has_pattern_fill:
            try:
                yield from hatching.hatch_entity(self)
            except hatching.HatchingError:
                return

    @abc.abstractmethod
    def set_solid_fill(self, color: int = 7, style: int = 1, rgb: Optional[RGB] = None):
        ...
    
    def audit(self, auditor: Auditor) -> None:
        super().audit(auditor)
        if not self.is_alive:
            return
        if not self.paths.is_valid():
            auditor.fixed_error(
                code=AuditError.INVALID_HATCH_BOUNDARY_PATH,
                message=f"Deleted entity {str(self)} containing invalid boundary paths."
            )
            auditor.trash(self)