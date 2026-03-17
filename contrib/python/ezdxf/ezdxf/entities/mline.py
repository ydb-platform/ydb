# Copyright (c) 2018-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Sequence,
    Iterator,
)
from typing_extensions  import Self

from collections import OrderedDict, namedtuple
import math

from ezdxf.audit import AuditError
from ezdxf.entities.factory import register_entity
from ezdxf.lldxf import const, validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.tags import Tags, group_tags
from ezdxf.math import NULLVEC, X_AXIS, Y_AXIS, Z_AXIS, UVec, Vec3, UCS, OCS

from .dxfentity import base_class, SubclassProcessor
from .dxfobj import DXFObject
from .dxfgfx import DXFGraphic, acdb_entity
from .objectcollection import ObjectCollection
from .copy import default_copy
import logging

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace, DXFEntity
    from ezdxf.layouts import BaseLayout
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44
    from ezdxf.query import EntityQuery
    from ezdxf import xref

__all__ = ["MLine", "MLineVertex", "MLineStyle", "MLineStyleCollection"]
logger = logging.getLogger("ezdxf")

# Usage example: CADKitSamples\Lock-Off.dxf


def filter_close_vertices(
    vertices: Iterable[Vec3], abs_tol: float = 1e-12
) -> Iterable[Vec3]:
    prev = None
    for vertex in vertices:
        if prev is None:
            yield vertex
            prev = vertex
        else:
            if not vertex.isclose(prev, abs_tol=abs_tol):
                yield vertex
                prev = vertex


acdb_mline = DefSubclass(
    "AcDbMline",
    OrderedDict(
        {
            "style_name": DXFAttr(2, default="Standard"),
            "style_handle": DXFAttr(340),
            "scale_factor": DXFAttr(
                40,
                default=1,
                validator=validator.is_not_zero,
                fixer=RETURN_DEFAULT,
            ),
            # Justification
            # 0 = Top (Right)
            # 1 = Zero (Center)
            # 2 = Bottom (Left)
            "justification": DXFAttr(
                70,
                default=0,
                validator=validator.is_in_integer_range(0, 3),
                fixer=RETURN_DEFAULT,
            ),
            # Flags (bit-coded values):
            # 1 = Has at least one vertex (code 72 is greater than 0)
            # 2 = Closed
            # 4 = Suppress start caps
            # 8 = Suppress end caps
            "flags": DXFAttr(71, default=1),
            # Number of MLINE vertices
            "count": DXFAttr(72, xtype=XType.callback, getter="__len__"),
            # Number of elements in MLINESTYLE definition
            "style_element_count": DXFAttr(73, default=2),
            # start location in WCS!
            "start_location": DXFAttr(
                10, xtype=XType.callback, getter="start_location"
            ),
            # Normal vector of the entity plane, but all vertices in WCS!
            "extrusion": DXFAttr(
                210,
                xtype=XType.point3d,
                default=Z_AXIS,
                validator=validator.is_not_null_vector,
                fixer=RETURN_DEFAULT,
            ),
            # MLine data:
            # 11: vertex coordinates
            #     Multiple entries; one entry for each vertex.
            # 12: Direction vector of segment starting at this vertex
            #     Multiple entries; one for each vertex.
            # 13: Direction vector of miter at this vertex
            #     Multiple entries: one for each vertex.
            # 74: Number of parameters for this element,
            #     repeats for each element in segment
            # 41: Element parameters,
            #     repeats based on previous code 74
            # 75: Number of area fill parameters for this element,
            #     repeats for each element in segment
            # 42: Area fill parameters,
            #     repeats based on previous code 75
        }
    ),
)
acdb_mline_group_codes = group_code_mapping(acdb_mline)


# For information about line- and fill parametrization see comments in class
# MLineVertex().
#
# The 2 group codes in mline entities and mlinestyle objects are redundant
# fields. These groups should not be modified under any circumstances, although
# it is safe to read them and use their values. The correct fields to modify
# are as follows:
#
# Mline
# The 340 group in the same object, which indicates the proper MLINESTYLE
# object.
#
# Mlinestyle
# The 3 group value in the MLINESTYLE dictionary, which precedes the 350 group
# that has the handle or entity name of
# the current mlinestyle.

# Facts and assumptions not clearly defined by the DXF reference:
# - the reference line is defined by the group code 11 points (fact)
# - all line segments are parallel to the reference line (assumption)
# - all line vertices are located in the same plane, the orientation of the plane
#   is defined by the extrusion vector (assumption)
# - the scale factor is applied to to all geometries
# - the start- and end angle (MLineStyle) is also applied to the first and last
#   miter direction vector
# - the last two points mean: all geometries and direction vectors can be used
#   as stored in the DXF file no additional scaling or rotation is necessary
#   for the MLINE rendering. Disadvantage: minor changes of DXF attributes
#   require a refresh of the MLineVertices.

# Ezdxf does not support the creation of line-break (gap) features, but will be
# preserving this data if the MLINE stays unchanged.
# Editing the MLINE entity by ezdxf removes the line-break features (gaps).


class MLineVertex:
    def __init__(self) -> None:
        self.location: Vec3 = NULLVEC
        self.line_direction: Vec3 = X_AXIS
        self.miter_direction: Vec3 = Y_AXIS

        # Line parametrization (74/41)
        # ----------------------------
        # The line parameterization is a list of float values.
        # The list may contain zero or more items.
        #
        # The first value (miter-offset) is the distance from the vertex
        # location along the miter direction vector to the point where the
        # line element's path intersects the miter vector.
        #
        # The next value (line-start-offset) is the distance along the line
        # direction from the miter/line path intersection point to the actual
        # start of the line element.
        #
        # The next value (dash-length) is the distance from the start of the
        # line element (dash) to the first break (or gap) in the line element.
        # The successive values continue to list the start and stop points of
        # the line element in this segment of the mline.
        # Linetypes do not affect the line parametrization.
        #
        #
        # 1. line element: [miter-offset, line-start-offset, dash, gap, dash, ...]
        # 2. line element: [...]
        # ...
        self.line_params: list[Sequence[float]] = []
        """ The line parameterization is a list of float values.
        The list may contain zero or more items.
        """

        # Fill parametrization (75/42)
        # ----------------------------
        #
        # The fill parameterization is also a list of float values.
        # Similar to the line parametrization, it describes the
        # parametrization of the fill area for this mline segment.
        # The values are interpreted identically to the line parameters and when
        # taken as a whole for all line elements in the mline segment, they
        # define the boundary of the fill area for the mline segment.
        #
        # A common example of the use of the fill mechanism is when an
        # unfilled mline crosses over a filled mline and "mledit" is used to
        # cause the filled mline to appear unfilled in the crossing area.
        # This would result in two fill parameters for each line element in the
        # affected mline segment; one for the fill stop and one for the fill
        # start.
        #
        # [dash-length, gap-length, ...]?
        self.fill_params: list[Sequence[float]] = []

    def __copy__(self) -> MLineVertex:
        vtx = self.__class__()
        vtx.location = self.location
        vtx.line_direction = self.line_direction
        vtx.miter_direction = self.miter_direction
        vtx.line_params = list(self.line_params)
        vtx.fill_params = list(self.fill_params)
        return vtx

    copy = __copy__

    @classmethod
    def load(cls, tags: Tags) -> MLineVertex:
        vtx = MLineVertex()
        line_params: list[float] = []
        line_params_count = 0
        fill_params: list[float] = []
        fill_params_count = 0
        for code, value in tags:
            if code == 11:
                vtx.location = Vec3(value)
            elif code == 12:
                vtx.line_direction = Vec3(value)
            elif code == 13:
                vtx.miter_direction = Vec3(value)
            elif code == 74:
                line_params_count = value
                if line_params_count == 0:
                    vtx.line_params.append(tuple())
                else:
                    line_params = []
            elif code == 41:
                line_params.append(value)
                line_params_count -= 1
                if line_params_count == 0:
                    vtx.line_params.append(tuple(line_params))
                    line_params = []
            elif code == 75:
                fill_params_count = value
                if fill_params_count == 0:
                    vtx.fill_params.append(tuple())
                else:
                    fill_params = []
            elif code == 42:
                fill_params.append(value)
                fill_params_count -= 1
                if fill_params_count == 0:
                    vtx.fill_params.append(tuple(fill_params))
        return vtx

    def export_dxf(self, tagwriter: AbstractTagWriter):
        tagwriter.write_vertex(11, self.location)
        tagwriter.write_vertex(12, self.line_direction)
        tagwriter.write_vertex(13, self.miter_direction)
        for line_params, fill_params in zip(self.line_params, self.fill_params):
            tagwriter.write_tag2(74, len(line_params))
            for param in line_params:
                tagwriter.write_tag2(41, param)
            tagwriter.write_tag2(75, len(fill_params))
            for param in fill_params:
                tagwriter.write_tag2(42, param)

    @classmethod
    def new(
        cls,
        start: UVec,
        line_direction: UVec,
        miter_direction: UVec,
        line_params: Optional[Iterable[Sequence[float]]] = None,
        fill_params: Optional[Iterable[Sequence[float]]] = None,
    ) -> MLineVertex:
        vtx = MLineVertex()
        vtx.location = Vec3(start)
        vtx.line_direction = Vec3(line_direction)
        vtx.miter_direction = Vec3(miter_direction)
        vtx.line_params = list(line_params or [])
        vtx.fill_params = list(fill_params or [])
        if len(vtx.line_params) != len(vtx.fill_params):
            raise const.DXFValueError("Count mismatch of line- and fill parameters")
        return vtx

    def transform(self, m: Matrix44) -> MLineVertex:
        """Transform MLineVertex by transformation matrix `m` inplace."""
        self.location = m.transform(self.location)
        self.line_direction = m.transform_direction(self.line_direction)
        self.miter_direction = m.transform_direction(self.miter_direction)
        return self


@register_entity
class MLine(DXFGraphic):
    DXFTYPE = "MLINE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_mline)
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000
    TOP = const.MLINE_TOP
    ZERO = const.MLINE_ZERO
    BOTTOM = const.MLINE_BOTTOM
    HAS_VERTICES = const.MLINE_HAS_VERTICES
    CLOSED = const.MLINE_CLOSED
    SUPPRESS_START_CAPS = const.MLINE_SUPPRESS_START_CAPS
    SUPPRESS_END_CAPS = const.MLINE_SUPPRESS_END_CAPS

    def __init__(self) -> None:
        super().__init__()
        # The MLINE geometry stored in vertices, is the final geometry,
        # scaling factor, justification and MLineStyle settings are already
        # applied. This is why the geometry has to be updated every time a
        # change is applied.
        self.vertices: list[MLineVertex] = []

    def __len__(self):
        """Count of MLINE vertices."""
        return len(self.vertices)

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, MLine)
        entity.vertices = [v.copy() for v in self.vertices]

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.fast_load_dxfattribs(
                dxf, acdb_mline_group_codes, 2, log=False
            )
            self.load_vertices(tags)
        return dxf

    def load_vertices(self, tags: Tags) -> None:
        self.vertices.extend(
            MLineVertex.load(tags) for tags in group_tags(tags, splitcode=11)
        )

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        # Do not export MLines without vertices
        return len(self.vertices) > 1
        # todo: check if line- and fill parametrization is compatible with
        #  MLINE style, requires same count of elements!

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        # ezdxf does not export MLINE entities without vertices,
        # see method preprocess_export()
        self.set_flag_state(self.HAS_VERTICES, True)
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_mline.name)
        self.dxf.export_dxf_attribs(tagwriter, acdb_mline.attribs.keys())
        self.export_vertices(tagwriter)

    def export_vertices(self, tagwriter: AbstractTagWriter) -> None:
        for vertex in self.vertices:
            vertex.export_dxf(tagwriter)

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        registry.add_handle(self.dxf.style_handle)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        super().map_resources(clone, mapping)
        style = mapping.get_reference_of_copy(self.dxf.style_handle)
        if not isinstance(style, MLineStyle):
            assert clone.doc is not None
            style = clone.doc.mline_styles.get("Standard")
        if isinstance(style, MLineStyle):
            clone.dxf.style_handle = style.dxf.handle
            clone.dxf.style_name = style.dxf.name
        else:
            clone.dxf.style_handle = "0"
            clone.dxf.style_name = "Standard"

    @property
    def is_closed(self) -> bool:
        """Returns ``True`` if MLINE is closed.
        Compatibility interface to :class:`Polyline`
        """
        return self.get_flag_state(self.CLOSED)

    def close(self, state: bool = True) -> None:
        """Get/set closed state of MLINE and update geometry accordingly.
        Compatibility interface to :class:`Polyline`
        """
        state = bool(state)
        if state != self.is_closed:
            self.set_flag_state(self.CLOSED, state)
            self.update_geometry()

    @property
    def start_caps(self) -> bool:
        """Get/Set start caps state. ``True`` to enable start caps and
        ``False`` tu suppress start caps."""
        return not self.get_flag_state(self.SUPPRESS_START_CAPS)

    @start_caps.setter
    def start_caps(self, value: bool) -> None:
        """Set start caps state."""
        self.set_flag_state(self.SUPPRESS_START_CAPS, not bool(value))

    @property
    def end_caps(self) -> bool:
        """Get/Set end caps state. ``True`` to enable end caps and
        ``False`` tu suppress start caps."""
        return not self.get_flag_state(self.SUPPRESS_END_CAPS)

    @end_caps.setter
    def end_caps(self, value: bool) -> None:
        """Set start caps state."""
        self.set_flag_state(self.SUPPRESS_END_CAPS, not bool(value))

    def set_scale_factor(self, value: float) -> None:
        """Set the scale factor and update geometry accordingly."""
        value = float(value)
        if not math.isclose(self.dxf.scale_factor, value):
            self.dxf.scale_factor = value
            self.update_geometry()

    def set_justification(self, value: int) -> None:
        """Set MLINE justification and update geometry accordingly.
        See :attr:`dxf.justification` for valid settings.
        """
        value = int(value)
        if self.dxf.justification != value:
            self.dxf.justification = value
            self.update_geometry()

    @property
    def style(self) -> Optional[MLineStyle]:
        """Get associated MLINESTYLE."""
        if self.doc is None:
            return None
        _style = self.doc.entitydb.get(self.dxf.style_handle)
        if _style is None:
            _style = self.doc.mline_styles.get(self.dxf.style_name)
        return _style  # type: ignore

    def set_style(self, name: str) -> None:
        """Set MLINESTYLE by name and update geometry accordingly.
        The MLINESTYLE definition must exist.
        """
        if self.doc is None:
            logger.debug("Can't change style of unbounded MLINE entity.")
            return
        try:
            style = self.doc.mline_styles[name]
        except const.DXFKeyError:
            raise const.DXFValueError(f"Undefined MLINE style: {name}")
        assert isinstance(style, MLineStyle)
        # Line- and fill parametrization depends on the count of
        # elements, a change in the number of elements triggers a
        # reset of the parametrization:
        old_style = self.style
        new_element_count = len(style.elements)
        reset = False
        if old_style is not None:
            # Do not trust the stored "style_element_count" value
            reset = len(old_style.elements) != new_element_count

        self.dxf.style_name = name
        self.dxf.style_handle = style.dxf.handle
        self.dxf.style_element_count = new_element_count
        if reset:
            self.update_geometry()

    def start_location(self) -> Vec3:
        """Returns the start location of the reference line. Callback function
        for :attr:`dxf.start_location`.
        """
        if len(self.vertices):
            return self.vertices[0].location
        else:
            return NULLVEC

    def get_locations(self) -> list[Vec3]:
        """Returns the vertices of the reference line."""
        return [v.location for v in self.vertices]

    def extend(self, vertices: Iterable[UVec]) -> None:
        """Append multiple vertices to the reference line.

        It is possible to work with 3D vertices, but all vertices have to be in
        the same plane and the normal vector of this plan is stored as
        extrusion vector in the MLINE entity.

        """
        vertices = Vec3.list(vertices)
        if not vertices:
            return
        all_vertices = []
        if len(self):
            all_vertices.extend(self.get_locations())
        all_vertices.extend(vertices)
        self.generate_geometry(all_vertices)

    def update_geometry(self) -> None:
        """Regenerate the MLINE geometry based on current settings."""
        self.generate_geometry(self.get_locations())

    def generate_geometry(self, vertices: list[Vec3]) -> None:
        """Regenerate the MLINE geometry for new reference line defined by
        `vertices`.
        """
        vertices = list(filter_close_vertices(vertices, abs_tol=1e-6))
        if len(vertices) == 0:
            self.clear()
            return
        elif len(vertices) == 1:
            self.vertices = [MLineVertex.new(vertices[0], X_AXIS, Y_AXIS)]
            return

        style = self.style
        assert style is not None, "valid MLINE style required"
        if len(style.elements) == 0:
            raise const.DXFStructureError(f"No line elements defined in {str(style)}.")

        def miter(dir1: Vec3, dir2: Vec3):
            return ((dir1 + dir2) * 0.5).normalize().orthogonal()

        ucs = UCS.from_z_axis_and_point_in_xz(
            origin=vertices[0],
            point=vertices[1],
            axis=self.dxf.extrusion,
        )
        # Transform given vertices into UCS and project them into the
        # UCS-xy-plane by setting the z-axis to 0:
        vertices = [v.replace(z=0.0) for v in ucs.points_from_wcs(vertices)]
        start_angle = style.dxf.start_angle
        end_angle = style.dxf.end_angle

        line_directions = [
            (v2 - v1).normalize() for v1, v2 in zip(vertices, vertices[1:])
        ]

        if self.is_closed:
            line_directions.append((vertices[0] - vertices[-1]).normalize())
            closing_miter = miter(line_directions[0], line_directions[-1])
            miter_directions = [closing_miter]
        else:
            closing_miter = None
            line_directions.append(line_directions[-1])
            miter_directions = [line_directions[0].rotate_deg(start_angle)]

        for d1, d2 in zip(line_directions, line_directions[1:]):
            miter_directions.append(miter(d1, d2))

        if closing_miter is None:
            miter_directions.pop()
            miter_directions.append(line_directions[-1].rotate_deg(end_angle))
        else:
            miter_directions.append(closing_miter)

        self.vertices = [
            MLineVertex.new(v, d, m)
            for v, d, m in zip(vertices, line_directions, miter_directions)
        ]
        self._update_parametrization()

        # reverse transformation into WCS
        for v in self.vertices:
            v.transform(ucs.matrix)

    def _update_parametrization(self):
        scale = self.dxf.scale_factor
        style = self.style

        justification = self.dxf.justification
        offsets = [e.offset for e in style.elements]
        min_offset = min(offsets)
        max_offset = max(offsets)
        shift = 0
        if justification == self.TOP:
            shift = -max_offset
        elif justification == self.BOTTOM:
            shift = -min_offset

        for vertex in self.vertices:
            angle = vertex.line_direction.angle_between(vertex.miter_direction)
            try:
                stretch = scale / math.sin(angle)
            except ZeroDivisionError:
                stretch = 1.0
            vertex.line_params = [
                ((element.offset + shift) * stretch, 0.0) for element in style.elements
            ]
            vertex.fill_params = [tuple() for _ in style.elements]

    def clear(self) -> None:
        """Remove all MLINE vertices."""
        self.vertices.clear()

    def remove_dependencies(self, other: Optional[Drawing] = None) -> None:
        """Remove all dependencies from current document.

        (internal API)
        """
        if not self.is_alive:
            return

        super().remove_dependencies(other)
        self.dxf.style_handle = "0"
        if other:
            style = other.mline_styles.get(self.dxf.style_name)
            if style:
                self.dxf.style_handle = style.dxf.handle
                return
        self.dxf.style_name = "Standard"

    def transform(self, m: Matrix44) -> Self:
        """Transform MLINE entity by transformation matrix `m` inplace."""
        for vertex in self.vertices:
            vertex.transform(m)
        self.dxf.extrusion = m.transform_direction(self.dxf.extrusion)
        scale = self.dxf.scale_factor
        scale_vec = m.transform_direction(Vec3(scale, scale, scale))
        if math.isclose(scale_vec.x, scale_vec.y, abs_tol=1e-6) and math.isclose(
            scale_vec.y, scale_vec.z, abs_tol=1e-6
        ):
            self.dxf.scale_factor = sum(scale_vec) / 3  # average error
        # None uniform scaling will not be applied to the scale_factor!
        self.update_geometry()
        self.post_transform(m)
        return self

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol.

        This protocol is for consistent internal usage and does not replace
        the method :meth:`virtual_entities`!
        """
        from ezdxf.render.mline import virtual_entities

        for e in virtual_entities(self):
            e.set_source_of_copy(self)
            yield e

    def virtual_entities(self) -> Iterator[DXFGraphic]:
        """Yields virtual DXF primitives of the MLINE entity  as LINE, ARC and HATCH
        entities.

        These entities are located at the original positions, but are not stored
        in the entity database, have no handle and are not assigned to any
        layout.

        """
        return self.__virtual_entities__()

    def explode(self, target_layout: Optional[BaseLayout] = None) -> EntityQuery:
        """Explode the MLINE entity as LINE, ARC and HATCH entities into target
        layout, if target layout is ``None``, the target layout is the layout
        of the MLINE. This method destroys the source entity.

        Returns an :class:`~ezdxf.query.EntityQuery` container referencing all DXF
        primitives.

        Args:
            target_layout: target layout for DXF primitives, ``None`` for same layout
                as source entity.
        """
        from ezdxf.explode import explode_entity

        return explode_entity(self, target_layout)

    def audit(self, auditor: Auditor) -> None:
        """Validity check."""

        def reset_mline_style(name="Standard"):
            auditor.fixed_error(
                code=AuditError.RESET_MLINE_STYLE,
                message=f'Reset MLINESTYLE to "{name}" in {str(self)}.',
                dxf_entity=self,
            )
            self.dxf.style_name = name
            style = doc.mline_styles.get(name)
            self.dxf.style_handle = style.dxf.handle

        super().audit(auditor)
        doc = auditor.doc
        if doc is None:
            return

        # Audit associated MLINESTYLE name and handle:
        style = doc.entitydb.get(self.dxf.style_handle)
        if not isinstance(style, MLineStyle):  # handle is invalid, get style by name
            style = doc.mline_styles.get(self.dxf.style_name, None)
            if style is None:
                reset_mline_style()
            else:  # fix MLINESTYLE handle:
                auditor.fixed_error(
                    code=AuditError.INVALID_MLINESTYLE_HANDLE,
                    message=f"Fixed invalid style handle in {str(self)}.",
                    dxf_entity=self,
                )
                self.dxf.style_handle = style.dxf.handle
        else:  # update MLINESTYLE name silently
            self.dxf.style_name = style.dxf.name

        # Get current (maybe fixed) MLINESTYLE:
        style = self.style
        assert style is not None, "valid MLINE style required"

        # Update style element count silently:
        element_count = len(style.elements)
        self.dxf.style_element_count = element_count

        # Audit vertices:
        for vertex in self.vertices:
            if NULLVEC.isclose(vertex.line_direction):
                break
            if NULLVEC.isclose(vertex.miter_direction):
                break
            if len(vertex.line_params) != element_count:
                break
            # Ignore fill parameters.
        else:  # no break
            return

        # Invalid vertices found:
        auditor.fixed_error(
            code=AuditError.INVALID_MLINE_VERTEX,
            message=f"Execute geometry update for {str(self)}.",
            dxf_entity=self,
        )
        self.update_geometry()

    def ocs(self) -> OCS:
        # WCS entity which supports the "extrusion" attribute in a
        # different way!
        return OCS()


acdb_mline_style = DefSubclass(
    "AcDbMlineStyle",
    {
        "name": DXFAttr(2, default="Standard"),
        # Flags (bit-coded):
        # 1 =Fill on
        # 2 = Display miters
        # 16 = Start square end (line) cap
        # 32 = Start inner arcs cap
        # 64 = Start round (outer arcs) cap
        # 256 = End square (line) cap
        # 512 = End inner arcs cap
        # 1024 = End round (outer arcs) cap
        "flags": DXFAttr(70, default=0),
        # Style description (string, 255 characters maximum):
        "description": DXFAttr(3, default=""),
        # Fill color (integer, default = 256):
        "fill_color": DXFAttr(
            62,
            default=256,
            validator=validator.is_valid_aci_color,
            fixer=RETURN_DEFAULT,
        ),
        # Start angle (real, default is 90 degrees):
        "start_angle": DXFAttr(51, default=90),
        # End angle (real, default is 90 degrees):
        "end_angle": DXFAttr(52, default=90),
        # 71: Number of elements
        # 49: Element offset (real, no default).
        #     Multiple entries can exist; one entry for each element
        # 62: Element color (integer, default = 0).
        #     Multiple entries can exist; one entry for each element
        # 6:  Element linetype (string, default = BYLAYER).
        #     Multiple entries can exist; one entry for each element
    },
)
acdb_mline_style_group_codes = group_code_mapping(acdb_mline_style)
MLineStyleElement = namedtuple("MLineStyleElement", "offset color linetype")


class MLineStyleElements:
    def __init__(self, tags: Optional[Tags] = None):
        self.elements: list[MLineStyleElement] = []
        if tags:
            for e in self.parse_tags(tags):
                data = MLineStyleElement(
                    e.get("offset", 1.0),
                    e.get("color", 0),
                    e.get("linetype", "BYLAYER"),
                )
                self.elements.append(data)

    def copy(self) -> MLineStyleElements:
        elements = MLineStyleElements()
        # new list of immutable data
        elements.elements = list(self.elements)
        return elements

    def __len__(self):
        return len(self.elements)

    def __getitem__(self, item):
        return self.elements[item]

    def __iter__(self):
        return iter(self.elements)

    def export_dxf(self, tagwriter: AbstractTagWriter):
        write_tag = tagwriter.write_tag2
        write_tag(71, len(self.elements))
        for offset, color, linetype in self.elements:
            write_tag(49, offset)
            write_tag(62, color)
            write_tag(6, linetype)

    def append(self, offset: float, color: int = 0, linetype: str = "BYLAYER") -> None:
        """Append a new line element.

        Args:
            offset: normal offset from the reference line: if justification is
                ``MLINE_ZERO``, positive values are above and negative values
                are below the reference line.
            color: :ref:`ACI` value
            linetype: linetype name

        """
        self.elements.append(
            MLineStyleElement(float(offset), int(color), str(linetype))
        )

    @staticmethod
    def parse_tags(tags: Tags) -> Iterator[dict]:
        collector = None
        for code, value in tags:
            if code == 49:
                if collector is not None:
                    yield collector
                collector = {"offset": value}
            elif code == 62:
                collector["color"] = value  # type: ignore
            elif code == 6:
                collector["linetype"] = value  # type: ignore
        if collector is not None:
            yield collector

    def ordered_indices(self) -> list[int]:
        offsets = [e.offset for e in self.elements]
        return [offsets.index(value) for value in sorted(offsets)]


@register_entity
class MLineStyle(DXFObject):
    DXFTYPE = "MLINESTYLE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_mline_style)
    FILL = const.MLINESTYLE_FILL
    MITER = const.MLINESTYLE_MITER
    START_SQUARE = const.MLINESTYLE_START_SQUARE
    START_INNER_ARC = const.MLINESTYLE_START_INNER_ARC
    START_ROUND = const.MLINESTYLE_START_ROUND
    END_SQUARE = const.MLINESTYLE_END_SQUARE
    END_INNER_ARC = const.MLINESTYLE_END_INNER_ARC
    END_ROUND = const.MLINESTYLE_END_ROUND

    def __init__(self):
        super().__init__()
        self.elements = MLineStyleElements()

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, MLineStyle)
        entity.elements = self.elements.copy()

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.subclass_by_index(1)
            if tags is None:
                raise const.DXFStructureError(
                    f"missing 'AcDbMLine' subclass in MLINE(#{dxf.handle})"
                )

            try:
                # Find index of the count tag:
                index71 = tags.tag_index(71)
            except const.DXFValueError:
                # The count tag does not exist: DXF structure error?
                pass
            else:
                self.elements = MLineStyleElements(tags[index71 + 1 :])  # type: ignore
                # Remove processed tags:
                del tags[index71:]
            processor.fast_load_dxfattribs(dxf, acdb_mline_style_group_codes, tags)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_mline_style.name)
        self.dxf.export_dxf_attribs(tagwriter, acdb_mline_style.attribs.keys())
        self.elements.export_dxf(tagwriter)

    def update_all(self):
        """Update all MLINE entities using this MLINESTYLE.

        The update is required if elements were added or removed or the offset
        of any element was changed.

        """
        if self.doc:
            handle = self.dxf.handle
            mlines = (e for e in self.doc.entitydb.values() if e.dxftype() == "MLINE")
            for mline in mlines:
                if mline.dxf.style_handle == handle:
                    mline.update_geometry()

    def ordered_indices(self) -> list[int]:
        return self.elements.ordered_indices()

    def audit(self, auditor: Auditor) -> None:
        super().audit(auditor)
        if len(self.elements) == 0:
            auditor.add_error(
                code=AuditError.INVALID_MLINESTYLE_ELEMENT_COUNT,
                message=f"No line elements defined in {str(self)}.",
                dxf_entity=self,
            )

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        for element in self.elements:
            registry.add_linetype(element.linetype)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, MLineStyle)
        super().map_resources(clone, mapping)
        self.elements.elements = [
            MLineStyleElement(
                element.offset,
                element.color,
                mapping.get_linetype(element.linetype),
            )
            for element in self.elements
        ]


class MLineStyleCollection(ObjectCollection[MLineStyle]):
    def __init__(self, doc: Drawing):
        super().__init__(doc, dict_name="ACAD_MLINESTYLE", object_type="MLINESTYLE")
        self.create_required_entries()

    def create_required_entries(self) -> None:
        if "Standard" not in self:
            entity: MLineStyle = self.new("Standard")
            entity.elements.append(0.5, 256)
            entity.elements.append(-0.5, 256)
