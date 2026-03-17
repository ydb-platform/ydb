# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Any,
    Iterable,
    Iterator,
    Optional,
    TYPE_CHECKING,
    Union,
    cast,
)
import abc
import math
import logging
import enum
from collections import defaultdict
from dataclasses import dataclass

from ezdxf import colors
from ezdxf.math import (
    BoundingBox,
    Matrix44,
    NULLVEC,
    OCS,
    UCS,
    UVec,
    Vec2,
    Vec3,
    X_AXIS,
    Z_AXIS,
    fit_points_to_cad_cv,
    is_point_left_of_line,
)
from ezdxf.entities import factory
from ezdxf.lldxf import const
from ezdxf.proxygraphic import ProxyGraphic
from ezdxf.render.arrows import ARROWS, arrow_length
from ezdxf.tools import text_size, text as text_tools
from ezdxf.entities.mleader import (
    AttribData,
    BlockData,
    LeaderData,
    LeaderLine,
    MLeaderContext,
    MLeaderStyle,
    MTextData,
    MultiLeader,
    acdb_mleader_style,
)

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.layouts import BlockLayout
    from ezdxf.entities import (
        DXFGraphic,
        Insert,
        MText,
        Spline,
        Textstyle,
    )

__all__ = [
    "virtual_entities",
    "MultiLeaderBuilder",
    "MultiLeaderMTextBuilder",
    "MultiLeaderBlockBuilder",
    "ConnectionSide",
    "HorizontalConnection",
    "VerticalConnection",
    "LeaderType",
    "TextAlignment",
    "BlockAlignment",
]

logger = logging.getLogger("ezdxf")

# How to render MLEADER: https://atlight.github.io/formats/dxf-leader.html
# DXF reference:
# http://help.autodesk.com/view/OARX/2018/ENU/?guid=GUID-72D20B8C-0F5E-4993-BEB7-0FCF94F32BE0

# AutoCAD and BricsCAD always (?) use values stored in the MULTILEADER
# entity, even if the override flag isn't set!
IGNORE_OVERRIDE_FLAGS = True

# Distance between baseline and underline as factor of cap-height
# Just guessing but the descender height is too much!
UNDERLINE_DISTANCE_FACTOR = 0.20


class ConnectionTypeError(const.DXFError):
    pass


OVERRIDE_FLAG = {
    "leader_type": 1 << 0,
    "leader_line_color": 1 << 1,
    "leader_linetype_handle": 1 << 2,
    "leader_lineweight": 1 << 3,
    "has_landing": 1 << 4,
    "landing_gap": 1 << 5,  # ??? not in MultiLeader
    "has_dogleg": 1 << 6,
    "dogleg_length": 1 << 7,
    "arrow_head_handle": 1 << 8,
    "arrow_head_size": 1 << 9,
    "content_type": 1 << 10,
    "text_style_handle": 1 << 11,
    "text_left_attachment_type": 1 << 12,
    "text_angle_type": 1 << 13,
    "text_alignment_type": 1 << 14,
    "text_color": 1 << 15,
    "char_height": 1 << 16,  # stored in MultiLeader.context.char_height
    "has_text_frame": 1 << 17,
    # 1 << 18 # use default content stored in MultiLeader.mtext.default_content
    "block_record_handle": 1 << 19,
    "block_color": 1 << 20,
    "block_scale_vector": 1 << 21,  # 3 values in MLeaderStyle
    "block_rotation": 1 << 22,
    "block_connection_type": 1 << 23,
    "scale": 1 << 24,
    "text_right_attachment_type": 1 << 25,
    "text_switch_alignment": 1 << 26,  # ??? not in MultiLeader/MLeaderStyle
    "text_attachment_direction": 1 << 27,  # this flag is not set by BricsCAD
    "text_top_attachment_type": 1 << 28,
    "text_bottom_attachment_type": 1 << 29,
}


class MLeaderStyleOverride:
    def __init__(self, style: MLeaderStyle, mleader: MultiLeader):
        self._style_dxf = style.dxf
        self._mleader_dxf = mleader.dxf
        self._context = mleader.context
        self._property_override_flags = mleader.dxf.get(
            "property_override_flags", 0
        )
        self._block_scale_vector = Vec3(
            (
                style.dxf.get("block_scale_x", 1.0),
                style.dxf.get("block_scale_y", 1.0),
                style.dxf.get("block_scale_z", 1.0),
            )
        )
        self.use_mtext_default_content = bool(
            self._property_override_flags & (1 << 18)
        )  # if False, what MTEXT content is used?

    def get(self, attrib_name: str) -> Any:
        # Set MLEADERSTYLE value as default value:
        if attrib_name == "block_scale_vector":
            value = self._block_scale_vector
        else:
            value = self._style_dxf.get_default(attrib_name)
        if IGNORE_OVERRIDE_FLAGS or self.is_overridden(attrib_name):
            # Get overridden value from MULTILEADER:
            if attrib_name == "char_height":
                value = self._context.char_height
            else:
                value = self._mleader_dxf.get(attrib_name, value)
        return value

    def is_overridden(self, attrib_name: str) -> bool:
        flag = OVERRIDE_FLAG.get(attrib_name, 0)
        return bool(flag & self._property_override_flags)


def virtual_entities(
    mleader: MultiLeader, proxy_graphic=False
) -> Iterator[DXFGraphic]:
    doc = mleader.doc
    assert doc is not None, "valid DXF document required"
    if proxy_graphic and mleader.proxy_graphic is not None:
        return ProxyGraphic(mleader.proxy_graphic, doc).virtual_entities()
    else:
        return iter(RenderEngine(mleader, doc).run())


def get_style(mleader: MultiLeader, doc: Drawing) -> MLeaderStyleOverride:
    handle = mleader.dxf.style_handle
    style = doc.entitydb.get(handle)
    if style is None:
        logger.warning(
            f"referenced MLEADERSTYLE(#{handle}) does not exist, "
            f"replaced by 'Standard'"
        )
        style = doc.mleader_styles.get("Standard")
    assert style is not None, "mandatory MLEADERSTYLE 'Standard' does not exist"
    return MLeaderStyleOverride(cast(MLeaderStyle, style), mleader)


def get_text_style(handle: str, doc: Drawing) -> Textstyle:
    text_style = doc.entitydb.get(handle)
    if text_style is None:
        logger.warning(
            f"referenced STYLE(#{handle}) does not exist, "
            f"replaced by 'Standard'"
        )
        text_style = doc.styles.get("Standard")
    assert text_style is not None, "mandatory STYLE 'Standard' does not exist"
    return text_style  # type: ignore


def get_arrow_direction(vertices: list[Vec3]) -> Vec3:
    if len(vertices) < 2:
        return X_AXIS
    direction = vertices[1] - vertices[0]
    return direction.normalize()


ACI_COLOR_TYPES = {
    colors.COLOR_TYPE_BY_BLOCK,
    colors.COLOR_TYPE_BY_LAYER,
    colors.COLOR_TYPE_ACI,
}


def decode_raw_color(raw_color: int) -> tuple[int, Optional[int]]:
    aci_color = colors.BYBLOCK
    true_color: Optional[int] = None
    color_type, color = colors.decode_raw_color_int(raw_color)
    if color_type in ACI_COLOR_TYPES:
        aci_color = color
    elif color_type == colors.COLOR_TYPE_RGB:
        true_color = color
    # COLOR_TYPE_WINDOW_BG: not supported as entity color
    return aci_color, true_color


def copy_mtext_data(mtext: MText, mtext_data: MTextData, scale: float) -> None:
    # MLEADERSTYLE has a flag "use_mtext_default_content", what else should be
    # used as content if this flag is false?
    mtext.text = mtext_data.default_content
    dxf = mtext.dxf
    aci_color, true_color = decode_raw_color(mtext_data.color)
    dxf.color = aci_color
    if true_color is not None:
        dxf.true_color = true_color
    dxf.insert = mtext_data.insert
    assert mtext.doc is not None
    mtext.dxf.style = get_text_style(
        mtext_data.style_handle, mtext.doc
    ).dxf.name
    if not mtext_data.extrusion.isclose(Z_AXIS):
        dxf.extrusion = mtext_data.extrusion
    dxf.text_direction = mtext_data.text_direction
    # ignore rotation!
    dxf.width = mtext_data.width * scale
    dxf.line_spacing_factor = mtext_data.line_spacing_factor
    dxf.line_spacing_style = mtext_data.line_spacing_style
    dxf.flow_direction = mtext_data.flow_direction
    # alignment=attachment_point: 1=top left, 2=top center, 3=top right
    dxf.attachment_point = mtext_data.alignment


def make_mtext(mleader: MultiLeader) -> MText:
    mtext = cast("MText", factory.new("MTEXT", doc=mleader.doc))
    mtext.dxf.layer = mleader.dxf.layer
    context = mleader.context
    mtext_data = context.mtext
    if mtext_data is None:
        raise TypeError(f"MULTILEADER has no MTEXT content")
    scale = context.scale
    # !char_height is the final already scaled value!
    mtext.dxf.char_height = context.char_height
    if mtext_data is not None:
        copy_mtext_data(mtext, mtext_data, scale)
        if mtext_data.has_bg_fill:
            set_mtext_bg_fill(mtext, mtext_data)
        set_mtext_columns(mtext, mtext_data, scale)
    return mtext


def set_mtext_bg_fill(mtext: MText, mtext_data: MTextData) -> None:
    # Note: the "text frame" flag (16) in "bg_fill" is never set by BricsCAD!
    # Set required DXF attributes:
    mtext.dxf.box_fill_scale = mtext_data.bg_scale_factor
    mtext.dxf.bg_fill = 1
    mtext.dxf.bg_fill_color = colors.BYBLOCK
    mtext.dxf.bg_fill_transparency = mtext_data.bg_transparency
    color_type, color = colors.decode_raw_color_int(mtext_data.bg_color)
    if color_type in ACI_COLOR_TYPES:
        mtext.dxf.bg_fill_color = color
    elif color_type == colors.COLOR_TYPE_RGB:
        mtext.dxf.bg_fill_true_color = color

    if (
        mtext_data.use_window_bg_color
        or color_type == colors.COLOR_TYPE_WINDOW_BG
    ):
        # override fill mode, but keep stored colors
        mtext.dxf.bg_fill = 3


def set_mtext_columns(
    mtext: MText, mtext_data: MTextData, scale: float
) -> None:
    # BricsCAD does not support columns for MTEXT content, so exploring
    # MLEADER with columns was not possible!
    pass


def _get_insert(entity: MultiLeader) -> Vec3:
    context = entity.context
    if context.mtext is not None:
        return context.mtext.insert
    elif context.block is not None:
        return context.block.insert
    return context.plane_origin


def _get_extrusion(entity: MultiLeader) -> Vec3:
    context = entity.context
    if context.mtext is not None:
        return context.mtext.extrusion
    elif context.block is not None:
        return context.block.extrusion
    return context.plane_z_axis


def _get_dogleg_vector(leader: LeaderData, default: Vec3 = X_AXIS) -> Vec3:
    # All leader vertices and directions in WCS!
    try:
        if leader.has_dogleg_vector:  # what else?
            return leader.dogleg_vector.normalize(leader.dogleg_length)
    except ZeroDivisionError:  # dogleg_vector is NULL
        pass
    return default.normalize(leader.dogleg_length)


def _get_block_name(handle: str, doc: Drawing) -> Optional[str]:
    block_record = doc.entitydb.get(handle)
    if block_record is None:
        logger.error(f"required BLOCK_RECORD entity #{handle} does not exist")
        return None
    return block_record.dxf.get("name")  # has no default value


class RenderEngine:
    def __init__(self, mleader: MultiLeader, doc: Drawing):
        self.entities: list[DXFGraphic] = []  # result container
        self.mleader = mleader
        self.doc = doc
        self.style: MLeaderStyleOverride = get_style(mleader, doc)
        self.context = mleader.context
        self.insert = _get_insert(mleader)
        self.extrusion: Vec3 = _get_extrusion(mleader)
        self.ocs: Optional[OCS] = None
        if not self.extrusion.isclose(Z_AXIS):
            self.ocs = OCS(self.extrusion)
        self.elevation: float = self.insert.z

        # Gather final parameters from various places:
        # This is the actual entity scaling, ignore scale in MLEADERSTYLE!
        self.scale: float = self.context.scale
        self.layer = mleader.dxf.layer
        self.linetype = self.linetype_name()
        self.lineweight = self.style.get("leader_lineweight")
        aci_color, true_color = decode_raw_color(
            self.style.get("leader_line_color")
        )
        self.leader_aci_color: int = aci_color
        self.leader_true_color: Optional[int] = true_color
        self.leader_type: int = self.style.get("leader_type")
        self.has_text_frame = False
        self.has_dogleg: bool = bool(self.style.get("has_dogleg"))
        self.arrow_heads: dict[int, str] = {
            head.index: head.handle for head in mleader.arrow_heads
        }
        self.arrow_head_handle = self.style.get("arrow_head_handle")
        self.dxf_mtext_entity: Optional["MText"] = None
        self._dxf_mtext_extents: Optional[tuple[float, float]] = None
        self.has_horizontal_attachment = bool(
            self.style.get("text_attachment_direction")
        )
        self.left_attachment_type = self.style.get("text_left_attachment_type")
        self.right_attachment_type = self.style.get(
            "text_right_attachment_type"
        )
        self.top_attachment_type = self.style.get("text_top_attachment_type")
        self.bottom_attachment_type = self.style.get(
            "text_bottom_attachment_type"
        )

    @property
    def has_extrusion(self) -> bool:
        return self.ocs is not None

    @property
    def has_text_content(self) -> bool:
        return self.context.mtext is not None

    @property
    def has_block_content(self) -> bool:
        return self.context.block is not None

    @property
    def mtext_extents(self) -> tuple[float, float]:
        """Calculate MTEXT width on demand."""

        if self._dxf_mtext_extents is not None:
            return self._dxf_mtext_extents
        if self.dxf_mtext_entity is not None:
            # TODO: this is very inaccurate if using inline codes, better
            #  solution is required like a text layout engine with column width
            #  calculation from the MTEXT content.
            width, height = text_size.estimate_mtext_extents(
                self.dxf_mtext_entity
            )
        else:
            width, height = 0.0, 0.0
        self._dxf_mtext_extents = (width, height)
        return self._dxf_mtext_extents

    def run(self) -> list[DXFGraphic]:
        """Entry point to render MLEADER entities."""
        self.entities.clear()
        if abs(self.scale) > 1e-9:
            self.add_content()
            self.add_leaders()
        # otherwise it vanishes by scaling down to almost "nothing"
        return self.entities

    def linetype_name(self) -> str:
        handle = self.style.get("leader_linetype_handle")
        ltype = self.doc.entitydb.get(handle)
        if ltype is not None:
            return ltype.dxf.name
        logger.warning(f"invalid linetype handle #{handle} in {self.mleader}")
        return "Continuous"

    def arrow_block_name(self, index: int) -> str:
        closed_filled = "_CLOSED_FILLED"
        handle = self.arrow_heads.get(index, self.arrow_head_handle)
        if handle is None or handle == "0":
            return closed_filled
        block_record = self.doc.entitydb.get(handle)
        if block_record is None:
            logger.warning(
                f"arrow block #{handle} in {self.mleader} does not exist, "
                f"replaced by closed filled arrow"
            )
            return closed_filled
        return block_record.dxf.name

    def leader_line_attribs(self, raw_color: Optional[int] = None) -> dict:
        aci_color = self.leader_aci_color
        true_color = self.leader_true_color

        # Ignore color override value BYBLOCK!
        if raw_color and raw_color is not colors.BY_BLOCK_RAW_VALUE:
            aci_color, true_color = decode_raw_color(raw_color)

        attribs = {
            "layer": self.layer,
            "color": aci_color,
            "linetype": self.linetype,
            "lineweight": self.lineweight,
        }
        if true_color is not None:
            attribs["true_color"] = true_color
        return attribs

    def add_content(self) -> None:
        # also check self.style.get("content_type") ?
        if self.has_text_content:
            self.add_mtext_content()
        elif self.has_block_content:
            self.add_block_content()

    def add_mtext_content(self) -> None:
        mtext = make_mtext(self.mleader)
        self.entities.append(mtext)
        self.dxf_mtext_entity = mtext
        if self.has_text_frame:
            self.add_text_frame()

    def add_text_frame(self) -> None:
        # not supported - yet?
        # 1. This requires a full MTEXT height calculation.
        # 2. Only the default rectangle and the rounded rectangle are
        #    acceptable every other text frame is just ugly, especially
        #    when the MTEXT gets more complex.
        pass

    def add_block_content(self) -> None:
        block = self.context.block
        assert block is not None
        block_name = _get_block_name(block.block_record_handle, self.doc)  # type: ignore
        if block_name is None:
            return
        location = block.insert  # in WCS, really funny for an OCS entity!
        rotation = math.degrees(block.rotation)
        if self.ocs is not None:
            location = self.ocs.from_wcs(location)
        aci_color, true_color = decode_raw_color(block.color)
        scale = block.scale

        attribs = {
            "name": block_name,
            "insert": location,
            "rotation": rotation,
            "color": aci_color,
            "extrusion": block.extrusion,
            "xscale": scale.x,
            "yscale": scale.y,
            "zscale": scale.z,
        }
        if true_color is not None:
            attribs["true_color"] = true_color
        insert = cast(
            "Insert", factory.new("INSERT", dxfattribs=attribs, doc=self.doc)
        )
        self.entities.append(insert)
        if self.mleader.block_attribs:
            self.add_block_attributes(insert)

    def add_block_attributes(self, insert: Insert):
        entitydb = self.doc.entitydb
        values: dict[str, str] = dict()
        for attrib in self.mleader.block_attribs:
            attdef = entitydb.get(attrib.handle)
            if attdef is None:
                logger.error(
                    f"required ATTDEF entity #{attrib.handle} does not exist"
                )
                continue
            tag = attdef.dxf.tag
            values[tag] = attrib.text
        if values:
            insert.add_auto_attribs(values)

    def add_leaders(self) -> None:
        if self.leader_type == 0:
            return

        for leader in self.context.leaders:
            for line in leader.lines:
                self.add_leader_line(leader, line)

            if self.has_text_content:
                if leader.has_horizontal_attachment:
                    # add text underlines for these horizontal attachment styles:
                    # 5 = bottom of bottom text line & underline bottom text line
                    # 6 = bottom of top text line & underline top text line
                    self.add_text_underline(leader)
                else:
                    # text with vertical attachment may have an extra "overline"
                    # across the text
                    self.add_overline(leader)

    def add_text_underline(self, leader: LeaderData):
        mtext = self.context.mtext
        if mtext is None:
            return
        has_left_underline = self.left_attachment_type in (5, 6, 8)
        has_right_underline = self.right_attachment_type in (5, 6, 8)
        if not (has_left_underline or has_right_underline):
            return
        connection_point = leader.last_leader_point + _get_dogleg_vector(leader)
        width, height = self.mtext_extents
        length = width + self.context.landing_gap_size
        if length < 1e-9:
            return
        # The connection point is on the "left" or "right" side of the
        # detection line, which is a "vertical" line through the text
        # insertion point.
        start = mtext.insert
        if self.ocs is None:  # text plane is parallel to the xy-plane
            start2d = start.vec2
            up2d = mtext.text_direction.vec2.orthogonal()
            cp2d = connection_point.vec2
        else:  # project points into the text plane
            from_wcs = self.ocs.from_wcs
            start2d = Vec2(from_wcs(start))
            up2d = Vec2(from_wcs(mtext.text_direction)).orthogonal()
            cp2d = Vec2(from_wcs(connection_point))
        is_left = is_point_left_of_line(cp2d, start2d, start2d + up2d)
        is_right = not is_left
        line = mtext.text_direction.normalize(length if is_left else -length)
        if (is_left and has_left_underline) or (
            is_right and has_right_underline
        ):
            self.add_dxf_line(connection_point, connection_point + line)

    def add_overline(self, leader: LeaderData):
        mtext = self.context.mtext
        if mtext is None:
            return
        has_bottom_line = self.bottom_attachment_type == 10
        has_top_line = self.top_attachment_type == 10
        if not (has_bottom_line or has_top_line):
            return
        length, height = self.mtext_extents
        if length < 1e-9:
            return

        # The end of the leader is the center of the "overline".
        # The leader is on the bottom of the text if the insertion
        # point of the text is left of "overline" (start -> end).
        center = leader.last_leader_point
        insert = mtext.insert
        line2 = mtext.text_direction.normalize(length / 2)
        start = center - line2
        end = center + line2

        if self.ocs is None:  # z-axis is ignored
            bottom = is_point_left_of_line(insert, start, end)
        else:  # project points into the text plane, z-axis is ignored
            from_wcs = self.ocs.from_wcs
            bottom = is_point_left_of_line(
                from_wcs(insert), from_wcs(start), from_wcs(end)
            )
        top = not bottom
        if (bottom and has_bottom_line) or (top and has_top_line):
            self.add_dxf_line(start, end)

    def leader_vertices(
        self, leader: LeaderData, line_vertices: list[Vec3], has_dogleg=False
    ) -> list[Vec3]:
        # All leader vertices and directions in WCS!
        vertices = list(line_vertices)
        end_point = leader.last_leader_point
        if leader.has_horizontal_attachment:
            if has_dogleg:
                vertices.append(end_point)
            vertices.append(end_point + _get_dogleg_vector(leader))
        else:
            vertices.append(end_point)
        return vertices

    def add_leader_line(self, leader: LeaderData, line: LeaderLine):
        # All leader vertices and directions in WCS!
        leader_type: int = self.leader_type
        if leader_type == 0:  # invisible leader lines
            return
        has_dogleg: bool = self.has_dogleg
        if leader_type == 2:  # splines do not have a dogleg!
            has_dogleg = False
        vertices: list[Vec3] = self.leader_vertices(
            leader, line.vertices, has_dogleg
        )
        if len(vertices) < 2:  # at least 2 vertices required
            return

        arrow_direction: Vec3 = get_arrow_direction(vertices)
        raw_color: int = line.color
        index: int = line.index
        block_name: str = self.create_arrow_block(self.arrow_block_name(index))
        arrow_size: float = self.context.arrow_head_size
        self.add_arrow(
            name=block_name,
            location=vertices[0],
            direction=arrow_direction,
            scale=arrow_size,
            color=raw_color,
        )
        arrow_offset: Vec3 = arrow_direction * arrow_length(
            block_name, arrow_size
        )
        vertices[0] += arrow_offset
        if leader_type == 1:  # add straight lines
            for s, e in zip(vertices, vertices[1:]):
                self.add_dxf_line(s, e, raw_color)
        elif leader_type == 2:  # add spline
            if leader.has_horizontal_attachment:
                end_tangent = _get_dogleg_vector(leader)
            else:
                end_tangent = vertices[-1] - vertices[-2]
            self.add_dxf_spline(
                vertices,
                # tangent normalization is not required
                tangents=[arrow_direction, end_tangent],
                color=raw_color,
            )

    def create_arrow_block(self, name: str) -> str:
        if name not in self.doc.blocks:
            # create predefined arrows
            arrow_name = ARROWS.arrow_name(name)
            if arrow_name not in ARROWS:
                arrow_name = ARROWS.closed_filled
            name = ARROWS.create_block(self.doc.blocks, arrow_name)
        return name

    def add_dxf_spline(
        self,
        fit_points: list[Vec3],
        tangents: Optional[Iterable[UVec]] = None,
        color: Optional[int] = None,
    ):
        attribs = self.leader_line_attribs(color)
        spline = cast(
            "Spline",
            factory.new("SPLINE", dxfattribs=attribs, doc=self.doc),
        )
        spline.apply_construction_tool(
            fit_points_to_cad_cv(fit_points, tangents=tangents)
        )
        self.entities.append(spline)

    def add_dxf_line(self, start: Vec3, end: Vec3, color: Optional[int] = None):
        attribs = self.leader_line_attribs(color)
        attribs["start"] = start
        attribs["end"] = end
        self.entities.append(
            factory.new("LINE", dxfattribs=attribs, doc=self.doc)  # type: ignore
        )

    def add_arrow(
        self,
        name: str,
        location: Vec3,
        direction: Vec3,
        scale: float,
        color: int,
    ):
        attribs = self.leader_line_attribs(color)
        attribs["name"] = name
        if self.ocs is not None:
            location = self.ocs.from_wcs(location)
            direction = self.ocs.from_wcs(direction)
            attribs["extrusion"] = self.extrusion

        attribs["insert"] = location
        attribs["rotation"] = direction.angle_deg + 180.0
        attribs["xscale"] = scale
        attribs["yscale"] = scale
        attribs["zscale"] = scale
        self.entities.append(
            factory.new("INSERT", dxfattribs=attribs, doc=self.doc)  # type: ignore
        )


class LeaderType(enum.IntEnum):
    """The leader type."""

    none = 0
    straight_lines = 1
    splines = 2


# noinspection PyArgumentList
class ConnectionSide(enum.Enum):
    """
    The leader connection side.

    Vertical (top, bottom) and horizontal attachment sides (left, right)
    can not be mixed in a single entity - this is a limitation of the
    MULTILEADER entity.
    """

    left = enum.auto()
    right = enum.auto()
    top = enum.auto()
    bottom = enum.auto()


DOGLEG_DIRECTIONS = {
    ConnectionSide.left: X_AXIS,
    ConnectionSide.right: -X_AXIS,
    ConnectionSide.top: -X_AXIS,  # ???
    ConnectionSide.bottom: X_AXIS,  # ???
}


class HorizontalConnection(enum.IntEnum):
    """The horizontal leader connection type."""

    by_style = -1
    top_of_top_line = 0
    middle_of_top_line = 1
    middle_of_text = 2
    middle_of_bottom_line = 3
    bottom_of_bottom_line = 4
    bottom_of_bottom_line_underline = 5
    bottom_of_top_line_underline = 6
    bottom_of_top_line = 7
    bottom_of_top_line_underline_all = 8


class VerticalConnection(enum.IntEnum):
    """The vertical leader connection type."""

    by_style = 0
    center = 9
    center_overline = 10


class TextAlignment(enum.IntEnum):
    """The :class:`MText` alignment type."""

    left = 1
    center = 2
    right = 3


class BlockAlignment(enum.IntEnum):
    """The :class:`Block` alignment type."""

    center_extents = 0
    insertion_point = 1


@dataclass
class ConnectionBox:
    """Contains the connection points for all 4 sides of the content, the
    landing gap is included.
    """

    left: Vec2
    right: Vec2
    top: Vec2
    bottom: Vec2

    def get(self, side: ConnectionSide) -> Vec2:
        if side == ConnectionSide.left:
            return self.left
        elif side == ConnectionSide.right:
            return self.right
        elif side == ConnectionSide.top:
            return self.top
        return self.bottom


def ocs_rotation(ucs: UCS) -> float:
    """Returns the ocs rotation angle of the UCS"""
    if not Z_AXIS.isclose(ucs.uz):
        ocs = OCS(ucs.uz)
        x_axis_in_ocs = Vec2(ocs.from_wcs(ucs.ux))
        return x_axis_in_ocs.angle  # in radians!
    return 0.0


class MultiLeaderBuilder(abc.ABC):
    def __init__(self, multileader: MultiLeader):
        doc = multileader.doc
        assert doc is not None, "valid DXF document required"
        handle = multileader.dxf.style_handle
        style: MLeaderStyle = doc.entitydb.get(handle)  # type: ignore
        if style is None:
            raise ValueError(f"invalid MLEADERSTYLE handle #{handle}")
        self._doc: "Drawing" = doc
        self._mleader_style: MLeaderStyle = style
        self._multileader = multileader
        self._leaders: dict[ConnectionSide, list[list[Vec2]]] = defaultdict(
            list
        )
        self.set_mleader_style(style)
        self._multileader.context.landing_gap_size = (
            self._mleader_style.dxf.landing_gap_size
        )

    @abc.abstractmethod
    def _init_content(self):
        ...

    def _reset_cache(self):
        pass

    @abc.abstractmethod
    def _build_connection_box(self) -> ConnectionBox:
        """Returns the connection box with the connection points on all 4 sides
        in build UCS coordinates. The origin of the build ucs is the insertion
        or attachment point of the content.
        """
        ...

    @abc.abstractmethod
    def _transform_content_to_render_ucs(
        self, insert: Vec2, rotation: float
    ) -> None:
        ...

    @abc.abstractmethod
    def _apply_conversion_factor(self, conversion_factor: float) -> None:
        ...

    @abc.abstractmethod
    def _set_base_point(self, left: Vec3, bottom: Vec3):
        ...

    @property
    def multileader(self) -> MultiLeader:
        """Returns the :class:`~ezdxf.entities.MultiLeader` entity."""
        return self._multileader

    @property
    def context(self) -> MLeaderContext:
        """Returns the context entity :class:`~ezdxf.entities.MLeaderContext`."""
        return self._multileader.context

    @property
    def _landing_gap_size(self) -> float:
        return self._multileader.context.landing_gap_size

    def set_mleader_style(self, style: MLeaderStyle):
        """Reset base properties by :class:`~ezdxf.entities.MLeaderStyle`
        properties. This also resets the content!
        """

        def copy_style_to_context():
            self.context.char_height = style_dxf.char_height
            self.context.landing_gap_size = style_dxf.landing_gap_size

        self._mleader_style = style
        multileader_dxf = self._multileader.dxf
        style_dxf = style.dxf
        keys = list(acdb_mleader_style.attribs.keys())
        for key in keys:
            if multileader_dxf.is_supported(key):
                multileader_dxf.set(key, style_dxf.get_default(key))
        copy_style_to_context()
        multileader_dxf.block_scale_vector = Vec3(
            style_dxf.block_scale_x,
            style_dxf.block_scale_y,
            style_dxf.block_scale_z,
        )
        # MLEADERSTYLE contains unscaled values
        self.context.set_scale(multileader_dxf.scale)
        self._init_content()

    def set_connection_properties(
        self,
        landing_gap: float = 0.0,  # unscaled value!
        dogleg_length: float = 0.0,  # unscaled value!
    ):
        """Set the properties how to connect the leader line to the content.

        The landing gap is the space between the content and the start of the
        leader line. The "dogleg" is the first line segment of the leader
        in the "horizontal" direction of the content.

        """
        multileader = self.multileader
        scale = multileader.dxf.scale
        if dogleg_length:
            multileader.dxf.has_dogleg = 1
            multileader.dxf.dogleg_length = dogleg_length * scale
        else:
            multileader.dxf.has_dogleg = 0
        multileader.context.landing_gap_size = landing_gap * scale

    def set_connection_types(
        self,
        left=HorizontalConnection.by_style,
        right=HorizontalConnection.by_style,
        top=VerticalConnection.by_style,
        bottom=VerticalConnection.by_style,
    ):
        """Set the connection type for each connection side."""
        context = self.context
        style = self._mleader_style
        if left == HorizontalConnection.by_style:
            context.left_attachment = style.dxf.text_left_attachment_type
        else:
            context.left_attachment = int(left)

        if right == HorizontalConnection.by_style:
            context.right_attachment = style.dxf.text_right_attachment_type
        else:
            context.right_attachment = int(right)

        if top == VerticalConnection.by_style:
            context.top_attachment = style.dxf.text_top_attachment_type
        else:
            context.top_attachment = int(top)

        if bottom == VerticalConnection.by_style:
            context.bottom_attachment = style.dxf.text_bottom_attachment_type
        else:
            context.bottom_attachment = int(bottom)

        dxf = self._multileader.dxf
        dxf.text_left_attachment_type = context.left_attachment
        dxf.text_right_attachment_type = context.right_attachment
        dxf.text_top_attachment_type = context.top_attachment
        dxf.text_bottom_attachment_type = context.bottom_attachment

    def set_overall_scaling(self, scale: float):
        """Set the overall scaling factor for the whole entity,
        except for the leader line vertices!

        Args:
            scale: scaling factor > 0.0

        """
        new_scale = float(scale)
        if new_scale <= 0.0:
            raise ValueError(f"invalid scaling factor: {scale}")
        context = self.context
        multileader = self.multileader
        old_scale = multileader.dxf.scale
        try:
            # convert from existing scaling to new scale factor
            conversion_factor = new_scale / old_scale
        except ZeroDivisionError:
            conversion_factor = new_scale

        multileader.dxf.scale = new_scale
        multileader.dxf.dogleg_length *= conversion_factor
        context.set_scale(new_scale)
        self._apply_conversion_factor(conversion_factor)

    def set_leader_properties(
        self,
        color: Union[int, colors.RGB] = colors.BYBLOCK,
        linetype: str = "BYBLOCK",
        lineweight: int = const.LINEWEIGHT_BYBLOCK,
        leader_type=LeaderType.straight_lines,
    ):
        """Set leader line properties.

        Args:
            color: line color as :ref:`ACI` or RGB tuple
            linetype: as name string, e.g. "BYLAYER"
            lineweight: as integer value, see: :ref:`lineweights`
            leader_type: straight lines of spline type

        """
        mleader = self._multileader
        mleader.dxf.leader_line_color = colors.encode_raw_color(color)
        linetype_ = self._doc.linetypes.get(linetype)
        if linetype_ is None:
            raise ValueError(f"invalid linetype name '{linetype}'")
        mleader.dxf.leader_linetype_handle = linetype_.dxf.handle
        mleader.dxf.leader_lineweight = lineweight
        mleader.dxf.leader_type = int(leader_type)

    def set_arrow_properties(
        self,
        name: str = "",
        size: float = 0.0,  # 0=by style
    ):
        """Set leader arrow properties all leader lines have the same arrow
        type.

        The MULTILEADER entity is able to support multiple arrows, but this
        seems to be unsupported by CAD applications and is therefore also not
        supported by the builder classes.

        """
        if size == 0.0:
            size = self._mleader_style.dxf.arrow_head_size
        self._multileader.dxf.arrow_head_size = size
        self.context.arrow_head_size = size
        if name:
            self._multileader.dxf.arrow_head_handle = ARROWS.arrow_handle(
                self._doc.blocks, name
            )
        else:
            # empty string is the default "closed filled" arrow,
            # no handle needed
            del self._multileader.dxf.arrow_head_handle

    def add_leader_line(
        self, side: ConnectionSide, vertices: Iterable[Vec2]
    ) -> None:
        """Add leader as iterable of vertices in render UCS coordinates
        (:ref:`WCS` by default).

        .. note::

            Vertical (top, bottom) and horizontal attachment sides (left, right)
            can not be mixed in a single entity - this is a limitation of the
            MULTILEADER entity.

        Args:
            side: connection side where to attach the leader line
            vertices: leader vertices

        """
        self._leaders[side].append(list(vertices))

    def build(
        self, insert: Vec2, rotation: float = 0.0, ucs: Optional[UCS] = None
    ) -> None:
        """Compute the required geometry data. The construction plane is
        the xy-plane of the given render :class:`~ezdxf.math.UCS`.

        Args:
            insert: insert location for the content in render UCS coordinates
            rotation: content rotation angle around the render UCS z-axis in degrees
            ucs: the render :class:`~ezdxf.math.UCS`, default is the :ref:`WCS`

        """
        assert isinstance(insert, Vec2), "insert has to be a Vec2() object"
        rotation = math.radians(rotation)
        # transformation matrix from build ucs to render ucs
        m = Matrix44.z_rotate(rotation)
        connection_box = self._build_connection_box()
        m.set_row(3, (insert.x, insert.y))  # fast translation

        self._set_required_multileader_attributes()
        self._transform_content_to_render_ucs(insert, rotation)
        self._set_attachment_direction()
        self._set_base_point(  # MTEXT requires "text_attachment_direction"
            left=m.transform(connection_box.left.vec3),
            bottom=m.transform(connection_box.bottom.vec3),
        )
        self.context.leaders.clear()

        for side, leader_lines in self._leaders.items():
            self._build_leader(leader_lines, side, connection_box.get(side), m)
        if ucs is not None:
            self._transform_to_wcs(ucs)
        self.multileader.update_proxy_graphic()

    def _set_attachment_direction(self):
        leaders = self._leaders
        if leaders:
            horizontal = (ConnectionSide.left in leaders) or (
                ConnectionSide.right in leaders
            )
            vertical = (ConnectionSide.top in leaders) or (
                ConnectionSide.bottom in leaders
            )
            if horizontal and vertical:
                raise ConnectionTypeError(
                    "invalid mix of horizontal and vertical connection types"
                )
            self._multileader.dxf.text_attachment_direction = (
                0 if horizontal else 1
            )
            if vertical:
                self._multileader.dxf.has_dogleg = False
        # else MULTILEADER without any leader lines!

    def _transform_to_wcs(self, ucs: UCS) -> None:
        # transformation from render UCS into WCS
        self.multileader.transform(ucs.matrix)

    def _set_required_multileader_attributes(self):
        dxf = self.multileader.dxf
        doc = self._doc
        handle = dxf.get("leader_linetype_handle")
        if handle is None or handle not in doc.entitydb:
            linetype = doc.linetypes.get("BYLAYER")
            if linetype is None:
                raise ValueError(f"required linetype 'BYLAYER' does not exist")
            dxf.leader_linetype_handle = linetype.dxf.handle
        dxf.property_override_flags = 0x7FFFFFFF

    def _build_leader(
        self,
        leader_lines: list[list[Vec2]],
        side: ConnectionSide,
        connection_point: Vec2,
        m: Matrix44,
    ):
        # The content is always aligned to the x- and y-axis of the build UCS!

        # The dogleg vector points to the content:
        dogleg_direction = DOGLEG_DIRECTIONS[side]
        leader = LeaderData()
        leader.index = len(self.context.leaders)

        # dogleg_length is the already scaled length!
        leader.dogleg_length = float(self._multileader.dxf.dogleg_length)
        leader.has_dogleg_vector = 1  # ignored by AutoCAD/BricsCAD
        leader.has_last_leader_line = (
            1  # leader is invisible in AutoCAD if 0 ...
        )
        # flag is ignored by BricsCAD
        leader.dogleg_vector = dogleg_direction
        if side == ConnectionSide.left or side == ConnectionSide.right:
            leader.attachment_direction = 0
        else:
            leader.attachment_direction = 1

        # setting last leader point:
        # landing gap is already included in connection box
        if self.multileader.dxf.has_dogleg:
            leader.last_leader_point = connection_point.vec3 + (
                dogleg_direction * -leader.dogleg_length
            )
        else:
            leader.last_leader_point = connection_point.vec3

        # transform leader from build ucs to user UCS
        leader.last_leader_point = m.transform(leader.last_leader_point)
        leader.dogleg_vector = m.transform_direction(leader.dogleg_vector)

        # leader line vertices in user UCS coordinates!
        self._append_leader_lines(leader, leader_lines)
        self.context.leaders.append(leader)

    @staticmethod
    def _append_leader_lines(
        leader: LeaderData, leader_lines: list[list[Vec2]]
    ) -> None:
        for index, vertices in enumerate(leader_lines):
            line = LeaderLine()
            line.index = index
            line.vertices = Vec3.list(vertices)
            leader.lines.append(line)


# TODO: MultiLeaderBuilder & MText horizontal connection type 6 issue in BricsCAD
# 6 = "underline the bottom of the top line"
# This connection is not rendered correct in BricsCAD, the rendering in AutoCAD
# is OK, also the rendering by the ezdxf multileader RenderEngine.
# Maybe an error in BricsCAD.
# BricsCAD can create correct MULTILEADER entities with this connection type but
# I couldn't find the difference in the DXF tags which causes this issue.


class MultiLeaderMTextBuilder(MultiLeaderBuilder):
    def _init_content(self):
        self._reset_cache()
        context = self.context
        style = self._mleader_style
        mleader = self._multileader

        # reset content type
        mleader.dxf.content_type = 2
        context.block = None

        # create default mtext content
        mtext = MTextData()
        context.mtext = mtext

        mtext.default_content = style.dxf.default_text_content
        mtext.style_handle = mleader.dxf.text_style_handle
        mtext.color = mleader.dxf.text_color
        mtext.alignment = mleader.dxf.text_attachment_point

        # The char height is stored in MLeader Context()!
        # The content dimensions (width, height) are not calculated yet,
        # therefore scaling is not necessary!

    def set_content(
        self,
        content: str,
        color: Optional[
            Union[int, colors.RGB]
        ] = None,  # None = uses MLEADERSTYLE value
        char_height: float = 0.0,  # unscaled char height, 0.0 is by style
        alignment: TextAlignment = TextAlignment.left,
        style: str = "",
    ):
        """Set MTEXT content.

        Args:
            content: MTEXT content as string
            color: block color as :ref:`ACI` or RGB tuple
            char_height: initial char height in drawing units
            alignment: :class:`TextAlignment` - left, center, right
            style: name of :class:`~ezdxf.entities.Textstyle` as string

        """
        mleader = self._multileader
        context = self.context
        # update MULTILEADER DXF namespace
        if color is not None:
            mleader.dxf.text_color = colors.encode_raw_color(color)
        mleader.dxf.text_attachment_point = int(alignment)
        self._init_content()
        # following attributes are not stored in the MULTILEADER DXF namespace
        assert context.mtext is not None
        context.mtext.default_content = text_tools.escape_dxf_line_endings(
            content
        )
        if char_height:
            context.char_height = char_height * self.multileader.dxf.scale
        if style:
            self._set_mtext_style(style)

    def _set_mtext_style(self, name: str):
        style = self._doc.styles.get(name)
        if style is not None:
            self._multileader.dxf.text_style_handle = style.dxf.handle
            assert self.context.mtext is not None
            self.context.mtext.style_handle = style.dxf.handle
        else:
            raise ValueError(f"text style '{name}' does not exist")

    def _build_connection_box(self) -> ConnectionBox:
        """Returns the connection box with the connection points on all 4 sides
        in build UCS coordinates. The origin of the build ucs is the attachment
        point of the MTEXT content.
        """

        def get_left_x() -> float:
            assert mtext is not None  # shut-up mypy!!!!
            # relative to the attachment point
            x = -gap  # left
            if mtext.alignment == 2:  # center
                x = -width * 0.5 - gap
            elif mtext.alignment == 3:  # right
                x = -width - gap
            return x

        def vertical_connection_height(
            connection: HorizontalConnection,
        ) -> float:
            underline_distance = char_height * UNDERLINE_DISTANCE_FACTOR
            if connection == HorizontalConnection.middle_of_top_line:
                return -char_height * 0.5
            elif connection == HorizontalConnection.middle_of_text:
                return -height * 0.5
            elif connection == HorizontalConnection.middle_of_bottom_line:
                return -height + char_height * 0.5
            elif connection in (
                HorizontalConnection.bottom_of_bottom_line,
                HorizontalConnection.bottom_of_bottom_line_underline,
            ):
                return -height - underline_distance
            elif connection in (
                HorizontalConnection.bottom_of_top_line,
                HorizontalConnection.bottom_of_top_line_underline,
                HorizontalConnection.bottom_of_top_line_underline_all,
            ):
                return -char_height - underline_distance
            return 0.0

        context = self.context
        mtext = context.mtext
        assert isinstance(mtext, MTextData), "undefined MTEXT content"
        left_attachment = HorizontalConnection(context.left_attachment)
        right_attachment = HorizontalConnection(context.right_attachment)
        char_height = context.char_height  # scaled value!
        gap = context.landing_gap_size  # scaled value!

        width: float
        height: float
        # required data: context.scale, context.char_height
        entity = make_mtext(self._multileader)
        if text_tools.has_inline_formatting_codes(entity.text):
            size = text_size.mtext_size(entity)
            width = size.total_width
            height = size.total_height
        else:
            width, height = text_size.estimate_mtext_extents(entity)

        left_x = get_left_x()
        right_x = left_x + width + 2.0 * gap
        center_x = (left_x + right_x) * 0.5

        # Connection box in build UCS coordinates:
        return ConnectionBox(
            left=Vec2(left_x, vertical_connection_height(left_attachment)),
            right=Vec2(right_x, vertical_connection_height(right_attachment)),
            top=Vec2(center_x, gap),
            bottom=Vec2(center_x, -height - gap),
        )

    def _transform_content_to_render_ucs(
        self, insert: Vec2, rotation: float
    ) -> None:
        mtext = self.context.mtext
        assert mtext is not None
        mtext.extrusion = Z_AXIS
        mtext.insert = Vec3(insert)
        mtext.text_direction = X_AXIS.rotate(rotation)
        # todo: text_angle_type?
        mtext.rotation = rotation

    def _apply_conversion_factor(self, conversion_factor: float) -> None:
        mtext = self.context.mtext
        assert mtext is not None
        mtext.apply_conversion_factor(conversion_factor)

    def quick_leader(
        self,
        content: str,
        target: Vec2,
        segment1: Vec2,
        segment2: Optional[Vec2] = None,
        connection_type: Union[
            HorizontalConnection, VerticalConnection
        ] = HorizontalConnection.middle_of_top_line,
        ucs: Optional[UCS] = None,
    ) -> None:
        """Creates a quick MTEXT leader. The `target` point defines where the
        leader points to.
        The `segment1` is the first segment of the leader line relative to the
        `target` point, `segment2` is an optional second line segment relative
        to the first line segment.
        The `connection_type` defines the type of connection (horizontal or
        vertical) and the MTEXT alignment (left, center or right).
        Horizontal connections are always left or right aligned, vertical
        connections are always center aligned.

        Args:
            content: MTEXT content string
            target: leader target point as :class:`Vec2`
            segment1: first leader line segment as relative distance to `insert`
            segment2: optional second leader line segment as relative distance to
                first line segment
            connection_type: one of :class:`HorizontalConnection` or
                :class:`VerticalConnection`
            ucs: the rendering :class:`~ezdxf.math.UCS`, default is the :ref:`WCS`

        """
        offset = segment1
        if segment2 is not None:
            offset += segment2
        if isinstance(connection_type, VerticalConnection):
            alignment = TextAlignment.center
        else:
            alignment = (
                TextAlignment.right if offset.x <= 0.0 else TextAlignment.left
            )
        # Use text color and char height defined by the associated MLEADERSTYLE.
        self.set_content(content, alignment=alignment)

        # Build a connection box to compute the required MTEXT movement.
        move_text_x: float  # relative x-movement of MTEXT from insert location
        move_text_y: float  # relative y-movement of MTEXT from insert location
        side: ConnectionSide  # left, right, top, bottom
        has_dogleg = bool(self.multileader.dxf.has_dogleg)
        connection_box = self._build_connection_box()
        if isinstance(connection_type, HorizontalConnection):
            move_text_x = 0.0  # MTEXT is aligned to the insert location
            if offset.x <= 0.0:
                side = ConnectionSide.right
                move_text_y = -connection_box.right.y  # opposite direction
            else:
                side = ConnectionSide.left
                move_text_y = -connection_box.left.y  # opposite direction
            self.set_connection_types(
                left=connection_type,
                right=connection_type,
            )
        elif isinstance(connection_type, VerticalConnection):
            # vertical connection never have doglegs!
            has_dogleg = False
            move_text_x = 0.0  # MTEXT is centered to the insert location
            if offset.y >= 0.0:
                side = ConnectionSide.bottom
                move_text_y = -connection_box.bottom.y  # opposite direction
            else:
                side = ConnectionSide.top
                move_text_y = -connection_box.top.y  # opposite direction
            self.set_connection_types(
                top=connection_type,
                bottom=connection_type,
            )
        else:
            raise ValueError("invalid connection type")

        # Build the leader lines.
        points = [target]
        if segment2 is not None:
            points.append(target + segment1)
        self.add_leader_line(side, points)

        # The dogleg_length and gap are already scaled!
        dogleg_length = float(self._multileader.dxf.dogleg_length)
        gap = self._landing_gap_size
        last_leader_point = target + offset
        dogleg_direction = DOGLEG_DIRECTIONS[side]
        if has_dogleg:
            last_segment = dogleg_direction * (dogleg_length + gap)
        else:
            last_segment = Vec2()
        insert = (
            last_leader_point + last_segment + Vec2(move_text_x, move_text_y)
        )
        self.build(insert, ucs=ucs)

    def _set_base_point(self, left: Vec3, bottom: Vec3):
        if self._multileader.dxf.text_attachment_direction == 0:  # horizontal
            self.context.base_point = left
        else:  # vertical
            self.context.base_point = bottom


class MultiLeaderBlockBuilder(MultiLeaderBuilder):
    def __init__(self, multileader: MultiLeader):
        super().__init__(multileader)
        self._block_layout: Optional["BlockLayout"] = None  # cache
        self._extents = BoundingBox()  # cache

    def _init_content(self):
        self._reset_cache()

        context = self.context
        multileader = self._multileader

        # set content type
        multileader.dxf.content_type = 1
        context.mtext = None

        # create default block content
        block = BlockData()
        context.block = block

        block.block_record_handle = multileader.dxf.block_record_handle
        # final scaling factors for the INSERT entity:
        block.scale = multileader.dxf.block_scale_vector * context.scale
        block.rotation = multileader.dxf.block_rotation
        block.color = multileader.dxf.block_color

    def _reset_cache(self):
        super()._reset_cache()
        self._block_layout = None
        self._extents = BoundingBox()

    @property
    def block_layout(self) -> "BlockLayout":
        """Returns the block layout."""
        if self._block_layout is not None:
            return self._block_layout

        block = self.context.block
        assert isinstance(block, BlockData), "undefined BLOCK content"

        handle = block.block_record_handle
        block_record = self._doc.entitydb.get(handle)  # type: ignore
        if block_record is None:
            raise ValueError(f"invalid BLOCK_RECORD handle #{handle}")
        name = block_record.dxf.name
        block_layout = self._doc.blocks.get(name)
        if block_layout is None:
            raise ValueError(
                f"BLOCK '{name}' defined by {str(block_record)} not found"
            )
        self._block_layout = block_layout
        return block_layout

    @property
    def extents(self) -> BoundingBox:
        """Returns the bounding box of the block."""
        if not self._extents.has_data:
            from ezdxf import bbox

            block_layout = self.block_layout
            extents = bbox.extents(
                e for e in block_layout if e.dxftype() != "ATTDEF"
            )
            if not extents.has_data:
                extents.extend([NULLVEC])
            self._extents = extents
        return self._extents

    def _build_connection_box(self) -> ConnectionBox:
        """Returns the connection box with the connection points on all 4 sides
        in build UCS coordinates. The origin of the build ucs is the insertion
        point of the BLOCK content.
        """
        # ignore the landing gap for BLOCK content
        block = self.context.block
        assert isinstance(block, BlockData), "undefined BLOCK content"

        block_connection_type = self._multileader.dxf.block_connection_type
        block_layout = self.block_layout
        extents = self.extents
        sx = block.scale.x
        sy = block.scale.y
        width2 = extents.size.x * 0.5 * sx
        height2 = extents.size.y * 0.5 * sx
        base_x = block_layout.base_point.x * sx
        base_y = block_layout.base_point.y * sy
        center_x = extents.center.x * sx - base_x
        center_y = extents.center.y * sy - base_y

        if block_connection_type == BlockAlignment.center_extents:
            # adjustment of the insert point is required!
            block.insert = Vec3(-center_x, -center_y, 0)
            center_x = 0.0
            center_y = 0.0
        return ConnectionBox(
            left=Vec2(center_x - width2, center_y),
            right=Vec2(center_x + width2, center_y),
            top=Vec2(center_x, center_y + height2),
            bottom=Vec2(center_x, center_y - height2),
        )

    def _transform_content_to_render_ucs(self, insert: Vec2, rotation: float):
        block = self.context.block
        assert isinstance(block, BlockData), "undefined BLOCK content"
        block.extrusion = Z_AXIS
        block.insert = block.insert.rotate(rotation) + Vec3(insert)
        block.rotation = rotation

    def _apply_conversion_factor(self, conversion_factor: float) -> None:
        block = self.context.block
        assert isinstance(block, BlockData), "undefined BLOCK content"
        block.apply_conversion_factor(conversion_factor)

    def set_content(
        self,
        name: str,  # block name
        color: Union[int, colors.RGB] = colors.BYBLOCK,
        scale: float = 1.0,
        alignment=BlockAlignment.center_extents,
    ):
        """Set BLOCK content.

        Args:
            name: the block name as string
            color: block color as :ref:`ACI` or RGB tuple
            scale: the block scaling, not to be confused with overall scaling
            alignment: the block insertion point or the center of extents

        """
        mleader = self._multileader
        # update MULTILEADER DXF namespace
        block = self._doc.blocks.get(name)
        if block is None:
            raise ValueError(f"undefined BLOCK '{name}'")
        # All angles in MultiLeader are radians!
        mleader.dxf.block_record_handle = block.block_record_handle
        mleader.dxf.block_color = colors.encode_raw_color(color)
        mleader.dxf.block_scale_vector = Vec3(scale, scale, scale)
        mleader.dxf.block_connection_type = int(alignment)
        self._init_content()

    def set_attribute(self, tag: str, text: str, width: float = 1.0):
        """Add BLOCK attributes based on an ATTDEF entity in the block
        definition. All properties of the new created ATTRIB entity are
        defined by the template ATTDEF entity including the location.

        Args:
            tag: attribute tag name
            text: attribute content string
            width: width factor

        """
        block_layout = self.block_layout
        block_attribs = self._multileader.block_attribs
        for index, attdef in enumerate(block_layout.attdefs()):
            if tag == attdef.dxf.tag:
                block_attribs.append(
                    AttribData(
                        handle=attdef.dxf.handle,
                        index=index,
                        width=float(width),  # width factor, do not scale!
                        text=str(text),
                    )
                )
                break

    def _set_base_point(self, left: Vec3, bottom: Vec3):
        # BricsCAD does not support vertical attached leaders
        self.context.base_point = left
