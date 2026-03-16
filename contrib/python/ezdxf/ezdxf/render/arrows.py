# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Iterator
from ezdxf.math import Vec2, Shape2d, NULLVEC, UVec
from .forms import open_arrow, arrow2

if TYPE_CHECKING:
    from ezdxf.entities import DXFGraphic
    from ezdxf.sections.blocks import BlocksSection
    from ezdxf.eztypes import GenericLayoutType

DEFAULT_ARROW_ANGLE = 18.924644
DEFAULT_BETA = 45.0


# The base arrow is oriented for the right hand side ->| of the dimension line,
# reverse is the left hand side |<-.
class BaseArrow:
    def __init__(self, vertices: Iterable[UVec]):
        self.shape = Shape2d(vertices)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        pass

    def place(self, insert: UVec, angle: float):
        self.shape.rotate(angle)
        self.shape.translate(insert)


class NoneStroke(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        super().__init__([Vec2(insert)])


class ObliqueStroke(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        self.size = size
        s2 = size / 2
        # shape = [center, lower left, upper right]
        super().__init__([Vec2((-s2, -s2)), Vec2((s2, s2))])
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_line(
            start=self.shape[0], end=self.shape[1], dxfattribs=dxfattribs
        )


class ArchTick(ObliqueStroke):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        width = self.size * 0.15
        dxfattribs = dxfattribs or {}
        if layout.dxfversion > "AC1009":
            dxfattribs["const_width"] = width
            layout.add_lwpolyline(
                self.shape, format="xy", dxfattribs=dxfattribs  # type: ignore
            )
        else:
            dxfattribs["default_start_width"] = width
            dxfattribs["default_end_width"] = width
            layout.add_polyline2d(self.shape, dxfattribs=dxfattribs)  # type: ignore


class ClosedArrowBlank(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        super().__init__(open_arrow(size, angle=DEFAULT_ARROW_ANGLE))
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        if layout.dxfversion > "AC1009":
            polyline = layout.add_lwpolyline(
                points=self.shape, dxfattribs=dxfattribs  # type: ignore
            )
        else:
            polyline = layout.add_polyline2d(  # type: ignore
                points=self.shape, dxfattribs=dxfattribs  # type: ignore
            )
        polyline.close(True)


class ClosedArrow(ClosedArrowBlank):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        super().render(layout, dxfattribs)
        end_point = self.shape[0].lerp(self.shape[2])

        layout.add_line(
            start=self.shape[1], end=end_point, dxfattribs=dxfattribs
        )


class ClosedArrowFilled(ClosedArrow):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_solid(
            points=self.shape,  # type: ignore
            dxfattribs=dxfattribs,
        )


class _OpenArrow(BaseArrow):
    def __init__(
        self,
        arrow_angle: float,
        insert: UVec,
        size: float = 1.0,
        angle: float = 0,
    ):
        points = list(open_arrow(size, angle=arrow_angle))
        points.append((-1, 0))
        super().__init__(points)
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        if layout.dxfversion > "AC1009":
            layout.add_lwpolyline(points=self.shape[:-1], dxfattribs=dxfattribs)
        else:
            layout.add_polyline2d(points=self.shape[:-1], dxfattribs=dxfattribs)
        layout.add_line(
            start=self.shape[1], end=self.shape[-1], dxfattribs=dxfattribs
        )


class OpenArrow(_OpenArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        super().__init__(DEFAULT_ARROW_ANGLE, insert, size, angle)


class OpenArrow30(_OpenArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        super().__init__(30, insert, size, angle)


class OpenArrow90(_OpenArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        super().__init__(90, insert, size, angle)


class Circle(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        self.radius = size / 2
        # shape = [center point, connection point]
        super().__init__(
            [
                Vec2((0, 0)),
                Vec2((-self.radius, 0)),
                Vec2((-size, 0)),
            ]
        )
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_circle(
            center=self.shape[0], radius=self.radius, dxfattribs=dxfattribs
        )


class Origin(Circle):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        super().render(layout, dxfattribs)
        layout.add_line(
            start=self.shape[0], end=self.shape[2], dxfattribs=dxfattribs
        )


class CircleBlank(Circle):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        super().render(layout, dxfattribs)
        layout.add_line(
            start=self.shape[1], end=self.shape[2], dxfattribs=dxfattribs
        )


class Origin2(Circle):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_circle(
            center=self.shape[0], radius=self.radius, dxfattribs=dxfattribs
        )
        layout.add_circle(
            center=self.shape[0], radius=self.radius / 2, dxfattribs=dxfattribs
        )
        layout.add_line(
            start=self.shape[1], end=self.shape[2], dxfattribs=dxfattribs
        )


class DotSmall(Circle):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        center = self.shape[0]
        d = Vec2((self.radius / 2, 0))
        p1 = center - d
        p2 = center + d
        dxfattribs = dxfattribs or {}
        if layout.dxfversion > "AC1009":
            dxfattribs["const_width"] = self.radius
            layout.add_lwpolyline(
                [(p1, 1), (p2, 1)],
                format="vb",
                close=True,
                dxfattribs=dxfattribs,
            )
        else:
            dxfattribs["default_start_width"] = self.radius
            dxfattribs["default_end_width"] = self.radius
            polyline = layout.add_polyline2d(
                points=[p1, p2], close=True, dxfattribs=dxfattribs
            )
            polyline[0].dxf.bulge = 1
            polyline[1].dxf.bulge = 1


class Dot(DotSmall):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_line(
            start=self.shape[1], end=self.shape[2], dxfattribs=dxfattribs
        )
        super().render(layout, dxfattribs)


class Box(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        # shape = [lower_left, lower_right, upper_right, upper_left, connection point]
        s2 = size / 2
        super().__init__(
            [
                Vec2((-s2, -s2)),
                Vec2((+s2, -s2)),
                Vec2((+s2, +s2)),
                Vec2((-s2, +s2)),
                Vec2((-s2, 0)),
                Vec2((-size, 0)),
            ]
        )
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        if layout.dxfversion > "AC1009":
            polyline = layout.add_lwpolyline(
                points=self.shape[0:4], dxfattribs=dxfattribs
            )
        else:
            polyline = layout.add_polyline2d(  # type: ignore
                points=self.shape[0:4], dxfattribs=dxfattribs
            )
        polyline.close(True)
        layout.add_line(
            start=self.shape[4], end=self.shape[5], dxfattribs=dxfattribs
        )


class BoxFilled(Box):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        def solid_order():
            v = self.shape.vertices
            return [v[0], v[1], v[3], v[2]]

        layout.add_solid(points=solid_order(), dxfattribs=dxfattribs)
        layout.add_line(
            start=self.shape[4], end=self.shape[5], dxfattribs=dxfattribs
        )


class Integral(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        self.radius = size * 0.3535534
        self.angle = angle
        # shape = [center, left_center, right_center]
        super().__init__(
            [
                Vec2((0, 0)),
                Vec2((-self.radius, 0)),
                Vec2((self.radius, 0)),
            ]
        )
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        angle = self.angle
        layout.add_arc(
            center=self.shape[1],
            radius=self.radius,
            start_angle=-90 + angle,
            end_angle=angle,
            dxfattribs=dxfattribs,
        )
        layout.add_arc(
            center=self.shape[2],
            radius=self.radius,
            start_angle=90 + angle,
            end_angle=180 + angle,
            dxfattribs=dxfattribs,
        )


class DatumTriangle(BaseArrow):
    REVERSE_ANGLE = 180

    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        d = 0.577350269 * size  # tan(30)
        # shape = [upper_corner, lower_corner, connection_point]
        super().__init__(
            [
                Vec2((0, d)),
                Vec2((0, -d)),
                Vec2((-size, 0)),
            ]
        )
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        if layout.dxfversion > "AC1009":
            polyline = layout.add_lwpolyline(
                points=self.shape, dxfattribs=dxfattribs  # type: ignore
            )
        else:
            polyline = layout.add_polyline2d(  # type: ignore
                points=self.shape, dxfattribs=dxfattribs  # type: ignore
            )
        polyline.close(True)


class DatumTriangleFilled(DatumTriangle):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        layout.add_solid(points=self.shape, dxfattribs=dxfattribs)  # type: ignore


class _EzArrow(BaseArrow):
    def __init__(self, insert: UVec, size: float = 1.0, angle: float = 0):
        points = list(arrow2(size, angle=DEFAULT_ARROW_ANGLE))
        points.append((-1, 0))
        super().__init__(points)
        self.place(insert, angle)

    def render(self, layout: GenericLayoutType, dxfattribs=None):
        if layout.dxfversion > "AC1009":
            polyline = layout.add_lwpolyline(
                self.shape[:-1], dxfattribs=dxfattribs
            )
        else:
            polyline = layout.add_polyline2d(  # type: ignore
                self.shape[:-1], dxfattribs=dxfattribs
            )
        polyline.close(True)


class EzArrowBlank(_EzArrow):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        super().render(layout, dxfattribs)
        layout.add_line(
            start=self.shape[-2], end=self.shape[-1], dxfattribs=dxfattribs
        )


class EzArrow(_EzArrow):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        super().render(layout, dxfattribs)
        layout.add_line(
            start=self.shape[1], end=self.shape[-1], dxfattribs=dxfattribs
        )


class EzArrowFilled(_EzArrow):
    def render(self, layout: GenericLayoutType, dxfattribs=None):
        points = self.shape.vertices
        layout.add_solid(
            [points[0], points[1], points[3], points[2]], dxfattribs=dxfattribs
        )
        layout.add_line(
            start=self.shape[-2], end=self.shape[-1], dxfattribs=dxfattribs
        )


class _Arrows:
    closed_filled = ""
    dot = "DOT"
    dot_small = "DOTSMALL"
    dot_blank = "DOTBLANK"
    origin_indicator = "ORIGIN"
    origin_indicator_2 = "ORIGIN2"
    open = "OPEN"
    right_angle = "OPEN90"
    open_30 = "OPEN30"
    closed = "CLOSED"
    dot_smallblank = "SMALL"
    none = "NONE"
    oblique = "OBLIQUE"
    box_filled = "BOXFILLED"
    box = "BOXBLANK"
    closed_blank = "CLOSEDBLANK"
    datum_triangle_filled = "DATUMFILLED"
    datum_triangle = "DATUMBLANK"
    integral = "INTEGRAL"
    architectural_tick = "ARCHTICK"
    # ezdxf special arrows
    ez_arrow = "EZ_ARROW"
    ez_arrow_blank = "EZ_ARROW_BLANK"
    ez_arrow_filled = "EZ_ARROW_FILLED"

    CLASSES = {
        closed_filled: ClosedArrowFilled,
        dot: Dot,
        dot_small: DotSmall,
        dot_blank: CircleBlank,
        origin_indicator: Origin,
        origin_indicator_2: Origin2,
        open: OpenArrow,
        right_angle: OpenArrow90,
        open_30: OpenArrow30,
        closed: ClosedArrow,
        dot_smallblank: Circle,
        none: NoneStroke,
        oblique: ObliqueStroke,
        box_filled: BoxFilled,
        box: Box,
        closed_blank: ClosedArrowBlank,
        datum_triangle: DatumTriangle,
        datum_triangle_filled: DatumTriangleFilled,
        integral: Integral,
        architectural_tick: ArchTick,
        ez_arrow: EzArrow,
        ez_arrow_blank: EzArrowBlank,
        ez_arrow_filled: EzArrowFilled,
    }
    # arrows with origin at dimension line start/end
    ORIGIN_ZERO = {
        architectural_tick,
        oblique,
        dot_small,
        dot_smallblank,
        integral,
        none,
    }

    __acad__ = {
        closed_filled,
        dot,
        dot_small,
        dot_blank,
        origin_indicator,
        origin_indicator_2,
        open,
        right_angle,
        open_30,
        closed,
        dot_smallblank,
        none,
        oblique,
        box_filled,
        box,
        closed_blank,
        datum_triangle,
        datum_triangle_filled,
        integral,
        architectural_tick,
    }
    __ezdxf__ = {
        ez_arrow,
        ez_arrow_blank,
        ez_arrow_filled,
    }
    __all_arrows__ = __acad__ | __ezdxf__

    EXTENSIONS_ALLOWED = {
        architectural_tick,
        oblique,
        none,
        dot_smallblank,
        integral,
        dot_small,
    }

    def is_acad_arrow(self, item: str) -> bool:
        """Returns ``True`` if `item` is a standard AutoCAD arrow."""
        return item.upper() in self.__acad__

    def is_ezdxf_arrow(self, item: str) -> bool:
        """Returns ``True`` if `item` is a special `ezdxf` arrow."""
        return item.upper() in self.__ezdxf__

    def has_extension_line(self, name):
        """Returns ``True`` if the arrow `name` supports extension lines."""
        return name in self.EXTENSIONS_ALLOWED

    def __contains__(self, item: str) -> bool:
        """Returns `True` if `item` is an arrow managed by this class."""
        if item is None:
            return False
        return item.upper() in self.__all_arrows__

    def create_block(self, blocks: BlocksSection, name: str) -> str:
        """Creates the BLOCK definition for arrow `name`."""
        block_name = self.block_name(name)
        if block_name not in blocks:
            block = blocks.new(block_name)
            arrow = self.arrow_shape(name, insert=(0, 0), size=1, rotation=0)
            arrow.render(block, dxfattribs={"color": 0, "linetype": "BYBLOCK"})
        return block_name

    def arrow_handle(self, blocks: BlocksSection, name: str) -> str:
        """Returns the BLOCK_RECORD handle for arrow `name`."""
        arrow_name = self.arrow_name(name)
        block_name = self.create_block(blocks, arrow_name)
        block = blocks.get(block_name)
        return block.block_record_handle

    def block_name(self, name: str) -> str:
        """Returns the block name."""
        if not self.is_acad_arrow(name):  # common BLOCK definition
            # e.g. Dimension.dxf.bkl = 'EZ_ARROW' == Insert.dxf.name
            return name.upper()
        elif name == "":
            # special AutoCAD arrow symbol 'CLOSED_FILLED' has no name
            # ezdxf uses blocks for ALL arrows, but '_' (closed filled) as block name?
            return "_CLOSEDFILLED"  # Dimension.dxf.bkl = '' != Insert.dxf.name = '_CLOSED_FILLED'
        else:
            # add preceding '_' to AutoCAD arrow symbol names
            # Dimension.dxf.bkl = 'DOT' != Insert.dxf.name = '_DOT'
            return "_" + name.upper()

    def arrow_name(self, block_name: str) -> str:
        """Returns the arrow name."""
        if block_name.startswith("_"):
            name = block_name[1:].upper()
            if name == "CLOSEDFILLED":
                return ""
            elif self.is_acad_arrow(name):
                return name
        return block_name

    def insert_arrow(
        self,
        layout: GenericLayoutType,
        name: str,
        insert: UVec = NULLVEC,
        size: float = 1.0,
        rotation: float = 0,
        *,
        dxfattribs=None,
    ) -> Vec2:
        """Insert arrow as block reference into `layout`."""
        block_name = self.create_block(layout.doc.blocks, name)

        dxfattribs = dict(dxfattribs or {})
        dxfattribs["rotation"] = rotation
        dxfattribs["xscale"] = size
        dxfattribs["yscale"] = size
        layout.add_blockref(block_name, insert=insert, dxfattribs=dxfattribs)
        return connection_point(
            name, insert=insert, scale=size, rotation=rotation
        )

    def render_arrow(
        self,
        layout: GenericLayoutType,
        name: str,
        insert: UVec = NULLVEC,
        size: float = 1.0,
        rotation: float = 0,
        *,
        dxfattribs=None,
    ) -> Vec2:
        """Render arrow as basic DXF entities into `layout`."""
        dxfattribs = dict(dxfattribs or {})
        arrow = self.arrow_shape(name, insert, size, rotation)
        arrow.render(layout, dxfattribs)
        return connection_point(
            name, insert=insert, scale=size, rotation=rotation
        )

    def virtual_entities(
        self,
        name: str,
        insert: UVec = NULLVEC,
        size: float = 0.625,
        rotation: float = 0,
        *,
        dxfattribs=None,
    ) -> Iterator[DXFGraphic]:
        """Returns all arrow components as virtual DXF entities."""
        from ezdxf.layouts import VirtualLayout

        if name in self:
            layout = VirtualLayout()
            ARROWS.render_arrow(
                layout,
                name,
                insert=insert,
                size=size,
                rotation=rotation,
                dxfattribs=dxfattribs,
            )
            yield from iter(layout)

    def arrow_shape(
        self, name: str, insert: UVec, size: float, rotation: float
    ) -> BaseArrow:
        """Returns an instance of the shape management class for arrow `name`."""
        # size depending shapes
        name = name.upper()
        if name == self.dot_small:
            size *= 0.25
        elif name == self.dot_smallblank:
            size *= 0.5
        cls = self.CLASSES[name]
        return cls(insert, size, rotation)


def connection_point(
    arrow_name: str, insert: UVec, scale: float = 1.0, rotation: float = 0.0
) -> Vec2:
    """Returns the connection point for `arrow_name`."""
    insert = Vec2(insert)
    if ARROWS.arrow_name(arrow_name) in _Arrows.ORIGIN_ZERO:
        return insert
    else:
        return insert - Vec2.from_deg_angle(rotation, scale)


def arrow_length(arrow_name: str, scale: float = 1.0) -> float:
    """Returns the scaled arrow length of `arrow_name`."""
    if ARROWS.arrow_name(arrow_name) in _Arrows.ORIGIN_ZERO:
        return 0.0
    else:
        return scale


ARROWS: _Arrows = _Arrows()
