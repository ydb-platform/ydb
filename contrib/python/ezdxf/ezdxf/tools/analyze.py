# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
# Debugging tools to analyze DXF entities.
from __future__ import annotations
from typing import Iterable, Sequence, Optional
import textwrap

import ezdxf
from ezdxf import colors
from ezdxf.math import Vec2
from ezdxf.lldxf import const
from ezdxf.enums import TextEntityAlignment
from ezdxf.tools.debug import print_bitmask
from ezdxf.render.mleader import MLeaderStyleOverride, OVERRIDE_FLAG
from ezdxf.entities import (
    EdgePath,
    PolylinePath,
    LineEdge,
    ArcEdge,
    EllipseEdge,
    SplineEdge,
    mleader,
)
from ezdxf.entities.polygon import DXFPolygon

EDGE_START_MARKER = "EDGE_START_MARKER"
EDGE_END_MARKER = "EDGE_END_MARKER"
HATCH_LAYER = "HATCH"
POLYLINE_LAYER = "POLYLINE_MARKER"
LINE_LAYER = "LINE_MARKER"
ARC_LAYER = "ARC_MARKER"
ELLIPSE_LAYER = "ELLIPSE_MARKER"
SPLINE_LAYER = "SPLINE_MARKER"


class HatchAnalyzer:
    def __init__(
        self,
        *,
        marker_size: float = 1.0,
        angle: float = 45,
    ):
        self.marker_size = marker_size
        self.angle = angle
        self.doc = ezdxf.new()
        self.msp = self.doc.modelspace()
        self.init_layers()
        self.init_markers()

    def init_layers(self):
        self.doc.layers.add(POLYLINE_LAYER, color=colors.YELLOW)
        self.doc.layers.add(LINE_LAYER, color=colors.RED)
        self.doc.layers.add(ARC_LAYER, color=colors.GREEN)
        self.doc.layers.add(ELLIPSE_LAYER, color=colors.MAGENTA)
        self.doc.layers.add(SPLINE_LAYER, color=colors.CYAN)
        self.doc.layers.add(HATCH_LAYER)

    def init_markers(self):
        blk = self.doc.blocks.new(EDGE_START_MARKER)
        attribs = {"layer": "0"}  # from INSERT
        radius = self.marker_size / 2.0
        height = radius

        # start marker: 0-- name
        blk.add_circle(
            center=(0, 0),
            radius=radius,
            dxfattribs=attribs,
        )
        text_start = radius * 4
        blk.add_line(
            start=(radius, 0),
            end=(text_start - radius / 2.0, 0),
            dxfattribs=attribs,
        )
        text = blk.add_attdef(
            tag="NAME",
            dxfattribs=attribs,
        )
        text.dxf.height = height
        text.set_placement(
            (text_start, 0), align=TextEntityAlignment.MIDDLE_LEFT
        )

        # end marker: name --X
        blk = self.doc.blocks.new(EDGE_END_MARKER)
        attribs = {"layer": "0"}  # from INSERT
        blk.add_line(
            start=(-radius, -radius),
            end=(radius, radius),
            dxfattribs=attribs,
        )
        blk.add_line(
            start=(-radius, radius),
            end=(radius, -radius),
            dxfattribs=attribs,
        )
        text_start = -radius * 4
        blk.add_line(
            start=(-radius, 0),
            end=(text_start + radius / 2.0, 0),
            dxfattribs=attribs,
        )
        text = blk.add_attdef(
            tag="NAME",
            dxfattribs=attribs,
        )
        text.dxf.height = height
        text.set_placement(
            (text_start, 0), align=TextEntityAlignment.MIDDLE_RIGHT
        )

    def export(self, name: str) -> None:
        self.doc.saveas(name)

    def add_hatch(self, hatch: DXFPolygon) -> None:
        hatch.dxf.discard("extrusion")
        hatch.dxf.layer = HATCH_LAYER
        self.msp.add_foreign_entity(hatch)

    def add_boundary_markers(self, hatch: DXFPolygon) -> None:
        hatch.dxf.discard("extrusion")
        path_num: int = 0

        for p in hatch.paths:
            path_num += 1
            if isinstance(p, PolylinePath):
                self.add_polyline_markers(p, path_num)
            elif isinstance(p, EdgePath):
                self.add_edge_markers(p, path_num)
            else:
                raise TypeError(f"unknown boundary path type: {type(p)}")

    def add_start_marker(self, location: Vec2, name: str, layer: str) -> None:
        self.add_marker(EDGE_START_MARKER, location, name, layer)

    def add_end_marker(self, location: Vec2, name: str, layer: str) -> None:
        self.add_marker(EDGE_END_MARKER, location, name, layer)

    def add_marker(
        self, blk_name: str, location: Vec2, name: str, layer: str
    ) -> None:
        blkref = self.msp.add_blockref(
            name=blk_name,
            insert=location,
            dxfattribs={
                "layer": layer,
                "rotation": self.angle,
            },
        )
        blkref.add_auto_attribs({"NAME": name})

    def add_polyline_markers(self, p: PolylinePath, num: int) -> None:
        self.add_start_marker(
            Vec2(p.vertices[0]), f"Poly-S({num})", POLYLINE_LAYER
        )
        self.add_end_marker(
            Vec2(p.vertices[0]), f"Poly-E({num})", POLYLINE_LAYER
        )

    def add_edge_markers(self, p: EdgePath, num: int) -> None:
        edge_num: int = 0
        for edge in p.edges:
            edge_num += 1
            name = f"({num}.{edge_num})"
            if isinstance(
                edge,
                LineEdge,
            ):
                self.add_line_edge_markers(edge, name)
            elif isinstance(edge, ArcEdge):
                self.add_arc_edge_markers(edge, name)
            elif isinstance(edge, EllipseEdge):
                self.add_ellipse_edge_markers(edge, name)
            elif isinstance(edge, SplineEdge):
                self.add_spline_edge_markers(edge, name)
            else:
                raise TypeError(f"unknown edge type: {type(edge)}")

    def add_line_edge_markers(self, line: LineEdge, name: str) -> None:
        self.add_start_marker(line.start, "Line-S" + name, LINE_LAYER)
        self.add_end_marker(line.end, "Line-E" + name, LINE_LAYER)

    def add_arc_edge_markers(self, arc: ArcEdge, name: str) -> None:
        self.add_start_marker(arc.start_point, "Arc-S" + name, ARC_LAYER)
        self.add_end_marker(arc.end_point, "Arc-E" + name, ARC_LAYER)

    def add_ellipse_edge_markers(self, ellipse: EllipseEdge, name: str) -> None:
        self.add_start_marker(
            ellipse.start_point, "Ellipse-S" + name, ELLIPSE_LAYER
        )
        self.add_end_marker(
            ellipse.end_point, "Ellipse-E" + name, ELLIPSE_LAYER
        )

    def add_spline_edge_markers(self, spline: SplineEdge, name: str) -> None:
        if len(spline.control_points):
            # Assuming a clamped B-spline, because this is the only practical
            # usable B-spline for edges.
            self.add_start_marker(
                spline.start_point, "SplineS" + name, SPLINE_LAYER
            )
            self.add_end_marker(
                spline.end_point, "SplineE" + name, SPLINE_LAYER
            )

    @staticmethod
    def report(hatch: DXFPolygon) -> list[str]:
        return hatch_report(hatch)

    @staticmethod
    def print_report(hatch: DXFPolygon) -> None:
        print("\n".join(hatch_report(hatch)))


def hatch_report(hatch: DXFPolygon) -> list[str]:
    dxf = hatch.dxf
    style = const.ISLAND_DETECTION[dxf.hatch_style]
    pattern_type = const.HATCH_PATTERN_TYPE[dxf.pattern_type]
    text = [
        f"{str(hatch)}",
        f"   solid fill: {bool(dxf.solid_fill)}",
        f"   pattern type: {pattern_type}",
        f"   pattern name: {dxf.pattern_name}",
        f"   associative: {bool(dxf.associative)}",
        f"   island detection: {style}",
        f"   has pattern data: {hatch.pattern is not None}",
        f"   has gradient data: {hatch.gradient is not None}",
        f"   seed value count: {len(hatch.seeds)}",
        f"   boundary path count: {len(hatch.paths)}",
    ]
    num = 0
    for path in hatch.paths:
        num += 1
        if isinstance(path, PolylinePath):
            text.extend(polyline_path_report(path, num))
        elif isinstance(path, EdgePath):
            text.extend(edge_path_report(path, num))
    return text


def polyline_path_report(p: PolylinePath, num: int) -> list[str]:
    path_type = ", ".join(const.boundary_path_flag_names(p.path_type_flags))
    return [
        f"{num}. Polyline Path, vertex count: {len(p.vertices)}",
        f"   path type:                      {path_type}",
    ]


def edge_path_report(p: EdgePath, num: int) -> list[str]:
    closed = False
    connected = False
    path_type = ", ".join(const.boundary_path_flag_names(p.path_type_flags))
    edges = p.edges
    if len(edges):
        closed = edges[0].start_point.isclose(edges[-1].end_point)
        connected = all(
            e1.end_point.isclose(e2.start_point)
            for e1, e2 in zip(edges, edges[1:])
        )

    return [
        f"{num}. Edge Path, edge count: {len(p.edges)}",
        f"   path type:                      {path_type}",
        f"   continuously connected edges:   {connected}",
        f"   closed edge loop:               {closed}",
    ]


MULTILEADER = "MULTILEADER_MARKER"
POINT_MARKER = "POINT_MARKER"


class MultileaderAnalyzer:
    """Multileader can not be added as foreign entity to a new document.
    Annotations have to be added to the source document.

    """

    def __init__(
        self,
        multileader: mleader.MultiLeader,
        *,
        marker_size: float = 1.0,
        report_width: int = 79,
    ):
        self.marker_size = marker_size
        self.report_width = report_width
        self.multileader = multileader
        assert self.multileader.doc is not None, "valid DXF document required"
        self.doc = self.multileader.doc
        self.msp = self.doc.modelspace()
        self.init_layers()
        self.init_markers()

    def init_layers(self):
        if self.doc.layers.has_entry(MULTILEADER):
            return
        self.doc.layers.add(MULTILEADER, color=colors.RED)

    def init_markers(self):
        if POINT_MARKER not in self.doc.blocks:
            blk = self.doc.blocks.new(POINT_MARKER)
            attribs = {"layer": "0"}  # from INSERT
            size = self.marker_size
            size_2 = size / 2.0
            radius = size / 4.0
            blk.add_circle(
                center=(0, 0),
                radius=radius,
                dxfattribs=attribs,
            )
            blk.add_line((-size_2, 0), (size_2, 0), dxfattribs=attribs)
            blk.add_line((0, -size_2), (0, size_2), dxfattribs=attribs)

    @property
    def context(self) -> mleader.MLeaderContext:
        return self.multileader.context

    @property
    def mleaderstyle(self) -> Optional[mleader.MLeaderStyle]:
        handle = self.multileader.dxf.get("style_handle")
        return self.doc.entitydb.get(handle)  # type: ignore

    def divider_line(self, symbol="-") -> str:
        return symbol * self.report_width

    def shorten_lines(self, lines: Iterable[str]) -> list[str]:
        return [
            textwrap.shorten(line, width=self.report_width) for line in lines
        ]

    def report(self) -> list[str]:
        report = [
            str(self.multileader),
            self.divider_line(),
            "Existing DXF attributes:",
            self.divider_line(),
        ]
        report.extend(self.multileader_attributes())
        report.extend(self.context_attributes())
        if self.context.mtext is not None:
            report.extend(self.mtext_attributes())
        if self.context.block is not None:
            report.extend(self.block_attributes())
            if self.multileader.block_attribs:
                report.extend(self.block_reference_attribs())
        if self.multileader.context.leaders:
            report.extend(self.leader_attributes())
        if self.multileader.arrow_heads:
            report.extend(self.arrow_heads())
        return self.shorten_lines(report)

    def print_report(self) -> None:
        width = self.report_width
        print()
        print("=" * width)
        print("\n".join(self.report()))
        print("=" * width)

    def print_overridden_properties(self):
        print("\n".join(self.overridden_attributes()))

    def overridden_attributes(self) -> list[str]:
        multileader = self.multileader
        style = self.mleaderstyle
        if style is not None:
            report = [
                self.divider_line(),
                f"Override attributes of {str(style)}: '{style.dxf.name}'",
                self.divider_line(),
            ]

            override = MLeaderStyleOverride(style, multileader)
            for name in OVERRIDE_FLAG.keys():
                if override.is_overridden(name):
                    report.append(f"{name}: {override.get(name)}")
            if override.use_mtext_default_content:
                report.append("use_mtext_default_content: 1")
        else:
            handle = self.multileader.dxf.get("style_handle")
            report = [
                self.divider_line(),
                f"MLEADERSTYLE(#{handle}) not found",
            ]
        return report

    def multileader_attributes(self) -> list[str]:
        attribs = self.multileader.dxf.all_existing_dxf_attribs()
        keys = sorted(attribs.keys())
        return [f"{key}: {attribs[key]}" for key in keys]

    def print_override_state(self):
        flags = self.multileader.dxf.property_override_flags
        print(f"\nproperty_override_flags:")
        print(f"dec: {flags}")
        print(f"hex: {hex(flags)}")
        print_bitmask(flags)

    def print_context_attributes(self):
        print("\n".join(self.context_attributes()))

    def context_attributes(self) -> list[str]:
        context = self.context
        if context is None:
            return []
        report = [
            self.divider_line(),
            "CONTEXT object attributes:",
            self.divider_line(),
            f"has MTEXT content: {yes_or_no(context.mtext)}",
            f"has BLOCK content: {yes_or_no(context.block)}",
            self.divider_line(),
        ]
        keys = [
            key
            for key in context.__dict__.keys()
            if key not in ("mtext", "block", "leaders")
        ]
        attributes = [f"{name}: {getattr(context, name)}" for name in keys]
        attributes.sort()
        report.extend(attributes)
        return self.shorten_lines(report)

    def print_mtext_attributes(self):
        print("\n".join(self.mtext_attributes()))

    def mtext_attributes(self) -> list[str]:
        report = [
            self.divider_line(),
            "MTEXT content attributes:",
            self.divider_line(),
        ]
        mtext = self.context.mtext
        if mtext is not None:
            report.extend(_content_attributes(mtext))
        return self.shorten_lines(report)

    def print_block_attributes(self):
        print("\n".join(self.block_attributes()))

    def block_attributes(self) -> list[str]:
        report = [
            self.divider_line(),
            "BLOCK content attributes:",
            self.divider_line(),
        ]
        block = self.context.block
        if block is not None:
            report.extend(_content_attributes(block))
        return self.shorten_lines(report)

    def print_leader_attributes(self):
        print("\n".join(self.leader_attributes()))

    def leader_attributes(self) -> list[str]:
        report = []
        leaders = self.context.leaders
        if leaders is not None:
            for index, leader in enumerate(leaders):
                report.extend(self._leader_attributes(index, leader))
        return self.shorten_lines(report)

    def _leader_attributes(
        self, index: int, leader: mleader.LeaderData
    ) -> list[str]:
        report = [
            self.divider_line(),
            f"{index+1}. LEADER attributes:",
            self.divider_line(),
        ]
        report.extend(_content_attributes(leader, exclude=("lines", "breaks")))
        s = ", ".join(map(str, leader.breaks))
        report.append(f"breaks: [{s}]")
        if leader.lines:
            report.extend(self._leader_lines(leader.lines))
        return report

    def _leader_lines(self, lines) -> list[str]:
        report = []
        for num, line in enumerate(lines):
            report.extend(
                [
                    self.divider_line(),
                    f"{num + 1}. LEADER LINE attributes:",
                    self.divider_line(),
                ]
            )
            for name, value in line.__dict__.items():
                if name in ("vertices", "breaks"):
                    vstr = ""
                    if value is not None:
                        vstr = ", ".join(map(str, value))
                    vstr = f"[{vstr}]"
                else:
                    vstr = str(value)
                report.append(f"{name}: {vstr}")
        return report

    def print_block_attribs(self):
        print("\n".join(self.block_reference_attribs()))

    def block_reference_attribs(self) -> list[str]:
        report = [
            self.divider_line(),
            f"BLOCK reference attributes:",
            self.divider_line(),
        ]
        for index, attr in enumerate(self.multileader.block_attribs):
            report.extend(
                [
                    f"{index+1}. Attributes",
                    self.divider_line(),
                ]
            )
            report.append(f"handle: {attr.handle}")
            report.append(f"index: {attr.index}")
            report.append(f"width: {attr.width}")
            report.append(f"text: {attr.text}")
        return report

    def arrow_heads(self) -> list[str]:
        report = [
            self.divider_line(),
            f"ARROW HEAD attributes:",
            self.divider_line(),
        ]
        for index, attr in enumerate(self.multileader.arrow_heads):
            report.extend(
                [
                    f"{index+1}. Arrow Head",
                    self.divider_line(),
                ]
            )
            report.append(f"handle: {attr.handle}")
            report.append(f"index: {attr.index}")
        return report

    def print_mleaderstyle(self):
        print("\n".join(self.mleaderstyle_attributes()))

    def mleaderstyle_attributes(self) -> list[str]:
        report = []
        style = self.mleaderstyle
        if style is not None:
            report.extend(
                [
                    self.divider_line("="),
                    str(style),
                    self.divider_line("="),
                ]
            )
            attribs = style.dxf.all_existing_dxf_attribs()
            keys = sorted(attribs.keys())
            report.extend([f"{name}: {attribs[name]}" for name in keys])
        else:
            handle = self.multileader.dxf.get("style_handle")
            report.append(f"MLEADERSTYLE(#{handle}) not found")
        return self.shorten_lines(report)


def _content_attributes(
    entity, exclude: Optional[Sequence[str]] = None
) -> list[str]:
    exclude = exclude or []
    if entity is not None:
        return [
            f"{name}: {value}"
            for name, value in entity.__dict__.items()
            if name not in exclude
        ]
    return []


def yes_or_no(data) -> str:
    return "yes" if data else "no"
