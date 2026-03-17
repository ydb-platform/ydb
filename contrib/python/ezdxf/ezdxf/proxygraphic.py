# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Optional,
    Iterable,
    Iterator,
    cast,
    Sequence,
    Any,
)
import sys
import struct
import math
from enum import IntEnum
from itertools import repeat
from ezdxf.lldxf import const
from ezdxf.tools.binarydata import bytes_to_hexstr, ByteStream, BitStream
from ezdxf import colors
from ezdxf.math import (
    Vec3,
    Vec2,
    Matrix44,
    Z_AXIS,
    ConstructionCircle,
    ConstructionArc,
    OCS,
    UCS,
    X_AXIS,
)
from ezdxf.entities import factory
import logging

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tags import Tags
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import (
        DXFGraphic,
        Polymesh,
        Polyface,
        Polyline,
        Hatch,
        LWPolyline,
    )

logger = logging.getLogger("ezdxf")

CHUNK_SIZE = 127


class ProxyGraphicError(Exception):
    pass


def load_proxy_graphic(
    tags: Tags, length_code: int = 160, data_code: int = 310
) -> Optional[bytes]:
    binary_data = [
        tag.value
        for tag in tags.pop_tags(codes=(length_code, data_code))
        if tag.code == data_code
    ]
    return b"".join(binary_data) if len(binary_data) else None


def export_proxy_graphic(
    data: bytes,
    tagwriter: AbstractTagWriter,
    length_code: int = 160,
    data_code: int = 310,
) -> None:
    # Do not export proxy graphic for DXF R12 files
    assert tagwriter.dxfversion > const.DXF12

    length = len(data)
    if length == 0:
        return

    tagwriter.write_tag2(length_code, length)
    index = 0
    while index < length:
        hex_str = bytes_to_hexstr(data[index : index + CHUNK_SIZE])
        tagwriter.write_tag2(data_code, hex_str)
        index += CHUNK_SIZE


def has_prim_traits(flags: int) -> bool:
    return bool(flags & 0xFFFF)


def prims_have_colors(flags: int) -> bool:
    return bool(flags & 0x0001)


def prims_have_layers(flags: int) -> bool:
    return bool(flags & 0x0002)


def prims_have_linetypes(flags: int) -> bool:
    return bool(flags & 0x0004)


def prims_have_markers(flags: int) -> bool:
    return bool(flags & 0x0020)


def prims_have_visibilities(flags: int) -> bool:
    return bool(flags & 0x0040)


def prims_have_normals(flags: int) -> bool:
    return bool(flags & 0x0080)


def prims_have_orientation(flags: int) -> bool:
    return bool(flags & 0x0400)


TRAIT_TESTER = {
    "colors": (prims_have_colors, "RL"),
    "layers": (prims_have_layers, "RL"),
    "linetypes": (prims_have_linetypes, "RL"),
    "markers": (prims_have_markers, "RL"),
    "visibilities": (prims_have_visibilities, "RL"),
    "normals": (prims_have_normals, "3RD"),
}


def read_prim_traits(
    bs: ByteStream, types: Sequence[str], prim_flags: int, count: int
) -> dict:
    def read_float_list():
        return [bs.read_long() for _ in range(count)]

    def read_vertices():
        return [Vec3(bs.read_vertex()) for _ in range(count)]

    data = dict()
    for t in types:
        test_trait, data_type = TRAIT_TESTER[t]
        if test_trait(prim_flags):
            if data_type == "3RD":
                data[t] = read_vertices()
            elif data_type == "RL":
                data[t] = read_float_list()
            else:
                raise TypeError(data_type)
    return data


def read_mesh_traits(
    bs: ByteStream, edge_count: int, face_count: int, vertex_count: int
):
    # Traits data format:
    # all entries are optional
    # traits: dict[str, dict]
    #     "edges": dict[str, list]
    #         "colors": list[int]
    #         "layers": list[int] as layer ids
    #         "linetypes": list[int] as linetype ids
    #         "markers": list[int]
    #         "visibilities": list[int]
    #     "faces": dict[str, list]
    #         "colors": list[int]
    #         "layers": list[int] as layer ids
    #         "markers": list[int]
    #         "normals": list[Vec3]
    #         "visibilities": list[int]
    #     "vertices": dict
    #         "normals": list[Vec3]
    #         "orientation": bool
    traits = dict()
    edge_flags = bs.read_long()
    if has_prim_traits(edge_flags):
        traits["edges"] = read_prim_traits(
            bs,
            ["colors", "layers", "linetypes", "markers", "visibilities"],
            edge_flags,
            edge_count,
        )
    face_flags = bs.read_long()
    if has_prim_traits(face_flags):
        traits["faces"] = read_prim_traits(
            bs,
            ["colors", "layers", "markers", "normals", "visibilities"],
            face_flags,
            face_count,
        )

    # Note: DXF entities PolyFaceMesh and Mesh do not support vertex normals!
    # disable useless reading process by vertex_count = 0
    if vertex_count > 0:
        vertex_flags = bs.read_long()
        if has_prim_traits(vertex_flags):
            vertices = dict()
            if prims_have_normals(vertex_flags):
                vertices["normals"] = [
                    Vec3(bs.read_vertex()) for _ in range(vertex_count)
                ]
            if prims_have_orientation(vertex_flags):
                vertices["orientation"] = bool(bs.read_long())  # type: ignore
            traits["vertices"] = vertices
    return traits


class ProxyGraphicTypes(IntEnum):
    EXTENTS = 1
    CIRCLE = 2
    CIRCLE_3P = 3
    CIRCULAR_ARC = 4
    CIRCULAR_ARC_3P = 5
    POLYLINE = 6
    POLYGON = 7
    MESH = 8
    SHELL = 9
    TEXT = 10
    TEXT2 = 11
    XLINE = 12
    RAY = 13
    ATTRIBUTE_COLOR = 14
    UNUSED_15 = 15
    ATTRIBUTE_LAYER = 16
    UNUSED_17 = 17
    ATTRIBUTE_LINETYPE = 18
    ATTRIBUTE_MARKER = 19
    ATTRIBUTE_FILL = 20
    UNUSED_21 = 21
    ATTRIBUTE_TRUE_COLOR = 22
    ATTRIBUTE_LINEWEIGHT = 23
    ATTRIBUTE_LTSCALE = 24
    ATTRIBUTE_THICKNESS = 25
    ATTRIBUTE_PLOT_STYLE_NAME = 26
    PUSH_CLIP = 27
    POP_CLIP = 28
    PUSH_MATRIX = 29
    PUSH_MATRIX2 = 30
    POP_MATRIX = 31
    POLYLINE_WITH_NORMALS = 32
    LWPOLYLINE = 33
    ATTRIBUTE_MATERIAL = 34
    ATTRIBUTE_MAPPER = 35
    UNICODE_TEXT = 36
    UNKNOWN_37 = 37
    UNICODE_TEXT2 = 38
    ELLIPTIC_ARC = 44  # found in test data of issue #832


class ProxyGraphic:
    def __init__(
        self, data: bytes, doc: Optional[Drawing] = None, *, dxfversion=const.DXF2000
    ):
        self._doc = doc
        self._factory = factory.new
        self._buffer: bytes = data
        self._index: int = 8
        self.dxfversion = doc.dxfversion if doc else dxfversion
        self.encoding: str = "cp1252" if self.dxfversion < const.DXF2007 else "utf-8"
        self.color: int = const.BYLAYER
        self.layer: str = "0"
        self.linetype: str = "BYLAYER"
        self.marker_index: int = 0
        self.fill: bool = False
        self.true_color: Optional[int] = None
        self.lineweight: int = const.LINEWEIGHT_DEFAULT
        self.ltscale: float = 1.0
        self.thickness: float = 0.0
        # Layer list in storage order
        self.layers: list[str] = []
        # Linetypes list in storage order
        self.linetypes: list[str] = []
        # List of text styles, with font name as key
        self.textstyles: dict[str, str] = dict()
        self.required_fonts: set[str] = set()
        self.matrices: list[Matrix44] = []

        if self._doc:
            self.layers = list(layer.dxf.name for layer in self._doc.layers)
            self.linetypes = list(linetype.dxf.name for linetype in self._doc.linetypes)
            self.textstyles = {
                style.dxf.font: style.dxf.name for style in self._doc.styles
            }
            self.encoding = self._doc.encoding

    def info(self) -> Iterable[tuple[int, int, str]]:
        index = self._index
        buffer = self._buffer
        while index < len(buffer):
            size, type_ = struct.unpack_from("<2L", self._buffer, offset=index)
            try:
                name = ProxyGraphicTypes(type_).name
            except ValueError:
                name = f"UNKNOWN_TYPE_{type_}"
            yield index, size, name
            index += size

    def virtual_entities(self) -> Iterator[DXFGraphic]:
        return self.__virtual_entities__()

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol."""
        try:
            yield from self.unsafe_virtual_entities()
        except Exception as e:
            raise ProxyGraphicError(f"Proxy graphic error: {str(e)}")

    def unsafe_virtual_entities(self) -> Iterable[DXFGraphic]:
        def transform(entity):
            if self.matrices:
                return entity.transform(self.matrices[-1])
            else:
                return entity

        index = self._index
        buffer = self._buffer
        while index < len(buffer):
            # read 2 unsigned long (8 bytes)
            size, type_ = struct.unpack_from("<2L", buffer, offset=index)
            # chunk size has to be >= 8 to advance to the next chunk.
            if size < 8:
                raise ValueError("parsing error: chunk size < 8 bytes")
            try:
                name = ProxyGraphicTypes(type_).name.lower()
            except ValueError:
                logger.debug(f"Unsupported Type Code: {type_}")
                index += size
                continue
            method = getattr(self, name, None)
            if method:
                result = method(self._buffer[index + 8 : index + size])
                if isinstance(result, tuple):
                    for entity in result:
                        yield transform(entity)
                elif result:
                    yield transform(result)
                if result:  # reset fill after each graphic entity
                    self.fill = False
            else:
                logger.debug(f"Unsupported feature ProxyGraphic.{name}()")
            index += size

    def push_matrix(self, data: bytes):
        values = struct.unpack("<16d", data)
        m = Matrix44(values)
        m.transpose()
        self.matrices.append(m)

    def pop_matrix(self, data: bytes):
        if self.matrices:
            self.matrices.pop()

    def reset_colors(self):
        self.color = const.BYLAYER
        self.true_color = None

    def attribute_color(self, data: bytes):
        self.reset_colors()
        self.color = struct.unpack("<L", data)[0]
        if self.color < 0 or self.color > 256:
            self.color = const.BYLAYER

    def attribute_layer(self, data: bytes):
        if self._doc:
            index = struct.unpack("<L", data)[0]
            if index < len(self.layers):
                self.layer = self.layers[index]

    def attribute_linetype(self, data: bytes):
        if self._doc:
            index = struct.unpack("<L", data)[0]
            try:
                # first two entries ByLayer and ByBlock are not included in CAD applications:
                self.linetype = self.linetypes[index + 2]
            except IndexError:
                if index == 32766:
                    self.linetype = "BYBLOCK"
                else:  # index is 32767 or invalid
                    self.linetype = "BYLAYER"

    def attribute_marker(self, data: bytes):
        self.marker_index = struct.unpack("<L", data)[0]

    def attribute_fill(self, data: bytes):
        self.fill = bool(struct.unpack("<L", data)[0])

    def attribute_true_color(self, data: bytes):
        self.reset_colors()
        code, value = colors.decode_raw_color(struct.unpack("<L", data)[0])
        if code == colors.COLOR_TYPE_RGB:
            self.true_color = colors.rgb2int(value)  # type: ignore
        else:  # ACI colors, BYLAYER, BYBLOCK
            self.color = value  # type: ignore

    def attribute_lineweight(self, data: bytes):
        lw = struct.unpack("<L", data)[0]
        if lw > const.MAX_VALID_LINEWEIGHT:
            self.lineweight = max(lw - 0x100000000, const.LINEWEIGHT_DEFAULT)
        else:
            self.lineweight = lw

    def attribute_ltscale(self, data: bytes):
        self.ltscale = struct.unpack("<d", data)[0]

    def attribute_thickness(self, data: bytes):
        self.thickness = struct.unpack("<d", data)[0]

    def circle(self, data: bytes):
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        center = Vec3(bs.read_vertex())
        attribs["radius"] = bs.read_float()
        normal = Vec3(bs.read_vertex())
        attribs["extrusion"] = normal
        if not normal.isclose(Z_AXIS):
            # TODO: (issue 873) circle has a normal vector but center is in WCS
            #  It's not clear if all coordinates in proxy graphics are WCS coordinates
            #  even for OCS entities - the ODA DWG documentation contains no information
            #  about that.
            ocs = OCS(normal)
            center = ocs.from_wcs(center)
        attribs["center"] = center
        return self._factory("CIRCLE", dxfattribs=attribs)

    def circle_3p(self, data: bytes):
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        p1 = Vec3(bs.read_vertex())
        p2 = Vec3(bs.read_vertex())
        p3 = Vec3(bs.read_vertex())
        circle = ConstructionCircle.from_3p(p1, p2, p3)
        attribs["center"] = circle.center
        attribs["radius"] = circle.radius
        return self._factory("CIRCLE", dxfattribs=attribs)

    def circular_arc(self, data: bytes):
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        center = Vec3(bs.read_vertex())  # in WCS
        attribs["radius"] = bs.read_float()
        normal = Vec3(bs.read_vertex())  # UCS z-axis
        start_vec = Vec3(bs.read_vertex())  # UCS x-axis
        # sweep angle around normal vector!
        sweep_angle = bs.read_float()  # in radians
        # arc_type = bs.read_long()  # unused yet - meaning?
        start_angle: float  # in degrees
        end_angle: float  # in degrees
        if not normal.isclose(Z_AXIS):
            # local UCS
            ucs = UCS(ux=start_vec, uz=normal)
            # target OCS
            ocs = OCS(normal)
            # convert start angle == UCS x-axis to OCS
            start_angle = ocs.from_wcs(ucs.to_wcs(X_AXIS)).angle_deg
            # convert end angle to OCS
            end_vec = Vec3.from_angle(sweep_angle)
            end_angle = ocs.from_wcs(ucs.to_wcs(end_vec)).angle_deg
            # setup OCS for ARC entity
            attribs["extrusion"] = normal
            # convert WCS center to OCS center
            center = ocs.from_wcs(center)
        else:
            start_angle = start_vec.angle_deg
            end_angle = start_angle + math.degrees(sweep_angle)
        attribs["center"] = center
        attribs["start_angle"] = start_angle
        attribs["end_angle"] = end_angle
        return self._factory("ARC", dxfattribs=attribs)

    def circular_arc_3p(self, data: bytes):
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        p1 = Vec3(bs.read_vertex())
        p2 = Vec3(bs.read_vertex())
        p3 = Vec3(bs.read_vertex())
        # arc_type = bs.read_long()  # unused yet
        arc = ConstructionArc.from_3p(p1, p3, p2)
        attribs["center"] = arc.center
        attribs["radius"] = arc.radius
        attribs["start_angle"] = arc.start_angle
        attribs["end_angle"] = arc.end_angle
        return self._factory("ARC", dxfattribs=attribs)

    def elliptic_arc(self, data: bytes):
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        attribs["center"] = Vec3(bs.read_vertex())
        extrusion = Vec3(bs.read_vertex())
        attribs["extrusion"] = extrusion
        major_axis_length = bs.read_float()
        minor_axis_length = bs.read_float()
        attribs["ratio"] = minor_axis_length / major_axis_length
        start_param = bs.read_float()
        end_param = bs.read_float()
        major_axis_angle = bs.read_float()

        ocs = OCS(extrusion)
        major_axis = ocs.to_wcs(Vec3.from_angle(major_axis_angle, major_axis_length))
        attribs["major_axis"] = major_axis
        attribs["start_param"] = start_param
        attribs["end_param"] = end_param
        return self._factory("ELLIPSE", dxfattribs=attribs)

    def _filled_polygon(self, vertices, attribs):
        hatch = cast("Hatch", self._factory("HATCH", dxfattribs=attribs))
        elevation = _get_elevation(vertices)
        hatch.paths.add_polyline_path(Vec2.generate(vertices), is_closed=True)
        if elevation:
            hatch.dxf.elevation = Vec3(0, 0, elevation)
        return hatch

    def _polyline(self, vertices: list[Vec3], *, close=False, normal=Z_AXIS):
        # Polyline without bulge values!
        # Current implementation ignores the normal vector!
        # Polyline ignores the filled flag, see #906
        attribs = self._build_dxf_attribs()
        count = len(vertices)
        if count == 1 or (count == 2 and vertices[0].isclose(vertices[1])):
            attribs["location"] = vertices[0]
            return self._factory("POINT", dxfattribs=attribs)
        if not is_2d_polyline(vertices):
            attribs["flags"] = const.POLYLINE_3D_POLYLINE
        polyline = cast("Polyline", self._factory("POLYLINE", dxfattribs=attribs))
        polyline.append_vertices(vertices)
        if close:
            polyline.close()
        polyline.new_seqend()
        return polyline

    def polyline_with_normals(self, data: bytes):
        # Polyline without bulge values!
        # Polyline ignores the filled flag, see #906
        vertices, normal = self._load_vertices(data, load_normal=True)
        return self._polyline(vertices, normal=normal)

    def polyline(self, data: bytes):
        # Polyline without bulge values!
        # Polyline ignores the filled flag, see #906
        vertices, normal = self._load_vertices(data, load_normal=False)
        return self._polyline(vertices)

    def polygon(self, data: bytes):
        # Polyline without bulge values!
        vertices, normal = self._load_vertices(data, load_normal=False)
        if self.fill:
            return self._filled_polygon(vertices, self._build_dxf_attribs())
        return self._polyline(vertices, close=True)

    def lwpolyline(self, data: bytes):
        # OpenDesign Specs LWPLINE: 20.4.85 Page 211
        attribs = self._build_dxf_attribs()
        num_bulges = 0
        num_vertex_ids = 0
        num_width = 0
        is_closed = False
        bs = BitStream(data)

        num_data_bytes: int = bs.read_unsigned_long()
        flag: int = bs.read_bit_short()
        if flag & 4:
            attribs["const_width"] = bs.read_bit_double()
        if flag & 8:
            attribs["elevation"] = bs.read_bit_double()
        if flag & 2:
            attribs["thickness"] = bs.read_bit_double()
        if flag & 1:
            attribs["extrusion"] = Vec3(bs.read_bit_double(3))
        if flag & 512:  # todo: is this correct? not documented by the ODA DWG ref.
            is_closed = True
        num_points = bs.read_bit_long()
        if num_points <= 0:
            return None  # ignored in method unsafe_virtual_entities()

        if flag & 16:
            num_bulges = bs.read_bit_long()

        if self.dxfversion >= "AC1024":  # R2010+
            if flag & 1024:
                num_vertex_ids = bs.read_bit_long()
            if flag & 32:
                num_width = bs.read_bit_long()
        # ignore DXF R13/14 special vertex order

        vertices: list[tuple[float, float]] = [bs.read_raw_double(2)]  # type: ignore
        prev_point = vertices[-1]
        for _ in range(num_points - 1):
            x = bs.read_bit_double_default(default=prev_point[0])  # type: ignore
            y = bs.read_bit_double_default(default=prev_point[1])  # type: ignore
            prev_point = (x, y)
            vertices.append(prev_point)
        bulges: list[float] = [bs.read_bit_double() for _ in range(num_bulges)]
        vertex_ids: list[int] = [bs.read_bit_long() for _ in range(num_vertex_ids)]
        widths: list[tuple[float, float]] = [
            (bs.read_bit_double(), bs.read_bit_double()) for _ in range(num_width)
        ]
        if len(bulges) == 0:
            bulges = list(repeat(0, num_points))
        if len(widths) == 0:
            widths = list(repeat((0, 0), num_points))
        points: list[Sequence[float]] = []
        for v, w, b in zip(vertices, widths, bulges):
            points.append((v[0], v[1], w[0], w[1], b))
        lwpolyline = cast("LWPolyline", self._factory("LWPOLYLINE", dxfattribs=attribs))
        lwpolyline.set_points(points)
        lwpolyline.closed = is_closed
        return lwpolyline

    def mesh(self, data: bytes):
        # Limitations of the PolyFacMesh entity:
        # - all VERTEX entities have to reside on the same layer
        # - does not support vertex normals
        # - all faces have the same color (no face record)

        logger.warning("Untested proxy graphic entity: MESH - Need examples!")
        bs = ByteStream(data)
        rows, columns = bs.read_struct("<2L")
        total_edge_count = (rows - 1) * columns + (columns - 1) * rows
        total_face_count = (rows - 1) * (columns - 1)
        total_vertex_count = rows * columns
        vertices = [Vec3(bs.read_vertex()) for _ in range(total_vertex_count)]

        traits = dict()
        try:
            traits = read_mesh_traits(
                bs, total_edge_count, total_face_count, vertex_count=0
            )
        except struct.error:
            logger.error("Structure error while parsing traits for MESH proxy graphic")
        if traits:
            # apply traits
            pass

        # create PolyMesh entity
        attribs = self._build_dxf_attribs()
        attribs["m_count"] = rows
        attribs["n_count"] = columns
        attribs["flags"] = const.POLYLINE_3D_POLYMESH
        polymesh = cast("Polymesh", self._factory("POLYLINE", dxfattribs=attribs))
        polymesh.append_vertices(vertices)
        return polymesh

    def shell(self, data: bytes):
        # Limitations of the PolyFacMesh entity:
        # - all VERTEX entities have to reside on the same layer
        # - does not support vertex normals
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        attribs["flags"] = const.POLYLINE_POLYFACE
        polyface = cast("Polyface", self._factory("POLYLINE", dxfattribs=attribs))
        total_vertex_count = bs.read_long()
        vertices = [Vec3(bs.read_vertex()) for _ in range(total_vertex_count)]
        face_entry_count = bs.read_long()
        faces = []
        read_count: int = 0
        total_face_count: int = 0
        total_edge_count: int = 0
        while read_count < face_entry_count:
            edge_count = abs(bs.read_signed_long())
            read_count += 1 + edge_count
            face_indices = [bs.read_long() for _ in range(edge_count)]
            face = [vertices[index] for index in face_indices]
            total_face_count += 1
            total_edge_count += edge_count
            faces.append(face)

        traits = dict()
        try:
            traits = read_mesh_traits(
                bs, total_edge_count, total_face_count, vertex_count=0
            )
        except struct.error:
            logger.error("Structure error while parsing traits for SHELL proxy graphic")
        polyface.append_faces(faces)
        if traits:
            face_traits = traits.get("faces")
            if face_traits:
                face_colors = face_traits.get("colors")
                if face_colors:
                    logger.warning(
                        "Untested proxy graphic feature for SHELL: "
                        "apply face colors - Need examples!"
                    )
                    assert isinstance(face_colors, list)
                    _apply_face_colors(polyface, face_colors)
        polyface.optimize()
        return polyface

    def text(self, data: bytes):
        return self._text(data, unicode=False)

    def unicode_text(self, data: bytes):
        return self._text(data, unicode=True)

    def _text(self, data: bytes, unicode: bool = False):
        bs = ByteStream(data)
        start_point = Vec3(bs.read_vertex())
        normal = Vec3(bs.read_vertex())
        text_direction = Vec3(bs.read_vertex())
        height, width_factor, oblique_angle = bs.read_struct("<3d")
        text = ""
        if unicode:
            try:
                text = bs.read_padded_unicode_string()
            except UnicodeDecodeError as e:
                logger.debug(f"ProxyGraphic._text(unicode=True); {str(e)}")
        else:
            try:
                text = bs.read_padded_string(self.encoding)
            except UnicodeDecodeError as e:
                logger.debug(
                    f"ProxyGraphic._text(unicode=False); encoding={self.encoding}; {str(e)}"
                )
        attribs = self._build_dxf_attribs()
        attribs["insert"] = start_point
        attribs["text"] = text
        attribs["height"] = height
        attribs["width"] = width_factor
        attribs["rotation"] = text_direction.angle_deg
        attribs["oblique"] = math.degrees(oblique_angle)
        attribs["extrusion"] = normal
        return self._factory("TEXT", dxfattribs=attribs)

    def text2(self, data: bytes):
        encoding = self.encoding
        bs = ByteStream(data)
        start_point = Vec3(bs.read_vertex())
        normal = Vec3(bs.read_vertex())
        text_direction = Vec3(bs.read_vertex())
        text = ""
        try:
            text = bs.read_padded_string(encoding=encoding)
        except UnicodeDecodeError as e:
            logger.debug(f"ProxyGraphic.text2(); text; encoding={encoding}; {str(e)}")

        ignore_length_of_string, raw = bs.read_struct("<2l")
        (
            height,
            width_factor,
            oblique_angle,
            tracking_percentage,
        ) = bs.read_struct("<4d")
        (
            is_backwards,
            is_upside_down,
            is_vertical,
            is_underline,
            is_overline,
        ) = bs.read_struct("<5L")
        font_filename: str = "TXT.SHX"
        big_font_filename: str = ""
        try:
            font_filename = bs.read_padded_string(encoding=encoding)
            big_font_filename = bs.read_padded_string(encoding=encoding)
        except UnicodeDecodeError as e:
            logger.debug(
                f"ProxyGraphic.text2(); fonts; encoding='{encoding}'; {str(e)}"
            )

        attribs = self._build_dxf_attribs()
        attribs["insert"] = start_point
        attribs["text"] = text
        attribs["height"] = height
        attribs["width"] = width_factor
        attribs["rotation"] = text_direction.angle_deg
        attribs["oblique"] = math.degrees(oblique_angle)
        attribs["style"] = self._get_style(font_filename, big_font_filename)
        attribs["text_generation_flag"] = 2 * is_backwards + 4 * is_upside_down
        attribs["extrusion"] = normal
        return self._factory("TEXT", dxfattribs=attribs)

    def unicode_text2(self, data: bytes):
        bs = ByteStream(data)
        start_point = Vec3(bs.read_vertex())
        normal = Vec3(bs.read_vertex())
        text_direction = Vec3(bs.read_vertex())
        text = ""
        try:
            text = bs.read_padded_unicode_string()
        except UnicodeDecodeError as e:
            logger.debug(f"ProxyGraphic.unicode_text2(); text; {str(e)}")

        ignore_length_of_string, ignore_raw = bs.read_struct("<2l")
        (
            height,
            width_factor,
            oblique_angle,
            tracking_percentage,
        ) = bs.read_struct("<4d")
        (
            is_backwards,
            is_upside_down,
            is_vertical,
            is_underline,
            is_overline,
        ) = bs.read_struct("<5L")
        is_bold, is_italic, charset, pitch = bs.read_struct("<4L")

        type_face: str = ""
        font_filename: str = "TXT.SHX"
        big_font_filename: str = ""
        try:
            type_face = bs.read_padded_unicode_string()
            font_filename = bs.read_padded_unicode_string()
            big_font_filename = bs.read_padded_unicode_string()
        except UnicodeDecodeError as e:
            logger.debug(f"ProxyGraphic.unicode_text2(); fonts; {str(e)}")

        attribs = self._build_dxf_attribs()
        attribs["insert"] = start_point
        attribs["text"] = text
        attribs["height"] = height
        attribs["width"] = width_factor
        attribs["rotation"] = text_direction.angle_deg
        attribs["oblique"] = math.degrees(oblique_angle)
        attribs["style"] = self._get_style(font_filename, big_font_filename)
        attribs["text_generation_flag"] = 2 * is_backwards + 4 * is_upside_down
        attribs["extrusion"] = normal
        return self._factory("TEXT", dxfattribs=attribs)

    def xline(self, data: bytes):
        return self._xline(data, "XLINE")

    def ray(self, data: bytes):
        return self._xline(data, "RAY")

    def _xline(self, data: bytes, type_: str):
        logger.warning("Untested proxy graphic entity: RAY/XLINE - Need examples!")
        bs = ByteStream(data)
        attribs = self._build_dxf_attribs()
        start_point = Vec3(bs.read_vertex())
        other_point = Vec3(bs.read_vertex())
        attribs["start"] = start_point
        attribs["unit_vector"] = (other_point - start_point).normalize()
        return self._factory(type_, dxfattribs=attribs)

    def _get_style(self, font: str, bigfont: str) -> str:
        self.required_fonts.add(font)
        if font in self.textstyles:
            style = self.textstyles[font]
        else:
            style = font
            if self._doc and not self._doc.styles.has_entry(style):
                self._doc.styles.new(
                    font, dxfattribs={"font": font, "bigfont": bigfont}
                )
                self.textstyles[font] = style
        return style

    @staticmethod
    def _load_vertices(data: bytes, load_normal=False) -> tuple[list[Vec3], Vec3]:
        normal = Z_AXIS
        bs = ByteStream(data)
        count = bs.read_long()
        if load_normal:
            count += 1
        vertices: list[Vec3] = []
        while count > 0:
            vertices.append(Vec3(bs.read_struct("<3d")))
            count -= 1
        if load_normal:
            normal = vertices.pop()
        return vertices, normal

    def _build_dxf_attribs(self) -> dict[str, Any]:
        attribs: dict[str, Any] = dict()
        if self.layer != "0":
            attribs["layer"] = self.layer
        if self.color != const.BYLAYER:
            attribs["color"] = self.color
        if self.linetype != "BYLAYER":
            attribs["linetype"] = self.linetype
        if self.lineweight != const.LINEWEIGHT_DEFAULT:
            attribs["lineweight"] = self.lineweight
        if self.ltscale != 1.0:
            attribs["ltscale"] = self.ltscale
        if self.true_color is not None:
            attribs["true_color"] = self.true_color
        return attribs


class ProxyGraphicDebugger(ProxyGraphic):
    def __init__(self, data: bytes, doc: Optional[Drawing] = None, debug_stream=None):
        super(ProxyGraphicDebugger, self).__init__(data, doc)
        if debug_stream is None:
            debug_stream = sys.stdout
        self._debug_stream = debug_stream

    def log_entities(self):
        self.log_separator(char="=", newline=False)
        self.log_message("Create virtual DXF entities:")
        self.log_separator(newline=False)
        for entity in self.virtual_entities():
            self.log_message(f"\n  * {entity.dxftype()}")
            self.log_message(f"  * {entity.graphic_properties()}\n")
        self.log_separator(char="=")

    def log_commands(self):
        self.log_separator(char="=", newline=False)
        self.log_message("Raw proxy commands:")
        self.log_separator(newline=False)
        for index, size, cmd in self.info():
            self.log_message(f"Command: {cmd} Index: {index} Size: {size}")
        self.log_separator(char="=")

    def log_separator(self, char="-", newline=True):
        self.log_message(char * 79)
        if newline:
            self.log_message("")

    def log_message(self, msg: str):
        print(msg, file=self._debug_stream)

    def log_state(self):
        self.log_message("> " + self.get_state())

    def get_state(self) -> str:
        return (
            f"ly: '{self.layer}', clr: {self.color}, lt: {self.linetype}, "
            f"lw: {self.lineweight}, ltscale: {self.ltscale}, "
            f"rgb: {self.true_color}, fill: {self.fill}"
        )

    def attribute_color(self, data: bytes):
        self.log_message("Command: set COLOR")
        super().attribute_color(data)
        self.log_state()

    def attribute_layer(self, data: bytes):
        self.log_message("Command: set LAYER")
        super().attribute_layer(data)
        self.log_state()

    def attribute_linetype(self, data: bytes):
        self.log_message("Command: set LINETYPE")
        super().attribute_linetype(data)
        self.log_state()

    def attribute_true_color(self, data: bytes):
        self.log_message("Command: set TRUE-COLOR")
        super().attribute_true_color(data)
        self.log_state()

    def attribute_lineweight(self, data: bytes):
        self.log_message("Command: set LINEWEIGHT")
        super().attribute_lineweight(data)
        self.log_state()

    def attribute_ltscale(self, data: bytes):
        self.log_message("Command: set LTSCALE")
        super().attribute_ltscale(data)
        self.log_state()

    def attribute_fill(self, data: bytes):
        self.log_message("Command: set FILL")
        super().attribute_fill(data)
        self.log_state()


def _apply_face_colors(polyface: Polyface, colors: list[int]) -> None:
    color_count: int = len(colors)
    if color_count == 0:
        return

    index: int = 0
    for vertex in polyface.vertices:
        if vertex.is_face_record:
            vertex.dxf.color = colors[index]
            index += 1
            if index >= color_count:
                return


def _get_elevation(vertices) -> float:
    if vertices:
        return vertices[0].z
    return 0.0


def is_2d_polyline(vertices: list[Vec3]) -> bool:
    if len(vertices) < 1:
        return True
    z = vertices[0].z
    return all(math.isclose(z, v.z) for v in vertices)
