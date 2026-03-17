#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Union, Sequence
import os
import struct
import uuid
import datetime
import enum
import zipfile

from ezdxf.math import Vec3, normal_vector_3p, BoundingBox
from ezdxf.render import MeshTransformer, MeshVertexMerger, MeshBuilder
from ezdxf import __version__


class UnsupportedFileFormat(Exception):
    pass


class ParsingError(Exception):
    pass


def stl_readfile(filename: Union[str, os.PathLike]) -> MeshTransformer:
    """Read ascii or binary `STL`_ file content as :class:`ezdxf.render.MeshTransformer`
    instance.

    Raises:
        ParsingError: vertex parsing error or invalid/corrupt data

    """
    with open(filename, "rb") as fp:
        buffer = fp.read()
    if buffer.startswith(b"solid"):
        s = buffer.decode("ascii", errors="ignore")
        return stl_loads(s)
    else:
        return stl_loadb(buffer)


def stl_loads(content: str) -> MeshTransformer:
    """Load a mesh from an ascii `STL`_ content string as :class:`ezdxf.render.MeshTransformer`
    instance.

    Raises:
        ParsingError: vertex parsing error

    """
    # http://www.fabbers.com/tech/STL_Format#Sct_ASCII
    # This implementation is not very picky and grabs only lines which start
    # with "vertex" or "endloop" and ignores the rest.
    def parse_vertex(line: str) -> Vec3:
        data = line.split()
        return Vec3(float(data[1]), float(data[2]), float(data[3]))

    mesh = MeshVertexMerger()
    face: list[Vec3] = []
    for num, line in enumerate(content.split("\n"), start=1):
        line = line.strip(" \r")
        if line.startswith("vertex"):
            try:
                face.append(parse_vertex(line))
            except (IndexError, ValueError):
                raise ParsingError(f"STL parsing error in line {num}: {line}")
        elif line.startswith("endloop"):
            if len(face) == 3:
                mesh.add_face(face)
            face.clear()
    return MeshTransformer.from_builder(mesh)


def stl_loadb(buffer: bytes) -> MeshTransformer:
    """Load a mesh from a binary `STL`_ data :class:`ezdxf.render.MeshTransformer`
    instance.

    Raises:
        ParsingError: invalid/corrupt data or not a binary STL file

    """
    # http://www.fabbers.com/tech/STL_Format#Sct_ASCII
    index = 80
    n_faces = struct.unpack_from("<I", buffer, index)[0]
    index += 4

    mesh = MeshVertexMerger()
    for _ in range(n_faces):
        try:
            face = struct.unpack_from("<12fH", buffer, index)
        except struct.error:
            raise ParsingError("binary STL parsing error")
        index += 50
        v1 = Vec3(face[3:6])
        v2 = Vec3(face[6:9])
        v3 = Vec3(face[9:12])
        mesh.add_face((v1, v2, v3))
    return MeshTransformer.from_builder(mesh)


def off_readfile(filename: Union[str, os.PathLike]) -> MeshTransformer:
    """Read `OFF`_ file content as :class:`ezdxf.render.MeshTransformer`
    instance.

    Raises:
        ParsingError: vertex or face parsing error

    """
    with open(filename, "rt", encoding="ascii", errors="ignore") as fp:
        content = fp.read()
    return off_loads(content)


def off_loads(content: str) -> MeshTransformer:
    """Load a mesh from a `OFF`_ content string as :class:`ezdxf.render.MeshTransformer`
    instance.

    Raises:
        ParsingError: vertex or face parsing error

    """
    # https://en.wikipedia.org/wiki/OFF_(file_format)
    mesh = MeshVertexMerger()
    lines: list[str] = []
    for line in content.split("\n"):
        line = line.strip(" \n\r")
        # "OFF" in a single line
        if line.startswith("#") or line == "OFF" or line == "":
            continue
        lines.append(line)

    if len(lines) == 0:
        raise ParsingError(f"OFF format parsing error: no data")

    if lines[0].startswith("OFF"):
        # OFF v f e
        lines[0] = lines[0][4:]

    n = lines[0].split()
    try:
        n_vertices, n_faces = int(n[0]), int(n[1])
    except ValueError:
        raise ParsingError(f"OFF format parsing error: {lines[0]}")

    if len(lines) < n_vertices + n_faces:
        raise ParsingError(f"OFF format parsing error: invalid data count")

    for vertex in lines[1 : n_vertices + 1]:
        v = vertex.split()
        try:
            vtx = Vec3(float(v[0]), float(v[1]), float(v[2]))
        except (ValueError, IndexError):
            raise ParsingError(f"OFF format vertex parsing error: {vertex}")
        mesh.vertices.append(vtx)

    index = n_vertices + 1
    face_indices = []
    for face in lines[index : index + n_faces]:
        f = face.split()
        try:
            vertex_count = int(f[0])
        except ValueError:
            raise ParsingError(f"OFF format face parsing error: {face}")
        for index in range(vertex_count):
            try:
                face_indices.append(int(f[1 + index]))
            except (ValueError, IndexError):
                raise ParsingError(
                    f"OFF format face index parsing error: {face}"
                )
        mesh.faces.append(tuple(face_indices))
        face_indices.clear()
    return MeshTransformer.from_builder(mesh)


def obj_readfile(filename: Union[str, os.PathLike]) -> list[MeshTransformer]:
    """Read `OBJ`_ file content as list of :class:`ezdxf.render.MeshTransformer`
    instances.

    Raises:
        ParsingError: vertex or face parsing error

    """
    with open(filename, "rt", encoding="ascii", errors="ignore") as fp:
        content = fp.read()
    return obj_loads(content)


def obj_loads(content: str) -> list[MeshTransformer]:
    """Load one or more meshes from an `OBJ`_ content string as list of
    :class:`ezdxf.render.MeshTransformer` instances.

    Raises:
        ParsingError: vertex parsing error

    """
    # https://en.wikipedia.org/wiki/Wavefront_.obj_file
    # This implementation is not very picky and grabs only lines which start
    # with "v", "g" or "f" and ignores the rest.
    def parse_vertex(l: str) -> Vec3:
        v = l.split()
        return Vec3(float(v[0]), float(v[1]), float(v[2]))

    def parse_face(l: str) -> Sequence[int]:
        return tuple(int(s.split("/")[0]) for s in l.split())

    vertices: list[Vec3] = [Vec3()]  # 1-indexed
    meshes: list[MeshTransformer] = []
    mesh = MeshVertexMerger()
    for num, line in enumerate(content.split("\n"), start=1):
        line = line.strip(" \r")
        if line.startswith("v"):
            try:
                vtx = parse_vertex(line[2:])
            except (IndexError, ValueError):
                raise ParsingError(
                    f"OBJ vertex parsing error in line {num}: {line}"
                )
            vertices.append(vtx)
        elif line.startswith("f"):
            try:
                mesh.add_face(vertices[i] for i in parse_face(line[2:]))
            except ValueError:
                raise ParsingError(
                    f"OBJ face parsing error in line {num}: {line}"
                )
            except IndexError:
                raise ParsingError(
                    f"OBJ face index error (n={len(vertices)}) in line {num}: {line}"
                )

        elif line.startswith("g") and len(mesh.vertices) > 0:
            meshes.append(MeshTransformer.from_builder(mesh))
            mesh = MeshVertexMerger()

    if len(mesh.vertices) > 0:
        meshes.append(MeshTransformer.from_builder(mesh))
    return meshes


def stl_dumps(mesh: MeshBuilder) -> str:
    """Returns the `STL`_ data as string for the given `mesh`.
    This function triangulates the meshes automatically because the `STL`_
    format supports only triangles as faces.

    This function does not check if the mesh obey the
    `STL`_ format `rules <http://www.fabbers.com/tech/STL_Format>`_:

        - The direction of the face normal is outward.
        - The face vertices are listed in counter-clockwise order when looking
          at the object from the outside (right-hand rule).
        - Each triangle must share two vertices with each of its adjacent triangles.
        - The object represented must be located in the all-positive octant
          (non-negative and nonzero).

    """
    lines: list[str] = [f"solid STL generated by ezdxf {__version__}"]
    for face in mesh.tessellation(max_vertex_count=3):
        if len(face) < 3:
            continue
        try:
            n = normal_vector_3p(face[0], face[1], face[2])
        except ZeroDivisionError:
            continue
        n = n.round(8)
        lines.append(f"  facet normal {n.x} {n.y} {n.z}")
        lines.append("    outer loop")
        for v in face:
            lines.append(f"      vertex {v.x} {v.y} {v.z}")
        lines.append("    endloop")
        lines.append("  endfacet")
    lines.append("endsolid\n")
    return "\n".join(lines)


STL_SIGNATURE = (b"STL generated ezdxf" + b" " * 80)[:80]


def stl_dumpb(mesh: MeshBuilder) -> bytes:
    """Returns the `STL`_ binary data as bytes for the given `mesh`.

    For more information see function: :func:`stl_dumps`
    """
    data: list[bytes] = [STL_SIGNATURE, b"0000"]
    count = 0
    for face in mesh.tessellation(max_vertex_count=3):
        try:
            n = normal_vector_3p(face[0], face[1], face[2])
        except ZeroDivisionError:
            continue
        count += 1
        values = list(n.xyz)
        for v in face:
            values.extend(v.xyz)
        values.append(0)
        data.append(struct.pack("<12fH", *values))
    data[1] = struct.pack("<I", count)
    return b"".join(data)


def off_dumps(mesh: MeshBuilder) -> str:
    """Returns the `OFF`_ data as string for the given `mesh`.
    The `OFF`_ format supports ngons as faces.

    """
    lines: list[str] = ["OFF", f"{len(mesh.vertices)} {len(mesh.faces)} 0"]
    for v in mesh.vertices:
        v = v.round(6)
        lines.append(f"{v.x} {v.y} {v.z}")
    for face in mesh.open_faces():
        lines.append(f"{len(face)} {' '.join(str(i) for i in face)}")
    lines[-1] += "\n"
    return "\n".join(lines)


def obj_dumps(mesh: MeshBuilder) -> str:
    """Returns the `OBJ`_ data as string for the given `mesh`.
    The `OBJ`_ format supports ngons as faces.

    """
    lines: list[str] = [f"# OBJ generated by ezdxf {__version__}"]
    for v in mesh.vertices:
        v = v.round(6)
        lines.append(f"v {v.x} {v.y} {v.z}")
    for face in mesh.open_faces():
        # OBJ is 1-index
        lines.append("f " + " ".join(str(i + 1) for i in face))
    lines[-1] += "\n"
    return "\n".join(lines)


def scad_dumps(mesh: MeshBuilder) -> str:
    """Returns the `OpenSCAD`_ `polyhedron`_ definition as string for the given
    `mesh`. `OpenSCAD`_ supports ngons as faces.

    .. Important::

        `OpenSCAD`_ requires the face normals pointing inwards, the method
        :meth:`~ezdxf.render.MeshBuilder.flip_normals` of the
        :class:`~ezdxf.render.MeshBuilder` class can flip the normals
        inplace.

    """
    # polyhedron( points = [ [X0, Y0, Z0], [X1, Y1, Z1], ... ], faces = [ [P0, P1, P2, P3, ...], ... ], convexity = N);   // 2014.03 & later
    lines: list[str] = ["polyhedron(points = ["]
    for v in mesh.vertices:
        v = v.round(6)
        lines.append(f"  [{v.x}, {v.y}, {v.z}],")
    # OpenSCAD accept the last ","
    lines.append("], faces = [")
    for face in mesh.open_faces():
        lines.append("  [" + ", ".join(str(i) for i in face) + "],")
    # OpenSCAD accept the last ","
    lines.append("], convexity = 10);\n")
    return "\n".join(lines)


def ply_dumpb(mesh: MeshBuilder) -> bytes:
    """Returns the `PLY`_ binary data as bytes for the given `mesh`.
    The `PLY`_ format supports ngons as faces.

    """
    if any(len(f) > 255 for f in mesh.faces):
        face_hdr_fmt = b"property list int int vertex_index"
        face_fmt = "<i{}i"
    else:
        face_hdr_fmt = b"property list uchar int vertex_index"
        face_fmt = "<B{}i"

    header: bytes = b"\n".join(
        [
            b"ply",
            b"format binary_little_endian 1.0",
            b"comment generated by ezdxf " + __version__.encode(),
            b"element vertex " + str(len(mesh.vertices)).encode(),
            b"property float x",
            b"property float y",
            b"property float z",
            b"element face " + str(len(mesh.faces)).encode(),
            face_hdr_fmt,
            b"end_header\n",
        ]
    )
    data: list[bytes] = [header]
    for vertex in mesh.vertices:
        data.append(struct.pack("<3f", vertex.x, vertex.y, vertex.z))
    for face in mesh.open_faces():
        count = len(face)
        fmt = face_fmt.format(count)
        data.append(struct.pack(fmt, count, *face))
    return b"".join(data)


def ifc_guid() -> str:
    return _guid_compress(uuid.uuid4().hex)


def _guid_compress(g: str) -> str:
    # https://github.com/IfcOpenShell/IfcOpenShell/blob/master/src/ifcopenshell-python/ifcopenshell/guid.py#L56
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_$"
    bs = [int(g[i : i + 2], 16) for i in range(0, len(g), 2)]

    def b64(v: int, size: int = 4):
        return "".join(
            [chars[(v // (64**i)) % 64] for i in range(size)][::-1]
        )

    return "".join(
        [b64(bs[0], 2)]
        + [
            b64((bs[i] << 16) + (bs[i + 1] << 8) + bs[i + 2])
            for i in range(1, 16, 3)
        ]
    )


class IfcEntityType(enum.Enum):
    POLYGON_FACE_SET = "POLY_FACE_SET"
    CLOSED_SHELL = "CLOSED_SHELL"
    OPEN_SHELL = "OPEN_SHELL"


class Records:
    def __init__(self) -> None:
        # Record numbers are 1-based, record index in list is 0-based!
        # records[0] is record #1
        self.records: list[str] = []

    @property
    def last_num(self) -> int:  # list index
        return len(self.records)

    @property
    def prev_num(self) -> int:  # list index
        return self.last_num - 1

    @property
    def next_num(self) -> int:  # list index
        return self.last_num + 1

    def add(self, record: str, num: int = 0) -> str:
        assert record.endswith(");"), "invalid structure"
        self.records.append(record)
        if num != 0 and num != len(self.records):
            raise ValueError("unexpected record number")
        num = len(self.records)
        return f"#{num}"

    def add_dummy(self):
        self.records.append("")

    def get(self, num: int) -> str:
        return self.records[num - 1]

    def update_all(self, tag: str, record_num: str):
        for index, record in enumerate(self.records):
            if tag in record:
                self.update_record(index, tag, record_num)

    def update_record(self, index, tag: str, record_num: str):
        self.records[index] = self.records[index].replace(tag, record_num)

    def dumps(self) -> str:
        return "\n".join(
            f"#{num+1}= {data}" for num, data in enumerate(self.records) if data
        )


def ifc4_dumps(
    mesh: MeshBuilder,
    entity_type=IfcEntityType.POLYGON_FACE_SET,
    *,
    layer: str = "MeshExport",
    color: tuple[float, float, float] = (1.0, 1.0, 1.0),
) -> str:
    """Returns the `IFC4`_ string for the given `mesh`. The caller is
    responsible for checking if the mesh is a closed or open surface
    (e.g. :code:`mesh.diagnose().euler_characteristic == 2`) and using the
    appropriate entity type.

    Args:
        mesh: :class:`~ezdxf.render.MeshBuilder`
        entity_type: :class:`IfcEntityType`
        layer: layer name as string
        color: entity color as RGB tuple, values in the range [0,1]

    .. warning::

        `IFC4`_ is a very complex data format and this is a minimal effort
        exporter, so the exported data may not be importable by all CAD
        applications.

        The exported `IFC4`_ data can be imported by the following applications:

        - BricsCAD
        - FreeCAD (IfcOpenShell)
        - Allplan
        - Tekla BIMsight

    """

    def make_header():
        date = datetime.datetime.now().isoformat()[:-7]
        return f"""ISO-10303-21;
HEADER;
FILE_DESCRIPTION(('ViewDefinition [CoordinationView_V2.0]'),'2;1');
FILE_NAME('undefined.ifc','{date}',('Undefined'),('Undefined'),'ezdxf {__version__}','ezdxf {__version__}','Undefined');
FILE_SCHEMA(('IFC4'));
ENDSEC;

DATA;
"""

    def make_data_records() -> Records:
        records = Records()
        # fmt: off
        if entity_type == IfcEntityType.POLYGON_FACE_SET:
            kind = "SurfaceModel"
        elif entity_type == IfcEntityType.OPEN_SHELL:
            kind = "SurfaceModel"
        elif entity_type == IfcEntityType.CLOSED_SHELL:
            kind = "Brep"
        else:
            raise ValueError(f"invalid entity type: {entity_type}")
        # for simplicity the first part has absolute record numbering:
        records.add(f"IFCPROJECT('{ifc_guid()}',#2,'MeshExport',$,$,$,$,(#7),#13);", 1)
        records.add("IFCOWNERHISTORY(#3,#6,$,$,$,$,$,0);", 2)
        records.add("IFCPERSONANDORGANIZATION(#4,#5,$);", 3)
        records.add("IFCPERSON($,$,'Undefined',$,$,$,$,$);", 4)
        records.add("IFCORGANIZATION($,'Undefined',$,$,$);", 5)
        records.add(f"IFCAPPLICATION(#5,'{__version__}','ezdxf','ezdxf');", 6)
        records.add("IFCGEOMETRICREPRESENTATIONCONTEXT($,'Model',3,1.000000000000000E-05,#8,#12);", 7)
        records.add("IFCAXIS2PLACEMENT3D(#9,#10,#11);", 8)
        records.add("IFCCARTESIANPOINT((0.,0.,0.));", 9)
        records.add("IFCDIRECTION((0.,0.,1.));", 10)
        records.add("IFCDIRECTION((1.,0.,0.));", 11)
        records.add("IFCDIRECTION((1.,0.));", 12)
        records.add("IFCUNITASSIGNMENT((#14,#15,#16,#17,#18,#19));", 13)
        records.add("IFCSIUNIT(*,.LENGTHUNIT.,$,.METRE.);", 14)
        records.add("IFCSIUNIT(*,.AREAUNIT.,$,.SQUARE_METRE.);", 15)
        records.add("IFCSIUNIT(*,.VOLUMEUNIT.,$,.CUBIC_METRE.);", 16)
        records.add("IFCSIUNIT(*,.PLANEANGLEUNIT.,$,.RADIAN.);", 17)
        records.add("IFCSIUNIT(*,.TIMEUNIT.,$,.SECOND.);", 18)
        records.add("IFCSIUNIT(*,.MASSUNIT.,$,.GRAM.);", 19)
        records.add("IFCLOCALPLACEMENT($,#8);", 20)
        building = records.add(f"IFCBUILDING('{ifc_guid()}',#2,'MeshExport',$,$, #20,$,$,.ELEMENT.,$,$,$);", 21)
        records.add_dummy()  # I don't wanna redo the absolute record numbering
        proxy = records.add(f"IFCBUILDINGELEMENTPROXY('{ifc_guid()}',#2,$,$,$,#24,#29,$,$);", 23)
        records.add("IFCLOCALPLACEMENT(#25,#26);", 24)
        records.add("IFCLOCALPLACEMENT(#20,#8);", 25)
        records.add("IFCAXIS2PLACEMENT3D(#27,#10,#28);", 26)
        records.add(f"IFCCARTESIANPOINT(({emin.x},{emin.y},{emin.z}));", 27)
        records.add("IFCDIRECTION((1.,0.,0.));", 28)
        records.add("IFCPRODUCTDEFINITIONSHAPE($,$,(#30));", 29)
        shape = records.add(f"IFCSHAPEREPRESENTATION(#31,'Body','{kind}',($ENTITY$));", 30)
        records.add("IFCGEOMETRICREPRESENTATIONSUBCONTEXT('Body','Model',*,*,*,*,#7,$,.MODEL_VIEW.,$);", 31)

        # from here on only relative record numbering:
        entity = "#0"
        if entity_type == IfcEntityType.POLYGON_FACE_SET:
            entity = make_polygon_face_set(records)
        elif entity_type == IfcEntityType.CLOSED_SHELL:
            entity = make_shell(records)
        elif entity_type == IfcEntityType.OPEN_SHELL:
            entity = make_shell(records)
        color_str = f"IFCCOLOURRGB($,{color[0]:.3f},{color[1]:.3f},{color[2]:.3f});"
        records.add(color_str)
        records.add(f"IFCSURFACESTYLESHADING(#{records.prev_num+1},0.);")
        records.add(f"IFCSURFACESTYLE($,.POSITIVE.,(#{records.prev_num+1}));")
        records.add(f"IFCPRESENTATIONSTYLEASSIGNMENT((#{records.prev_num+1}));")
        records.add(f"IFCSTYLEDITEM($ENTITY$,(#{records.prev_num+1}),$);")
        records.add(f"IFCPRESENTATIONLAYERWITHSTYLE('{layer}',$,({shape}),$,.T.,.F.,.F.,(#{records.next_num+1}));")
        records.add(f"IFCSURFACESTYLE($,.POSITIVE.,(#{records.next_num+1}));")
        records.add(f"IFCSURFACESTYLESHADING(#{records.next_num+1},0.);")
        records.add(color_str)
        records.add(f"IFCRELCONTAINEDINSPATIALSTRUCTURE('{ifc_guid()}',#2,$,$,({proxy}),{building});")
        records.add(f"IFCRELAGGREGATES('{ifc_guid()}',#2,$,$,#1,({building}));")
        # fmt: on
        records.update_all("$ENTITY$", entity)
        return records

    def make_polygon_face_set(records: Records) -> str:
        entity = records.add(
            f"IFCPOLYGONALFACESET(#{records.next_num+1},$,($FACES$), $);"
        )
        entity_num = records.last_num
        vertices = ",".join([str(v.xyz) for v in mesh.vertices])
        records.add(f"IFCCARTESIANPOINTLIST3D(({vertices}));")
        face_records: list[str] = []
        for face in mesh.open_faces():
            indices = ",".join(str(i + 1) for i in face)  # 1-based indexing
            face_records.append(
                records.add(f"IFCINDEXEDPOLYGONALFACE(({indices}));")
            )
        # list index required
        records.update_record(entity_num - 1, "$FACES$", ",".join(face_records))
        return entity

    def make_shell(records: Records) -> str:
        if entity_type == IfcEntityType.CLOSED_SHELL:
            entity = records.add(f"IFCFACETEDBREP(#{records.next_num+1});")
            records.add(f"IFCCLOSEDSHELL(($FACES$));")
        elif entity_type == IfcEntityType.OPEN_SHELL:
            entity = records.add(
                f"IFCSHELLBASEDSURFACEMODEL((#{records.next_num + 1}));"
            )
            records.add(f"IFCOPENSHELL(($FACES$));")
        else:
            raise ValueError(f"invalid entity type: {entity_type}")
        shell_num = records.last_num
        # add vertices
        first_vertex = records.next_num
        for v in mesh.vertices:
            records.add(f"IFCCARTESIANPOINT({str(v.xyz)});")
        # add faces
        face_records: list[str] = []
        for face in mesh.open_faces():
            vertices = ",".join("#" + str(first_vertex + i) for i in face)
            records.add(f"IFCPOLYLOOP(({vertices}));")
            records.add(f"IFCFACEOUTERBOUND(#{records.prev_num+1},.T.);")
            face_records.append(
                records.add(f"IFCFACE((#{records.prev_num+1}));")
            )
        # list index required
        records.update_record(shell_num - 1, "$FACES$", ",".join(face_records))
        return entity

    if len(mesh.vertices) == 0:
        return ""
    bbox = BoundingBox(mesh.vertices)
    emin = Vec3()
    assert bbox.extmin is not None
    if bbox.extmin.x < 0 or bbox.extmin.y < 0 or bbox.extmin.z < 0:
        # Allplan (IFC?) requires all mesh vertices in the all-positive octant
        # (non-negative).  Record #27 does the final placement at the correct
        # location.
        emin = bbox.extmin
        mesh = MeshTransformer.from_builder(mesh)
        mesh.translate(-emin.x, -emin.y, -emin.z)

    header = make_header()
    data = make_data_records()
    return header + data.dumps() + "\nENDSEC;\nEND-ISO-10303-21;\n"


def export_ifcZIP(
    filename: Union[str, os.PathLike],
    mesh: MeshBuilder,
    entity_type=IfcEntityType.POLYGON_FACE_SET,
    *,
    layer: str = "MeshExport",
    color: tuple[float, float, float] = (1.0, 1.0, 1.0),
):
    """Export the given `mesh` as zip-compressed `IFC4`_ file. The filename
    suffix should be ``.ifcZIP``. For more information see function
    :func:`ifc4_dumps`.

    Args:
        filename: zip filename, the data file has the same name with suffix ``.ifc``
        mesh: :class:`~ezdxf.render.MeshBuilder`
        entity_type: :class:`IfcEntityType`
        layer: layer name as string
        color: entity color as RGB tuple, values in the range [0,1]

    Raises:
        IOError: IO error when opening the zip-file for writing

    """
    name = os.path.basename(filename) + ".ifc"
    zf = zipfile.ZipFile(filename, mode="w", compression=zipfile.ZIP_DEFLATED)
    try:
        zf.writestr(
            name, ifc4_dumps(mesh, entity_type, layer=layer, color=color)
        )
    finally:
        zf.close()
