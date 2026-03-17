#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Callable, Type, Any, Sequence, Iterator
import abc

from . import sab, sat, const, hdr
from .const import Features
from .abstract import DataLoader, AbstractEntity, DataExporter
from .type_hints import EncodedData
from ezdxf.math import Matrix44, Vec3, NULLVEC

Factory = Callable[[AbstractEntity], "AcisEntity"]

ENTITY_TYPES: dict[str, Type[AcisEntity]] = {}
INF = float("inf")


def load(data: EncodedData) -> list[Body]:
    """Returns a list of :class:`Body` entities from :term:`SAT` or :term:`SAB`
    data. Accepts :term:`SAT` data as a single string or a sequence of strings
    and :term:`SAB` data as bytes or bytearray.

    """
    if isinstance(data, (bytes, bytearray)):
        return SabLoader.load(data)
    return SatLoader.load(data)


def export_sat(
    bodies: Sequence[Body], version: int = const.DEFAULT_SAT_VERSION
) -> list[str]:
    """Export one or more :class:`Body` entities as text based :term:`SAT` data.

    ACIS version 700 is sufficient for DXF versions R2000, R2004, R2007 and
    R2010, later DXF versions require :term:`SAB` data.

    Raises:
        ExportError: ACIS structures contain unsupported entities
        InvalidLinkStructure: corrupt link structure

    """
    if version < const.MIN_EXPORT_VERSION:
        raise const.ExportError(f"invalid ACIS version: {version}")
    exporter = sat.SatExporter(_setup_export_header(version))
    exporter.header.asm_end_marker = False
    for body in bodies:
        exporter.export(body)
    return exporter.dump_sat()


def export_sab(
    bodies: Sequence[Body], version: int = const.DEFAULT_SAB_VERSION
) -> bytes:
    """Export one or more :class:`Body` entities as binary encoded :term:`SAB`
    data.

    ACIS version 21800 is sufficient for DXF versions R2013 and R2018, earlier
    DXF versions require :term:`SAT` data.

    Raises:
        ExportError: ACIS structures contain unsupported entities
        InvalidLinkStructure: corrupt link structure

    """
    if version < const.MIN_EXPORT_VERSION:
        raise const.ExportError(f"invalid ACIS version: {version}")
    exporter = sab.SabExporter(_setup_export_header(version))
    exporter.header.asm_end_marker = True
    for body in bodies:
        exporter.export(body)
    return exporter.dump_sab()


def _setup_export_header(version) -> hdr.AcisHeader:
    if not const.is_valid_export_version(version):
        raise const.ExportError(f"invalid export version: {version}")
    header = hdr.AcisHeader()
    header.set_version(version)
    return header


def register(cls):
    ENTITY_TYPES[cls.type] = cls
    return cls


class NoneEntity:
    type: str = const.NONE_ENTITY_NAME

    @property
    def is_none(self) -> bool:
        return self.type == const.NONE_ENTITY_NAME


NONE_REF: Any = NoneEntity()


class AcisEntity(NoneEntity):
    """Base ACIS entity which also represents unsupported entities.

    Unsupported entities are entities whose internal structure are not fully
    known or user defined entity types.

    The content of these unsupported entities is not loaded and lost by
    exporting such entities, therefore exporting unsupported entities raises
    an :class:`ExportError` exception.

    """

    type: str = "unsupported-entity"
    id: int = -1
    attributes: AcisEntity = NONE_REF

    def __str__(self) -> str:
        return f"{self.type}({self.id})"

    def load(self, loader: DataLoader, entity_factory: Factory) -> None:
        """Load the ACIS entity content from `loader`."""
        self.restore_common(loader, entity_factory)
        self.restore_data(loader)

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        """Load the common part of an ACIS entity."""
        pass

    def restore_data(self, loader: DataLoader) -> None:
        """Load the data part of an ACIS entity."""
        pass

    def export(self, exporter: DataExporter) -> None:
        """Write the ACIS entity content to `exporter`."""
        self.write_common(exporter)
        self.write_data(exporter)

    def write_common(self, exporter: DataExporter) -> None:
        """Write the common part of the ACIS entity.

        It is not possible to export :class:`Body` entities including
        unsupported entities, doing so would cause data loss or worse data
        corruption!

        """
        raise const.ExportError(f"unsupported entity type: {self.type}")

    def write_data(self, exporter: DataExporter) -> None:
        """Write the data part of the ACIS entity."""
        pass

    def entities(self) -> Iterator[AcisEntity]:
        """Yield all attributes of this entity of type AcisEntity."""
        for e in vars(self).values():
            if isinstance(e, AcisEntity):
                yield e


def restore_entity(
    expected_type: str, loader: DataLoader, entity_factory: Factory
) -> Any:
    raw_entity = loader.read_ptr()
    if raw_entity.is_null_ptr:
        return NONE_REF
    if raw_entity.name.endswith(expected_type):
        return entity_factory(raw_entity)
    else:
        raise const.ParsingError(
            f"expected entity type '{expected_type}', got '{raw_entity.name}'"
        )


@register
class Transform(AcisEntity):
    type: str = "transform"
    matrix = Matrix44()

    def restore_data(self, loader: DataLoader) -> None:
        data = loader.read_transform()
        # insert values of the 4th matrix column (0, 0, 0, 1)
        data.insert(3, 0.0)
        data.insert(7, 0.0)
        data.insert(11, 0.0)
        data.append(1.0)
        self.matrix = Matrix44(data)

    def write_common(self, exporter: DataExporter) -> None:
        def write_double(value: float):
            data.append(f"{value:g}")

        data: list[str] = []
        for row in self.matrix.rows():
            write_double(row[0])
            write_double(row[1])
            write_double(row[2])
        test_vector = Vec3(1, 0, 0)
        result = self.matrix.transform_direction(test_vector)
        # A uniform scaling in x- y- and z-axis is assumed:
        write_double(round(result.magnitude, 6))  # scale factor
        is_rotated = not result.normalize().isclose(test_vector)
        data.append("rotate" if is_rotated else "no_rotate")
        data.append("no_reflect")
        data.append("no_shear")
        exporter.write_transform(data)


@register
class AsmHeader(AcisEntity):
    type: str = "asmheader"

    def __init__(self, version: str = ""):
        self.version = version

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        self.version = loader.read_str()

    def write_common(self, exporter: DataExporter) -> None:
        exporter.write_str(self.version)


class SupportsPattern(AcisEntity):
    pattern: Pattern = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        if loader.version >= Features.PATTERN:
            self.pattern = restore_entity("pattern", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        exporter.write_ptr(self.pattern)


@register
class Body(SupportsPattern):
    type: str = "body"
    pattern: Pattern = NONE_REF
    lump: Lump = NONE_REF
    wire: Wire = NONE_REF
    transform: Transform = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.lump = restore_entity("lump", loader, entity_factory)
        self.wire = restore_entity("wire", loader, entity_factory)
        self.transform = restore_entity("transform", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.lump)
        exporter.write_ptr(self.wire)
        exporter.write_ptr(self.transform)

    def append_lump(self, lump: Lump) -> None:
        """Append a :class:`Lump` entity as last lump."""
        lump.body = self
        if self.lump.is_none:
            self.lump = lump
        else:
            current_lump = self.lump
            while not current_lump.next_lump.is_none:
                current_lump = current_lump.next_lump
            current_lump.next_lump = lump

    def lumps(self) -> list[Lump]:
        """Returns all linked :class:`Lump` entities as a list."""
        lumps = []
        current_lump = self.lump
        while not current_lump.is_none:
            lumps.append(current_lump)
            current_lump = current_lump.next_lump
        return lumps


@register
class Wire(SupportsPattern):  # not implemented
    type: str = "wire"


@register
class Pattern(AcisEntity):  # not implemented
    type: str = "pattern"


@register
class Lump(SupportsPattern):
    type: str = "lump"
    next_lump: Lump = NONE_REF
    shell: Shell = NONE_REF
    body: Body = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.next_lump = restore_entity("lump", loader, entity_factory)
        self.shell = restore_entity("shell", loader, entity_factory)
        self.body = restore_entity("body", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.next_lump)
        exporter.write_ptr(self.shell)
        exporter.write_ptr(self.body)

    def append_shell(self, shell: Shell) -> None:
        """Append a :class:`Shell` entity as last shell."""
        shell.lump = self
        if self.shell.is_none:
            self.shell = shell
        else:
            current_shell = self.shell
            while not current_shell.next_shell.is_none:
                current_shell = current_shell.next_shell
            current_shell.next_shell = shell

    def shells(self) -> list[Shell]:
        """Returns all linked :class:`Shell` entities as a list."""
        shells = []
        current_shell = self.shell
        while not current_shell.is_none:
            shells.append(current_shell)
            current_shell = current_shell.next_shell
        return shells


@register
class Shell(SupportsPattern):
    type: str = "shell"
    next_shell: Shell = NONE_REF
    subshell: Subshell = NONE_REF
    face: Face = NONE_REF
    wire: Wire = NONE_REF
    lump: Lump = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.next_shell = restore_entity("next_shell", loader, entity_factory)
        self.subshell = restore_entity("subshell", loader, entity_factory)
        self.face = restore_entity("face", loader, entity_factory)
        self.wire = restore_entity("wire", loader, entity_factory)
        self.lump = restore_entity("lump", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.next_shell)
        exporter.write_ptr(self.subshell)
        exporter.write_ptr(self.face)
        exporter.write_ptr(self.wire)
        exporter.write_ptr(self.lump)

    def append_face(self, face: Face) -> None:
        """Append a :class:`Face` entity as last face."""
        face.shell = self
        if self.face.is_none:
            self.face = face
        else:
            current_face = self.face
            while not current_face.next_face.is_none:
                current_face = current_face.next_face
            current_face.next_face = face

    def faces(self) -> list[Face]:
        """Returns all linked :class:`Face` entities as a list."""
        faces = []
        current_face = self.face
        while not current_face.is_none:
            faces.append(current_face)
            current_face = current_face.next_face
        return faces


@register
class Subshell(SupportsPattern):  # not implemented
    type: str = "subshell"


@register
class Face(SupportsPattern):
    type: str = "face"
    next_face: "Face" = NONE_REF
    loop: Loop = NONE_REF
    shell: Shell = NONE_REF
    subshell: Subshell = NONE_REF
    surface: Surface = NONE_REF
    # sense: face normal with respect to the surface
    sense = False  # True = reversed; False = forward
    double_sided = False  # True = double (hollow body); False = single (solid body)
    containment = False  # if double_sided: True = in, False = out

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.next_face = restore_entity("face", loader, entity_factory)
        self.loop = restore_entity("loop", loader, entity_factory)
        self.shell = restore_entity("shell", loader, entity_factory)
        self.subshell = restore_entity("subshell", loader, entity_factory)
        self.surface = restore_entity("surface", loader, entity_factory)
        self.sense = loader.read_bool("reversed", "forward")
        self.double_sided = loader.read_bool("double", "single")
        if self.double_sided:
            self.containment = loader.read_bool("in", "out")

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.next_face)
        exporter.write_ptr(self.loop)
        exporter.write_ptr(self.shell)
        exporter.write_ptr(self.subshell)
        exporter.write_ptr(self.surface)
        exporter.write_bool(self.sense, "reversed", "forward")
        exporter.write_bool(self.double_sided, "double", "single")
        if self.double_sided:
            exporter.write_bool(self.containment, "in", "out")

    def append_loop(self, loop: Loop) -> None:
        """Append a :class:`Loop` entity as last loop."""
        loop.face = self
        if self.loop.is_none:
            self.loop = loop
        else:  # order of coedges is important! (right-hand rule)
            current_loop = self.loop
            while not current_loop.next_loop.is_none:
                current_loop = current_loop.next_loop
            current_loop.next_loop = loop

    def loops(self) -> list[Loop]:
        """Returns all linked :class:`Loop` entities as a list."""
        loops = []
        current_loop = self.loop
        while not current_loop.is_none:
            loops.append(current_loop)
            current_loop = current_loop.next_loop
        return loops


@register
class Surface(SupportsPattern):
    type: str = "surface"
    u_bounds = INF, INF
    v_bounds = INF, INF

    def restore_data(self, loader: DataLoader) -> None:
        self.u_bounds = loader.read_interval(), loader.read_interval()
        self.v_bounds = loader.read_interval(), loader.read_interval()

    def write_data(self, exporter: DataExporter):
        exporter.write_interval(self.u_bounds[0])
        exporter.write_interval(self.u_bounds[1])
        exporter.write_interval(self.v_bounds[0])
        exporter.write_interval(self.v_bounds[1])

    @abc.abstractmethod
    def evaluate(self, u: float, v: float) -> Vec3:
        """Returns the spatial location at the parametric surface for the given
        parameters `u` and `v`.

        """
        pass


@register
class Plane(Surface):
    type: str = "plane-surface"
    origin = Vec3(0, 0, 0)
    normal = Vec3(0, 0, 1)  # pointing outside
    u_dir = Vec3(1, 0, 0)  # unit vector!
    v_dir = Vec3(0, 1, 0)  # unit vector!
    # reverse_v:
    # True: "reverse_v" - the normal vector does not follow the right-hand rule
    # False: "forward_v" - the normal vector follows right-hand rule
    reverse_v = False

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.origin = Vec3(loader.read_vec3())
        self.normal = Vec3(loader.read_vec3())
        self.u_dir = Vec3(loader.read_vec3())
        self.reverse_v = loader.read_bool("reverse_v", "forward_v")
        self.update_v_dir()

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_loc_vec3(self.origin)
        exporter.write_dir_vec3(self.normal)
        exporter.write_dir_vec3(self.u_dir)
        exporter.write_bool(self.reverse_v, "reverse_v", "forward_v")
        # v_dir is not exported

    def update_v_dir(self):
        v_dir = self.normal.cross(self.u_dir)
        if self.reverse_v:
            v_dir = -v_dir
        self.v_dir = v_dir

    def evaluate(self, u: float, v: float) -> Vec3:
        return self.origin + (self.u_dir * u) + (self.v_dir * v)


@register
class Loop(SupportsPattern):
    type: str = "loop"
    next_loop: Loop = NONE_REF
    coedge: Coedge = NONE_REF
    face: Face = NONE_REF  # parent/owner

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.next_loop = restore_entity("loop", loader, entity_factory)
        self.coedge = restore_entity("coedge", loader, entity_factory)
        self.face = restore_entity("face", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.next_loop)
        exporter.write_ptr(self.coedge)
        exporter.write_ptr(self.face)

    def set_coedges(self, coedges: list[Coedge], close=True) -> None:
        """Set all coedges of a loop at once."""
        assert len(coedges) > 0
        self.coedge = coedges[0]
        next_coedges = coedges[1:]
        prev_coedges = coedges[:-1]
        if close:
            next_coedges.append(coedges[0])
            prev_coedges.insert(0, coedges[-1])
        else:
            next_coedges.append(NONE_REF)
            prev_coedges.insert(0, NONE_REF)

        for coedge, next, prev in zip(coedges, next_coedges, prev_coedges):
            coedge.loop = self
            coedge.prev_coedge = prev
            coedge.next_coedge = next

    def coedges(self) -> list[Coedge]:
        """Returns all linked :class:`Coedge` entities as a list."""
        coedges = []

        current_coedge = self.coedge
        while not current_coedge.is_none:  # open loop if none
            coedges.append(current_coedge)
            current_coedge = current_coedge.next_coedge
            if current_coedge is self.coedge:  # circular linked list!
                break  # closed loop
        return coedges


@register
class Coedge(SupportsPattern):
    type: str = "coedge"
    next_coedge: Coedge = NONE_REF
    prev_coedge: Coedge = NONE_REF
    # The partner_coedge points to the coedge of an adjacent face, in a
    # manifold body each coedge has zero (open) or one (closed) partner edge.
    # ACIS supports also non-manifold bodies, so there can be more than one
    # partner coedges which are organized in a circular linked list.
    partner_coedge: Coedge = NONE_REF
    edge: Edge = NONE_REF
    # sense: True = reversed; False = forward;
    # coedge has the same direction as the underlying edge
    sense: bool = True
    loop: Loop = NONE_REF  # parent/owner
    unknown: int = 0  # only in SAB file!?
    pcurve: PCurve = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.next_coedge = restore_entity("coedge", loader, entity_factory)
        self.prev_coedge = restore_entity("coedge", loader, entity_factory)
        self.partner_coedge = restore_entity("coedge", loader, entity_factory)
        self.edge = restore_entity("edge", loader, entity_factory)
        self.sense = loader.read_bool("reversed", "forward")
        self.loop = restore_entity("loop", loader, entity_factory)
        self.unknown = loader.read_int(skip_sat=0)
        self.pcurve = restore_entity("pcurve", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.next_coedge)
        exporter.write_ptr(self.prev_coedge)
        exporter.write_ptr(self.partner_coedge)
        exporter.write_ptr(self.edge)
        exporter.write_bool(self.sense, "reversed", "forward")
        exporter.write_ptr(self.loop)
        # TODO: write_int() ?
        exporter.write_int(0, skip_sat=True)
        exporter.write_ptr(self.pcurve)

    def add_partner_coedge(self, coedge: Coedge) -> None:
        assert coedge.partner_coedge.is_none
        partner_coedge = self.partner_coedge
        if partner_coedge.is_none:
            partner_coedge = self
        # insert new coedge as first partner coedge:
        self.partner_coedge = coedge
        coedge.partner_coedge = partner_coedge
        self.order_partner_coedges()

    def order_partner_coedges(self) -> None:
        # todo: the referenced faces of non-manifold coedges have to be ordered
        #  by the right-hand rule around this edge.
        pass

    def partner_coedges(self) -> list[Coedge]:
        """Returns all partner coedges of this coedge without `self`."""
        coedges: list[Coedge] = []
        partner_coedge = self.partner_coedge
        if partner_coedge.is_none:
            return coedges
        while True:
            coedges.append(partner_coedge)
            partner_coedge = partner_coedge.partner_coedge
            if partner_coedge.is_none or partner_coedge is self:
                break
        return coedges


@register
class Edge(SupportsPattern):
    type: str = "edge"

    # The parent edge of the start_vertex doesn't have to be this edge!
    start_vertex: Vertex = NONE_REF
    start_param: float = 0.0

    # The parent edge of the end_vertex doesn't have to be this edge!
    end_vertex: Vertex = NONE_REF
    end_param: float = 0.0
    coedge: Coedge = NONE_REF
    curve: Curve = NONE_REF
    # sense: True = reversed; False = forward;
    # forward: edge has the same direction as the underlying curve
    sense: bool = False
    convexity: str = "unknown"

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.start_vertex = restore_entity("vertex", loader, entity_factory)
        if loader.version >= Features.TOL_MODELING:
            self.start_param = loader.read_double()
        self.end_vertex = restore_entity("vertex", loader, entity_factory)
        if loader.version >= Features.TOL_MODELING:
            self.end_param = loader.read_double()
        self.coedge = restore_entity("coedge", loader, entity_factory)
        self.curve = restore_entity("curve", loader, entity_factory)
        self.sense = loader.read_bool("reversed", "forward")
        if loader.version >= Features.TOL_MODELING:
            self.convexity = loader.read_str()

    def write_common(self, exporter: DataExporter) -> None:
        # write support >= version 700 only
        super().write_common(exporter)
        exporter.write_ptr(self.start_vertex)
        exporter.write_double(self.start_param)
        exporter.write_ptr(self.end_vertex)
        exporter.write_double(self.end_param)
        exporter.write_ptr(self.coedge)
        exporter.write_ptr(self.curve)
        exporter.write_bool(self.sense, "reversed", "forward")
        exporter.write_str(self.convexity)


@register
class PCurve(SupportsPattern):  # not implemented
    type: str = "pcurve"


@register
class Vertex(SupportsPattern):
    type: str = "vertex"
    edge: Edge = NONE_REF
    ref_count: int = 0  # only in SAB files
    point: Point = NONE_REF

    def restore_common(self, loader: DataLoader, entity_factory: Factory) -> None:
        super().restore_common(loader, entity_factory)
        self.edge = restore_entity("edge", loader, entity_factory)
        self.ref_count = loader.read_int(skip_sat=0)
        self.point = restore_entity("point", loader, entity_factory)

    def write_common(self, exporter: DataExporter) -> None:
        super().write_common(exporter)
        exporter.write_ptr(self.edge)
        exporter.write_int(self.ref_count, skip_sat=True)
        exporter.write_ptr(self.point)


@register
class Curve(SupportsPattern):
    type: str = "curve"
    bounds = INF, INF

    def restore_data(self, loader: DataLoader) -> None:
        self.bounds = loader.read_interval(), loader.read_interval()

    def write_data(self, exporter: DataExporter) -> None:
        exporter.write_interval(self.bounds[0])
        exporter.write_interval(self.bounds[1])

    @abc.abstractmethod
    def evaluate(self, param: float) -> Vec3:
        """Returns the spatial location at the parametric curve for the given
        parameter.

        """
        pass


@register
class StraightCurve(Curve):
    type: str = "straight-curve"
    origin = Vec3(0, 0, 0)
    direction = Vec3(1, 0, 0)

    def restore_data(self, loader: DataLoader) -> None:
        self.origin = Vec3(loader.read_vec3())
        self.direction = Vec3(loader.read_vec3())
        super().restore_data(loader)

    def write_data(self, exporter: DataExporter) -> None:
        exporter.write_loc_vec3(self.origin)
        exporter.write_dir_vec3(self.direction)
        super().write_data(exporter)

    def evaluate(self, param: float) -> Vec3:
        return self.origin + (self.direction * param)


@register
class Point(SupportsPattern):
    type: str = "point"
    location: Vec3 = NULLVEC

    def restore_data(self, loader: DataLoader) -> None:
        self.location = Vec3(loader.read_vec3())

    def write_data(self, exporter: DataExporter) -> None:
        exporter.write_loc_vec3(self.location)


class FileLoader(abc.ABC):
    records: Sequence[sat.SatEntity | sab.SabEntity]

    def __init__(self, version: int):
        self.entities: dict[int, AcisEntity] = {}
        self.version: int = version

    def entity_factory(self, raw_entity: AbstractEntity) -> AcisEntity:
        uid = id(raw_entity)
        try:
            return self.entities[uid]
        except KeyError:  # create a new entity
            entity = ENTITY_TYPES.get(raw_entity.name, AcisEntity)()
            self.entities[uid] = entity
            return entity

    def bodies(self) -> list[Body]:
        # noinspection PyTypeChecker
        return [e for e in self.entities.values() if isinstance(e, Body)]

    def load_entities(self):
        entity_factory = self.entity_factory

        for raw_entity in self.records:
            entity = entity_factory(raw_entity)
            entity.id = raw_entity.id
            attributes = raw_entity.attributes
            if not attributes.is_null_ptr:
                entity.attributes = entity_factory(attributes)
            data_loader = self.make_data_loader(raw_entity.data)
            entity.load(data_loader, entity_factory)

    @abc.abstractmethod
    def make_data_loader(self, data: list[Any]) -> DataLoader:
        pass


class SabLoader(FileLoader):
    def __init__(self, data: bytes | bytearray):
        builder = sab.parse_sab(data)
        super().__init__(builder.header.version)
        self.records = builder.entities

    def make_data_loader(self, data: list[Any]) -> DataLoader:
        return sab.SabDataLoader(data, self.version)

    @classmethod
    def load(cls, data: bytes | bytearray) -> list[Body]:
        loader = cls(data)
        loader.load_entities()
        return loader.bodies()


class SatLoader(FileLoader):
    def __init__(self, data: str | Sequence[str]):
        builder = sat.parse_sat(data)
        super().__init__(builder.header.version)
        self.records = builder.entities

    def make_data_loader(self, data: list[Any]) -> DataLoader:
        return sat.SatDataLoader(data, self.version)

    @classmethod
    def load(cls, data: str | Sequence[str]) -> list[Body]:
        loader = cls(data)
        loader.load_entities()
        return loader.bodies()
