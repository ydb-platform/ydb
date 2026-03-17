#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, Callable, Any
from .entities import (
    AcisEntity,
    NONE_REF,
    Face,
    Coedge,
    Loop,
    Vertex,
)
from . import sab


class AcisDebugger:
    def __init__(self, root: AcisEntity = NONE_REF, start_id: int = 1):
        self._next_id = start_id - 1
        self._root: AcisEntity = root
        self.entities: dict[int, AcisEntity] = dict()
        if not root.is_none:
            self._store_entities(root)

    @property
    def root(self) -> AcisEntity:
        return self._root

    def _get_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _store_entities(self, entity: AcisEntity) -> None:
        if not entity.is_none and entity.id == -1:
            entity.id = self._get_id()
        self.entities[entity.id] = entity
        for e in vars(entity).values():
            if isinstance(e, AcisEntity) and e.id == -1:
                self._store_entities(e)

    def set_entities(self, entity: AcisEntity) -> None:
        self.entities.clear()
        self._root = entity
        self._store_entities(entity)

    def walk(self, root: AcisEntity = NONE_REF) -> Iterator[AcisEntity]:
        def _walk(entity: AcisEntity):
            if entity.is_none:
                return
            yield entity
            done.add(entity.id)
            for e in vars(entity).values():
                if isinstance(e, AcisEntity) and e.id not in done:
                    yield from _walk(e)

        if root.is_none:
            root = self._root
        done: set[int] = set()
        yield from _walk(root)

    def filter(
        self, func: Callable[[AcisEntity], bool], entity: AcisEntity = NONE_REF
    ) -> Iterator[Any]:
        if entity.is_none:
            entity = self._root
        yield from filter(func, self.walk(entity))

    def filter_type(
        self, name: str, entity: AcisEntity = NONE_REF
    ) -> Iterator[Any]:
        if entity.is_none:
            entity = self._root
        yield from filter(lambda x: x.type == name, self.walk(entity))

    @staticmethod
    def entity_attributes(entity: AcisEntity, indent: int = 0) -> Iterator[str]:
        indent_str = " " * indent
        for name, data in vars(entity).items():
            if name == "id":
                continue
            yield f"{indent_str}{name}: {data}"

    def face_link_structure(self, face: Face, indent: int = 0) -> Iterator[str]:
        indent_str = " " * indent

        while not face.is_none:
            partner_faces = list(self.partner_faces(face))
            error = ""
            linked_partner_faces = []
            unlinked_partner_faces = []
            for pface_id in partner_faces:
                pface = self.entities.get(pface_id)
                if pface is None:
                    error += f" face {pface_id} does not exist;"
                if isinstance(pface, Face):
                    reverse_faces = self.partner_faces(pface)
                    if face.id in reverse_faces:
                        linked_partner_faces.append(pface_id)
                    else:
                        unlinked_partner_faces.append(pface_id)
                else:
                    error += f" entity {pface_id} is not a face;"
            if unlinked_partner_faces:
                error = f"unlinked partner faces: {unlinked_partner_faces} {error}"
            yield f"{indent_str}{str(face)} >> {partner_faces} {error}"
            face = face.next_face

    @staticmethod
    def partner_faces(face: Face) -> Iterator[int]:
        coedges: list[Coedge] = []
        loop = face.loop
        while not loop.is_none:
            coedges.extend(co for co in loop.coedges())
            loop = loop.next_loop
        for coedge in coedges:
            for partner_coedge in coedge.partner_coedges():
                yield partner_coedge.loop.face.id

    @staticmethod
    def coedge_structure(face: Face, ident: int = 4) -> list[str]:
        lines: list[str] = []
        coedges: list[Coedge] = []
        loop = face.loop

        while not loop.is_none:
            coedges.extend(co for co in loop.coedges())
            loop = loop.next_loop
        for coedge in coedges:
            edge1 = coedge.edge
            sense1 = coedge.sense
            lines.append(f"Coedge={coedge.id} edge={edge1.id} sense={sense1}")
            for partner_coedge in coedge.partner_coedges():
                edge2 = partner_coedge.edge
                sense2 = partner_coedge.sense
                lines.append(
                    f"    Partner Coedge={partner_coedge.id} edge={edge2.id} sense={sense2}"
                )
        ident_str = " " * ident
        return [ident_str + line for line in lines]

    @staticmethod
    def loop_vertices(loop: Loop, indent: int = 0) -> str:
        indent_str = " " * indent
        return f"{indent_str}{loop} >> {list(AcisDebugger.loop_edges(loop))}"

    @staticmethod
    def loop_edges(loop: Loop) -> Iterator[list[int]]:
        coedge = loop.coedge
        first = coedge
        while not coedge.is_none:
            edge = coedge.edge
            sv = edge.start_vertex
            ev = edge.end_vertex
            if coedge.sense:
                yield [ev.id, sv.id]
            else:
                yield [sv.id, ev.id]
            coedge = coedge.next_coedge
            if coedge is first:
                break

    def vertex_to_edge_relation(self) -> Iterator[str]:
        for vertex in (
            e for e in self.entities.values() if isinstance(e, Vertex)
        ):
            edge = vertex.edge
            sv = edge.start_vertex
            ev = edge.end_vertex
            yield f"{vertex}: parent edge is {edge.id}; {sv.id} => {ev.id}; {edge.curve}"

    def is_manifold(self) -> bool:
        for coedge in self.filter_type("coedge"):
            if len(coedge.partner_coedges()) > 1:
                return False
        return True


def dump_sab_as_text(data: bytes) -> Iterator[str]:
    def entity_data(e):
        for tag, value in e:
            name = sab.Tags(tag).name
            yield f"{name} = {value}"

    decoder = sab.Decoder(data)
    header = decoder.read_header()
    yield from header.dumps()
    index = 0
    try:
        for record in decoder.read_records():
            yield f"--------------------- record: {index}"
            yield from entity_data(record)
            index += 1
    except sab.ParsingError as e:
        yield str(e)
