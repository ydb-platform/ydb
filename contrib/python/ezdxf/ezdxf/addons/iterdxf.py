# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    cast,
    BinaryIO,
    Optional,
    Union,
    Any,
)
from io import StringIO
from pathlib import Path
from ezdxf.lldxf.const import DXFStructureError
from ezdxf.lldxf.extendedtags import ExtendedTags, DXFTag
from ezdxf.lldxf.tagwriter import TagWriter
from ezdxf.lldxf.tagger import tag_compiler, ascii_tags_loader
from ezdxf.filemanagement import dxf_file_info
from ezdxf.lldxf import fileindex

from ezdxf.entities import DXFGraphic, DXFEntity, Polyline, Insert
from ezdxf.entities import factory
from ezdxf.entities.subentity import entity_linker
from ezdxf.tools.codepage import toencoding

__all__ = ["opendxf", "single_pass_modelspace", "modelspace"]

SUPPORTED_TYPES = {
    "ARC",
    "LINE",
    "CIRCLE",
    "ELLIPSE",
    "POINT",
    "LWPOLYLINE",
    "SPLINE",
    "3DFACE",
    "SOLID",
    "TRACE",
    "SHAPE",
    "POLYLINE",
    "VERTEX",
    "SEQEND",
    "MESH",
    "TEXT",
    "MTEXT",
    "HATCH",
    "INSERT",
    "ATTRIB",
    "ATTDEF",
    "RAY",
    "XLINE",
    "DIMENSION",
    "LEADER",
    "IMAGE",
    "WIPEOUT",
    "HELIX",
    "MLINE",
    "MLEADER",
}

Filename = Union[Path, str]


class IterDXF:
    """Iterator for DXF entities stored in the modelspace.

    Args:
        name: filename, has to be a seekable file.
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError`exception  for invalid data

    Raises:
        DXFStructureError: invalid or incomplete DXF file
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """

    def __init__(self, name: Filename, errors: str = "surrogateescape"):
        self.structure, self.sections = self._load_index(str(name))
        self.errors = errors
        self.file: BinaryIO = open(name, mode="rb")
        if "ENTITIES" not in self.sections:
            raise DXFStructureError("ENTITIES section not found.")
        if self.structure.version > "AC1009" and "OBJECTS" not in self.sections:
            raise DXFStructureError("OBJECTS section not found.")

    def _load_index(
        self, name: str
    ) -> tuple[fileindex.FileStructure, dict[str, int]]:
        structure = fileindex.load(name)
        sections: dict[str, int] = dict()
        new_index = []
        for e in structure.index:
            if e.code == 0:
                new_index.append(e)
            elif e.code == 2:
                sections[e.value] = len(new_index) - 1
            # remove all other tags like handles (code == 5)
        structure.index = new_index
        return structure, sections

    @property
    def encoding(self):
        return self.structure.encoding

    @property
    def dxfversion(self):
        return self.structure.version

    def export(self, name: Filename) -> IterDXFWriter:
        """Returns a companion object to export parts from the source DXF file
        into another DXF file, the new file will have the same HEADER, CLASSES,
        TABLES, BLOCKS and OBJECTS sections, which guarantees all necessary
        dependencies are present in the new file.

        Args:
            name: filename, no special requirements

        """
        doc = IterDXFWriter(name, self)
        # Copy everything from start of source DXF until the first entity
        # of the ENTITIES section to the new DXF.
        location = self.structure.index[self.sections["ENTITIES"] + 1].location
        self.file.seek(0)
        data = self.file.read(location)
        doc.write_data(data)
        return doc

    def copy_objects_section(self, f: BinaryIO) -> None:
        start_index = self.sections["OBJECTS"]
        try:
            end_index = self.structure.get(0, "ENDSEC", start_index)
        except ValueError:
            raise DXFStructureError(f"ENDSEC of OBJECTS section not found.")

        start_location = self.structure.index[start_index].location
        end_location = self.structure.index[end_index + 1].location
        count = end_location - start_location
        self.file.seek(start_location)
        data = self.file.read(count)
        f.write(data)

    def modelspace(
        self, types: Optional[Iterable[str]] = None
    ) -> Iterable[DXFGraphic]:
        """Returns an iterator for all supported DXF entities in the
        modelspace. These entities are regular :class:`~ezdxf.entities.DXFGraphic`
        objects but without a valid document assigned. It is **not**
        possible to add these entities to other `ezdxf` documents.

        It is only possible to recreate the objects by factory functions base
        on attributes of the source entity.
        For MESH, POLYMESH and POLYFACE it is possible to use the
        :class:`~ezdxf.render.MeshTransformer` class to render (recreate) this
        objects as new entities in another document.

        Args:
            types: DXF types like ``['LINE', '3DFACE']`` which should be
                returned, ``None`` returns all supported types.

        """
        linked_entity = entity_linker()
        queued = None
        requested_types = _requested_types(types)
        for entity in self.load_entities(
            self.sections["ENTITIES"] + 1, requested_types
        ):
            if not linked_entity(entity) and entity.dxf.paperspace == 0:
                # queue one entity for collecting linked entities:
                # VERTEX, ATTRIB
                if queued:
                    yield queued
                queued = entity
        if queued:
            yield queued

    def load_entities(
        self, start: int, requested_types: set[str]
    ) -> Iterable[DXFGraphic]:
        def to_str(data: bytes) -> str:
            return data.decode(self.encoding, errors=self.errors).replace(
                "\r\n", "\n"
            )

        index = start
        entry = self.structure.index[index]
        self.file.seek(entry.location)
        while entry.value != "ENDSEC":
            index += 1
            next_entry = self.structure.index[index]
            size = next_entry.location - entry.location
            data = self.file.read(size)
            if entry.value in requested_types:
                xtags = ExtendedTags.from_text(to_str(data))
                yield factory.load(xtags)  # type: ignore
            entry = next_entry

    def close(self):
        """Safe closing source DXF file."""
        self.file.close()


class IterDXFWriter:
    def __init__(self, name: Filename, loader: IterDXF):
        self.name = str(name)
        self.file: BinaryIO = open(name, mode="wb")
        self.text = StringIO()
        self.entity_writer = TagWriter(self.text, loader.dxfversion)
        self.loader = loader

    def write_data(self, data: bytes):
        self.file.write(data)

    def write(self, entity: DXFGraphic):
        """Write a DXF entity from the source DXF file to the export file.

        Don't write entities from different documents than the source DXF file,
        dependencies and resources will not match, maybe it will work once, but
        not in a reliable way for different DXF documents.

        """
        # Not necessary to remove this dependencies by copying
        # them into the same document frame
        # ---------------------------------
        # remove all possible dependencies
        # entity.xdata = None
        # entity.appdata = None
        # entity.extension_dict = None
        # entity.reactors = None
        # reset text stream
        self.text.seek(0)
        self.text.truncate()

        if entity.dxf.handle is None:  # DXF R12 without handles
            self.entity_writer.write_handles = False

        entity.export_dxf(self.entity_writer)
        if entity.dxftype() == "POLYLINE":
            polyline = cast(Polyline, entity)
            for vertex in polyline.vertices:
                vertex.export_dxf(self.entity_writer)
            polyline.seqend.export_dxf(self.entity_writer)  # type: ignore
        elif entity.dxftype() == "INSERT":
            insert = cast(Insert, entity)
            if insert.attribs_follow:
                for attrib in insert.attribs:
                    attrib.export_dxf(self.entity_writer)
                insert.seqend.export_dxf(self.entity_writer)  # type: ignore
        data = self.text.getvalue().encode(self.loader.encoding)
        self.file.write(data)

    def close(self):
        """Safe closing of exported DXF file. Copying of OBJECTS section
        happens only at closing the file, without closing the new DXF file is
        invalid.
        """
        self.file.write(b"  0\r\nENDSEC\r\n")  # for ENTITIES section
        if self.loader.dxfversion > "AC1009":
            self.loader.copy_objects_section(self.file)
        self.file.write(b"  0\r\nEOF\r\n")
        self.file.close()


def opendxf(filename: Filename, errors: str = "surrogateescape") -> IterDXF:
    """Open DXF file for iterating, be sure to open valid DXF files, no DXF
    structure checks will be applied.

    Use this function to split up big DXF files as shown in the example above.

    Args:
        filename: DXF filename of a seekable DXF file.
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: invalid or incomplete DXF file
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    return IterDXF(filename, errors=errors)


def modelspace(
    filename: Filename,
    types: Optional[Iterable[str]] = None,
    errors: str = "surrogateescape",
) -> Iterable[DXFGraphic]:
    """Iterate over all modelspace entities as :class:`DXFGraphic` objects of
    a seekable file.

    Use this function to iterate "quick" over modelspace entities of a DXF file,
    filtering DXF types may speed up things if many entity types will be skipped.

    Args:
        filename: filename of a seekable DXF file
        types: DXF types like ``['LINE', '3DFACE']`` which should be returned,
            ``None`` returns all supported types.
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: invalid or incomplete DXF file
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    info = dxf_file_info(str(filename))
    prev_code: int = -1
    prev_value: Any = ""
    entities = False
    requested_types = _requested_types(types)

    with open(filename, mode="rt", encoding=info.encoding, errors=errors) as fp:
        tagger = ascii_tags_loader(fp)
        queued: Optional[DXFEntity] = None
        tags: list[DXFTag] = []
        linked_entity = entity_linker()

        for tag in tag_compiler(tagger):
            code = tag.code
            value = tag.value
            if entities:
                if code == 0:
                    if len(tags) and tags[0].value in requested_types:
                        entity = factory.load(ExtendedTags(tags))
                        if (
                            not linked_entity(entity)
                            and entity.dxf.paperspace == 0
                        ):
                            # queue one entity for collecting linked entities:
                            # VERTEX, ATTRIB
                            if queued:
                                yield queued  # type: ignore
                            queued = entity
                    tags = [tag]
                else:
                    tags.append(tag)
                if code == 0 and value == "ENDSEC":
                    if queued:
                        yield queued  # type: ignore
                    return
                continue  # if entities - nothing else matters
            elif code == 2 and prev_code == 0 and prev_value == "SECTION":
                entities = value == "ENTITIES"

            prev_code = code
            prev_value = value


def single_pass_modelspace(
    stream: BinaryIO,
    types: Optional[Iterable[str]] = None,
    errors: str = "surrogateescape",
) -> Iterable[DXFGraphic]:
    """Iterate over all modelspace entities as :class:`DXFGraphic` objects in
    a single pass.

    Use this function to 'quick' iterate over modelspace entities of a **not**
    seekable binary DXF stream, filtering DXF types may speed up things if many
    entity types will be skipped.

    Args:
        stream: (not seekable) binary DXF stream
        types: DXF types like ``['LINE', '3DFACE']`` which should be returned,
            ``None`` returns all supported types.
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: Invalid or incomplete DXF file
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    fetch_header_var: Optional[str] = None
    encoding = "cp1252"
    version = "AC1009"
    prev_code: int = -1
    prev_value: str = ""
    entities = False
    requested_types = _requested_types(types)

    for code, value in binary_tagger(stream):
        if code == 0 and value == b"ENDSEC":
            break
        elif code == 2 and prev_code == 0 and value != b"HEADER":
            # (0, SECTION), (2, name)
            # First section is not the HEADER section
            entities = value == b"ENTITIES"
            break
        elif code == 9 and value == b"$DWGCODEPAGE":
            fetch_header_var = "ENCODING"
        elif code == 9 and value == b"$ACADVER":
            fetch_header_var = "VERSION"
        elif fetch_header_var == "ENCODING":
            encoding = toencoding(value.decode())
            fetch_header_var = None
        elif fetch_header_var == "VERSION":
            version = value.decode()
            fetch_header_var = None
        prev_code = code

    if version >= "AC1021":
        encoding = "utf-8"

    queued: Optional[DXFGraphic] = None
    tags: list[DXFTag] = []
    linked_entity = entity_linker()

    for tag in tag_compiler(binary_tagger(stream, encoding, errors)):
        code = tag.code
        value = tag.value
        if entities:
            if code == 0 and value == "ENDSEC":
                if queued:
                    yield queued
                return
            if code == 0:
                if len(tags) and tags[0].value in requested_types:
                    entity = cast(DXFGraphic, factory.load(ExtendedTags(tags)))
                    if not linked_entity(entity) and entity.dxf.paperspace == 0:
                        # queue one entity for collecting linked entities:
                        # VERTEX, ATTRIB
                        if queued:
                            yield queued
                        queued = entity
                tags = [tag]
            else:
                tags.append(tag)
            continue  # if entities - nothing else matters
        elif code == 2 and prev_code == 0 and prev_value == "SECTION":
            entities = value == "ENTITIES"

        prev_code = code
        prev_value = value


def binary_tagger(
    file: BinaryIO,
    encoding: Optional[str] = None,
    errors: str = "surrogateescape",
) -> Iterator[DXFTag]:
    while True:
        try:
            try:
                code = int(file.readline())
            except ValueError:
                raise DXFStructureError(f"Invalid group code")
            value = file.readline().rstrip(b"\r\n")
            yield DXFTag(
                code,
                value.decode(encoding, errors=errors) if encoding else value,
            )
        except IOError:
            return


def _requested_types(types: Optional[Iterable[str]]) -> set[str]:
    if types:
        requested = SUPPORTED_TYPES.intersection(set(types))
        if "POLYLINE" in requested:
            requested.add("SEQEND")
            requested.add("VERTEX")
        if "INSERT" in requested:
            requested.add("SEQEND")
            requested.add("ATTRIB")
    else:
        requested = SUPPORTED_TYPES
    return requested
