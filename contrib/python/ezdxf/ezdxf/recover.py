#  Copyright (c) 2020-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations

import string
import typing
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Iterable,
    Iterator,
    Callable,
    Union,
    Optional,
)
import itertools
import re
from collections import defaultdict
from pathlib import Path
import logging

from ezdxf.lldxf import const
from ezdxf.lldxf import repair
from ezdxf.lldxf.encoding import (
    has_dxf_unicode,
    decode_dxf_unicode,
    has_mif_encoding,
    decode_mif_to_unicode,
)
from ezdxf.lldxf.types import (
    DXFTag,
    DXFVertex,
    DXFBinaryTag,
    POINT_CODES,
    BINARY_DATA,
    TYPE_TABLE,
    MAX_GROUP_CODE,
)
from ezdxf.lldxf.tags import group_tags, Tags
from ezdxf.lldxf.validator import entity_structure_validator
from ezdxf.tools.codepage import toencoding
from ezdxf.audit import Auditor, AuditError

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.eztypes import SectionDict

__all__ = ["read", "readfile"]

EXCLUDE_STRUCTURE_CHECK = {
    "SECTION",
    "ENDSEC",
    "EOF",
    "TABLE",
    "ENDTAB",
    "ENDBLK",
    "SEQEND",
}
logger = logging.getLogger("ezdxf")


def readfile(
    filename: Union[str, Path], errors: str = "surrogateescape"
) -> tuple[Drawing, Auditor]:
    """Read a DXF document from file system similar to :func:`ezdxf.readfile`,
    but this function will repair as many flaws as possible, runs the required
    audit process automatically the DXF document and the :class:`Auditor`.

    Args:
        filename: file-system name of the DXF document to load
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    filename = str(filename)
    with open(filename, mode="rb") as fp:
        doc, auditor = read(fp, errors=errors)
    doc.filename = filename
    return doc, auditor


def read(stream: BinaryIO, errors: str = "surrogateescape") -> tuple[Drawing, Auditor]:
    """Read a DXF document from a binary-stream similar to :func:`ezdxf.read`,
    but this function will detect the text encoding automatically and repair
    as many flaws as possible, runs the required audit process afterwards
    and returns the DXF document and the :class:`Auditor`.

    Args:
        stream: data stream to load in binary read mode
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    recover_tool = Recover.run(stream, errors=errors)
    return _load_and_audit_document(recover_tool)


def explore(
    filename: Union[str, Path], errors: str = "ignore"
) -> tuple[Drawing, Auditor]:
    """Read a DXF document from file system similar to :func:`readfile`,
    but this function will use a special tag loader, which tries to recover the
    tag stream if invalid tags occur.  This function is intended to load
    corrupted DXF files and should only be used to explore such files, data loss
    is very likely.

    Args:
        filename: file-system name of the DXF document to load
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    filename = str(filename)
    with open(filename, mode="rb") as fp:
        recover_tool = Recover.run(fp, errors=errors, loader=synced_bytes_loader)
        doc, auditor = _load_and_audit_document(recover_tool)
    doc.filename = filename
    return doc, auditor


def _load_and_audit_document(recover_tool) -> tuple[Drawing, Auditor]:
    from ezdxf.document import Drawing

    doc = Drawing()
    doc._load_section_dict(recover_tool.section_dict)

    auditor = Auditor(doc)
    for code, msg in recover_tool.errors:
        auditor.add_error(code, msg)
    for code, msg in recover_tool.fixes:
        auditor.fixed_error(code, msg)
    auditor.run()
    return doc, auditor


# noinspection PyMethodMayBeStatic
class Recover:
    """Loose coupled recovering tools."""

    def __init__(self, loader: Optional[Callable] = None):
        # different tag loading strategies can be used:
        #  - bytes_loader(): expects a valid low level structure
        #  - synced_bytes_loader(): loads everything which looks like a tag
        #    and skip other content (dangerous!)
        self.tag_loader = loader or bytes_loader

        # The main goal of all efforts, a Drawing compatible dict of sections:
        self.section_dict: "SectionDict" = dict()

        # Store error messages from low level processes
        self.errors: list[tuple[int, str]] = []
        self.fixes: list[tuple[int, str]] = []

        # Detected DXF version
        self.dxfversion = const.DXF12

    @classmethod
    def run(
        cls,
        stream: BinaryIO,
        loader: Optional[Callable] = None,
        errors: str = "surrogateescape",
    ) -> Recover:
        """Execute the recover process."""
        recover_tool = Recover(loader)
        tags = recover_tool.load_tags(stream, errors)
        sections = recover_tool.rebuild_sections(tags)
        recover_tool.load_section_dict(sections)
        tables = recover_tool.section_dict.get("TABLES")
        if tables:
            tables = recover_tool.rebuild_tables(tables)  # type: ignore
            recover_tool.section_dict["TABLES"] = tables
        if recover_tool.dxfversion > "AC1009":
            recover_tool.recover_rootdict()
            recover_tool.fix_broken_layout_links()
        section_dict = recover_tool.section_dict

        is_r12 = recover_tool.dxfversion <= "AC1009"
        for name, entities in section_dict.items():
            if name in {"TABLES", "BLOCKS", "OBJECTS", "ENTITIES"}:
                section_dict[name] = list(
                    recover_tool.check_entities(entities, is_r12)  # type: ignore
                )

        return recover_tool

    def load_tags(self, stream: BinaryIO, errors: str) -> Iterator[DXFTag]:
        return safe_tag_loader(
            stream, self.tag_loader, messages=self.errors, errors=errors
        )

    def rebuild_sections(self, tags: Iterable[DXFTag]) -> list[list[DXFTag]]:
        """Collect tags between SECTION and ENDSEC or next SECTION tag
        as list of DXFTag objects, collects tags outside of sections
        as an extra section.

        Returns:
            List of sections as list of DXFTag() objects, the last section
            contains orphaned tags found outside of sections

        """

        # Invalid placed DXF entities are removed in the audit process!
        def close_section():
            # ENDSEC tag is not collected
            nonlocal collector, inside_section
            if inside_section:
                sections.append(collector)
            else:  # missing SECTION
                # ignore this tag, it is even not an orphan
                self.fixes.append(
                    (
                        AuditError.MISSING_SECTION_TAG,
                        "DXF structure error: missing SECTION tag.",
                    )
                )
            collector = []
            inside_section = False

        def open_section():
            nonlocal inside_section
            if inside_section:  # missing ENDSEC
                self.fixes.append(
                    (
                        AuditError.MISSING_ENDSEC_TAG,
                        "DXF structure error: missing ENDSEC tag.",
                    )
                )
                close_section()
            collector.append(tag)
            inside_section = True

        def process_structure_tag():
            if value == "SECTION":
                open_section()
            elif value == "ENDSEC":
                close_section()
            elif value == "EOF":
                if inside_section:
                    self.fixes.append(
                        (
                            AuditError.MISSING_ENDSEC_TAG,
                            "DXF structure error: missing ENDSEC tag.",
                        )
                    )
                    close_section()
            else:
                collect()

        def collect():
            if inside_section:
                collector.append(tag)
            else:
                self.fixes.append(
                    (
                        AuditError.FOUND_TAG_OUTSIDE_SECTION,
                        f"DXF structure error: found tag outside section: "
                        f"({code}, {value})",
                    )
                )
                orphans.append(tag)

        orphans: list[DXFTag] = []
        sections: list[list[DXFTag]] = []
        collector: list[DXFTag] = []
        inside_section = False
        for tag in tags:
            code, value = tag
            if code == 0:
                process_structure_tag()
            else:
                collect()

        sections.append(orphans)
        return sections

    def load_section_dict(self, sections: list[list[DXFTag]]) -> None:
        """Merge sections of same type."""

        def add_section(name: str, tags) -> None:
            if name in section_dict:
                section_dict[name].extend(tags[2:])
            else:
                section_dict[name] = tags

        def _build_section_dict(d: dict) -> None:
            for name, section in d.items():
                if name in const.MANAGED_SECTIONS:
                    self.section_dict[name] = list(group_tags(section, 0))

        def _remove_unsupported_sections(d: dict):
            for name in ("CLASSES", "OBJECTS", "ACDSDATA"):
                if name in d:
                    del d[name]
                    self.fixes.append(
                        (
                            AuditError.REMOVED_UNSUPPORTED_SECTION,
                            f"Removed unsupported {name} section for DXF R12.",
                        )
                    )

        # Last section could be orphaned tags:
        orphans = sections.pop()
        if orphans and orphans[0] == (0, "SECTION"):
            # The last section contains not the orphaned tags:
            sections.append(orphans)
            orphans = []

        section_dict: "SectionDict" = dict()
        for section in sections:
            code, name = section[1]
            if code == 2:
                add_section(name, section)
            else:  # invalid section name tag e.g. (2, "HEADER")
                self.fixes.append(
                    (
                        AuditError.MISSING_SECTION_NAME_TAG,
                        "DXF structure error: missing section name tag, ignore section.",
                    )
                )

        header = section_dict.setdefault(
            "HEADER",
            [
                DXFTag(0, "SECTION"),  # type: ignore
                DXFTag(2, "HEADER"),  # type: ignore
            ],
        )
        self.rescue_orphaned_header_vars(header, orphans)  # type: ignore
        self.dxfversion = _detect_dxf_version(header)
        if self.dxfversion <= const.DXF12:
            _remove_unsupported_sections(section_dict)
        _build_section_dict(section_dict)

    def rebuild_tables(self, tables: list[Tags]) -> list[Tags]:
        """Rebuild TABLES section."""

        # Note: the recover module does not report invalid placed table entries,
        # it just recovers them. The "normal" loading process ignore these
        # misplaced table entries and logs a warning.

        def append_table(name: str):
            if name not in content:
                return

            head = heads.get(name)
            if head:
                tables.append(head)
            else:
                # The new table head gets a valid handle from Auditor.
                tables.append(Tags([DXFTag(0, "TABLE"), DXFTag(2, name)]))
            tables.extend(content[name])
            tables.append(Tags([DXFTag(0, "ENDTAB")]))

        heads = dict()
        content = defaultdict(list)
        valid_tables = set(const.TABLE_NAMES_ACAD_ORDER)

        for entry in tables:
            name = entry[0].value.upper()
            if name == "TABLE":
                try:
                    table_name = entry[1].value.upper()
                except (IndexError, AttributeError):
                    pass
                else:
                    heads[table_name] = entry
            elif name in valid_tables:
                content[name].append(entry)
        tables = [Tags([DXFTag(0, "SECTION"), DXFTag(2, "TABLES")])]

        names = list(const.TABLE_NAMES_ACAD_ORDER)
        if self.dxfversion <= const.DXF12:
            # Ignore BLOCK_RECORD table
            names.remove("BLOCK_RECORD")
            if "BLOCK_RECORD" in content:
                self.fixes.append(
                    (
                        AuditError.REMOVED_UNSUPPORTED_TABLE,
                        f"Removed unsupported BLOCK_RECORD table for DXF R12.",
                    )
                )

        for name in names:
            append_table(name)
        return tables

    def rescue_orphaned_header_vars(
        self, header: list[DXFTag], orphans: Iterable[DXFTag]
    ) -> None:
        var_name = None
        for tag in orphans:
            code, value = tag
            if code == 9:
                var_name = tag
            elif var_name is not None:
                header.append(var_name)
                header.append(tag)
                var_name = None

    def check_entities(self, entities: list[Tags], is_r12: bool) -> Iterator[Tags]:
        subclass_markers = (100,)
        for entity in entities:
            _, dxftype = entity[0]
            if dxftype in EXCLUDE_STRUCTURE_CHECK:
                yield entity
            else:
                # raises DXFStructureError() for invalid entities
                tags = Tags(entity_structure_validator(entity))
                if is_r12:
                    # subclass markers (100, ...) in DXF R12 files confuses the 
                    # ezdxf parser #1106
                    tags.remove_tags(subclass_markers)
                yield tags

    def recover_rootdict(self):
        objects = self.section_dict.get("OBJECTS")
        if not objects or len(objects) < 2:
            return  # empty OBJECTS section
        # index 0 is [DXFTag(0, 'SECTION'), DXFTag(2, 'OBJECTS')], this is a
        # requirement to be stored in the section_dict!
        if _is_rootdict(objects[1]):
            return  # everything is fine
        index, rootdict = _find_rootdict(objects)
        if index:  # make rootdict to first entity in OBJECTS section
            objects[index] = objects[1]
            objects[1] = rootdict
            try:
                handle = rootdict.get_handle()
            except const.DXFValueError:
                handle = "None"
            self.fixes.append(
                (
                    AuditError.MISPLACED_ROOT_DICT,
                    f"Recovered misplaced root DICTIONARY(#{handle}).",
                )
            )

    def fix_broken_layout_links(self):
        """Fixes broke links (block_record_handle) between LAYOUT and BLOCK_RECORD
        entities. See issue #997 for more information.
        """
        pass


def _detect_dxf_version(header: list) -> str:
    next_is_dxf_version = False
    for tag in header:
        if next_is_dxf_version:
            dxfversion = str(tag[1]).strip()
            if re.fullmatch(r"AC[0-9]{4}", dxfversion):
                return dxfversion
            else:
                break
        if tag == (9, "$ACADVER"):
            next_is_dxf_version = True
    return const.DXF12


def _is_rootdict(entity: Tags) -> bool:
    if entity[0] != (0, "DICTIONARY"):
        return False
    # The entry "ACAD_GROUP" in the rootdict is absolutely necessary!
    return any(tag == (3, "ACAD_GROUP") for tag in entity)


def _find_rootdict(objects: list[Tags]) -> tuple[int, Tags]:
    for index, entity in enumerate(objects):
        if _is_rootdict(entity):
            return index, entity
    return 0, Tags()


def safe_tag_loader(
    stream: BinaryIO,
    loader: Optional[Callable] = None,
    messages: Optional[list] = None,
    errors: str = "surrogateescape",
) -> Iterator[DXFTag]:
    """Yields :class:``DXFTag`` objects from a bytes `stream`
    (untrusted external  source), skips all comment tags (group code == 999).

    - Fixes unordered and invalid vertex tags.
    - Pass :func:`synced_bytes_loader` as argument `loader` to brute force
      load invalid tag structure.

    Args:
        stream: input data stream as bytes
        loader: low level tag loader, default loader is :func:`bytes_loader`
        messages: list to store error messages
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    """
    if loader is None:
        loader = bytes_loader
    tags, detector_stream = itertools.tee(loader(stream), 2)
    encoding = detect_encoding(detector_stream)

    # Apply repair filter:
    tags = repair.tag_reorder_layer(tags)
    tags = repair.filter_invalid_point_codes(tags)  # type: ignore
    tags = repair.filter_invalid_handles(tags)
    return byte_tag_compiler(tags, encoding, messages=messages, errors=errors)


INT_PATTERN_S = re.compile(r"[+-]?\d+")
INT_PATTERN_B = re.compile(rb"[+-]?\d+")


def _search_int(s: Union[str, bytes]) -> int:
    """Emulate the behavior of the C function stoll(), which just stop
    converting strings to integers at the first invalid char without raising
    an exception. e.g. "42xyz" is a valid integer 42

    """
    res = re.search(
        INT_PATTERN_S if isinstance(s, str) else INT_PATTERN_B, s  # type: ignore
    )
    if res:
        s = res.group()
    return int(s)


FLOAT_PATTERN_S = re.compile(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
FLOAT_PATTERN_B = re.compile(rb"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")


def _search_float(s: Union[str, bytes]) -> float:
    """Emulate the behavior of the C function stod(), which just stop
    converting strings to doubles at the first invalid char without raising
    an exception. e.g. "47.11xyz" is a valid double 47.11

    """
    res = re.search(
        FLOAT_PATTERN_S if isinstance(s, str) else FLOAT_PATTERN_B, s  # type: ignore
    )
    if res:
        s = res.group()
    return float(s)


@typing.no_type_check
def bytes_loader(stream: BinaryIO) -> Iterator[DXFTag]:
    """Yields :class:``DXFTag`` objects from a bytes `stream`
    (untrusted external  source), skips all comment tags (group code == 999).

    ``DXFTag.code`` is always an ``int`` and ``DXFTag.value`` is always a
    raw bytes value without line endings. Works with file system streams and
    :class:`BytesIO` streams.

    Raises:
        DXFStructureError: Found invalid group code.

    """
    eof = False
    line = 1
    readline = stream.readline
    while not eof:
        code = readline()
        # ByteIO(): empty strings indicates EOF - does not raise an exception
        if code:
            try:
                code = int(code)
            except ValueError:
                try:  # harder to find an int
                    code = _search_int(code)
                except ValueError:
                    code = code.decode(errors="ignore").rstrip("\r\n")
                    raise const.DXFStructureError(
                        f'Invalid group code "{code}" at line {line}.'
                    )
        else:
            return

        value = readline()
        # ByteIO(): empty strings indicates EOF
        if value:
            value = value.rstrip(b"\r\n")
            if code == 0 and value == b"EOF":
                eof = True
            if code != 999:
                yield DXFTag(code, value)
            line += 2
        else:
            return


def synced_bytes_loader(stream: BinaryIO) -> Iterator[DXFTag]:
    """Yields :class:``DXFTag`` objects from a bytes `stream`
    (untrusted external source), skips all comment tags (group code == 999).

    ``DXFTag.code`` is always an ``int`` and ``DXFTag.value`` is always a
    raw bytes value without line endings. Works with file system streams and
    :class:`BytesIO` streams.

    Does not raise DXFStructureError on invalid group codes, instead skips
    lines until a valid group code or EOF is found.

    This can remove invalid lines before group codes, but can not
    detect invalid lines between group code and tag value.

    """
    code = 999
    upper_boundary = MAX_GROUP_CODE + 1
    readline = stream.readline
    while True:
        seeking_valid_group_code = True
        while seeking_valid_group_code:
            code = readline()  # type: ignore
            if code:
                try:  # hard to find an int
                    code = _search_int(code)  # type: ignore
                except ValueError:
                    pass
                else:
                    if 0 <= code < upper_boundary:
                        seeking_valid_group_code = False
            else:
                return  # empty string is EOF
        value = readline()
        if value:
            if code != 999:
                yield DXFTag(code, value.rstrip(b"\r\n"))
        else:
            return  # empty string is EOF


DWGCODEPAGE = b"$DWGCODEPAGE"
ACADVER = b"$ACADVER"


def _strip_whitespace(s: str) -> str:
    ws = set(string.whitespace)
    return "".join([c for c in s if c not in ws])


def detect_encoding(tags: Iterable[DXFTag]) -> str:
    """Detect text encoding from header variables $DWGCODEPAGE and $ACADVER
    out of a stream of DXFTag objects.

    Assuming a malformed DXF file:

    The header variables could reside outside of the HEADER section,
    an ENDSEC tag is not a reliable fact that no $DWGCODEPAGE or
    $ACADVER header variable will show up in the remaining tag stream.

    Worst case: DXF file without a $ACADVER var, and a $DWGCODEPAGE
    unequal to "ANSI_1252" at the end of the file.

    """
    encoding = None
    dxfversion = None
    next_tag = None

    for code, value in tags:
        if code == 9:
            if value == DWGCODEPAGE:
                next_tag = DWGCODEPAGE  # e.g. (3, "ANSI_1252")
            elif value == ACADVER:
                next_tag = ACADVER  # e.g. (1, "AC1012")
        elif code == 3 and next_tag == DWGCODEPAGE:
            encoding = toencoding(value.decode(const.DEFAULT_ENCODING))
            next_tag = None
        elif code == 1 and next_tag == ACADVER:
            dxfversion = value.decode(const.DEFAULT_ENCODING)
            next_tag = None

        if encoding and dxfversion:
            return "utf8" if dxfversion >= const.DXF2007 else encoding

    return const.DEFAULT_ENCODING


@typing.no_type_check
def byte_tag_compiler(
    tags: Iterable[DXFTag],
    encoding=const.DEFAULT_ENCODING,
    messages: Optional[list] = None,
    errors: str = "surrogateescape",
) -> Iterator[DXFTag]:
    """Compiles DXF tag values imported by bytes_loader() into Python types.

    Raises DXFStructureError() for invalid float values and invalid coordinate
    values.

    Expects DXF coordinates written in x, y[, z] order, see function
    :func:`safe_tag_loader` for usage with applied repair filters.

    Args:
        tags: DXF tag generator, yielding tag values as bytes like bytes_loader()
        encoding: text encoding
        messages: list to store error messages
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: Found invalid DXF tag or unexpected coordinate order.

    """

    def error_msg(tag):
        code = tag.code
        value = tag.value.decode(encoding)
        return f'Invalid tag ({code}, "{value}") near line: {line}.'

    def recover_int(s: Union[str, bytes]) -> int:
        if isinstance(s, bytes):
            s = s.decode(encoding="utf8", errors="ignore")
        value = _search_int(s)
        msg = f'recovered invalid integer value "{s}" near line {line} as "{value}"'
        messages.append((AuditError.INVALID_INTEGER_VALUE, msg))
        logger.warning(msg)
        return value

    def recover_float(s: Union[str, bytes]) -> float:
        if isinstance(s, bytes):
            s = s.decode(encoding="utf8", errors="ignore")
        s = _strip_whitespace(s)
        value = _search_float(s)
        msg = f'recovered invalid floating point value "{s}" near line {line} as "{value}"'
        messages.append((AuditError.INVALID_FLOATING_POINT_VALUE, msg))
        logger.warning(msg)
        return value

    assert isinstance(encoding, str)
    assert isinstance(errors, str)

    if messages is None:
        messages = []
    tags = iter(tags)
    undo_tag = None
    line = 0
    while True:
        try:
            if undo_tag is not None:
                x = undo_tag
                undo_tag = None
            else:
                x = next(tags)
                line += 2
            code = x.code
            if code in POINT_CODES:
                y = next(tags)  # y coordinate is mandatory
                line += 2
                # e.g. y-code for x-code=10 is 20
                if y.code != code + 10:
                    raise const.DXFStructureError(
                        f"Missing required y-coordinate near line: {line}."
                    )
                # optional z coordinate
                z = next(tags)
                line += 2
                try:
                    # is it a z-coordinate like (30, 0.0) for base x-code=10
                    if z.code == code + 20:
                        try:
                            point = (
                                float(x.value),
                                float(y.value),
                                float(z.value),
                            )
                        except ValueError:  # search for any float values
                            point = (
                                recover_float(x.value),
                                recover_float(y.value),
                                recover_float(z.value),
                            )
                    else:
                        try:
                            point = (float(x.value), float(y.value))
                        except ValueError:  # search for any float values
                            point = (
                                recover_float(x.value),
                                recover_float(y.value),
                            )
                        undo_tag = z
                except ValueError:
                    raise const.DXFStructureError(
                        f"Invalid floating point values near line: {line}."
                    )
                yield DXFVertex(code, point)
            elif code in BINARY_DATA:
                # maybe pre compiled in low level tagger (binary DXF)
                if isinstance(x, DXFBinaryTag):
                    tag = x
                else:
                    try:
                        tag = DXFBinaryTag.from_string(code, x.value)
                    except ValueError:
                        raise const.DXFStructureError(
                            f"Invalid binary data near line: {line}."
                        )
                yield tag
            else:  # just a single tag
                type_ = TYPE_TABLE.get(code, str)
                value: bytes = x.value
                if type_ is str:
                    if code == 0:
                        # remove white space from structure tags
                        value = x.value.strip().upper()
                    try:  # 2 stages to document decoding errors
                        str_ = value.decode(encoding, errors="strict")
                    except UnicodeDecodeError:
                        str_ = value.decode(encoding, errors=errors)
                        messages.append(
                            (
                                AuditError.DECODING_ERROR,
                                f"Fixed unicode decoding error near line {line}",
                            )
                        )

                    # exclude structure tags (code == 0):
                    if code:
                        # Convert DXF-Unicode notation "\U+xxxx" to unicode
                        if has_dxf_unicode(str_):
                            str_ = decode_dxf_unicode(str_)
                        # Convert MIF notation "\M+cxxxx" to unicode
                        elif has_mif_encoding(str_):
                            str_ = decode_mif_to_unicode(str_)
                    yield DXFTag(code, str_)
                else:
                    try:
                        # fast path for int and float
                        yield DXFTag(code, type_(value))
                    except ValueError:
                        # slow path - e.g. ProE stores int values as floats :((
                        if type_ is int:
                            try:
                                yield DXFTag(code, recover_int(x.value))
                            except ValueError:
                                raise const.DXFStructureError(error_msg(x))
                        elif type_ is float:
                            try:
                                yield DXFTag(code, recover_float(x.value))
                            except ValueError:
                                raise const.DXFStructureError(error_msg(x))
                        else:
                            raise const.DXFStructureError(error_msg(x))
        except StopIteration:
            return
