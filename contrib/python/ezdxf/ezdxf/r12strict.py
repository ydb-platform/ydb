#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations

import string
from ezdxf import const
from ezdxf.lldxf.types import dxftag
from ezdxf.entities import XData, DXFEntity, is_graphic_entity
from ezdxf.document import Drawing
from ezdxf.sections.table import Table

__all__ = ["make_acad_compatible", "translate_names", "clean", "R12NameTranslator"]


def make_acad_compatible(doc: Drawing) -> None:
    """Apply all DXF R12 requirements, so Autodesk products will load the document."""
    if doc.dxfversion != const.DXF12:
        raise const.DXFVersionError(
            f"expected DXF document version R12, got: {doc.acad_release}"
        )
    clean(doc)
    translate_names(doc)


def translate_names(doc: Drawing) -> None:
    r"""Translate table and block names into strict DXF R12 names.

    ACAD Releases upto 14 limit names to 31 characters in length and all names are
    uppercase.  Names can include the letters A to Z, the numerals 0 to 9, and the
    special characters, dollar sign ($), underscore (_), hyphen (-) and the
    asterix (\*) as first character for special names like anonymous blocks.

    Most applications do not care about that and work fine with longer names and
    any characters used in names for some exceptions, but of course Autodesk
    applications are very picky about that.

    .. note::

        This is a destructive process and modifies the internals of the DXF document.

    """
    if doc.dxfversion != const.DXF12:
        raise const.DXFVersionError(
            f"expected DXF document version R12, got: {doc.acad_release}"
        )
    _R12StrictRename(doc).execute()


def clean(doc: Drawing) -> None:
    """Removes all features that are not supported for DXF R12 by Autodesk products."""
    if doc.dxfversion != const.DXF12:
        raise const.DXFVersionError(
            f"expected DXF document version R12, got: {doc.acad_release}"
        )
    _remove_table_xdata(doc.appids)
    _remove_table_xdata(doc.linetypes)
    _remove_table_xdata(doc.layers)
    _remove_table_xdata(doc.styles)
    _remove_table_xdata(doc.dimstyles)
    _remove_table_xdata(doc.ucs)
    _remove_table_xdata(doc.views)
    _remove_table_xdata(doc.viewports)
    _remove_legacy_blocks(doc)


def _remove_table_xdata(table: Table) -> None:
    """Autodesk products do not accept XDATA in table entries for DXF R12."""
    for entry in list(table):
        entry.xdata = None


def _remove_legacy_blocks(doc: Drawing) -> None:
    """Due to bad conversion some DXF files contain after loading the blocks
    "$MODEL_SPACE" and "$PAPER_SPACE". This function removes these empty blocks,
    because they will clash with the translated layout names of "*Model_Space" and
    "*Paper_Space".
    """
    for name in ("$MODEL_SPACE", "$PAPER_SPACE"):
        try:
            doc.blocks.delete_block(name, safe=False)
        except const.DXFKeyError:
            pass


class R12NameTranslator:
    r"""Translate table and block names into strict DXF R12 names.

    ACAD Releases upto 14 limit names to 31 characters in length and all names are
    uppercase.  Names can include the letters A to Z, the numerals 0 to 9, and the
    special characters, dollar sign ($), underscore (_), hyphen (-) and the
    asterix (\*) as first character for special names like anonymous blocks.

    """

    VALID_R12_NAME_CHARS = set(string.ascii_uppercase + string.digits + "$_-")

    def __init__(self) -> None:
        self.translated_names: dict[str, str] = {}
        self.used_r12_names: set[str] = set()

    def reset(self) -> None:
        self.translated_names.clear()
        self.used_r12_names.clear()

    def translate(self, name: str) -> str:
        name = name.upper()
        r12_name = self.translated_names.get(name)
        if r12_name is None:
            r12_name = self._name_sanitizer(name, self.VALID_R12_NAME_CHARS)
            r12_name = self._get_unique_r12_name(r12_name)
            self.translated_names[name] = r12_name
        return r12_name

    def _get_unique_r12_name(self, name: str) -> str:
        name0 = name
        counter = 0
        while name in self.used_r12_names:
            ext = str(counter)
            name = name0[: (31 - len(ext))] + ext
            counter += 1
        self.used_r12_names.add(name)
        return name

    @staticmethod
    def _name_sanitizer(name: str, valid_chars: set[str]) -> str:
        # `name` has to be upper case!
        if not name:
            return ""
        new_name = "".join(
                (char if char in valid_chars else "_") for char in name[:31]
            )
        # special names like anonymous blocks or the "*ACTIVE" viewport configuration
        if name[0] == "*":
            return "*" + new_name[1:31]
        else:
            return new_name


COMMON_ATTRIBS = ["layer", "linetype", "style", "tag", "name", "dimstyle"]
DIMSTYLE_ATTRIBS = ["dimblk", "dimblk1", "dimblk2"]
LAYER_ATTRIBS = ["linetype"]
BLOCK_ATTRIBS = ["layer"]


class _R12StrictRename:
    def __init__(self, doc: Drawing) -> None:
        assert doc.dxfversion == const.DXF12, "expected DXF version R12"
        self.doc = doc
        self.translator = R12NameTranslator()

    def execute(self) -> None:
        self.process_tables()
        self.process_header_vars()
        self.process_entities()

    def process_tables(self) -> None:
        tables = self.doc.tables
        self.rename_table_entries(tables.appids)
        self.rename_table_entries(tables.linetypes)
        self.rename_table_entries(tables.layers)
        self.process_table_entries(tables.layers, LAYER_ATTRIBS)
        self.rename_table_entries(tables.styles)
        self.rename_table_entries(tables.dimstyles)
        self.process_table_entries(tables.dimstyles, DIMSTYLE_ATTRIBS)
        self.rename_table_entries(tables.ucs)
        self.rename_table_entries(tables.views)
        self.rename_vports()
        self.rename_block_layouts()
        self.process_blocks()

    def rename_table_entries(self, table: Table) -> None:
        translate = self.translator.translate
        for entry in list(table):
            name = entry.dxf.name
            entry.dxf.name = translate(name)
            table.replace(name, entry)
            # XDATA for table entries is not accepted by Autodesk products for R12,
            # but for consistency a translation is applied
            if entry.xdata:
                self.translate_xdata(entry.xdata)

    def rename_vports(self):
        translate = self.translator.translate
        for config in list(self.doc.viewports.entries.values()):
            if not config:  # multiple entries in a sequence
                continue
            old_name = config[0].dxf.name
            new_name = translate(old_name)
            for entry in config:
                entry.dxf.name = new_name
            self.doc.viewports.replace(old_name, config)

    def process_table_entries(self, table: Table, attribute_names: list[str]) -> None:
        for entry in table:
            self.translate_entity_attributes(entry, attribute_names)

    def rename_block_layouts(self) -> None:
        translate = self.translator.translate
        blocks = self.doc.blocks
        for name in blocks.block_names():
            blocks.rename_block(name, translate(name))

    def process_blocks(self):
        for block_record in self.doc.block_records:
            block = block_record.block
            self.translate_entity_attributes(block, BLOCK_ATTRIBS)
            if block.xdata:
                self.translate_xdata(block.xdata)

    def process_header_vars(self) -> None:
        header = self.doc.header
        translate = self.translator.translate

        for key in (
            "$CELTYPE",
            "$CLAYER",
            "$DIMBLK",
            "$DIMBLK1",
            "$DIMBLK2",
            "$DIMSTYLE",
            "$UCSNAME",
            "$PUCSNAME",
            "$TEXTSTYLE",
        ):
            value = self.doc.header.get(key)
            if value:
                header[key] = translate(value)

    def process_entities(self) -> None:
        for entity in self.doc.entitydb.values():
            if not is_graphic_entity(entity):
                continue
            if entity.MIN_DXF_VERSION_FOR_EXPORT > const.DXF12:
                continue
            if entity.xdata:
                self.translate_xdata(entity.xdata)
            self.translate_entity_attributes(entity, COMMON_ATTRIBS)

    def translate_entity_attributes(
        self, entity: DXFEntity, attribute_names: list[str]
    ) -> None:
        translate = self.translator.translate
        for attrib_name in attribute_names:
            if not entity.dxf.hasattr(attrib_name):
                continue
            name = entity.dxf.get(attrib_name)
            if name:
                entity.dxf.set(attrib_name, translate(name))

    def translate_xdata(self, xdata: XData) -> None:
        translate = self.translator.translate
        for tags in xdata.data.values():
            for index, (code, value) in enumerate(tags):
                # 1001: APPID
                # 1003: layer name
                if code == 1001 or code == 1003:
                    tags[index] = dxftag(code, translate(value))
        xdata.update_keys()
