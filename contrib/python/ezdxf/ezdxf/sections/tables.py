# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Sequence, Optional
import logging
from ezdxf.lldxf.const import DXFStructureError, DXF12
from .table import (
    Table,
    ViewportTable,
    TextstyleTable,
    LayerTable,
    LinetypeTable,
    AppIDTable,
    ViewTable,
    BlockRecordTable,
    DimStyleTable,
    UCSTable,
)

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity, DXFTagStorage
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

logger = logging.getLogger("ezdxf")

TABLENAMES = {
    "LAYER": "layers",
    "LTYPE": "linetypes",
    "APPID": "appids",
    "DIMSTYLE": "dimstyles",
    "STYLE": "styles",
    "UCS": "ucs",
    "VIEW": "views",
    "VPORT": "viewports",
    "BLOCK_RECORD": "block_records",
}


class TablesSection:
    def __init__(self, doc: Drawing, entities: Optional[list[DXFEntity]] = None):
        assert doc is not None
        self.doc = doc
        # not loaded tables: table.doc is None
        self.layers = LayerTable()
        self.linetypes = LinetypeTable()
        self.appids = AppIDTable()
        self.dimstyles = DimStyleTable()
        self.styles = TextstyleTable()
        self.ucs = UCSTable()
        self.views = ViewTable()
        self.viewports = ViewportTable()
        self.block_records = BlockRecordTable()

        if entities is not None:
            self._load(entities)
        self._reset_not_loaded_tables()

    def tables(self) -> Sequence[Table]:
        return (
            self.layers,
            self.linetypes,
            self.appids,
            self.dimstyles,
            self.styles,
            self.ucs,
            self.views,
            self.viewports,
            self.block_records,
        )

    def _load(self, entities: list[DXFEntity]) -> None:
        section_head: "DXFTagStorage" = entities[0]  # type: ignore
        if section_head.dxftype() != "SECTION" or section_head.base_class[
            1
        ] != (2, "TABLES"):
            raise DXFStructureError(
                "Critical structure error in TABLES section."
            )
        del entities[0]  # delete first entity (0, SECTION)

        table_records: list[DXFEntity] = []
        table_name = None
        for entity in entities:
            if entity.dxftype() == "TABLE":
                if len(table_records):
                    # TABLE entity without preceding ENDTAB entity, should we care?
                    logger.debug(
                        f'Ignore missing ENDTAB entity in table "{table_name}".'
                    )
                    self._load_table(table_name, table_records)  # type: ignore
                table_name = entity.dxf.name
                table_records = [entity]  # collect table head
            elif entity.dxftype() == "ENDTAB":  # do not collect (0, 'ENDTAB')
                self._load_table(table_name, table_records)  # type: ignore
                table_records = (
                    []
                )  # collect entities outside of tables, but ignore it
            else:  # collect table entries
                table_records.append(entity)

        if len(table_records):
            # last ENDTAB entity is missing, should we care?
            logger.debug(
                'Ignore missing ENDTAB entity in table "{}".'.format(table_name)
            )
            self._load_table(table_name, table_records)  # type: ignore

    def _load_table(
        self, name: str, table_entities: Iterable[DXFEntity]
    ) -> None:
        """
        Load table from tags.

        Args:
            name: table name e.g. VPORT
            table_entities: iterable of table records

        """
        table = getattr(self, TABLENAMES[name])
        if isinstance(table, Table):
            table.load(self.doc, iter(table_entities))

    def _reset_not_loaded_tables(self) -> None:
        entitydb = self.doc.entitydb
        for table in self.tables():
            if table.doc is None:
                handle = entitydb.next_handle()
                table.reset(self.doc, handle)
                entitydb.add(table.head)

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_str("  0\nSECTION\n  2\nTABLES\n")
        version = tagwriter.dxfversion
        self.viewports.export_dxf(tagwriter)
        self.linetypes.export_dxf(tagwriter)
        self.layers.export_dxf(tagwriter)
        self.styles.export_dxf(tagwriter)
        self.views.export_dxf(tagwriter)
        self.ucs.export_dxf(tagwriter)
        self.appids.export_dxf(tagwriter)
        self.dimstyles.export_dxf(tagwriter)
        if version > DXF12:
            self.block_records.export_dxf(tagwriter)
        tagwriter.write_tag2(0, "ENDSEC")

    def create_table_handles(self):
        # DXF R12: TABLE does not require a handle and owner tag
        # DXF R2000+: TABLE requires a handle and an owner tag
        for table in self.tables():
            handle = self.doc.entitydb.next_handle()
            table.set_handle(handle)
