# Copyright (c) 2020-2021, Manfred Moitzi
# License: MIT License
from typing import Dict

from ezdxf.document import Drawing
from ezdxf.tools import codepage

from ezdxf.sections.header import HeaderSection
from ezdxf.sections.classes import ClassesSection
from ezdxf.sections.tables import TablesSection
from ezdxf.sections.blocks import BlocksSection
from ezdxf.sections.entities import EntitySection
from ezdxf.sections.objects import ObjectsSection
from ezdxf.sections.acdsdata import AcDsDataSection

from .const import *
from .fileheader import FileHeader
from .header_section import load_header_section
from .classes_section import load_classes_section

__all__ = ["readfile", "load"]


def readfile(filename: str, crc_check=False) -> "Drawing":
    data = open(filename, "rb").read()
    return load(data, crc_check)


def load(data: bytes, crc_check=False) -> Drawing:
    doc = DwgDocument(data, crc_check=crc_check)
    doc.load()
    return doc.doc


class DwgDocument:
    def __init__(self, data: Bytes, crc_check=False):
        self.data = memoryview(data)
        self.crc_check = crc_check
        self.specs = FileHeader(data, crc_check=crc_check)
        self.doc: Drawing = self._setup_doc()
        # Store DXF object types by class number:
        self.dxf_object_types: Dict[int, str] = dict()

    def _setup_doc(self) -> Drawing:
        doc = Drawing(dxfversion=self.specs.version)
        doc.encoding = self.specs.encoding
        doc.header = HeaderSection.new()

        # Setup basic header variables not stored in the header section of the DWG file.
        doc.header["$ACADVER"] = self.specs.version
        doc.header["$ACADMAINTVER"] = self.specs.maintenance_release_version
        doc.header["$DWGCODEPAGE"] = codepage.tocodepage(self.specs.encoding)

        doc.classes = ClassesSection(doc)
        # doc.tables = TablesSection(doc)
        # doc.blocks = BlocksSection(doc)
        # doc.entities = EntitySection(doc)
        # doc.objects = ObjectsSection(doc)
        # doc.acdsdata = AcDsDataSection(doc)
        return doc

    def load(self):
        self.load_header()
        self.load_classes()
        self.load_objects()
        self.store_objects()

    def load_header(self) -> None:
        hdr_section = load_header_section(self.specs, self.data, self.crc_check)
        hdr_vars = hdr_section.load_header_vars()
        self.set_header_vars(hdr_vars)

    def set_header_vars(self, hdr_vars: Dict):
        pass

    def load_classes(self) -> None:
        cls_section = load_classes_section(
            self.specs, self.data, self.crc_check
        )
        for class_num, dxfclass in cls_section.load_classes():
            self.doc.classes.register(dxfclass)
            self.dxf_object_types[class_num] = dxfclass.dxf.name

    def load_objects(self) -> None:
        pass

    def store_objects(self) -> None:
        pass
