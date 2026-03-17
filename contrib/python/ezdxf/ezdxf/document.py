# Copyright (c) 2011-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    TextIO,
    BinaryIO,
    Iterable,
    Iterator,
    Union,
    Callable,
    cast,
    Optional,
    Sequence,
    Any,
)
import abc
import base64
import io
import logging
import os
import pathlib
from datetime import datetime, timezone
from itertools import chain

import ezdxf
from ezdxf.audit import Auditor
from ezdxf.entities.dxfgroups import GroupCollection
from ezdxf.entities.material import MaterialCollection
from ezdxf.entities.mleader import MLeaderStyleCollection
from ezdxf.entities.mline import MLineStyleCollection
from ezdxf.entitydb import EntityDB
from ezdxf.groupby import groupby
from ezdxf.layouts import Modelspace, Paperspace
from ezdxf.layouts.layouts import Layouts
from ezdxf.lldxf import const
from ezdxf.lldxf.const import (
    BLK_XREF,
    BLK_EXTERNAL,
    DXF13,
    DXF14,
    DXF2000,
    DXF2007,
    DXF12,
    DXF2013,
)
from ezdxf.lldxf import loader
from ezdxf.lldxf.tagwriter import (
    AbstractTagWriter,
    TagWriter,
    BinaryTagWriter,
    JSONTagWriter,
)
from ezdxf.query import EntityQuery
from ezdxf.render.dimension import DimensionRenderer
from ezdxf.sections.acdsdata import AcDsDataSection, new_acds_data_section
from ezdxf.sections.blocks import BlocksSection
from ezdxf.sections.classes import ClassesSection
from ezdxf.sections.entities import EntitySection, StoredSection
from ezdxf.sections.header import HeaderSection
from ezdxf.sections.objects import ObjectsSection
from ezdxf.sections.tables import TablesSection
from ezdxf.tools import guid
from ezdxf.tools.codepage import tocodepage, toencoding
from ezdxf.tools.juliandate import juliandate
from ezdxf.tools.text import safe_string, MAX_STR_LEN
from ezdxf import messenger, msgtypes


logger = logging.getLogger("ezdxf")

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity, Layer, VPort, Dictionary
    from ezdxf.eztypes import GenericLayoutType
    from ezdxf.layouts import Layout
    from ezdxf.lldxf.tags import Tags
    from ezdxf.lldxf.types import DXFTag
    from ezdxf.sections.tables import (
        LayerTable,
        LinetypeTable,
        TextstyleTable,
        DimStyleTable,
        AppIDTable,
        UCSTable,
        ViewTable,
        ViewportTable,
        BlockRecordTable,
    )

CONST_GUID = "{00000000-0000-0000-0000-000000000000}"
CONST_MARKER_STRING = "0.0 @ 2000-01-01T00:00:00.000000+00:00"
CREATED_BY_EZDXF = "CREATED_BY_EZDXF"
WRITTEN_BY_EZDXF = "WRITTEN_BY_EZDXF"
EZDXF_META = "EZDXF_META"


def _validate_handle_seed(seed: str) -> str:
    from ezdxf.tools.handle import START_HANDLE

    if seed is None:
        seed = START_HANDLE
    try:
        v = int(seed, 16)
        if v < 1:
            seed = START_HANDLE
    except ValueError:
        seed = START_HANDLE
    return seed


class Drawing:
    def __init__(self, dxfversion=DXF2013) -> None:
        self.entitydb = EntityDB()
        self.messenger = messenger.Messenger(self)
        target_dxfversion = dxfversion.upper()
        self._dxfversion: str = const.acad_release_to_dxf_version.get(
            target_dxfversion, target_dxfversion
        )
        if self._dxfversion not in const.versions_supported_by_new:
            raise const.DXFVersionError(f'Unsupported DXF version "{self.dxfversion}".')
        # Store original dxf version if loaded (and maybe converted R13/14)
        # from file.
        self._loaded_dxfversion: Optional[str] = None

        # Status flag which is True while loading content from a DXF file:
        self.is_loading = False
        self.encoding: str = "cp1252"  # read/write
        self.filename: Optional[str] = None

        # Reason for using "type: ignore".
        # I won't use "Optional" for the following attributes because these
        # objects are required, just not yet! Setting up an empty DXF Document
        # is not necessary if the document is read from the file system,
        # see class-methods new() and read().

        # named objects dictionary
        self.rootdict: Dictionary = None  # type: ignore

        # DXF sections
        self.header: HeaderSection = None  # type: ignore
        self.classes: ClassesSection = None  # type: ignore
        self.tables: TablesSection = None  # type: ignore
        self.blocks: BlocksSection = None  # type: ignore
        self.entities: EntitySection = None  # type: ignore
        self.objects: ObjectsSection = None  # type: ignore

        # DXF R2013 and later
        self.acdsdata: AcDsDataSection = None  # type: ignore

        self.stored_sections: list[StoredSection] = []
        self.layouts: Layouts = None  # type: ignore
        self.groups: GroupCollection = None  # type: ignore
        self.materials: MaterialCollection = None  # type: ignore
        self.mleader_styles: MLeaderStyleCollection = None  # type: ignore
        self.mline_styles: MLineStyleCollection = None  # type: ignore

        # Set to False if the generated DXF file will be incompatible to AutoCAD
        self._acad_compatible = True
        # Store reasons for AutoCAD incompatibility:
        self._acad_incompatibility_reason: set[str] = set()

        # DIMENSION rendering engine can be replaced by a custom Dimension
        # render: see property Drawing.dimension_renderer
        self._dimension_renderer = DimensionRenderer()

        # Some fixes can't be applied while the DXF document is not fully
        # initialized, store this fixes as callable object:
        self._post_init_commands: list[Callable] = []
        # Don't create any new entities here:
        # New created handles could collide with handles loaded from DXF file.
        assert len(self.entitydb) == 0

    @classmethod
    def new(cls, dxfversion: str = DXF2013) -> Drawing:
        """Create new drawing. Package users should use the factory function
        :func:`ezdxf.new`. (internal API)
        """
        doc = cls(dxfversion)
        doc._setup()
        doc._create_ezdxf_metadata()
        return doc

    def _setup(self):
        self.header = HeaderSection.new()
        self.classes = ClassesSection(self)
        self.tables = TablesSection(self)
        self.blocks = BlocksSection(self)
        self.entities = EntitySection(self)
        self.objects = ObjectsSection(self)
        self.acdsdata = new_acds_data_section(self)
        self.rootdict = self.objects.rootdict
        # Create missing tables:
        self.objects.setup_object_management_tables(self.rootdict)
        self.layouts = Layouts.setup(self)
        self._finalize_setup()

    def _finalize_setup(self):
        """Common setup tasks for new and loaded DXF drawings."""
        self.groups = GroupCollection(self)
        self.materials = MaterialCollection(self)

        self.mline_styles = MLineStyleCollection(self)
        # all required internal structures are ready
        # now do the stuff to please AutoCAD
        self._create_required_table_entries()

        # mleader_styles requires text styles
        self.mleader_styles = MLeaderStyleCollection(self)
        self._set_required_layer_attributes()
        self._setup_metadata()
        self._execute_post_init_commands()

    def _execute_post_init_commands(self):
        for cmd in self._post_init_commands:
            cmd()
        del self._post_init_commands

    def _create_required_table_entries(self):
        self._create_required_vports()
        self._create_required_linetypes()
        self._create_required_layers()
        self._create_required_styles()
        self._create_required_appids()
        self._create_required_dimstyles()

    def _set_required_layer_attributes(self):
        for layer in self.layers:
            layer.set_required_attributes()

    def _create_required_vports(self):
        if "*Active" not in self.viewports:
            self.viewports.new("*Active")

    def _create_required_appids(self):
        if "ACAD" not in self.appids:
            self.appids.new("ACAD")

    def _create_required_linetypes(self):
        linetypes = self.linetypes
        for name in ("ByBlock", "ByLayer", "Continuous"):
            if name not in linetypes:
                linetypes.new(name)

    def _create_required_dimstyles(self):
        if "Standard" not in self.dimstyles:
            self.dimstyles.new("Standard")

    def _create_required_styles(self):
        if "Standard" not in self.styles:
            self.styles.new("Standard")

    def _create_required_layers(self):
        layers = self.layers
        if "0" not in layers:
            layers.new("0")
        if "Defpoints" not in layers:
            layers.new("Defpoints", dxfattribs={"plot": 0})  # do not plot
        else:
            # AutoCAD requires a plot flag = 0
            layers.get("Defpoints").dxf.plot = 0

    def _setup_metadata(self):
        self.header["$ACADVER"] = self.dxfversion
        self.header["$TDCREATE"] = juliandate(datetime.now())
        if self.header.get("$FINGERPRINTGUID", CONST_GUID) == CONST_GUID:
            self.reset_fingerprint_guid()
        if self.header.get("$VERSIONGUID", CONST_GUID) == CONST_GUID:
            self.reset_version_guid()

    @property
    def dxfversion(self) -> str:
        """Get current DXF version."""
        return self._dxfversion

    @dxfversion.setter
    def dxfversion(self, version: str) -> None:
        """Set current DXF version."""
        self._dxfversion = self._validate_dxf_version(version)
        self.header["$ACADVER"] = version

    @property
    def loaded_dxfversion(self) -> Optional[str]:
        return self._loaded_dxfversion

    @property
    def output_encoding(self):
        """Returns required output encoding for writing document to a text
        streams.
        """
        return "utf-8" if self.dxfversion >= DXF2007 else self.encoding

    @property
    def units(self) -> int:
        """Get and set the document/modelspace base units as enum, for more
        information read this: :ref:`dxf units`. Requires DXF R2000 or newer.

        """
        return self.header.get("$INSUNITS", 0)

    @units.setter
    def units(self, unit_enum: int) -> None:
        if 0 <= unit_enum < 25:
            if self.dxfversion < DXF2000:
                logger.warning(
                    "Drawing units ($INSUNITS) are not exported for DXF R12."
                )
            self.header["$INSUNITS"] = unit_enum
        else:
            raise ValueError(f"Invalid units enum: {unit_enum}")

    def _validate_dxf_version(self, version: str) -> str:
        version = version.upper()
        # translates 'R12' -> 'AC1009'
        version = const.acad_release_to_dxf_version.get(version, version)
        if version not in const.versions_supported_by_save:
            raise const.DXFVersionError(f'Unsupported DXF version "{version}".')
        if version == DXF12:
            if self._dxfversion > DXF12:
                logger.warning(
                    f"Downgrade from DXF {self.acad_release} to R12 may create "
                    f"an invalid DXF file."
                )
        elif version < self._dxfversion:
            logger.info(
                f"Downgrade from DXF {self.acad_release} to "
                f"{const.acad_release[version]} can cause lost of features."
            )
        return version

    def get_abs_filepath(self) -> pathlib.Path:
        """Returns the absolute filepath of the document."""
        if not self.filename:
            return pathlib.Path(".")
        return pathlib.Path(self.filename).resolve()

    @classmethod
    def read(cls, stream: TextIO) -> Drawing:
        """Open an existing drawing. Package users should use the factory
        function :func:`ezdxf.read`. To preserve possible binary data in
        XRECORD entities use :code:`errors='surrogateescape'` as error handler
        for the import stream.

        Args:
             stream: text stream yielding text (unicode) strings by readline()

        """
        from .lldxf.tagger import ascii_tags_loader

        tag_loader = ascii_tags_loader(stream)
        return cls.load(tag_loader)

    @classmethod
    def load(cls, tag_loader: Iterable[DXFTag]) -> Drawing:
        """Load DXF document from a DXF tag loader, in general an external
        untrusted source.

        Args:
            tag_loader: DXF tag loader

        """
        from .lldxf.tagger import tag_compiler

        tag_loader = tag_compiler(tag_loader)  # type: ignore
        doc = cls()
        doc._load(tag_loader)
        return doc

    @classmethod
    def from_tags(cls, compiled_tags: Iterable[DXFTag]) -> Drawing:
        """Create new drawing from compiled tags. (internal API)"""
        doc = cls()
        doc._load(tagger=compiled_tags)
        return doc

    def _load(self, tagger: Iterable[DXFTag]) -> None:
        # 1st Loading stage: load complete DXF entity structure
        self.is_loading = True
        sections = loader.load_dxf_structure(tagger)
        if "THUMBNAILIMAGE" in sections:
            del sections["THUMBNAILIMAGE"]
        self._load_section_dict(sections)

    def _load_section_dict(self, sections: loader.SectionDict) -> None:
        """Internal API to load a DXF document from a section dict."""
        self.is_loading = True
        # Create header section:
        header_entities: list[Tags] = sections.get("HEADER", [])  # type: ignore
        if header_entities:
            # All header tags are the first DXF structure entity
            self.header = HeaderSection.load(header_entities[0])
        else:
            # Create default header, files without header are by default DXF R12
            self.header = HeaderSection.new(dxfversion=DXF12)
        self._dxfversion = self.header.get("$ACADVER", DXF12)

        # Store original DXF version of loaded file.
        self._loaded_dxfversion = self._dxfversion

        # Content encoding:
        self.encoding = toencoding(self.header.get("$DWGCODEPAGE", "ANSI_1252"))

        # Set handle seed:
        seed: str = self.header.get("$HANDSEED", str(self.entitydb.handles))
        self.entitydb.handles.reset(_validate_handle_seed(seed))

        # Store all necessary DXF entities in the entity database:
        loader.load_and_bind_dxf_content(sections, self)

        # End of 1. loading stage, all entities of the DXF file are
        # stored in the entity database.

        # Create sections:
        self.classes = ClassesSection(
            self, sections.get("CLASSES", None)  # type: ignore
        )
        self.tables = TablesSection(self, sections.get("TABLES", None))  # type: ignore

        # Create *Model_Space and *Paper_Space BLOCK_RECORDS
        # BlockSection setup takes care about the rest:
        self._create_required_block_records()

        # At this point all table entries are required:
        self.blocks = BlocksSection(self, sections.get("BLOCKS", None))  # type: ignore
        self.entities = EntitySection(
            self, sections.get("ENTITIES", None)  # type: ignore
        )
        self.objects = ObjectsSection(
            self, sections.get("OBJECTS", None)  # type: ignore
        )

        # only DXF R2013+
        self.acdsdata = AcDsDataSection(
            self, sections.get("ACDSDATA", None)  # type: ignore
        )

        # Store unmanaged sections as raw tags:
        for name, data in sections.items():
            if name not in const.MANAGED_SECTIONS:
                self.stored_sections.append(StoredSection(data))  # type: ignore

        # Objects section is not initialized!
        self._2nd_loading_stage()

        # DXF version upgrades:
        if self.dxfversion < DXF12:
            logger.info("DXF version upgrade to DXF R12.")
            self.dxfversion = DXF12

        if self.dxfversion == DXF12:
            self.tables.create_table_handles()

        if self.dxfversion in (DXF13, DXF14):
            logger.info("DXF version upgrade to DXF R2000.")
            self.dxfversion = DXF2000
            self.create_all_arrow_blocks()

        # Objects section setup:
        rootdict = self.objects.rootdict
        if rootdict.DXFTYPE != "DICTIONARY":
            raise const.DXFStructureError(
                f"invalid root dictionary entity {str(rootdict)} "
            )
        self.rootdict = rootdict

        # Create missing management tables (DICTIONARY):
        self.objects.setup_object_management_tables(self.rootdict)

        # Setup modelspace- and paperspace layouts:
        self.layouts = Layouts.load(self)

        # Additional work is common to the new and load process:
        self.is_loading = False
        self._finalize_setup()

    def _2nd_loading_stage(self):
        """Load additional resources from entity database into DXF entities.

        e.g. convert handles into DXFEntity() objects

        """
        db = self.entitydb
        for entity in db.values():
            # The post_load_hook() can return a callable, which should be
            # executed, when the DXF document is fully initialized.
            cmd = entity.post_load_hook(self)
            if cmd is not None:
                self._post_init_commands.append(cmd)

    def create_all_arrow_blocks(self):
        """For upgrading DXF R12/13/14 files to R2000, it is necessary to
        create all used arrow blocks before saving the DXF file, else $HANDSEED
        is not the next available handle, which is a problem for AutoCAD.

        Create all known AutoCAD arrows to be on the safe side, because
        references to arrow blocks can be in DIMSTYLE, DIMENSION override,
        LEADER override and maybe other locations.

        """
        from ezdxf.render.arrows import ARROWS

        for arrow_name in ARROWS.__acad__:
            ARROWS.create_block(self.blocks, arrow_name)

    def _create_required_block_records(self):
        if "*Model_Space" not in self.block_records:
            self.block_records.new("*Model_Space")
        if "*Paper_Space" not in self.block_records:
            self.block_records.new("*Paper_Space")

    def saveas(
        self,
        filename: Union[os.PathLike, str],
        encoding: Optional[str] = None,
        fmt: str = "asc",
    ) -> None:
        """Set :class:`Drawing` attribute :attr:`filename` to `filename` and
        write drawing to the file system. Override file encoding by argument
        `encoding`, handle with care, but this option allows you to create DXF
        files for applications that handles file encoding different than
        AutoCAD.

        Args:
            filename: file name as string
            encoding: override default encoding as Python encoding string like ``'utf-8'``
            fmt: ``'asc'`` for ASCII DXF (default) or ``'bin'`` for Binary DXF

        """
        self.filename = str(filename)
        self.save(encoding=encoding, fmt=fmt)

    def save(self, encoding: Optional[str] = None, fmt: str = "asc") -> None:
        """Write drawing to file-system by using the :attr:`filename` attribute
        as filename. Override file encoding by argument `encoding`, handle with
        care, but this option allows you to create DXF files for applications
        that handle file encoding different from AutoCAD.

        Args:
            encoding: override default encoding as Python encoding string like ``'utf-8'``
            fmt: ``'asc'`` for ASCII DXF (default) or ``'bin'`` for Binary DXF

        """
        # DXF R12, R2000, R2004 - ASCII encoding
        # DXF R2007 and newer - UTF-8 encoding
        # in ASCII mode, unknown characters will be escaped as \U+nnnn unicode
        # characters.

        if encoding is None:
            enc = self.output_encoding
        else:
            # override default encoding, for applications that handle encoding
            # different than AutoCAD
            enc = encoding

        if fmt.startswith("asc"):
            fp = io.open(
                self.filename, mode="wt", encoding=enc, errors="dxfreplace"  # type: ignore
            )
        elif fmt.startswith("bin"):
            fp = open(self.filename, "wb")  # type: ignore
        else:
            raise ValueError(f"Unknown output format: '{fmt}'.")
        try:
            self.write(fp, fmt=fmt)  # type: ignore
        finally:
            fp.close()

    def encode(self, s: str) -> bytes:
        """Encode string `s` with correct encoding and error handler."""
        return s.encode(encoding=self.output_encoding, errors="dxfreplace")

    def write(self, stream: Union[TextIO, BinaryIO], fmt: str = "asc") -> None:
        """Write drawing as ASCII DXF to a text stream or as Binary DXF to a
        binary stream. For DXF R2004 (AC1018) and prior open stream with
        drawing :attr:`encoding` and :code:`mode='wt'`. For DXF R2007 (AC1021)
        and later use :code:`encoding='utf-8'`, or better use the later added
        :class:`Drawing` property :attr:`output_encoding` which returns the
        correct encoding automatically. The correct and required error handler
        is :code:`errors='dxfreplace'`!

        If writing to a :class:`StringIO` stream, use :meth:`Drawing.encode` to
        encode the result string from :meth:`StringIO.get_value`::

            binary = doc.encode(stream.get_value())

        Args:
            stream: output text stream or binary stream
            fmt: "asc" for ASCII DXF (default) or "bin" for binary DXF

        """
        # These changes may alter the document content (create new entities, blocks ...)
        # and have to be done before the export and the update of internal structures
        # can be done.
        self.commit_pending_changes()

        dxfversion = self.dxfversion
        if dxfversion == DXF12:
            handles = bool(self.header.get("$HANDLING", 0))
        else:
            handles = True
        if dxfversion > DXF12:
            self.classes.add_required_classes(dxfversion)

        self.update_all()
        if fmt.startswith("asc"):
            tagwriter = TagWriter(
                stream,  # type: ignore
                write_handles=handles,
                dxfversion=dxfversion,
            )
        elif fmt.startswith("bin"):
            tagwriter = BinaryTagWriter(  # type: ignore
                stream,  # type: ignore
                write_handles=handles,
                dxfversion=dxfversion,
                encoding=self.output_encoding,
            )
            tagwriter.write_signature()  # type: ignore
        else:
            raise ValueError(f"Unknown output format: '{fmt}'.")

        self.export_sections(tagwriter)

    def encode_base64(self) -> bytes:
        """Returns DXF document as base64 encoded binary data."""
        stream = io.StringIO()
        self.write(stream)
        # Create binary data:
        binary_data = self.encode(stream.getvalue())
        # Create Windows line endings and do base64 encoding:
        return base64.encodebytes(binary_data.replace(b"\n", b"\r\n"))

    def export_sections(self, tagwriter: AbstractTagWriter) -> None:
        """DXF export sections. (internal API)"""
        dxfversion = tagwriter.dxfversion
        self.header.export_dxf(tagwriter)
        if dxfversion > DXF12:
            self.classes.export_dxf(tagwriter)
        self.tables.export_dxf(tagwriter)
        self.blocks.export_dxf(tagwriter)
        self.entities.export_dxf(tagwriter)
        if dxfversion > DXF12:
            self.objects.export_dxf(tagwriter)
        if self.acdsdata.is_valid:
            self.acdsdata.export_dxf(tagwriter)
        for section in self.stored_sections:
            section.export_dxf(tagwriter)

        tagwriter.write_tag2(0, "EOF")

    def commit_pending_changes(self) -> None:
        self.messenger.broadcast(msgtypes.COMMIT_PENDING_CHANGES)

    def update_all(self) -> None:
        if self.dxfversion > DXF12:
            self.classes.add_required_classes(self.dxfversion)
        self._create_appids()
        self._update_header_vars()
        self.update_extents()
        self.update_limits()
        self._update_metadata()

    def update_extents(self):
        msp = self.modelspace()
        # many applications not maintain these values
        extmin = msp.dxf.extmin
        extmax = msp.dxf.extmax
        if bool(extmin) and bool(extmax):
            self.header["$EXTMIN"] = extmin
            self.header["$EXTMAX"] = extmax
        active_layout = self.active_layout()
        self.header["$PEXTMIN"] = active_layout.dxf.extmin
        self.header["$PEXTMAX"] = active_layout.dxf.extmax

    def update_limits(self):
        msp = self.modelspace()
        self.header["$LIMMIN"] = msp.dxf.limmin
        self.header["$LIMMAX"] = msp.dxf.limmax
        active_layout = self.active_layout()
        self.header["$PLIMMIN"] = active_layout.dxf.limmin
        self.header["$PLIMMAX"] = active_layout.dxf.limmax

    def _update_header_vars(self):
        from ezdxf.lldxf.const import acad_maint_ver

        # set or correct $CMATERIAL handle
        material = self.entitydb.get(self.header.get("$CMATERIAL", None))
        if material is None or material.dxftype() != "MATERIAL":
            handle = "0"
            if "ByLayer" in self.materials:
                handle = self.materials.get("ByLayer").dxf.handle
            elif "Global" in self.materials:
                handle = self.materials.get("Global").dxf.handle
            self.header["$CMATERIAL"] = handle
        # set ACAD maintenance version - same values as used by BricsCAD
        self.header["$ACADMAINTVER"] = acad_maint_ver.get(self.dxfversion, 0)

    def _update_metadata(self):
        self._update_ezdxf_metadata()
        if ezdxf.options.write_fixed_meta_data_for_testing:
            fixed_date = juliandate(datetime(2000, 1, 1, 0, 0))
            self.header["$TDCREATE"] = fixed_date
            self.header["$TDUCREATE"] = fixed_date
            self.header["$TDUPDATE"] = fixed_date
            self.header["$TDUUPDATE"] = fixed_date
            self.header["$VERSIONGUID"] = CONST_GUID
            self.header["$FINGERPRINTGUID"] = CONST_GUID
        else:
            now = datetime.now()
            self.header["$TDUPDATE"] = juliandate(now)
            self.reset_version_guid()
        self.header["$HANDSEED"] = str(self.entitydb.handles)  # next handle
        self.header["$DWGCODEPAGE"] = tocodepage(self.encoding)

    def ezdxf_metadata(self) -> MetaData:
        """Returns the *ezdxf* :class:`ezdxf.document.MetaData` object, which
        manages  *ezdxf* and custom metadata in DXF files.
        For more information see:  :ref:`ezdxf_metadata`.

        """
        return R12MetaData(self) if self.dxfversion <= DXF12 else R2000MetaData(self)

    def _create_ezdxf_metadata(self):
        ezdxf_meta = self.ezdxf_metadata()
        ezdxf_meta[CREATED_BY_EZDXF] = ezdxf_marker_string()

    def _update_ezdxf_metadata(self):
        ezdxf_meta = self.ezdxf_metadata()
        ezdxf_meta[WRITTEN_BY_EZDXF] = ezdxf_marker_string()

    def _create_appid_if_not_exist(self, name: str, flags: int = 0) -> None:
        if name not in self.appids:
            self.appids.new(name, {"flags": flags})

    def _create_appids(self):
        self._create_appid_if_not_exist("HATCHBACKGROUNDCOLOR", 0)
        self._create_appid_if_not_exist("EZDXF", 0)

    @property
    def acad_release(self) -> str:
        """Returns the AutoCAD release abbreviation like "R12" or "R2000"."""
        return const.acad_release.get(self.dxfversion, "unknown")

    @property
    def layers(self) -> LayerTable:
        return self.tables.layers

    @property
    def linetypes(self) -> LinetypeTable:
        return self.tables.linetypes

    @property
    def styles(self) -> TextstyleTable:
        return self.tables.styles

    @property
    def dimstyles(self) -> DimStyleTable:
        return self.tables.dimstyles

    @property
    def ucs(self) -> UCSTable:
        return self.tables.ucs

    @property
    def appids(self) -> AppIDTable:
        return self.tables.appids

    @property
    def views(self) -> ViewTable:
        return self.tables.views

    @property
    def block_records(self) -> BlockRecordTable:
        return self.tables.block_records

    @property
    def viewports(self) -> ViewportTable:
        return self.tables.viewports

    @property
    def plotstyles(self) -> Dictionary:
        return self.rootdict["ACAD_PLOTSTYLENAME"]  # type: ignore

    @property
    def dimension_renderer(self) -> DimensionRenderer:
        return self._dimension_renderer

    @dimension_renderer.setter
    def dimension_renderer(self, renderer: DimensionRenderer) -> None:
        """
        Set your own dimension line renderer if needed.

        see also: ezdxf.render.dimension

        """
        self._dimension_renderer = renderer

    def modelspace(self) -> Modelspace:
        r"""Returns the modelspace layout, displayed as "Model" tab in CAD
        applications, defined by block record named "\*Model_Space".
        """
        return self.layouts.modelspace()

    def layout(self, name: str = "") -> Layout:
        """Returns paperspace layout `name` or the first layout in tab-order
        if no name is given.

        Args:
            name: paperspace name or empty string for the first paperspace in
                tab-order

        Raises:
            KeyError: layout `name` does not exist

        """
        return self.layouts.get(name)

    def paperspace(self, name: str = "") -> Paperspace:
        """Returns paperspace layout `name` or the active paperspace if no
        name is given.

        Args:
            name: paperspace name or empty string for the active paperspace

        Raises:
            KeyError: if the modelspace was acquired or layout `name` does not exist

        """
        if name:
            psp = self.layouts.get(name)
            if isinstance(psp, Paperspace):
                return psp
            raise KeyError("use method modelspace() to acquire the modelspace.")
        else:
            return self.active_layout()

    def active_layout(self) -> Paperspace:
        r"""Returns the active paperspace layout, defined by block record name
        "\*Paper_Space".
        """
        return self.layouts.active_layout()

    def layout_names(self) -> Iterable[str]:
        """Returns all layout names in arbitrary order."""
        return list(self.layouts.names())

    def layout_names_in_taborder(self) -> Iterable[str]:
        """Returns all layout names in tab-order, "Model" is always the first name."""
        return list(self.layouts.names_in_taborder())

    def reset_fingerprint_guid(self):
        """Reset fingerprint GUID."""
        self.header["$FINGERPRINTGUID"] = guid()

    def reset_version_guid(self):
        """Reset version GUID."""
        self.header["$VERSIONGUID"] = guid()

    @property
    def is_acad_compatible(self) -> bool:
        """Returns ``True`` if the current state of the document is compatible to
        AutoCAD.
        """
        return self._acad_compatible

    @property
    def acad_incompatibility_reasons(self) -> list[str]:
        """Returns a list of reasons why this document is not compatible to AutoCAD."""
        return list(self._acad_incompatibility_reason)

    def add_acad_incompatibility_message(self, msg: str):
        """Add a reason why this document is not compatible to AutoCAD.
        (internal API)
        """
        self._acad_compatible = False
        if msg not in self._acad_incompatibility_reason:
            self._acad_incompatibility_reason.add(msg)
            logger.warning(f"DXF document is not compatible to AutoCAD! {msg}.")

    def query(self, query: str = "*") -> EntityQuery:
        """Entity query over all layouts and blocks, excluding the OBJECTS section and
        the resource tables of the TABLES section.

        Args:
            query: query string

        .. seealso::

            :ref:`entity query string` and :ref:`entity queries`

        """
        return EntityQuery(self.chain_layouts_and_blocks(), query)

    def groupby(self, dxfattrib="", key=None) -> dict:
        """Groups DXF entities of all layouts and blocks (excluding the
        OBJECTS section) by a DXF attribute or a key function.

        Args:
            dxfattrib: grouping DXF attribute like "layer"
            key: key function, which accepts a :class:`DXFEntity` as argument
                and returns a hashable grouping key or ``None`` to ignore
                this entity.

        .. seealso::

            :func:`~ezdxf.groupby.groupby` documentation

        """
        return groupby(self.chain_layouts_and_blocks(), dxfattrib, key)

    def chain_layouts_and_blocks(self) -> Iterator[DXFEntity]:
        """Chain entity spaces of all layouts and blocks. Yields an iterator
        for all entities in all layouts and blocks.
        """
        layouts = list(self.layouts_and_blocks())
        return chain.from_iterable(layouts)

    def layouts_and_blocks(self) -> Iterator[GenericLayoutType]:
        """Iterate over all layouts (modelspace and paperspace) and all block definitions."""
        return iter(self.blocks)

    def delete_layout(self, name: str) -> None:
        """Delete paper space layout `name` and all entities owned by this layout.
        Available only for DXF R2000 or later, DXF R12 supports only one
        paperspace, and it can't be deleted.
        """
        if name not in self.layouts:
            raise const.DXFValueError(f"Layout '{name}' does not exist.")
        else:
            self.layouts.delete(name)

    def new_layout(self, name, dxfattribs=None) -> Paperspace:
        """Create a new paperspace layout `name`. Returns a :class:`~ezdxf.layouts.Paperspace`
        object. DXF R12 (AC1009) supports only one paperspace layout, only the active
        paperspace layout is saved, other layouts are dismissed.

        Args:
            name: unique layout name
            dxfattribs: additional DXF attributes for the
                :class:`~ezdxf.entities.layout.DXFLayout` entity

        Raises:
            DXFValueError: paperspace layout `name` already exist

        """
        if name in self.layouts:
            raise const.DXFValueError(f"Layout '{name}' already exists.")
        else:
            return self.layouts.new(name, dxfattribs)

    def page_setup(
        self,
        name: str = "Layout1",
        fmt: str = "ISO A3",
        landscape=True,
    ) -> Paperspace:
        """Creates a new paperspace layout if `name` does not exist or reset the
        existing layout.  This method requires DXF R2000 or newer.
        The paper format name `fmt` defines one of the following paper sizes,
        measures in landscape orientation:

        =========== ======= ======= =======
        Name        Units   Width   Height
        =========== ======= ======= =======
        ISO A0      mm      1189    841
        ISO A1      mm      841     594
        ISO A2      mm      594     420
        ISO A3      mm      420     297
        ISO A4      mm      297     210
        ANSI A      inch    11      8.5
        ANSI B      inch    17      11
        ANSI C      inch    22      17
        ANSI D      inch    34      22
        ANSI E      inch    44      34
        ARCH C      inch    24      18
        ARCH D      inch    36      24
        ARCH E      inch    48      36
        ARCH E1     inch    42      30
        Letter      inch    11      8.5
        Legal       inch    14      8.5
        =========== ======= ======= =======

        The layout uses the associated units of the paper format as drawing
        units, has no margins or offset defined and the scale of the paperspace
        layout is 1:1.

        Args:
            name: paperspace layout name
            fmt: paper format
            landscape: ``True`` for landscape orientation, ``False`` for portrait
                orientation

        """
        from ezdxf.tools.standards import PAGE_SIZES

        if self.acad_release == "R12":
            raise const.DXFVersionError("method call no supported for DXF R12")
        width: float
        height: float
        try:
            units, width, height = PAGE_SIZES[fmt]
        except KeyError:
            raise ValueError(f"unknown paper format: {fmt}")
        if not landscape:
            width, height = height, width
        try:
            psp = self.paperspace(name)
        except KeyError:
            psp = self.new_layout(name)

        psp.page_setup(size=(width, height), margins=(0, 0, 0, 0), units=units)
        return psp

    def acquire_arrow(self, name: str):
        """For standard AutoCAD and ezdxf arrows create block definitions if
        required, otherwise check if block `name` exist. (internal API)

        """
        from ezdxf.render.arrows import ARROWS

        if ARROWS.is_acad_arrow(name) or ARROWS.is_ezdxf_arrow(name):
            ARROWS.create_block(self.blocks, name)
        elif name not in self.blocks:
            raise const.DXFValueError(f'Arrow block "{name}" does not exist.')

    def add_image_def(self, filename: str, size_in_pixel: tuple[int, int], name=None):
        """Add an image definition to the objects section.

        Add an :class:`~ezdxf.entities.image.ImageDef` entity to the drawing
        (objects section). `filename` is the image file name as relative or
        absolute path and `size_in_pixel` is the image size in pixel as (x, y)
        tuple. To avoid dependencies to external packages, `ezdxf` can not
        determine the image size by itself. Returns a
        :class:`~ezdxf.entities.image.ImageDef` entity which is needed to
        create an image reference. `name` is the internal image name, if set to
        ``None``, name is auto-generated.

        Absolute image paths works best for AutoCAD but not perfect, you
        have to update external references manually in AutoCAD, which is not
        possible in TrueView. If the drawing units differ from 1 meter, you
        also have to use: :meth:`set_raster_variables`.

        Args:
            filename: image file name (absolute path works best for AutoCAD)
            size_in_pixel: image size in pixel as (x, y) tuple
            name: image name for internal use, None for using filename as name
                (best for AutoCAD)

        .. seealso::

            :ref:`tut_image`

        """
        if "ACAD_IMAGE_VARS" not in self.rootdict:
            self.objects.set_raster_variables(frame=0, quality=1, units="m")
        if name is None:
            name = filename
        return self.objects.add_image_def(filename, size_in_pixel, name)

    def set_raster_variables(self, frame: int = 0, quality: int = 1, units: str = "m"):
        """Set raster variables.

        Args:
            frame: 0 = do not show image frame; 1 = show image frame
            quality: 0 = draft; 1 = high
            units: units for inserting images. This defines the real world unit
                for one drawing unit for the purpose of inserting and scaling
                images with an associated resolution.

                ===== ===========================
                mm    Millimeter
                cm    Centimeter
                m     Meter (ezdxf default)
                km    Kilometer
                in    Inch
                ft    Foot
                yd    Yard
                mi    Mile
                ===== ===========================

        """
        self.objects.set_raster_variables(frame=frame, quality=quality, units=units)

    def set_wipeout_variables(self, frame=0):
        """Set wipeout variables.

        Args:
            frame: 0 = do not show image frame; 1 = show image frame

        """
        self.objects.set_wipeout_variables(frame=frame)
        var_dict = self.rootdict.get_required_dict("AcDbVariableDictionary")
        var_dict.set_or_add_dict_var("WIPEOUTFRAME", str(frame))

    def add_underlay_def(
        self, filename: str, fmt: str = "ext", name: Optional[str] = None
    ):
        """Add an :class:`~ezdxf.entities.underlay.UnderlayDef` entity to the drawing
        (OBJECTS section). The `filename` is the underlay file name as relative or
        absolute path and `fmt` as string (pdf, dwf, dgn).
        The underlay definition is required to create an underlay reference.

        Args:
            filename: underlay file name
            fmt: file format as string "pdf"|"dwf"|"dgn" or "ext"
                for getting file format from filename extension
            name: pdf format = page number to display; dgn format = "default"; dwf: ????

        .. seealso::

            :ref:`tut_underlay`

        """
        if fmt == "ext":
            fmt = filename[-3:]
        return self.objects.add_underlay_def(filename, fmt, name)

    def add_xref_def(
        self, filename: str, name: str, flags: int = BLK_XREF | BLK_EXTERNAL
    ):
        """Add an external reference (xref) definition to the blocks section.

        Args:
            filename: external reference filename
            name: name of the xref block
            flags: block flags

        """
        self.blocks.new(name=name, dxfattribs={"flags": flags, "xref_path": filename})

    def audit(self) -> Auditor:
        """Checks document integrity and fixes all fixable problems, not
        fixable problems are stored in :attr:`Auditor.errors`.

        If you are messing around with internal structures, call this method
        before saving to be sure to export valid DXF documents, but be aware
        this is a long-running task.

        """
        auditor = Auditor(self)
        auditor.run()
        return auditor

    def validate(self, print_report=True) -> bool:
        """Simple way to run an audit process. Fixes all fixable problems,
        return ``False`` if not fixable errors occurs. Prints a report of resolved and
        unrecoverable errors, if requested.

        Args:
            print_report: print report to stdout

        Returns: ``False`` if unrecoverable errors exist

        """
        auditor = self.audit()
        if print_report:
            auditor.print_fixed_errors()
            auditor.print_error_report()
            if not self.is_acad_compatible:
                print("DXF document is not AutoCAD compatible:")
                for msg in self.acad_incompatibility_reasons:
                    print(msg)
        return len(auditor.errors) == 0

    def set_modelspace_vport(self, height, center=(0, 0), *, dxfattribs=None) -> VPort:
        r"""Set initial view/zoom location for the modelspace, this replaces
        the current "\*Active" viewport configuration
        (:class:`~ezdxf.entities.VPort`) and reset the coordinate system to the
        :ref:`WCS`.

        Args:
             height: modelspace area to view
             center: modelspace location to view in the center of the CAD
                application window.
             dxfattribs: additional DXF attributes for the VPORT entity

        """
        self.viewports.delete_config("*Active")
        dxfattribs = dict(dxfattribs or {})
        vport = cast("VPort", self.viewports.new("*Active", dxfattribs=dxfattribs))
        vport.dxf.center = center
        vport.dxf.height = height
        vport.reset_wcs()
        self.header.reset_wcs()
        return vport


class MetaData(abc.ABC):
    """Manage ezdxf meta-data by dict-like interface. Values are limited to
    strings with a maximum length of 254 characters.
    """

    @abc.abstractmethod
    def __getitem__(self, key: str) -> str:
        """Returns the value for self[`key`].

        Raises:
             KeyError: `key` does not exist
        """
        ...

    def get(self, key: str, default: str = "") -> str:
        """Returns the value for `key`. Returns `default` if `key` not exist."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return safe_string(default, MAX_STR_LEN)

    @abc.abstractmethod
    def __setitem__(self, key: str, value: str) -> None:
        """Set self[`key`] to `value`."""
        ...

    @abc.abstractmethod
    def __delitem__(self, key: str) -> None:
        """Delete self[`key`].

        Raises:
             KeyError: `key` does not exist
        """
        ...

    @abc.abstractmethod
    def __contains__(self, key: str) -> bool:
        """Returns `key` ``in`` self."""
        ...

    def discard(self, key: str) -> None:
        """Remove `key`, does **not** raise an exception if `key` not exist."""
        try:
            self.__delitem__(key)
        except KeyError:
            pass


def ezdxf_marker_string():
    if ezdxf.options.write_fixed_meta_data_for_testing:
        return CONST_MARKER_STRING
    else:
        now = datetime.now(tz=timezone.utc)
        return ezdxf.__version__ + " @ " + now.isoformat()


class R12MetaData(MetaData):
    """Manage ezdxf meta data for DXF version R12 as XDATA of layer "0".

    Layer "0" is mandatory and can not be deleted.

    """

    def __init__(self, doc: Drawing):
        # storing XDATA in layer 0 does not work (Autodesk!)
        self._msp_block = doc.modelspace().block_record.block
        self._data = self._load()

    def __contains__(self, key: str) -> bool:
        return safe_string(key, MAX_STR_LEN) in self._data

    def __getitem__(self, key: str) -> str:
        return self._data[safe_string(key, MAX_STR_LEN)]

    def __setitem__(self, key: str, value: str) -> None:
        self._data[safe_string(key)] = safe_string(value, MAX_STR_LEN)
        self._commit()

    def __delitem__(self, key: str) -> None:
        del self._data[safe_string(key, MAX_STR_LEN)]
        self._commit()

    def _commit(self) -> None:
        # write all metadata as strings with group code 1000
        tags = []
        for key, value in self._data.items():
            tags.append((1000, str(key)))
            tags.append((1000, str(value)))
        self._msp_block.set_xdata("EZDXF", tags)  # type: ignore

    def _load(self) -> dict:
        data = dict()
        if self._msp_block.has_xdata("EZDXF"):  # type: ignore
            xdata = self._msp_block.get_xdata("EZDXF")  # type: ignore
            index = 0
            count = len(xdata) - 1
            while index < count:
                name = xdata[index].value
                data[name] = xdata[index + 1].value
                index += 2
        return data


class R2000MetaData(MetaData):
    """Manage ezdxf metadata for DXF version R2000+ as DICTIONARY object in
    the rootdict.
    """

    def __init__(self, doc: Drawing):
        self._data: Dictionary = doc.rootdict.get_required_dict(
            EZDXF_META, hard_owned=True
        )

    def __contains__(self, key: str) -> bool:
        return safe_string(key, MAX_STR_LEN) in self._data

    def __getitem__(self, key: str) -> str:
        v = self._data[safe_string(key, MAX_STR_LEN)]
        return v.dxf.get("value", "")

    def __setitem__(self, key: str, value: str) -> None:
        self._data.set_or_add_dict_var(
            safe_string(key, MAX_STR_LEN), safe_string(value, MAX_STR_LEN)
        )

    def __delitem__(self, key: str) -> None:
        self._data.remove(safe_string(key, MAX_STR_LEN))


def info(doc: Drawing, verbose=False, content=False, fmt="ASCII") -> list[str]:
    from ezdxf.units import unit_name
    from collections import Counter

    def count(entities, indent="  ") -> Iterator[str]:
        counter: Counter = Counter()
        for e in entities:
            counter[e.dxftype()] += 1
        for name in sorted(counter.keys()):
            yield f"{indent}{name} ({counter[name]})"

    def append_container(table, name: str, container="table"):
        indent = "  "
        data.append(f"{name} {container} entries: {len(table)}")
        if verbose:
            if name == "STYLE":
                names: list[str] = []
                for entry in table:
                    name = entry.dxf.name
                    if name == "":
                        names.append(f'{indent}*shape-file: "{entry.dxf.font}"')
                    else:
                        names.append(f"{indent}{name}")
            else:
                names = [f"{indent}{entry.dxf.name}" for entry in table]
            names.sort()
            data.extend(names)

    def user_vars(kind: str, indent="") -> Iterator[str]:
        for i in range(5):
            name = f"{kind}{i+1}"
            if name in header:
                yield f"{indent}{name}={header[name]}"

    def append_header_var(name: str, indent=""):
        data.append(f"{indent}{name}: {header.get(name, '<undefined>').strip()}")

    header = doc.header
    loaded_dxf_version = doc.loaded_dxfversion
    if loaded_dxf_version is None:
        loaded_dxf_version = doc.dxfversion
    data: list[str] = []
    data.append(f'Filename: "{doc.filename}"')
    data.append(f"Format: {fmt}")
    if loaded_dxf_version != doc.dxfversion:
        msg = f"Loaded content was upgraded from DXF Version {loaded_dxf_version}"
        release = const.acad_release.get(loaded_dxf_version, "")
        if release:
            msg += f" ({release})"
        data.append(msg)
    data.append(f"Release: {doc.acad_release}")
    data.append(f"DXF Version: {doc.dxfversion}")
    if verbose:
        data.append(
            f"Maintenance Version: {header.get('$ACADMAINTVER', '<undefined>')}"
        )
    data.append(f"Codepage: {header.get('$DWGCODEPAGE', 'ANSI_1252')}")
    data.append(f"Encoding: {doc.output_encoding}")
    data.append("Layouts:")
    data.extend([f"  '{name}'" for name in doc.layout_names_in_taborder()])

    measurement = "Metric" if header.get("$MEASUREMENT", 0) else "Imperial"
    if verbose:
        data.append(f"Unit system: {measurement}")
        data.append(f"Modelspace units: {unit_name(doc.units)}")
        append_header_var("$LASTSAVEDBY")
        append_header_var("$HANDSEED")
        append_header_var("$FINGERPRINTGUID")
        append_header_var("$VERSIONGUID")
        data.extend(user_vars(kind="$USERI"))
        data.extend(user_vars(kind="$USERR"))
        for name, value in header.custom_vars:
            data.append(f'Custom property "{name}": "{value}"')

    ezdxf_metadata = doc.ezdxf_metadata()
    if CREATED_BY_EZDXF in ezdxf_metadata:
        data.append(f"Created by ezdxf: {ezdxf_metadata.get(CREATED_BY_EZDXF)}")
    elif verbose:
        data.append("File was not created by ezdxf >= 0.16.4")
    if WRITTEN_BY_EZDXF in ezdxf_metadata:
        data.append(f"Written by ezdxf: {ezdxf_metadata.get(WRITTEN_BY_EZDXF)}")
    elif verbose:
        data.append("File was not written by ezdxf >= 0.16.4")
    if content:
        data.append("Content stats:")
        append_container(doc.layers, "LAYER")
        append_container(doc.linetypes, "LTYPE")
        append_container(doc.styles, "STYLE")
        append_container(doc.dimstyles, "DIMSTYLE")
        append_container(doc.appids, "APPID")
        append_container(doc.ucs, "UCS")
        append_container(doc.views, "VIEW")
        append_container(doc.viewports, "VPORT")
        append_container(doc.block_records, "BLOCK_RECORD")
        if doc.dxfversion > DXF12:
            append_container(list(doc.classes), "CLASS", container="section")
        data.append(f"Entities in modelspace: {len(doc.modelspace())}")
        if verbose:
            data.extend(count(doc.modelspace()))

        data.append(f"Entities in OBJECTS section: {len(doc.objects)}")
        if verbose:
            data.extend(count(doc.objects))

        unknown_entities = _get_unknown_entities(doc)
        if len(unknown_entities):
            data.append(f"Unknown/unsupported entities: {len(unknown_entities)}")
            if verbose:
                data.extend(count(unknown_entities))
    return data


def _get_unknown_entities(doc: Drawing) -> list[DXFEntity]:
    from ezdxf.entities import DXFTagStorage, ACADProxyEntity, OLE2Frame

    data: list[DXFEntity] = []
    for entity in doc.entitydb.values():
        if isinstance(entity, (DXFTagStorage, ACADProxyEntity, OLE2Frame)):
            data.append(entity)
    return data


def custom_export(doc: Drawing, tagwriter: AbstractTagWriter):
    """Export a DXF document via a custom tag writer."""
    dxfversion = doc.dxfversion
    if dxfversion != DXF12:
        tagwriter.write_handles = True
    doc.update_all()
    doc.export_sections(tagwriter)


def export_json_tags(doc: Drawing, compact=True) -> str:
    """Export a DXF document as JSON formatted tags.

    The `compact` format is a list of ``[group-code, value]`` pairs where each pair is
    a DXF tag. The group-code has to be an integer and the value has to be a string,
    integer, float or list of floats for vertices.

    The `verbose` format (`compact` is ``False``) is a list of ``[group-code, value]``
    pairs where each pair is a 1:1 representation of a DXF tag. The group-code has to be
    an integer and the value has to be a string.

    """
    stream = io.StringIO()
    json_writer = JSONTagWriter(stream, dxfversion=doc.dxfversion, compact=compact)
    custom_export(doc, json_writer)
    return stream.getvalue()


def load_json_tags(data: Sequence[Any]) -> Drawing:
    """Load DXF document from JSON formatted tags.

    The expected JSON format is a list of [group-code, value] pairs where each pair is
    a DXF tag. The `compact` and the `verbose` format is supported.

    Args:
        data: JSON data structure as a sequence of [group-code, value] pairs

    """
    from ezdxf.lldxf.tagger import json_tag_loader

    return Drawing.load(json_tag_loader(data))
