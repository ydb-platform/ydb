#  Copyright (c) 2023-2024, Manfred Moitzi
#  License: MIT License
""" Resource management module for transferring DXF resources between documents.
"""
from __future__ import annotations
from typing import Optional, Sequence, Callable, Iterable
from typing_extensions import Protocol, TypeAlias
import enum
import pathlib
import logging
import os

import ezdxf
from ezdxf.lldxf import const, validator, types
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.validator import DXFInfo
from ezdxf.document import Drawing
from ezdxf.layouts import BaseLayout, Paperspace, BlockLayout
from ezdxf.entities import (
    is_graphic_entity,
    is_dxf_object,
    DXFEntity,
    DXFClass,
    factory,
    BlockRecord,
    Layer,
    Linetype,
    Textstyle,
    DimStyle,
    UCSTableEntry,
    Material,
    MLineStyle,
    MLeaderStyle,
    Block,
    EndBlk,
    Insert,
    DXFLayout,
    VisualStyle,
)
from ezdxf.entities.copy import CopyStrategy, CopySettings
from ezdxf.math import UVec, Vec3

__all__ = [
    "define",
    "attach",
    "embed",
    "detach",
    "write_block",
    "load_modelspace",
    "load_paperspace",
    "Registry",
    "ResourceMapper",
    "ConflictPolicy",
    "Loader",
    "dxf_info",
    "DXFInfo",
    "XrefError",
    "XrefDefinitionError",
    "EntityError",
    "LayoutError",
]

logger = logging.getLogger("ezdxf")
NO_BLOCK = "0"
DEFAULT_LINETYPES = {"CONTINUOUS", "BYLAYER", "BYBLOCK"}
DEFAULT_LAYER = "0"
STANDARD = "STANDARD"
FilterFunction: TypeAlias = Callable[[DXFEntity], bool]
LoadFunction: TypeAlias = Callable[[str], Drawing]


# I prefer to see the debug messages stored in the object, because I mostly debug test
# code and pytest does not show logging or print messages by default.
def _log_debug_messages(messages: Iterable[str]) -> None:
    for msg in messages:
        logger.debug(msg)


class XrefError(Exception):
    """base exception for the xref module"""

    pass


class XrefDefinitionError(XrefError):
    pass


class EntityError(XrefError):
    pass


class LayoutError(XrefError):
    pass


class InternalError(XrefError):
    pass


class ConflictPolicy(enum.Enum):
    """These conflict policies define how to handle resource name conflicts.

    .. versionadded:: 1.1

    Attributes:
        KEEP: Keeps the existing resource name of the target document and ignore the
            resource from the source document.
        XREF_PREFIX: This policy handles the resource import like CAD applications by
            **always** renaming the loaded resources to `<xref>$0$<name>`, where `xref`
            is the name of source document, the `$0$` part is a number to create a
            unique resource name and `<name>` is the name of the resource itself.
        NUM_PREFIX: This policy renames the loaded resources to `$0$<name>` only if the
            resource `<name>` already exists. The `$0$` prefix is a number to create a
            unique resource name and `<name>` is the name of the resource itself.

    """

    KEEP = enum.auto()
    XREF_PREFIX = enum.auto()
    NUM_PREFIX = enum.auto()


def dxf_info(filename: str | os.PathLike) -> DXFInfo:
    """Scans the HEADER section of a DXF document and returns a :class:`DXFInfo`
    object, which contains information about the DXF version, text encoding, drawing
    units and insertion base point.

    Raises:
        IOError: not a DXF file or a generic IO error

    """
    filename = str(filename)
    if validator.is_binary_dxf_file(filename):
        with open(filename, "rb") as fp:
            # The HEADER section of a DXF R2018 file has a length of ~5300 bytes.
            data = fp.read(8192)
        return validator.binary_dxf_info(data)
    if validator.is_dxf_file(filename):
        # the relevant information has 7-bit ASCII encoding
        with open(filename, "rt", errors="ignore") as fp:
            return validator.dxf_info(fp)
    else:
        raise IOError("Not a DXF files.")


# Exceptions from the ConflictPolicy
# ----------------------------------
# Resources named "STANDARD" will be preserved (KEEP).
# Materials "GLOBAL", "BYLAYER" and "BYBLOCK" will be preserved (KEEP).
# Plot style "NORMAL" will be preserved (KEEP).
# Layers "0", "DEFPOINTS" and special Autodesk layers starting with "*" will be preserved (KEEP).
# Linetypes "CONTINUOUS", "BYLAYER" and "BYBLOCK" will be preserved (KEEP)
# Special blocks like arrow heads will be preserved (KEEP).
# Anonymous blocks get a new arbitrary name following the rules of anonymous block names.

# Notes about DXF files as XREFs
# ------------------------------
# AutoCAD cannot use DXF R12 files as external references (BricsCAD can)!
# AutoCAD may use DXF R2000+ as external references, but does not accept DXF files
# created by ezdxf nor BricsCAD, which opened for itself are total valid DXF documents.
#
# Autodesk DWG TrueView V2022:
# > Error: Unable to load <absolute file path>.
# > Drawing may need recovery.
#
# Using the RECOVER command of BricsCAD and rewriting the DXF files by BricsCAD does
# not work.  Replacing the XREF by a newly created DXF file by BricsCAD does not work
# either.
#
# BricsCAD accepts any DXF/DWG file as XREF!

# Idea for automated object loading from the OBJECTS section:
# -----------------------------------------------------------
# EXT_DICT = Extension  DICTIONARY; ROOT_DICT = "unnamed" DICTIONARY
# Pointer and owner handles in XRECORD or unknown objects and entities have well-defined
# group codes, if the creator app follow the rules of the DXF reference.
#
# Object types:
# A. object owner path ends at a graphical entity, e.g. LINE -> EXT_DICT -> XRECORD
# B. owner path of an object ends at the root dictionary and contains only objects
#    from the OBJECTS section, e.g. ROOT_DICT -> DICTIONARY -> MATERIAL
#    or ROOT_DICT -> XRECORD -> CUSTOM_OBJECT -> XRECORD.
#    The owner path of object type B cannot contain a graphical entity because the owner
#    of a graphical entity is always a BLOCK_RECORD.
# C. owner path ends at an object that does not exist or with an owner handle "0",
#    so the last owner handle of the owner path is invalid, this seems to be an invalid
#    construct
#
# Automated object loading with reconstruction of the owner path:
# ---------------------------------------------------------------
# example MATERIAL object:
# - find the owner path of the source MATERIAL object:
#   ROOT_DICT -> DICTIONARY (material collection) -> MATERIAL
# 1 does the parent object of MATERIAL in the target doc exist:
#   2 YES: add MATERIAL to the owner DICTIONARY (conflict policy!)
#   3 NO:
#     4 create the immediate parent DICTIONARY of MATERIAL in the target dict and add
#       it to the ROOT_DICT of the target doc
#     GOTO 2 add the MATERIAL to the new owner DICTIONARY
#
# Top management layer of the OBJECTS section
# -------------------------------------------
# Create a standard mapping for the ROOT_DICT and its entries (DICTIONARY objects)
# from the source doc to the target doc.  I think these are always basic management
# structures which shouldn't be duplicated.


def define(doc: Drawing, block_name: str, filename: str, overlay=False) -> None:
    """Add an external reference (xref) definition to a document.

    XREF attachment types:

    - attached: the XREF that's inserted into this drawing is also present in a
      document to which this document is inserted as an XREF.
    - overlay: the XREF that's inserted into this document is **not** present in a
      document to which this document is inserted as an XREF.

    Args:
        doc: host document
        block_name: name of the xref block
        filename: external reference filename
        overlay: creates an XREF overlay if ``True`` and an XREF attachment otherwise

    Raises:
        XrefDefinitionError: block with same name exist

    .. versionadded:: 1.1

    """
    if block_name in doc.blocks:
        raise XrefDefinitionError(f"block '{block_name}' already exist")
    doc.blocks.new(
        name=block_name,
        dxfattribs={
            "flags": make_xref_flags(overlay),
            "xref_path": filename,
        },
    )


def make_xref_flags(overlay: bool) -> int:
    if overlay:
        return const.BLK_XREF_OVERLAY | const.BLK_EXTERNAL
    else:
        return const.BLK_XREF | const.BLK_EXTERNAL


def attach(
    doc: Drawing,
    *,
    block_name: str,
    filename: str,
    insert: UVec = (0, 0, 0),
    scale: float = 1.0,
    rotation: float = 0.0,
    overlay=False,
) -> Insert:
    """Attach the file `filename` to the host document as external reference (XREF) and
    creates a default block reference for the XREF in the modelspace of the document.
    The function raises an :class:`XrefDefinitionError` exception if the block definition
    already exist, but an XREF can be inserted multiple times by adding additional block
    references::

        msp.add_blockref(block_name, insert=another_location)

    .. important::

        If the XREF has different drawing units than the host document, the scale
        factor between these units must be applied as a uniform scale factor to the
        block reference!  Unfortunately the XREF drawing units can only be detected by
        scanning the HEADER section of a document by the function :func:`dxf_info` and
        is therefore not done automatically by this function.
        Advice: always use the same units for all drawings of a project!

    Args:
        doc: host DXF document
        block_name: name of the XREF definition block
        filename: file name of the XREF
        insert: location of the default block reference
        scale: uniform scaling factor
        rotation: rotation angle in degrees
        overlay: creates an XREF overlay if ``True`` and an XREF attachment otherwise

    Returns:
        Insert: default block reference for the XREF

    Raises:
        XrefDefinitionError: block with same name exist

    .. versionadded:: 1.1

    """
    define(doc, block_name, filename, overlay=overlay)
    dxfattribs = dict()
    if rotation:
        dxfattribs["rotation"] = float(rotation)
    if scale != 1.0:
        scale = float(scale)
        dxfattribs["xscale"] = scale
        dxfattribs["yscale"] = scale
        dxfattribs["zscale"] = scale
    location = Vec3(insert)
    msp = doc.modelspace()
    return msp.add_blockref(block_name, insert=location, dxfattribs=dxfattribs)


def find_xref(xref_filename: str, search_paths: Sequence[pathlib.Path]) -> pathlib.Path:
    """Returns the path of the XREF file.

    Args:
        xref_filename: filename of the XREF, absolute or relative path
        search_paths: search paths where to look for the XREF file

    .. versionadded:: 1.1

    """
    filepath = pathlib.Path(xref_filename)
    # 1. check absolute xref_filename
    if filepath.exists():
        return filepath

    name = filepath.name
    for path in search_paths:
        if not path.is_dir():
            path = path.parent
        search_path = path.resolve()
        # 2. check relative xref path to search path
        filepath = search_path / xref_filename
        if filepath.exists():
            return filepath
        # 3. check if the file is in the search folder
        filepath = search_path / name
        if filepath.exists():
            return filepath

    return pathlib.Path(xref_filename)


def embed(
    xref: BlockLayout,
    *,
    load_fn: Optional[LoadFunction] = None,
    search_paths: Iterable[pathlib.Path | str] = tuple(),
    conflict_policy=ConflictPolicy.XREF_PREFIX,
) -> None:
    """Loads the modelspace of the XREF as content into a block layout.

    The loader function loads the XREF as `Drawing` object, by default the
    function :func:`ezdxf.readfile` is used to load DXF files. To load DWG files use the
    :func:`~ezdxf.addons.odafc.readfile` function from the :mod:`ezdxf.addons.odafc`
    add-on. The :func:`ezdxf.recover.readfile` function is very robust for reading DXF
    files with errors.

    If the XREF path isn't absolute the XREF is searched in the folder of the host DXF
    document and in the `search_path` folders.

    Args:
        xref: :class:`BlockLayout` of the XREF document
        load_fn: function to load the content of the XREF as `Drawing` object
        search_paths: list of folders to search for XREFS, default is the folder of the
            host document or the current directory if no filepath is set
        conflict_policy: how to resolve name conflicts

    Raises:
        XrefDefinitionError: argument `xref` is not a XREF definition
        FileNotFoundError: XREF file not found
        DXFVersionError: cannot load a XREF with a newer DXF version than the host
            document, try the :mod:`~ezdxf.addons.odafc` add-on to downgrade the XREF
            document or upgrade the host document

    .. versionadded:: 1.1

    """
    assert isinstance(xref, BlockLayout), "expected BLOCK definition of XREF"
    target_doc = xref.doc
    assert target_doc is not None, "valid DXF document required"
    block = xref.block
    assert isinstance(block, Block)
    if not block.is_xref:
        raise XrefDefinitionError("argument 'xref' is not a XREF definition")

    xref_path: str = block.dxf.get("xref_path", "")
    if not xref_path:
        raise XrefDefinitionError("no xref path defined")

    _search_paths = [pathlib.Path(p) for p in search_paths]
    _search_paths.insert(0, target_doc.get_abs_filepath())

    filepath = find_xref(xref_path, _search_paths)
    if not filepath.exists():
        raise FileNotFoundError(f"file not found: '{filepath}'")

    if load_fn:
        source_doc = load_fn(str(filepath))
    else:
        source_doc = ezdxf.readfile(filepath)
    if source_doc.dxfversion > target_doc.dxfversion:
        raise const.DXFVersionError(
            "cannot embed a XREF with a newer DXF version into the host document"
        )
    loader = Loader(source_doc, target_doc, conflict_policy=conflict_policy)
    loader.load_modelspace(xref)
    loader.execute(xref_prefix=xref.name)
    # reset XREF flags:
    block.set_flag_state(const.BLK_XREF | const.BLK_EXTERNAL, state=False)
    # update BLOCK origin:
    origin = source_doc.header.get("$INSBASE")
    if origin:
        block.dxf.base_point = Vec3(origin)


def detach(
    block: BlockLayout, *, xref_filename: str | os.PathLike, overlay=False
) -> Drawing:
    """Write the content of `block` into the modelspace of a new DXF document and
    convert `block` to an external reference (XREF).  The new DXF document has to be
    written by the caller: :code:`xref_doc.saveas(xref_filename)`.
    This way it is possible to convert the DXF document to DWG by the
    :mod:`~ezdxf.addons.odafc` add-on if necessary::

        xref_doc = xref.detach(my_block, "my_block.dwg")
        odafc.export_dwg(xref_doc, "my_block.dwg")

    It's recommended to clean up the entity database of the host document afterwards::

        doc.entitydb.purge()

    The function does not create any block references. These references should already
    exist and do not need to be changed since references to blocks and XREFs are the
    same.

    Args:
        block: block definition to detach
        xref_filename: name of the external referenced file
        overlay: creates an XREF overlay if ``True`` and an XREF attachment otherwise

    .. versionadded:: 1.1

    """
    source_doc = block.doc
    assert source_doc is not None, "valid DXF document required"
    target_doc = ezdxf.new(dxfversion=source_doc.dxfversion, units=block.units)
    loader = Loader(source_doc, target_doc, conflict_policy=ConflictPolicy.KEEP)
    loader.load_block_layout_into(block, target_doc.modelspace())
    loader.execute()
    target_doc.header["$INSBASE"] = block.base_point
    block_to_xref(block, xref_filename, overlay=overlay)
    return target_doc


def block_to_xref(
    block: BlockLayout, xref_filename: str | os.PathLike, *, overlay=False
) -> None:
    """Convert a block definition into an external reference.

    (internal API)
    """
    block.delete_all_entities()
    block_entity = block.block
    assert block_entity is not None, "invalid BlockLayout"
    block_entity.dxf.xref_path = str(xref_filename)
    block_entity.dxf.flags = make_xref_flags(overlay)


def write_block(entities: Sequence[DXFEntity], *, origin: UVec = (0, 0, 0)) -> Drawing:
    """Write `entities` into the modelspace of a new DXF document.

    This function is called "write_block" because the new DXF document can be used as
    an external referenced block.  This function is similar to the WBLOCK command in CAD
    applications.

    Virtual entities are not supported, because each entity needs a real database- and
    owner handle.

    Args:
        entities: DXF entities to write
        origin: block origin, defines the point in the modelspace which will be inserted
            at the insert location of the block reference

    Raises:
        EntityError: virtual entities are not supported

    .. versionadded:: 1.1

    """
    if len(entities) == 0:
        return ezdxf.new()
    if any(e.dxf.owner is None for e in entities):
        raise EntityError("virtual entities are not supported")
    source_doc = entities[0].doc
    assert source_doc is not None, "expected a valid source document"
    target_doc = ezdxf.new(dxfversion=source_doc.dxfversion, units=source_doc.units)
    loader = Loader(source_doc, target_doc)
    loader.add_command(LoadEntities(entities, target_doc.modelspace()))
    loader.execute()
    target_doc.header["$INSBASE"] = Vec3(origin)
    return target_doc


def load_modelspace(
    sdoc: Drawing,
    tdoc: Drawing,
    filter_fn: Optional[FilterFunction] = None,
    conflict_policy=ConflictPolicy.KEEP,
) -> None:
    """Loads the modelspace content of the source document into the modelspace
    of the target document.  The filter function `filter_fn` gets every source entity as
    input and returns ``True`` to load the entity or ``False`` otherwise.

    Args:
        sdoc: source document
        tdoc: target document
        filter_fn: optional function to filter entities from the source modelspace
        conflict_policy: how to resolve name conflicts

    .. versionadded:: 1.1

    """
    loader = Loader(sdoc, tdoc, conflict_policy=conflict_policy)
    loader.load_modelspace(filter_fn=filter_fn)
    loader.execute()


def load_paperspace(
    psp: Paperspace,
    tdoc: Drawing,
    filter_fn: Optional[FilterFunction] = None,
    conflict_policy=ConflictPolicy.KEEP,
) -> None:
    """Loads the paperspace layout `psp` into the target document.  The filter function
    `filter_fn` gets every source entity as input and returns ``True`` to load the
    entity or ``False`` otherwise.

    Args:
        psp: paperspace layout to load
        tdoc: target document
        filter_fn: optional function to filter entities from the source paperspace layout
        conflict_policy: how to resolve name conflicts

    .. versionadded:: 1.1

    """
    if psp.doc is tdoc:
        raise LayoutError("Source paperspace layout cannot be from target document.")
    loader = Loader(psp.doc, tdoc, conflict_policy=conflict_policy)
    loader.load_paperspace_layout(psp, filter_fn=filter_fn)
    loader.execute()


class Registry(Protocol):
    def add_entity(self, entity: DXFEntity, block_key: str = NO_BLOCK) -> None:
        ...

    def add_block(self, block_record: BlockRecord) -> None:
        ...

    def add_handle(self, handle: Optional[str]) -> None:
        ...

    def add_layer(self, name: str) -> None:
        ...

    def add_linetype(self, name: str) -> None:
        ...

    def add_text_style(self, name: str) -> None:
        ...

    def add_dim_style(self, name: str) -> None:
        ...

    def add_block_name(self, name: str) -> None:
        ...

    def add_appid(self, name: str) -> None:
        ...


class ResourceMapper(Protocol):
    def get_handle(self, handle: str, default="0") -> str:
        ...

    def get_reference_of_copy(self, handle: str) -> Optional[DXFEntity]:
        ...

    def get_layer(self, name: str) -> str:
        ...

    def get_linetype(self, name: str) -> str:
        ...

    def get_text_style(self, name: str) -> str:
        ...

    def get_dim_style(self, name: str) -> str:
        ...

    def get_block_name(self, name: str) -> str:
        ...

    def map_resources_of_copy(self, entity: DXFEntity) -> None:
        ...

    def map_pointers(self, tags: Tags, new_owner_handle: str = "") -> None:
        ...

    def map_acad_dict_entry(
        self, dict_name: str, entry_name: str, entity: DXFEntity
    ) -> tuple[str, DXFEntity]:
        ...

    def map_existing_handle(
        self, source: DXFEntity, clone: DXFEntity, attrib_name: str, *, optional=False
    ) -> None:
        ...


class LoadingCommand:
    def register_resources(self, registry: Registry) -> None:
        pass

    def execute(self, transfer: _Transfer) -> None:
        pass


class LoadEntities(LoadingCommand):
    """Loads all given entities into the target layout."""

    def __init__(
        self, entities: Sequence[DXFEntity], target_layout: BaseLayout
    ) -> None:
        self.entities = entities
        if not isinstance(target_layout, BaseLayout):
            raise LayoutError(f"invalid target layout type: {type(target_layout)}")
        self.target_layout = target_layout

    def register_resources(self, registry: Registry) -> None:
        for e in self.entities:
            registry.add_entity(e, block_key=e.dxf.owner)

    def execute(self, transfer: _Transfer) -> None:
        target_layout = self.target_layout
        for entity in self.entities:
            clone = transfer.get_entity_copy(entity)
            if clone is None:
                transfer.debug(f"xref:cannot copy {str(entity)}")
                continue
            if is_graphic_entity(clone):
                target_layout.add_entity(clone)  # type: ignore
            else:
                transfer.debug(
                    f"found non-graphic entity {str(clone)} as layout content"
                )
        if isinstance(target_layout, Paperspace):
            _reorganize_paperspace_viewports(target_layout)


def _reorganize_paperspace_viewports(paperspace: Paperspace) -> None:
    main_vp = paperspace.main_viewport()
    if main_vp is None:
        main_vp = paperspace.add_new_main_viewport()
    # destroy loaded main VIEWPORT entities
    for vp in paperspace.viewports():
        if vp.dxf.id == 1 and vp is not main_vp:
            paperspace.delete_entity(vp)
    paperspace.set_current_viewport_handle(main_vp.dxf.handle)


class LoadPaperspaceLayout(LoadingCommand):
    """Loads a paperspace layout as a new paperspace layout into the target document.
    If a paperspace layout with same name already exists the layout will be renamed
    to  "<layout name> (x)" where x is 2 or the next free number.
    """

    def __init__(self, psp: Paperspace, filter_fn: Optional[FilterFunction]) -> None:
        if not isinstance(psp, Paperspace):
            raise LayoutError(f"invalid paperspace layout type: {type(psp)}")
        self.paperspace_layout = psp
        self.filter_fn = filter_fn

    def source_entities(self) -> list[DXFEntity]:
        filter_fn = self.filter_fn
        if filter_fn:
            return [e for e in self.paperspace_layout if filter_fn(e)]
        else:
            return list(self.paperspace_layout)

    def register_resources(self, registry: Registry) -> None:
        registry.add_entity(self.paperspace_layout.dxf_layout)
        block_key = self.paperspace_layout.layout_key
        for e in self.source_entities():
            registry.add_entity(e, block_key=block_key)

    def execute(self, transfer: _Transfer) -> None:
        source_dxf_layout = self.paperspace_layout.dxf_layout
        target_dxf_layout = transfer.get_reference_of_copy(source_dxf_layout.dxf.handle)
        assert isinstance(target_dxf_layout, DXFLayout)
        target_layout = transfer.registry.target_doc.paperspace(
            target_dxf_layout.dxf.name
        )
        for entity in self.source_entities():
            clone = transfer.get_entity_copy(entity)
            if clone and is_graphic_entity(clone):
                target_layout.add_entity(clone)  # type: ignore
            else:
                transfer.debug(
                    f"found non-graphic entity {str(clone)} as layout content"
                )
        _reorganize_paperspace_viewports(target_layout)


class LoadBlockLayout(LoadingCommand):
    """Loads a block layout as a new block layout into the target document. If a block
    layout with the same name exists the conflict policy will be applied.
    """

    def __init__(self, block: BlockLayout) -> None:
        if not isinstance(block, BlockLayout):
            raise LayoutError(f"invalid block layout type: {type(block)}")
        self.block_layout = block

    def register_resources(self, registry: Registry) -> None:
        block_record = self.block_layout.block_record
        if isinstance(block_record, BlockRecord):
            registry.add_entity(block_record)


class LoadResources(LoadingCommand):
    """Loads table entries into the target document. If a table entry with the same name
    exists the conflict policy will be applied.
    """

    def __init__(self, entities: Sequence[DXFEntity]) -> None:
        self.entities = entities

    def register_resources(self, registry: Registry) -> None:
        for e in self.entities:
            registry.add_entity(e, block_key=NO_BLOCK)


class Loader:
    """Load entities and resources from the source DXF document `sdoc` into the
    target DXF document.

    Args:
        sdoc: source DXF document
        tdoc: target DXF document
        conflict_policy: :class:`ConflictPolicy`

    """

    def __init__(
        self, sdoc: Drawing, tdoc: Drawing, conflict_policy=ConflictPolicy.KEEP
    ) -> None:
        assert isinstance(sdoc, Drawing), "a valid source document is mandatory"
        assert isinstance(tdoc, Drawing), "a valid target document is mandatory"
        assert sdoc is not tdoc, "source and target document cannot be the same"
        if tdoc.dxfversion < sdoc.dxfversion:
            logger.warning(
                "target document has older DXF version than the source document"
            )
        self.sdoc: Drawing = sdoc
        self.tdoc: Drawing = tdoc
        self.conflict_policy = conflict_policy
        self._commands: list[LoadingCommand] = []

    def add_command(self, command: LoadingCommand) -> None:
        self._commands.append(command)

    def load_modelspace(
        self,
        target_layout: Optional[BaseLayout] = None,
        filter_fn: Optional[FilterFunction] = None,
    ) -> None:
        """Loads the content of the modelspace of the source document into a layout of
        the target document, the modelspace of the target document is the default target
        layout. The filter function `filter_fn` is used to skip source entities, the
        function should return ``False`` for entities to ignore and ``True`` otherwise.

        Args:
            target_layout: target layout can be any layout: modelspace, paperspace
                layout or block layout.
            filter_fn: function to filter source entities

        """
        if target_layout is None:
            target_layout = self.tdoc.modelspace()
        elif not isinstance(target_layout, BaseLayout):
            raise LayoutError(f"invalid target layout type: {type(target_layout)}")
        if target_layout.doc is not self.tdoc:
            raise LayoutError(
                f"given target layout does not belong to the target document"
            )
        if filter_fn is None:
            entities = list(self.sdoc.modelspace())
        else:
            entities = [e for e in self.sdoc.modelspace() if filter_fn(e)]
        self.add_command(LoadEntities(entities, target_layout))

    def load_paperspace_layout(
        self,
        psp: Paperspace,
        filter_fn: Optional[FilterFunction] = None,
    ) -> None:
        """Loads a paperspace layout as a new paperspace layout into the target document.
        If a paperspace layout with same name already exists the layout will be renamed
        to  "<layout name> (2)" or "<layout name> (3)" and so on.  The filter function
        `filter_fn` is used to skip source entities, the function should return ``False``
        for entities to ignore and ``True`` otherwise.

        The content of the modelspace which may be displayed through a VIEWPORT entity
        will **not** be loaded!

        Args:
            psp: the source paperspace layout
            filter_fn: function to filter source entities

        """
        if not isinstance(psp, Paperspace):
            raise const.DXFTypeError(f"invalid paperspace layout type: {type(psp)}")
        if psp.doc is not self.sdoc:
            raise LayoutError(
                f"given paperspace layout does not belong to the source document"
            )
        self.add_command(LoadPaperspaceLayout(psp, filter_fn))

    def load_paperspace_layout_into(
        self,
        psp: Paperspace,
        target_layout: BaseLayout,
        filter_fn: Optional[FilterFunction] = None,
    ) -> None:
        """Loads the content of a paperspace layout into an existing layout of the target
        document.  The filter function `filter_fn` is used to skip source entities, the
        function should return ``False`` for entities to ignore and ``True`` otherwise.

        The content of the modelspace which may be displayed through a
        VIEWPORT entity will **not** be loaded!

        Args:
            psp: the source paperspace layout
            target_layout: target layout can be any layout: modelspace, paperspace
                layout or block layout.
            filter_fn: function to filter source entities

        """
        if not isinstance(psp, Paperspace):
            raise LayoutError(f"invalid paperspace layout type: {type(psp)}")
        if not isinstance(target_layout, BaseLayout):
            raise LayoutError(f"invalid target layout type: {type(target_layout)}")
        if psp.doc is not self.sdoc:
            raise LayoutError(
                f"given paperspace layout does not belong to the source document"
            )
        if target_layout.doc is not self.tdoc:
            raise LayoutError(
                f"given target layout does not belong to the target document"
            )
        if filter_fn is None:
            entities = list(psp)
        else:
            entities = [e for e in psp if filter_fn(e)]
        self.add_command(LoadEntities(entities, target_layout))

    def load_block_layout(
        self,
        block_layout: BlockLayout,
    ) -> None:
        """Loads a block layout (block definition) as a new block layout into the target
        document. If a block layout with the same name exists the conflict policy will
        be applied.  This method cannot load modelspace or paperspace layouts.

        Args:
            block_layout: the source block layout

        """
        if not isinstance(block_layout, BlockLayout):
            raise LayoutError(f"invalid block layout type: {type(block_layout)}")
        if block_layout.doc is not self.sdoc:
            raise LayoutError(
                f"given block layout does not belong to the source document"
            )
        self.add_command(LoadBlockLayout(block_layout))

    def load_block_layout_into(
        self,
        block_layout: BlockLayout,
        target_layout: BaseLayout,
    ) -> None:
        """Loads the content of a block layout (block definition) into an existing layout
        of the target document.  This method cannot load the content of
        modelspace or paperspace layouts.

        Args:
            block_layout: the source block layout
            target_layout: target layout can be any layout: modelspace, paperspace
                layout or block layout.

        """
        if not isinstance(block_layout, BlockLayout):
            raise LayoutError(f"invalid block layout type: {type(block_layout)}")
        if not isinstance(target_layout, BaseLayout):
            raise LayoutError(f"invalid target layout type: {type(target_layout)}")
        if block_layout.doc is not self.sdoc:
            raise LayoutError(
                f"given block layout does not belong to the source document"
            )
        if target_layout.doc is not self.tdoc:
            raise LayoutError(
                f"given target layout does not belong to the target document"
            )
        self.add_command(LoadEntities(list(block_layout), target_layout))

    def load_layers(self, names: Sequence[str]) -> None:
        """Loads the layers defined by the argument `names` into the target document.
        In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.layers)
        self.add_command(LoadResources(entities))

    def load_linetypes(self, names: Sequence[str]) -> None:
        """Loads the linetypes defined by the argument `names` into the target document.
        In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.linetypes)
        self.add_command(LoadResources(entities))

    def load_text_styles(self, names: Sequence[str]) -> None:
        """Loads the TEXT styles defined by the argument `names` into the target document.
        In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.styles)
        self.add_command(LoadResources(entities))

    def load_dim_styles(self, names: Sequence[str]) -> None:
        """Loads the DIMENSION styles defined by the argument `names` into the target
        document. In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.dimstyles)
        self.add_command(LoadResources(entities))

    def load_mline_styles(self, names: Sequence[str]) -> None:
        """Loads the MLINE styles defined by the argument `names` into the target
        document. In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.mline_styles)
        self.add_command(LoadResources(entities))

    def load_mleader_styles(self, names: Sequence[str]) -> None:
        """Loads the MULTILEADER styles defined by the argument `names` into the target
        document. In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.mleader_styles)
        self.add_command(LoadResources(entities))

    def load_materials(self, names: Sequence[str]) -> None:
        """Loads the MATERIALS defined by the argument `names` into the target
        document. In the case of a name conflict the conflict policy will be applied.
        """
        entities = _get_table_entries(names, self.sdoc.materials)
        self.add_command(LoadResources(entities))

    def execute(self, xref_prefix: str = "") -> None:
        """Execute all loading commands. The `xref_prefix` string is used as XREF name
        when the conflict policy :attr:`ConflictPolicy.XREF_PREFIX` is applied.
        """
        registry = _Registry(self.sdoc, self.tdoc)
        debug = ezdxf.options.debug

        for cmd in self._commands:
            cmd.register_resources(registry)

        if debug:
            _log_debug_messages(registry.debug_messages)

        cpm = CopyMachine(self.tdoc)

        cpm.copy_blocks(registry.source_blocks)
        transfer = _Transfer(
            registry=registry,
            copies=cpm.copies,
            objects=cpm.objects,
            handle_mapping=cpm.handle_mapping,
            conflict_policy=self.conflict_policy,
            copy_errors=cpm.copy_errors,
        )
        if xref_prefix:
            transfer.xref_prefix = str(xref_prefix)
        transfer.add_object_copies(cpm.objects)
        transfer.register_classes(cpm.classes)
        transfer.register_table_resources()
        transfer.register_object_resources()
        transfer.redirect_handle_mapping()
        transfer.map_object_resources()
        transfer.map_entity_resources()
        transfer.copy_settings()

        for cmd in self._commands:
            cmd.execute(transfer)
        transfer.finalize()


def _get_table_entries(names: Iterable[str], table) -> list[DXFEntity]:
    entities: list[DXFEntity] = []
    for name in names:
        try:
            entry = table.get(name)
            if entry:
                entities.append(entry)
        except const.DXFTableEntryError:
            pass
    return entities


class _Registry:
    def __init__(self, sdoc: Drawing, tdoc: Drawing) -> None:
        self.source_doc = sdoc
        self.target_doc = tdoc

        # source_blocks:
        # - key is the owner handle (layout key)
        # - value is a dict(): key is source-entity-handle, value is source-entity
        #   Storing the entities to copy in a dict() guarantees that each entity is only
        #   copied once and a dict() preserves the order which a set() doesn't and
        #   that is nice for testing.
        # - entry NO_BLOCK (layout key "0") contains table entries and DXF objects
        self.source_blocks: dict[str, dict[str, DXFEntity]] = {NO_BLOCK: {}}
        self.appids: set[str] = set()
        self.debug_messages: list[str] = []

    def debug(self, msg: str) -> None:
        self.debug_messages.append(msg)

    def add_entity(self, entity: DXFEntity, block_key: str = NO_BLOCK) -> None:
        assert entity is not None, "internal error: entity is None"
        block = self.source_blocks.setdefault(block_key, {})
        entity_handle = entity.dxf.handle
        if entity_handle in block:
            return
        block[entity_handle] = entity
        entity.register_resources(self)

    def add_block(self, block_record: BlockRecord) -> None:
        # add resource entity BLOCK_RECORD to NO_BLOCK
        self.add_entity(block_record)
        # block content in block <block_key>
        block_handle = block_record.dxf.handle
        self.add_entity(block_record.block, block_handle)  # type: ignore
        for entity in block_record.entity_space:
            self.add_entity(entity, block_handle)
        self.add_entity(block_record.endblk, block_handle)  # type: ignore

    def add_handle(self, handle: Optional[str]) -> None:
        """Add resource by handle (table entry or object), cannot add graphic entities.

        Raises:
            EntityError: cannot add graphic entity

        """
        if handle is None or handle == "0":
            return
        entity = self.source_doc.entitydb.get(handle)
        if entity is None:
            self.debug(f"source entity #{handle} does not exist")
            return
        if is_graphic_entity(entity):
            raise EntityError(f"cannot add graphic entity: {str(entity)}")
        self.add_entity(entity)

    def add_layer(self, name: str) -> None:
        if name == DEFAULT_LAYER:
            # Layer name "0" gets never mangled and always exist in the target document.
            return
        try:
            layer = self.source_doc.layers.get(name)
            self.add_entity(layer)
        except const.DXFTableEntryError:
            self.debug(f"source layer '{name}' does not exist")

    def add_linetype(self, name: str) -> None:
        # These linetype names get never mangled and always exist in the target document.
        if name.upper() in DEFAULT_LINETYPES:
            return
        try:
            linetype = self.source_doc.linetypes.get(name)
            self.add_entity(linetype)
        except const.DXFTableEntryError:
            self.debug(f"source linetype '{name}' does not exist")

    def add_text_style(self, name) -> None:
        try:
            text_style = self.source_doc.styles.get(name)
            self.add_entity(text_style)
        except const.DXFTableEntryError:
            self.debug(f"source text style '{name}' does not exist")

    def add_dim_style(self, name: str) -> None:
        try:
            dim_style = self.source_doc.dimstyles.get(name)
            self.add_entity(dim_style)
        except const.DXFTableEntryError:
            self.debug(f"source dimension style '{name}' does not exist")

    def add_block_name(self, name: str) -> None:
        try:
            block_record = self.source_doc.block_records.get(name)
            self.add_entity(block_record)
        except const.DXFTableEntryError:
            self.debug(f"source block '{name}' does not exist")

    def add_appid(self, name: str) -> None:
        self.appids.add(name.upper())


class _Transfer:
    def __init__(
        self,
        registry: _Registry,
        copies: dict[str, dict[str, DXFEntity]],
        objects: dict[str, DXFEntity],
        handle_mapping: dict[str, str],
        *,
        conflict_policy=ConflictPolicy.KEEP,
        copy_errors: set[str],
    ) -> None:
        self.registry = registry
        # entry NO_BLOCK (layout key "0") contains table entries
        self.copied_blocks = copies
        self.copied_objects = objects
        self.copy_errors = copy_errors
        self.conflict_policy = conflict_policy
        self.xref_prefix = get_xref_name(registry.source_doc)
        self.layer_mapping: dict[str, str] = {}
        self.linetype_mapping: dict[str, str] = {}
        self.text_style_mapping: dict[str, str] = {}
        self.dim_style_mapping: dict[str, str] = {}
        self.block_name_mapping: dict[str, str] = {}
        self.handle_mapping: dict[str, str] = handle_mapping
        self._replace_handles: dict[str, str] = {}
        self.debug_messages: list[str] = []

    def debug(self, msg: str) -> None:
        self.debug_messages.append(msg)

    def get_handle(self, handle: str, default="0") -> str:
        return self.handle_mapping.get(handle, default)

    def get_reference_of_copy(self, handle: str) -> Optional[DXFEntity]:
        handle_of_copy = self.handle_mapping.get(handle)
        if handle_of_copy:
            return self.registry.target_doc.entitydb.get(handle_of_copy)
        return None

    def get_layer(self, name: str) -> str:
        return self.layer_mapping.get(name, name)

    def get_linetype(self, name: str) -> str:
        return self.linetype_mapping.get(name, name)

    def get_text_style(self, name: str) -> str:
        return self.text_style_mapping.get(name, name)

    def get_dim_style(self, name: str) -> str:
        return self.dim_style_mapping.get(name, name)

    def get_block_name(self, name: str) -> str:
        return self.block_name_mapping.get(name, name)

    def get_entity_copy(self, entity: DXFEntity) -> Optional[DXFEntity]:
        """Returns the copy of graphic entities."""
        try:
            return self.copied_blocks[entity.dxf.owner][entity.dxf.handle]
        except KeyError:
            pass
        return None

    def map_resources_of_copy(self, entity: DXFEntity) -> None:
        clone = self.get_entity_copy(entity)
        if clone:
            entity.map_resources(clone, self)
        elif entity.dxf.handle in self.copy_errors:
            pass
        else:
            raise InternalError(f"copy of {entity} not found")

    def map_pointers(self, tags: Tags, new_owner_handle: str = "") -> None:
        for index, tag in enumerate(tags):
            if types.is_translatable_pointer(tag):
                handle = self.get_handle(tag.value, default="0")
                tags[index] = types.DXFTag(tag.code, handle)
                if new_owner_handle and types.is_hard_owner(tag):
                    copied_object = self.registry.target_doc.entitydb.get(handle)
                    if copied_object is None:
                        continue
                    copied_object.dxf.owner = new_owner_handle

    def map_acad_dict_entry(
        self, dict_name: str, entry_name: str, entity: DXFEntity
    ) -> tuple[str, DXFEntity]:
        """Map and add `entity` to a top level ACAD dictionary `dict_name` in root
        dictionary.
        """
        tdoc = self.registry.target_doc
        acad_dict = tdoc.rootdict.get_required_dict(dict_name)
        existing_entry = acad_dict.get(entry_name)

        # DICTIONARY entries are only renamed if they exist, this is different to TABLE
        # entries:
        if isinstance(existing_entry, DXFEntity):
            if self.conflict_policy == ConflictPolicy.KEEP:
                return entry_name, existing_entry
            elif self.conflict_policy == ConflictPolicy.XREF_PREFIX:
                entry_name = get_unique_dict_key(
                    entry_name, self.xref_prefix, acad_dict
                )
            elif self.conflict_policy == ConflictPolicy.NUM_PREFIX:
                entry_name = get_unique_dict_key(entry_name, "", acad_dict)

        loaded_entry = self.get_reference_of_copy(entity.dxf.handle)
        if loaded_entry is None:
            return "", entity
        acad_dict.add(entry_name, loaded_entry)  # type: ignore
        loaded_entry.dxf.owner = acad_dict.dxf.handle
        return entry_name, loaded_entry

    def map_existing_handle(
        self, source: DXFEntity, clone: DXFEntity, attrib_name: str, *, optional=False
    ) -> None:
        """Map handle attribute if the original handle exist and the references entity
        was really copied.
        """
        handle = source.dxf.get(attrib_name, "")
        if not handle:
            return  # handle does not exist in source entity
        new_handle = self.get_handle(handle)
        if new_handle and new_handle != "0":  # copied entity exist
            clone.dxf.set(attrib_name, new_handle)
        else:
            if optional:  # discard handle attribute
                clone.dxf.discard(attrib_name)
            else:  # set mandatory attribute to null-ptr
                clone.dxf.set(attrib_name, "0")

    def register_table_resources(self) -> None:
        """Register copied table-entries in resource tables of the target document."""
        self.register_appids()
        # process copied table-entries, layout key is "0":
        for source_entity_handle, entity in self.copied_blocks[NO_BLOCK].items():
            if entity.dxf.owner is not None:
                continue  # already processed!

            # add copied table-entries to tables in the target document
            if isinstance(entity, Layer):
                self.add_layer_entry(entity)
            elif isinstance(entity, Linetype):
                self.add_linetype_entry(entity)
            elif isinstance(entity, Textstyle):
                if entity.is_shape_file:
                    self.add_shape_file_entry(entity)
                else:
                    self.add_text_style_entry(entity)
            elif isinstance(entity, DimStyle):
                self.add_dim_style_entry(entity)
            elif isinstance(entity, BlockRecord):
                self.add_block_record_entry(entity, source_entity_handle)
            elif isinstance(entity, UCSTableEntry):
                self.add_ucs_entry(entity)

    def register_object_resources(self) -> None:
        """Register copied objects in object collections of the target document."""
        # Note: BricsCAD does not rename conflicting entries in object collections and
        # always keeps the existing entry:
        # - MATERIAL
        # - MLINESTYLE
        # - MLEADERSTYLE
        # Ezdxf does rename conflicting entries according to self.conflict_policy,
        # exceptions are only the system entries.
        tdoc = self.registry.target_doc
        for _, entity in self.copied_objects.items():
            if isinstance(entity, Material):
                self.add_collection_entry(
                    tdoc.materials,
                    entity,
                    system_entries={"GLOBAL", "BYLAYER", "BYBLOCK"},
                )
            elif isinstance(entity, MLineStyle):
                self.add_collection_entry(
                    tdoc.mline_styles,
                    entity,
                    system_entries={
                        STANDARD,
                    },
                )
            elif isinstance(entity, MLeaderStyle):
                self.add_collection_entry(
                    tdoc.mleader_styles,
                    entity,
                    system_entries={
                        STANDARD,
                    },
                )
            elif isinstance(entity, VisualStyle):
                self.add_visualstyle_entry(entity)
            elif isinstance(entity, DXFLayout):
                self.create_empty_paperspace_layout(entity)
            # TODO:
            #  add GROUP to ACAD_GROUP dictionary
            #  add SCALE to ACAD_SCALELIST dictionary
            #  add TABLESTYLE to ACAD_TABLESTYLE dictionary

    def replace_handle_mapping(self, old_target, new_target) -> None:
        self._replace_handles[old_target] = new_target

    def redirect_handle_mapping(self) -> None:
        """Redirect handle mapping from copied entity to a handle of an existing entity
        in the target document.
        """
        temp_mapping: dict[str, str] = {}
        replace_handles = self._replace_handles
        # redirect source entity -> new target entity
        for source_handle, target_handle in self.handle_mapping.items():
            if target_handle in replace_handles:
                # build temp mapping, while iterating dict
                temp_mapping[source_handle] = replace_handles[target_handle]

        for source_handle, new_target_handle in temp_mapping.items():
            self.handle_mapping[source_handle] = new_target_handle

    def register_appids(self) -> None:
        tdoc = self.registry.target_doc
        for appid in self.registry.appids:
            try:
                tdoc.appids.new(appid)
            except const.DXFTableEntryError:
                pass

    def register_classes(self, classes: Sequence[DXFClass]) -> None:
        self.registry.target_doc.classes.register(classes)

    def map_entity_resources(self) -> None:
        source_db = self.registry.source_doc.entitydb
        for block_key, block in self.copied_blocks.items():
            for source_entity_handle, clone in block.items():
                source_entity = source_db.get(source_entity_handle)
                if source_entity is None:
                    raise InternalError("database error, source entity not found")
                if clone is not None and clone.is_alive:
                    source_entity.map_resources(clone, self)

    def map_object_resources(self) -> None:
        source_db = self.registry.source_doc.entitydb
        for source_object_handle, clone in self.copied_objects.items():
            source_entity = source_db.get(source_object_handle)
            if source_entity is None:
                raise InternalError("database error, source object not found")
            if clone is not None and clone.is_alive:
                source_entity.map_resources(clone, self)

    def add_layer_entry(self, layer: Layer) -> None:
        tdoc = self.registry.target_doc
        layer_name = layer.dxf.name.upper()

        # special layers - only copy if not exist
        if layer_name in ("0", "DEFPOINTS") or validator.is_adsk_special_layer(
            layer_name
        ):
            try:
                special = tdoc.layers.get(layer_name)
            except const.DXFTableEntryError:
                special = None
            if special:
                # map copied layer handle to existing special layer
                self.replace_handle_mapping(layer.dxf.handle, special.dxf.handle)
                layer.destroy()
                return
        old_name = layer.dxf.name
        self.add_table_entry(tdoc.layers, layer)
        if layer.is_alive:
            self.layer_mapping[old_name] = layer.dxf.name

    def add_linetype_entry(self, linetype: Linetype) -> None:
        tdoc = self.registry.target_doc
        if linetype.dxf.name.upper() in DEFAULT_LINETYPES:
            standard = tdoc.linetypes.get(linetype.dxf.name)
            self.replace_handle_mapping(linetype.dxf.handle, standard.dxf.handle)
            linetype.destroy()
            return
        old_name = linetype.dxf.name
        self.add_table_entry(tdoc.linetypes, linetype)
        if linetype.is_alive:
            self.linetype_mapping[old_name] = linetype.dxf.name

    def add_text_style_entry(self, text_style: Textstyle) -> None:
        tdoc = self.registry.target_doc
        old_name = text_style.dxf.name
        self.add_table_entry(tdoc.styles, text_style)
        if text_style.is_alive:
            self.text_style_mapping[old_name] = text_style.dxf.name

    def add_shape_file_entry(self, text_style: Textstyle) -> None:
        # A shape file (SHX file) entry is a special text style entry which name is "".
        shape_file_name = text_style.dxf.font
        if not shape_file_name:
            return
        tdoc = self.registry.target_doc
        shape_file = tdoc.styles.find_shx(shape_file_name)
        if shape_file is None:
            shape_file = tdoc.styles.add_shx(shape_file_name)
        self.replace_handle_mapping(text_style.dxf.handle, shape_file.dxf.handle)

    def add_dim_style_entry(self, dim_style: DimStyle) -> None:
        tdoc = self.registry.target_doc
        old_name = dim_style.dxf.name
        self.add_table_entry(tdoc.dimstyles, dim_style)
        if dim_style.is_alive:
            self.dim_style_mapping[old_name] = dim_style.dxf.name

    def add_block_record_entry(self, block_record: BlockRecord, handle: str) -> None:
        tdoc = self.registry.target_doc
        block_name = block_record.dxf.name.upper()
        old_name = block_record.dxf.name

        # is anonymous block name?
        if len(block_name) > 1 and block_name[0] == "*":
            # anonymous block names are always translated to another non-existing
            # anonymous block name in the target document of the same type:
            # e.g. "*D01" -> "*D0815"
            block_record.dxf.name = tdoc.blocks.anonymous_block_name(block_name[1])
            tdoc.block_records.add_entry(block_record)
        else:
            # Standard arrow blocks are handled the same way as every other block,
            # tested with BricsCAD V23:
            # e.g. arrow block "_Dot" is loaded as "<xref-name>$0$_Dot"
            self.add_table_entry(tdoc.block_records, block_record)

        if block_record.is_alive:
            self.block_name_mapping[old_name] = block_record.dxf.name
            self.restore_block_content(block_record, handle)
            tdoc.blocks.add(block_record)  # create BlockLayout

    def restore_block_content(self, block_record: BlockRecord, handle: str) -> None:
        content = self.copied_blocks.get(handle, dict())
        block: Optional[Block] = None
        endblk: Optional[EndBlk] = None
        for entity in content.values():
            if isinstance(entity, (Block, EndBlk)):
                if isinstance(entity, Block):
                    block = entity
                else:
                    endblk = entity
            elif is_graphic_entity(entity):
                block_record.add_entity(entity)  # type: ignore
            else:
                name = block_record.dxf.name
                msg = f"skipping non-graphic DXF entity in BLOCK_RECORD('{name}', #{handle}): {str(entity)}"
                logging.warning(msg)  # this is a DXF structure error
                self.debug(msg)
        if isinstance(block, Block) and isinstance(endblk, EndBlk):
            block_record.set_block(block, endblk)
        else:
            raise InternalError("invalid BLOCK_RECORD copy")

    def add_ucs_entry(self, ucs: UCSTableEntry) -> None:
        # name mapping is not supported for UCS table entries
        tdoc = self.registry.target_doc
        self.add_table_entry(tdoc.ucs, ucs)

    def add_table_entry(self, table, entity: DXFEntity) -> None:
        name = entity.dxf.name
        if self.conflict_policy == ConflictPolicy.KEEP:
            if table.has_entry(name):
                existing_entry = table.get(name)
                self.replace_handle_mapping(
                    entity.dxf.handle, existing_entry.dxf.handle
                )
                entity.destroy()
                return
        elif self.conflict_policy == ConflictPolicy.XREF_PREFIX:
            # always rename
            entity.dxf.name = get_unique_table_name(name, self.xref_prefix, table)
        elif self.conflict_policy == ConflictPolicy.NUM_PREFIX:
            if table.has_entry(name):  # rename only if exist
                entity.dxf.name = get_unique_table_name(name, "", table)
        table.add_entry(entity)

    def add_collection_entry(
        self, collection, entry: DXFEntity, system_entries: set[str]
    ) -> None:
        # Note: BricsCAD does not rename conflicting entries in object collections and
        # always keeps the existing entry:
        # - MATERIAL
        # - MLINESTYLE
        # - MLEADERSTYLE
        # Ezdxf does rename conflicting entries according to self.conflict_policy,
        # exceptions are only the given `system_entries`.
        name = entry.dxf.name
        if name.upper() in system_entries:
            special = collection.object_dict.get(name)
            if special:
                self.replace_handle_mapping(entry.dxf.handle, special.dxf.handle)
                entry.destroy()
                return
        if self.conflict_policy == ConflictPolicy.KEEP:
            existing_entry = collection.get(name)
            if existing_entry:
                self.replace_handle_mapping(entry.dxf.handle, existing_entry.dxf.handle)
                entry.destroy()
                return
        elif self.conflict_policy == ConflictPolicy.XREF_PREFIX:
            # always rename
            entry.dxf.name = get_unique_table_name(name, self.xref_prefix, collection)
        elif self.conflict_policy == ConflictPolicy.NUM_PREFIX:
            if collection.has_entry(name):  # rename only if exist
                entry.dxf.name = get_unique_table_name(name, "", collection)
        collection.object_dict.add(entry.dxf.name, entry)
        # a resource collection is hard owner
        entry.dxf.owner = collection.handle

    def add_visualstyle_entry(self, visualstyle: VisualStyle) -> None:
        visualstyle_dict = self.registry.target_doc.rootdict.get_required_dict(
            "ACAD_VISUALSTYLE"
        )
        name = visualstyle.dxf.description
        existing_entry = visualstyle_dict.get(name)
        if existing_entry:  # keep existing
            self.replace_handle_mapping(
                visualstyle.dxf.handle, existing_entry.dxf.handle
            )
            visualstyle.destroy()
        else:  # add new entry; rename policy is not supported
            visualstyle_dict.take_ownership(name, visualstyle)

    def create_empty_paperspace_layout(self, layout: DXFLayout) -> None:
        tdoc = self.registry.target_doc
        # The layout content will not be copied automatically!
        # create new empty block layout:
        block_name = tdoc.layouts.unique_paperspace_name()
        block_layout = tdoc.blocks.new(block_name)
        # link block layout and layout entity:
        layout.dxf.block_record_handle = block_layout.block_record_handle
        block_layout.block_record.dxf.layout = layout.dxf.handle

        paperspace = Paperspace(layout, tdoc)
        tdoc.layouts.append_layout(paperspace)

    def add_object_copies(self, copies: dict[str, DXFEntity]) -> None:
        """Add copied DXF objects to the OBJECTS section of the target document."""
        objects = self.registry.target_doc.objects
        for _, obj in copies.items():
            if obj and obj.is_alive:
                objects.add_object(obj)  # type: ignore

    def copy_settings(self):
        self.copy_raster_vars()
        self.copy_wipeout_vars()

    def copy_raster_vars(self):
        sdoc = self.registry.source_doc
        tdoc = self.registry.target_doc
        if (
            "ACAD_IMAGE_VARS" not in sdoc.rootdict  # not required
            or "ACAD_IMAGE_VARS" in tdoc.rootdict  # do not replace existing values
        ):
            return
        frame, quality, units = sdoc.objects.get_raster_variables()
        tdoc.objects.set_raster_variables(frame=frame, quality=quality, units=units)

    def copy_wipeout_vars(self):
        sdoc = self.registry.source_doc
        tdoc = self.registry.target_doc
        if (
            "ACAD_WIPEOUT_VARS" not in sdoc.rootdict  # not required
            or "ACAD_WIPEOUT_VARS" in tdoc.rootdict  # do not replace existing values
        ):
            return
        tdoc.objects.set_wipeout_variables(
            frame=sdoc.objects.get_wipeout_frame_setting()
        )

    def finalize(self) -> None:
        # remove replaced entities:
        self.registry.target_doc.entitydb.purge()
        for msg in self.debug_messages:
            logger.log(logging.INFO, msg)


def get_xref_name(doc: Drawing) -> str:
    if doc.filename:
        return pathlib.Path(doc.filename).stem
    return ""


def is_anonymous_block_name(name: str) -> bool:
    return len(name) > 1 and name.startswith("*")


def get_unique_table_name(name: str, xref: str, table) -> str:
    index: int = 0
    while True:
        new_name = f"{xref}${index}${name}"
        if not table.has_entry(new_name):
            return new_name
        index += 1


def get_unique_dict_key(key: str, xref: str, dictionary) -> str:
    index: int = 0
    while True:
        new_key = f"{xref}${index}${key}"
        if new_key not in dictionary:
            return new_key
        index += 1


class CopyMachine:
    def __init__(self, tdoc: Drawing) -> None:
        self.target_doc = tdoc
        self.copies: dict[str, dict[str, DXFEntity]] = {}
        self.classes: list[DXFClass] = []
        self.objects: dict[str, DXFEntity] = {}
        self.copy_errors: set[str] = set()
        self.copy_strategy = CopyStrategy(CopySettings(set_source_of_copy=False))

        # mapping from the source entity handle to the handle of the copied entity
        self.handle_mapping: dict[str, str] = {}

    def copy_blocks(self, blocks: dict[str, dict[str, DXFEntity]]) -> None:
        for handle, block in blocks.items():
            self.copies[handle] = self.copy_block(block)

    def copy_block(self, block: dict[str, DXFEntity]) -> dict[str, DXFEntity]:
        copies: dict[str, DXFEntity] = {}
        tdoc = self.target_doc
        handle_mapping = self.handle_mapping

        for handle, entity in block.items():
            if isinstance(entity, DXFClass):
                self.copy_dxf_class(entity)
                continue
            clone = self.copy_entity(entity)
            if clone is None:
                continue
            factory.bind(clone, tdoc)
            handle_mapping[handle] = clone.dxf.handle
            # Get handle mapping for in-object copies: DICTIONARY
            if hasattr(entity, "get_handle_mapping"):
                self.handle_mapping.update(entity.get_handle_mapping(clone))

            if is_dxf_object(clone):
                self.objects[handle] = clone
            else:
                copies[handle] = clone
        return copies

    def copy_entity(self, entity: DXFEntity) -> Optional[DXFEntity]:
        try:
            return entity.copy(copy_strategy=self.copy_strategy)
        except const.DXFError:
            self.copy_errors.add(entity.dxf.handle)
        return None

    def copy_dxf_class(self, cls: DXFClass) -> None:
        self.classes.append(cls.copy(copy_strategy=self.copy_strategy))
