# Purpose: Import data from another DXF document
# Copyright (c) 2013-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, cast, Union, Optional
import logging
from ezdxf.lldxf import const
from ezdxf.render.arrows import ARROWS
from ezdxf.entities import (
    DXFEntity,
    DXFGraphic,
    Hatch,
    Polyline,
    DimStyle,
    Dimension,
    Viewport,
    Linetype,
    Insert,
)

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.layouts import BaseLayout, Layout

logger = logging.getLogger("ezdxf")

IMPORT_TABLES = ["linetypes", "layers", "styles", "dimstyles"]
IMPORT_ENTITIES = {
    "LINE",
    "POINT",
    "CIRCLE",
    "ARC",
    "TEXT",
    "SOLID",
    "TRACE",
    "3DFACE",
    "SHAPE",
    "POLYLINE",
    "ATTRIB",
    "INSERT",
    "ELLIPSE",
    "MTEXT",
    "LWPOLYLINE",
    "SPLINE",
    "HATCH",
    "MESH",
    "XLINE",
    "RAY",
    "ATTDEF",
    "DIMENSION",
    "LEADER",  # dimension style override not supported!
    "VIEWPORT",
}


class Importer:
    """
    The :class:`Importer` class is central element for importing data from
    other DXF documents.

    Args:
        source: source :class:`~ezdxf.drawing.Drawing`
        target: target :class:`~ezdxf.drawing.Drawing`

    Attributes:
        source: source DXF document
        target: target DXF document
        used_layers: Set of used layer names as string, AutoCAD accepts layer
            names without a LAYER table entry.
        used_linetypes: Set of used linetype names as string, these linetypes
            require a TABLE entry or AutoCAD will crash.
        used_styles: Set of used text style names, these text styles require
            a TABLE entry or AutoCAD will crash.
        used_dimstyles: Set of used dimension style names, these dimension
            styles require a TABLE entry or AutoCAD will crash.

    """

    def __init__(self, source: Drawing, target: Drawing):
        self.source: Drawing = source
        self.target: Drawing = target

        self.used_layers: set[str] = set()
        self.used_linetypes: set[str] = set()
        self.used_styles: set[str] = set()
        self.used_shape_files: set[str] = set()  # style entry without a name!
        self.used_dimstyles: set[str] = set()
        self.used_arrows: set[str] = set()
        self.handle_mapping: dict[str, str] = dict()  # old_handle: new_handle

        # collects all imported INSERT entities, for later name resolving.
        self.imported_inserts: list[DXFEntity] = list()  # imported inserts

        # collects all imported block names and their assigned new name
        # imported_block[original_name] = new_name
        self.imported_blocks: dict[str, str] = dict()
        self._default_plotstyle_handle = target.plotstyles["Normal"].dxf.handle
        self._default_material_handle = target.materials["Global"].dxf.handle

    def _add_used_resources(self, entity: DXFEntity) -> None:
        """Register used resources."""
        self.used_layers.add(entity.get_dxf_attrib("layer", "0"))
        self.used_linetypes.add(entity.get_dxf_attrib("linetype", "BYLAYER"))
        if entity.is_supported_dxf_attrib("style"):
            self.used_styles.add(entity.get_dxf_attrib("style", "Standard"))
        if entity.is_supported_dxf_attrib("dimstyle"):
            self.used_dimstyles.add(entity.get_dxf_attrib("dimstyle", "Standard"))

    def _add_dimstyle_resources(self, dimstyle: DimStyle) -> None:
        self.used_styles.add(dimstyle.get_dxf_attrib("dimtxsty", "Standard"))
        self.used_linetypes.add(dimstyle.get_dxf_attrib("dimltype", "BYLAYER"))
        self.used_linetypes.add(dimstyle.get_dxf_attrib("dimltex1", "BYLAYER"))
        self.used_linetypes.add(dimstyle.get_dxf_attrib("dimltex2", "BYLAYER"))
        self.used_arrows.add(dimstyle.get_dxf_attrib("dimblk", ""))
        self.used_arrows.add(dimstyle.get_dxf_attrib("dimblk1", ""))
        self.used_arrows.add(dimstyle.get_dxf_attrib("dimblk2", ""))
        self.used_arrows.add(dimstyle.get_dxf_attrib("dimldrblk", ""))

    def _add_linetype_resources(self, linetype: Linetype) -> None:
        if not linetype.pattern_tags.is_complex_type():
            return
        style_handle = linetype.pattern_tags.get_style_handle()
        style = self.source.entitydb.get(style_handle)
        if style is None:
            return
        if style.dxf.name == "":
            # Shape file entries have no name!
            self.used_shape_files.add(style.dxf.font)
        else:
            self.used_styles.add(style.dxf.name)

    def import_tables(
        self, table_names: Union[str, Iterable[str]] = "*", replace=False
    ) -> None:
        """Import DXF tables from the source document into the target document.

        Args:
            table_names: iterable of tables names as strings, or a single table
                name as string or "*" for all supported tables
            replace: ``True`` to replace already existing table entries else
                ignore existing entries

        Raises:
            TypeError: unsupported table type

        """
        if isinstance(table_names, str):
            if table_names == "*":  # import all supported tables
                table_names = IMPORT_TABLES
            else:  # import one specific table
                table_names = (table_names,)
        for table_name in table_names:
            self.import_table(table_name, entries="*", replace=replace)

    def import_table(
        self, name: str, entries: Union[str, Iterable[str]] = "*", replace=False
    ) -> None:
        """
        Import specific table entries from the source document into the
        target document.

        Args:
            name: valid table names are "layers", "linetypes" and "styles"
            entries: Iterable of table names as strings, or a single table name
                or "*" for all table entries
            replace: ``True`` to replace the already existing table entry else
                ignore existing entries

        Raises:
            TypeError: unsupported table type

        """
        if name not in IMPORT_TABLES:
            raise TypeError(f'Table "{name}" import not supported.')
        source_table = getattr(self.source.tables, name)
        target_table = getattr(self.target.tables, name)

        if isinstance(entries, str):
            if entries == "*":  # import all table entries
                entries = (entry.dxf.name for entry in source_table)
            else:  # import just one table entry
                entries = (entries,)
        for entry_name in entries:
            try:
                table_entry = source_table.get(entry_name)
            except const.DXFTableEntryError:
                logger.warning(
                    f'Required table entry "{entry_name}" in table f{name} '
                    f"not found."
                )
                continue
            entry_name = table_entry.dxf.name
            if entry_name in target_table:
                if replace:
                    logger.debug(
                        f'Replacing already existing entry "{entry_name}" '
                        f"of {name} table."
                    )
                    target_table.remove(table_entry.dxf.name)
                else:
                    logger.debug(
                        f'Discarding already existing entry "{entry_name}" '
                        f"of {name} table."
                    )
                    continue

            if name == "layers":
                self.used_linetypes.add(
                    table_entry.get_dxf_attrib("linetype", "Continuous")
                )
            elif name == "dimstyles":
                self._add_dimstyle_resources(table_entry)
            elif name == "linetypes":
                self._add_linetype_resources(table_entry)

            # Duplicate table entry:
            new_table_entry = self._duplicate_table_entry(table_entry)
            target_table.add_entry(new_table_entry)

            # Register resource handles for mapping:
            self.handle_mapping[table_entry.dxf.handle] = new_table_entry.dxf.handle

    def import_shape_files(self, fonts: set[str]) -> None:
        """Import shape file table entries from the source document into the
        target document.
        Shape file entries are stored in the styles table but without a name.

        """
        for font in fonts:
            table_entry = self.source.styles.find_shx(font)
            # copy is not necessary, just create a new entry:
            new_table_entry = self.target.styles.get_shx(font)
            if table_entry:
                # Register resource handles for mapping:
                self.handle_mapping[table_entry.dxf.handle] = new_table_entry.dxf.handle
            else:
                logger.warning(f'Required shape file entry "{font}" not found.')

    def _set_table_entry_dxf_attribs(self, entity: DXFEntity) -> None:
        entity.doc = self.target
        if entity.dxf.hasattr("plotstyle_handle"):
            entity.dxf.plotstyle_handle = self._default_plotstyle_handle
        if entity.dxf.hasattr("material_handle"):
            entity.dxf.material_handle = self._default_material_handle

    def _duplicate_table_entry(self, entry: DXFEntity) -> DXFEntity:
        # duplicate table entry
        new_entry = new_clean_entity(entry)
        self._set_table_entry_dxf_attribs(entry)

        # create a new handle and add entity to target entity database
        self.target.entitydb.add(new_entry)
        return new_entry

    def import_entity(
        self, entity: DXFEntity, target_layout: Optional[BaseLayout] = None
    ) -> None:
        """
        Imports a single DXF `entity` into `target_layout` or the modelspace
        of the target document, if `target_layout` is ``None``.

        Args:
            entity: DXF entity to import
            target_layout: any layout (modelspace, paperspace or block) from
                the target document

        Raises:
            DXFStructureError: `target_layout` is not a layout of target document

        """

        def set_dxf_attribs(e):
            e.doc = self.target
            # remove invalid resources
            e.dxf.discard("plotstyle_handle")
            e.dxf.discard("material_handle")
            e.dxf.discard("visualstyle_handle")

        if target_layout is None:
            target_layout = self.target.modelspace()
        elif target_layout.doc != self.target:
            raise const.DXFStructureError(
                "Target layout has to be a layout or block from the target " "document."
            )

        dxftype = entity.dxftype()
        if dxftype not in IMPORT_ENTITIES:
            logger.debug(f"Import of {str(entity)} not supported")
            return
        self._add_used_resources(entity)

        try:
            new_entity = cast(DXFGraphic, new_clean_entity(entity))
        except const.DXFTypeError:
            logger.debug(f"Copying for DXF type {dxftype} not supported.")
            return

        set_dxf_attribs(new_entity)
        self.target.entitydb.add(new_entity)
        target_layout.add_entity(new_entity)

        try:  # additional processing
            getattr(self, "_import_" + dxftype.lower())(new_entity)
        except AttributeError:
            pass

    def _import_insert(self, insert: Insert):
        self.imported_inserts.append(insert)
        # remove all possible source document dependencies from sub entities
        for attrib in insert.attribs:
            remove_dependencies(attrib)

    def _import_polyline(self, polyline: Polyline):
        # remove all possible source document dependencies from sub entities
        for vertex in polyline.vertices:
            remove_dependencies(vertex)

    def _import_hatch(self, hatch: Hatch):
        hatch.dxf.discard("associative")

    def _import_viewport(self, viewport: Viewport):
        viewport.dxf.discard("sun_handle")
        viewport.dxf.discard("clipping_boundary_handle")
        viewport.dxf.discard("ucs_handle")
        viewport.dxf.discard("ucs_base_handle")
        viewport.dxf.discard("background_handle")
        viewport.dxf.discard("shade_plot_handle")
        viewport.dxf.discard("visual_style_handle")
        viewport.dxf.discard("ref_vp_object_1")
        viewport.dxf.discard("ref_vp_object_2")
        viewport.dxf.discard("ref_vp_object_3")
        viewport.dxf.discard("ref_vp_object_4")

    def _import_dimension(self, dimension: Dimension):
        if dimension.virtual_block_content:
            for entity in dimension.virtual_block_content:
                if isinstance(entity, Insert):  # import arrow blocks
                    self.import_block(entity.dxf.name, rename=False)
                self._add_used_resources(entity)
        else:
            logger.error("The required geometry block for DIMENSION is not defined.")

    def import_entities(
        self,
        entities: Iterable[DXFEntity],
        target_layout: Optional[BaseLayout] = None,
    ) -> None:
        """Import all `entities` into `target_layout` or the modelspace of the
        target document, if `target_layout` is ``None``.

        Args:
            entities: Iterable of DXF entities
            target_layout: any layout (modelspace, paperspace or block) from
                the target document

        Raises:
            DXFStructureError: `target_layout` is not a layout of target document

        """
        for entity in entities:
            self.import_entity(entity, target_layout)

    def import_modelspace(self, target_layout: Optional[BaseLayout] = None) -> None:
        """Import all entities from source modelspace into `target_layout` or
        the modelspace of the target document, if `target_layout` is ``None``.

        Args:
            target_layout: any layout (modelspace, paperspace or block) from
                the target document

        Raises:
            DXFStructureError: `target_layout` is not a layout of target document

        """
        self.import_entities(self.source.modelspace(), target_layout=target_layout)

    def recreate_source_layout(self, name: str) -> Layout:
        """Recreate source paperspace layout `name` in the target document.
        The layout will be renamed if `name` already exist in the target
        document. Returns target modelspace for layout name "Model".

        Args:
            name: layout name as string

        Raises:
            KeyError: if source layout `name` not exist

        """

        def get_target_name():
            tname = name
            base_name = name
            count = 1
            while tname in self.target.layouts:
                tname = base_name + str(count)
                count += 1

            return tname

        def clear(dxfattribs: dict) -> dict:
            def discard(name: str):
                try:
                    del dxfattribs[name]
                except KeyError:
                    pass

            discard("handle")
            discard("owner")
            discard("taborder")
            discard("shade_plot_handle")
            discard("block_record_handle")
            discard("viewport_handle")
            discard("ucs_handle")
            discard("base_ucs_handle")
            return dxfattribs

        if name.lower() == "model":
            return self.target.modelspace()

        source_layout = self.source.layouts.get(name)  # raises KeyError
        target_name = get_target_name()
        dxfattribs = clear(source_layout.dxf_layout.dxfattribs())
        target_layout = self.target.layouts.new(target_name, dxfattribs=dxfattribs)
        return target_layout

    def import_paperspace_layout(self, name: str) -> Layout:
        """Import paperspace layout `name` into the target document.

        Recreates the source paperspace layout in the target document, renames
        the target paperspace if a paperspace with same `name` already exist
        and imports all entities from the source paperspace into the target
        paperspace.

        Args:
            name: source paper space name as string

        Returns: new created target paperspace :class:`Layout`

        Raises:
            KeyError: source paperspace does not exist
            DXFTypeError: invalid modelspace import

        """
        if name.lower() == "model":
            raise const.DXFTypeError(
                "Can not import modelspace, use method import_modelspace()."
            )
        source_layout = self.source.layouts.get(name)
        target_layout = self.recreate_source_layout(name)
        self.import_entities(source_layout, target_layout)
        return target_layout

    def import_paperspace_layouts(self) -> None:
        """Import all paperspace layouts and their content into the target
        document.
        Target layouts will be renamed if a layout with the same name already
        exist. Layouts will be imported in original tab order.

        """
        for name in self.source.layouts.names_in_taborder():
            if name.lower() != "model":  # do not import modelspace
                self.import_paperspace_layout(name)

    def import_blocks(self, block_names: Iterable[str], rename=False) -> None:
        """Import all BLOCK definitions from source document.

        If a BLOCK already exist the BLOCK will be renamed if argument
        `rename` is ``True``, otherwise the existing BLOCK in the target
        document will be used instead of the BLOCK from the source document.
        Required name resolving for imported BLOCK references (INSERT), will be
        done in the :meth:`Importer.finalize` method.

        Args:
            block_names: names of BLOCK definitions to import
            rename: rename BLOCK if a BLOCK with the same name already exist in
                target document

        Raises:
            ValueError: BLOCK in source document not found (defined)

        """
        for block_name in block_names:
            self.import_block(block_name, rename=rename)

    def import_block(self, block_name: str, rename=True) -> str:
        """Import one BLOCK definition from source document.

        If the BLOCK already exist the BLOCK will be renamed if argument
        `rename` is ``True``, otherwise the existing BLOCK in the target
        document will be used instead of the BLOCK in the source document.
        Required name resolving for imported block references (INSERT), will be
        done in the :meth:`Importer.finalize` method.

        To replace an existing BLOCK in the target document, just delete it
        before importing data:
        :code:`target.blocks.delete_block(block_name, safe=False)`

        Args:
            block_name: name of BLOCK to import
            rename: rename BLOCK if a BLOCK with the same name already exist in
                target document

        Returns: (renamed) BLOCK name

        Raises:
            ValueError: BLOCK in source document not found (defined)

        """

        def get_new_block_name() -> str:
            num = 0
            name = block_name
            while name in target_blocks:
                name = block_name + str(num)
                num += 1
            return name

        try:  # already imported block?
            return self.imported_blocks[block_name]
        except KeyError:
            pass

        try:
            source_block = self.source.blocks[block_name]
        except const.DXFKeyError:
            raise ValueError(f'Source block "{block_name}" not found.')

        target_blocks = self.target.blocks
        if (block_name in target_blocks) and (rename is False):
            self.imported_blocks[block_name] = block_name
            return block_name

        new_block_name = get_new_block_name()
        block = source_block.block
        assert block is not None
        target_block = target_blocks.new(
            new_block_name,
            base_point=block.dxf.base_point,
            dxfattribs={
                "description": block.dxf.description,
                "flags": block.dxf.flags,
                "xref_path": block.dxf.xref_path,
            },
        )
        self.import_entities(source_block, target_layout=target_block)
        self.imported_blocks[block_name] = new_block_name
        return new_block_name

    def _create_missing_arrows(self):
        """Create or import required arrow blocks, used by the LEADER or the
        DIMSTYLE entity, which are not imported automatically because they are
        not used in an anonymous DIMENSION geometry BLOCK.

        """
        self.used_arrows.discard(
            ""
        )  # standard default arrow '' needs no block definition
        for arrow_name in self.used_arrows:
            if ARROWS.is_acad_arrow(arrow_name):
                self.target.acquire_arrow(arrow_name)
            else:
                self.import_block(arrow_name, rename=False)

    def _resolve_inserts(self) -> None:
        """Resolve BLOCK names of imported BLOCK reference entities (INSERT).

        This is required for the case the name of the imported BLOCK collides
        with an already existing BLOCK in the target document and the conflict
        resolving method is "rename".

        """
        while len(self.imported_inserts):
            inserts = list(self.imported_inserts)
            # clear imported inserts, block import may append additional inserts
            self.imported_inserts = []
            for insert in inserts:
                block_name = self.import_block(insert.dxf.name)
                insert.dxf.name = block_name

    def _import_required_table_entries(self) -> None:
        """Import required table entries collected while importing entities
        into the target document.

        """
        # 1. dimstyles import adds additional required linetype and style
        # resources and required arrows
        if len(self.used_dimstyles):
            self.import_table("dimstyles", self.used_dimstyles)

        # 2. layers import adds additional required linetype resources
        if len(self.used_layers):
            self.import_table("layers", self.used_layers)

        # 3. complex linetypes adds additional required style resources
        if len(self.used_linetypes):
            self.import_table("linetypes", self.used_linetypes)

        # 4. Text styles do not add additional required resources
        if len(self.used_styles):
            self.import_table("styles", self.used_styles)

        # 5. Shape files are text style entries without a name
        if len(self.used_shape_files):
            self.import_shape_files(self.used_shape_files)

        # 6. Update text style handles of imported complex linetypes:
        self.update_complex_linetypes()

    def _add_required_complex_linetype_resources(self):
        for ltype_name in self.used_linetypes:
            try:
                ltype = self.source.linetypes.get(ltype_name)
            except const.DXFTableEntryError:
                continue
            self._add_linetype_resources(ltype)

    def update_complex_linetypes(self):
        std_handle = self.target.styles.get("STANDARD").dxf.handle
        for linetype in self.target.linetypes:
            if linetype.pattern_tags.is_complex_type():
                old_handle = linetype.pattern_tags.get_style_handle()
                new_handle = self.handle_mapping.get(old_handle, std_handle)
                linetype.pattern_tags.set_style_handle(new_handle)

    def finalize(self) -> None:
        """Finalize the import by importing required table entries and BLOCK
        definitions, without finalization the target document is maybe invalid
        for AutoCAD. Call the :meth:`~Importer.finalize()` method as last step
        of the import process.

        """
        self._resolve_inserts()
        self._add_required_complex_linetype_resources()
        self._import_required_table_entries()
        self._create_missing_arrows()


def new_clean_entity(entity: DXFEntity, keep_xdata: bool = False) -> DXFEntity:
    """Copy entity and remove all external dependencies.

    Args:
        entity: DXF entity
        keep_xdata: keep xdata flag

    """
    new_entity = entity.copy()
    new_entity.doc = None
    return remove_dependencies(new_entity, keep_xdata=keep_xdata)


def remove_dependencies(entity: DXFEntity, keep_xdata: bool = False) -> DXFEntity:
    """Remove all external dependencies.

    Args:
        entity: DXF entity
        keep_xdata: keep xdata flag

    """
    entity.appdata = None
    entity.reactors = None
    entity.extension_dict = None
    if not keep_xdata:
        entity.xdata = None
    return entity
