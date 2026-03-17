# Copyright (c) 2011-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, cast, Optional
import logging
from ezdxf.lldxf.const import DXFKeyError, DXFValueError, DXFStructureError
from ezdxf.lldxf.const import (
    MODEL_SPACE_R2000,
    PAPER_SPACE_R2000,
    TMP_PAPER_SPACE_NAME,
)
from ezdxf.lldxf.validator import is_valid_table_name
from .layout import Layout, Modelspace, Paperspace
from ezdxf.entities import DXFEntity, BlockRecord

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import Dictionary, DXFLayout

logger = logging.getLogger("ezdxf")


def key(name: str) -> str:
    """AutoCAD uses case-insensitive layout names, but stores the name case-sensitive."""
    return name.upper()


MODEL = key("Model")


class Layouts:
    def __init__(self, doc: Drawing):
        """Default constructor. (internal API)"""
        self.doc = doc
        # Store layout names in normalized form: key(name)
        self._layouts: dict[str, Layout] = {}
        # key: layout name as original case-sensitive string; value: DXFLayout()
        self._dxf_layouts: Dictionary = cast(
            "Dictionary", self.doc.rootdict["ACAD_LAYOUT"]
        )

    @classmethod
    def setup(cls, doc: Drawing):
        """Constructor from scratch. (internal API)"""
        layouts = Layouts(doc)
        layouts.setup_modelspace()
        layouts.setup_paperspace()
        return layouts

    def __len__(self) -> int:
        """Returns count of existing layouts, including the modelspace
        layout."""
        return len(self._layouts)

    def __contains__(self, name: str) -> bool:
        """Returns ``True`` if layout `name` exist."""
        assert isinstance(name, str), type(str)
        return key(name) in self._layouts

    def __iter__(self) -> Iterator[Layout]:
        """Returns iterable of all layouts as :class:`~ezdxf.layouts.Layout`
        objects, including the modelspace layout.
        """
        return iter(self._layouts.values())

    def _add_layout(self, name: str, layout: Layout):
        dxf_layout = layout.dxf_layout
        dxf_layout.dxf.name = name
        dxf_layout.dxf.owner = self._dxf_layouts.dxf.handle
        self._layouts[key(name)] = layout
        self._dxf_layouts[name] = dxf_layout

    def _discard(self, layout: Layout):
        name = layout.name
        self._dxf_layouts.discard(name)
        del self._layouts[key(name)]

    def append_layout(self, layout: Layout) -> None:
        """Append an existing (copied) paperspace layout as last layout tab."""
        index = 1
        base_layout_name = layout.dxf.name
        layout_name = base_layout_name
        while layout_name in self:
            index += 1
            layout_name = base_layout_name + f" ({index})"
        layout.dxf.taborder = len(self._layouts) + 1
        self._add_layout(layout_name, layout)

    def setup_modelspace(self):
        """Modelspace setup. (internal API)"""
        self._new_special(
            Modelspace, "Model", MODEL_SPACE_R2000, dxfattribs={"taborder": 0}
        )

    def setup_paperspace(self):
        """First layout setup. (internal API)"""
        self._new_special(
            Paperspace, "Layout1", PAPER_SPACE_R2000, dxfattribs={"taborder": 1}
        )

    def _new_special(self, cls, name: str, block_name: str, dxfattribs: dict) -> Layout:
        if name in self._layouts:
            raise DXFValueError(f'Layout "{name}" already exists')
        dxfattribs["owner"] = self._dxf_layouts.dxf.handle
        layout = cls.new(name, block_name, self.doc, dxfattribs=dxfattribs)
        self._add_layout(name, layout)
        return layout

    def unique_paperspace_name(self) -> str:
        """Returns a unique paperspace name. (internal API)"""
        blocks = self.doc.blocks
        count = 0
        while "*Paper_Space%d" % count in blocks:
            count += 1
        return "*Paper_Space%d" % count

    def new(self, name: str, dxfattribs=None) -> Paperspace:
        """Returns a new :class:`~ezdxf.layouts.Paperspace` layout.

        Args:
            name: layout name as shown in tabs in :term:`CAD` applications
            dxfattribs: additional DXF attributes for the
                :class:`~ezdxf.entities.layout.DXFLayout` entity

        Raises:
            DXFValueError: Invalid characters in layout name.
            DXFValueError: Layout `name` already exist.

        """
        assert isinstance(name, str), type(str)
        if not is_valid_table_name(name):
            raise DXFValueError("Layout name contains invalid characters.")

        if name in self:
            raise DXFValueError(f'Layout "{name}" already exist.')

        dxfattribs = dict(dxfattribs or {})  # copy attribs
        dxfattribs["owner"] = self._dxf_layouts.dxf.handle
        dxfattribs.setdefault("taborder", len(self._layouts) + 1)
        block_name = self.unique_paperspace_name()
        layout = Paperspace.new(name, block_name, self.doc, dxfattribs=dxfattribs)
        # Default extents are ok!
        # Reset limits to (0, 0) and (paper width, paper height)
        layout.reset_limits()
        self._add_layout(name, layout)
        return layout  # type: ignore

    @classmethod
    def load(cls, doc: "Drawing") -> "Layouts":
        """Constructor if loading from file. (internal API)"""
        layouts = cls(doc)
        layouts.setup_from_rootdict()

        # DXF R12: block/block_record for *Model_Space and *Paper_Space
        # already exist:
        if len(layouts) < 2:  # restore missing DXF Layouts
            layouts.restore("Model", MODEL_SPACE_R2000, taborder=0)
            layouts.restore("Layout1", PAPER_SPACE_R2000, taborder=1)
        return layouts

    def restore(self, name: str, block_record_name: str, taborder: int) -> None:
        """Restore layout from block if DXFLayout does not exist.
        (internal API)"""
        if name in self:
            return
        block_layout = self.doc.blocks.get(block_record_name)
        self._new_from_block_layout(name, block_layout, taborder)

    def _new_from_block_layout(self, name, block_layout, taborder: int) -> "Layout":
        dxfattribs = {
            "owner": self._dxf_layouts.dxf.handle,
            "name": name,
            "block_record_handle": block_layout.block_record_handle,
            "taborder": taborder,
        }
        dxf_layout = cast(
            "DXFLayout",
            self.doc.objects.new_entity("LAYOUT", dxfattribs=dxfattribs),
        )
        if key(name) == MODEL:
            layout = Modelspace.load(dxf_layout, self.doc)
        else:
            layout = Paperspace.load(dxf_layout, self.doc)
        self._add_layout(name, layout)
        return layout

    def setup_from_rootdict(self) -> None:
        """Setup layout manager from root dictionary. (internal API)"""
        layout: Layout
        for name, dxf_layout in self._dxf_layouts.items():
            if isinstance(dxf_layout, str):
                logger.debug(f"ignore missing LAYOUT(#{dxf_layout}) entity '{name}'")
                continue
            if key(name) == MODEL:
                layout = Modelspace(dxf_layout, self.doc)
            else:
                layout = Paperspace(dxf_layout, self.doc)
            # assert name == layout.dxf.name
            self._layouts[key(name)] = layout

    def modelspace(self) -> Modelspace:
        """Returns the :class:`~ezdxf.layouts.Modelspace` layout."""
        return cast(Modelspace, self.get("Model"))

    def names(self) -> list[str]:
        """Returns a list of all layout names, all names in original case-sensitive form."""
        return [layout.name for layout in self._layouts.values()]

    def get(self, name: Optional[str]) -> Layout:
        """Returns :class:`~ezdxf.layouts.Layout` by `name`, case-insensitive
        "Model" == "MODEL".

        Args:
            name: layout name as shown in tab, e.g. ``'Model'`` for modelspace

        """
        name = name or self.names_in_taborder()[1]  # first paperspace layout
        return self._layouts[key(name)]

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename a layout from `old_name` to `new_name`.
        Can not rename layout ``'Model'`` and the new name of a layout must
        not exist.

        Args:
            old_name: actual layout name, case-insensitive
            new_name: new layout name, case-insensitive

        Raises:
            DXFValueError: try to rename ``'Model'``
            DXFValueError: Layout `new_name` already exist.

        """
        assert isinstance(old_name, str), type(old_name)
        assert isinstance(new_name, str), type(new_name)
        if key(old_name) == MODEL:
            raise DXFValueError("Can not rename model space.")
        if new_name in self:
            raise DXFValueError(f'Layout "{new_name}" already exist.')
        if old_name not in self:
            raise DXFValueError(f'Layout "{old_name}" does not exist.')

        layout = self.get(old_name)
        self._discard(layout)
        self._add_layout(new_name, layout)

    def names_in_taborder(self) -> list[str]:
        """Returns all layout names in tab order as shown in :term:`CAD`
        applications."""
        names = [
            (layout.dxf.taborder, layout.name) for layout in self._layouts.values()
        ]
        return [name for order, name in sorted(names)]

    def get_layout_for_entity(self, entity: DXFEntity) -> Layout:
        """Returns the owner layout for a DXF `entity`."""
        owner = entity.dxf.owner
        if owner is None:
            raise DXFKeyError("No associated layout, owner is None.")
        return self.get_layout_by_key(entity.dxf.owner)

    def get_layout_by_key(self, layout_key: str) -> Layout:
        """Returns a layout by its `layout_key`. (internal API)"""
        assert isinstance(layout_key, str), type(layout_key)
        error_msg = f'Layout with key "{layout_key}" does not exist.'
        try:
            block_record = self.doc.entitydb[layout_key]
        except KeyError:
            raise DXFKeyError(error_msg)
        if not isinstance(block_record, BlockRecord):
            # VERTEX, ATTRIB, SEQEND are owned by the parent entity
            raise DXFKeyError(error_msg)
        try:
            dxf_layout = self.doc.entitydb[block_record.dxf.layout]
        except KeyError:
            raise DXFKeyError(error_msg)
        return self.get(dxf_layout.dxf.name)

    def get_active_layout_key(self):
        """Returns layout kay for the active paperspace layout.
        (internal API)"""
        active_layout_block_record = self.doc.block_records.get(PAPER_SPACE_R2000)
        return active_layout_block_record.dxf.handle

    def set_active_layout(self, name: str) -> None:
        """Set layout `name` as active paperspace layout."""
        assert isinstance(name, str), type(name)
        if key(name) == MODEL:  # reserved layout name
            raise DXFValueError("Can not set model space as active layout")
        # raises KeyError if layout 'name' does not exist
        new_active_layout = self.get(name)
        old_active_layout_key = self.get_active_layout_key()
        if old_active_layout_key == new_active_layout.layout_key:
            return  # layout 'name' is already the active layout

        blocks = self.doc.blocks
        new_active_paper_space_name = new_active_layout.block_record_name

        blocks.rename_block(PAPER_SPACE_R2000, TMP_PAPER_SPACE_NAME)
        blocks.rename_block(new_active_paper_space_name, PAPER_SPACE_R2000)
        blocks.rename_block(TMP_PAPER_SPACE_NAME, new_active_paper_space_name)

    def delete(self, name: str) -> None:
        """Delete layout `name` and destroy all entities in that layout.

        Args:
            name (str): layout name as shown in tabs

        Raises:
            DXFKeyError: if layout `name` do not exist
            DXFValueError: deleting modelspace layout is not possible
            DXFValueError: deleting last paperspace layout is not possible

        """
        assert isinstance(name, str), type(name)
        if key(name) == MODEL:
            raise DXFValueError("Can not delete modelspace layout.")

        layout = self.get(name)
        if len(self) < 3:
            raise DXFValueError("Can not delete last paperspace layout.")
        if layout.layout_key == self.get_active_layout_key():
            # Layout `name` is the active layout:
            for layout_name in self._layouts:
                # Set any other paperspace layout as active layout
                if layout_name not in (key(name), MODEL):
                    self.set_active_layout(layout_name)
                    break
        self._discard(layout)
        layout.destroy()

    def active_layout(self) -> Paperspace:
        """Returns the active paperspace layout."""
        for layout in self:
            if layout.is_active_paperspace:
                return cast(Paperspace, layout)
        raise DXFStructureError("No active paperspace layout found.")

    def audit(self, auditor: Auditor):
        from ezdxf.audit import AuditError

        doc = auditor.doc

        # Find/remove orphaned LAYOUT objects:
        layouts = (o for o in doc.objects if o.dxftype() == "LAYOUT")
        for layout in layouts:
            name = layout.dxf.get("name")
            if name not in self:
                auditor.fixed_error(
                    code=AuditError.ORPHANED_LAYOUT_ENTITY,
                    message=f'Removed orphaned {str(layout)} "{name}"',
                )
                doc.objects.delete_entity(layout)

        # Find/remove orphaned paperspace BLOCK_RECORDS named: *Paper_Space...
        psp_br_handles = {
            br.dxf.handle
            for br in doc.block_records
            if br.dxf.name.lower().startswith("*paper_space")
        }
        psp_layout_br_handles = {
            layout.dxf.block_record_handle
            for layout in self._layouts.values()
            if key(layout.name) != MODEL
        }
        mismatch = psp_br_handles.difference(psp_layout_br_handles)
        if len(mismatch):
            for handle in mismatch:
                br = doc.entitydb.get(handle)
                name = br.dxf.get("name")  # type: ignore
                auditor.fixed_error(
                    code=AuditError.ORPHANED_PAPER_SPACE_BLOCK_RECORD_ENTITY,
                    message=f'Removed orphaned layout {str(br)} "{name}"',
                )
                if name in doc.blocks:
                    doc.blocks.delete_block(name, safe=False)
                else:
                    doc.block_records.remove(name)
        # Does not check the LAYOUT content this is done in the BlockSection,
        # because the content of layouts is stored as blocks.
