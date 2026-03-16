# Copyright (c) 2017-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    TextIO,
    Any,
    Optional,
    Callable,
)
import sys
from enum import IntEnum
from ezdxf.lldxf import const, validator
from ezdxf.entities import factory, DXFEntity
from ezdxf.math import NULLVEC

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFGraphic
    from ezdxf.sections.blocks import BlocksSection


__all__ = ["Auditor", "AuditError", "audit", "BlockCycleDetector"]


class AuditError(IntEnum):
    # DXF structure errors:
    MISSING_REQUIRED_ROOT_DICT_ENTRY = 1
    DUPLICATE_TABLE_ENTRY_NAME = 2
    POINTER_TARGET_NOT_EXIST = 3
    TABLE_NOT_FOUND = 4
    MISSING_SECTION_TAG = 5
    MISSING_SECTION_NAME_TAG = 6
    MISSING_ENDSEC_TAG = 7
    FOUND_TAG_OUTSIDE_SECTION = 8
    REMOVED_UNSUPPORTED_SECTION = 9
    REMOVED_UNSUPPORTED_TABLE = 10
    REMOVED_INVALID_GRAPHIC_ENTITY = 11
    REMOVED_INVALID_DXF_OBJECT = 12
    REMOVED_STANDALONE_ATTRIB_ENTITY = 13
    MISPLACED_ROOT_DICT = 14
    ROOT_DICT_NOT_FOUND = 15
    REMOVED_ENTITY_WITH_INVALID_OWNER_HANDLE = 16
    MODELSPACE_NOT_FOUND = 17
    ACTIVE_PAPERSPACE_LAYOUT_NOT_FOUND = 18

    UNDEFINED_LINETYPE = 100
    UNDEFINED_DIMENSION_STYLE = 101
    UNDEFINED_TEXT_STYLE = 102
    UNDEFINED_BLOCK = 103
    INVALID_BLOCK_REFERENCE_CYCLE = 104
    REMOVE_EMPTY_GROUP = 105
    GROUP_ENTITIES_IN_DIFFERENT_LAYOUTS = 106
    MISSING_REQUIRED_SEQEND = 107
    ORPHANED_LAYOUT_ENTITY = 108
    ORPHANED_PAPER_SPACE_BLOCK_RECORD_ENTITY = 109
    INVALID_TABLE_HANDLE = 110
    DECODING_ERROR = 111
    CREATED_MISSING_OBJECT = 112
    RESET_MLINE_STYLE = 113
    INVALID_GROUP_ENTITIES = 114
    UNDEFINED_BLOCK_NAME = 115
    INVALID_INTEGER_VALUE = 116
    INVALID_FLOATING_POINT_VALUE = 117
    MISSING_PERSISTENT_REACTOR = 118
    BLOCK_NAME_MISMATCH = 119

    # DXF entity property errors:
    INVALID_ENTITY_HANDLE = 201
    INVALID_OWNER_HANDLE = 202
    INVALID_LAYER_NAME = 203
    INVALID_COLOR_INDEX = 204
    INVALID_LINEWEIGHT = 205
    INVALID_MLINESTYLE_HANDLE = 206
    INVALID_DIMSTYLE = 207

    # DXF entity geometry or content errors:
    INVALID_EXTRUSION_VECTOR = 210
    INVALID_MAJOR_AXIS = 211
    INVALID_VERTEX_COUNT = 212
    INVALID_DICTIONARY_ENTRY = 213
    INVALID_CHARACTER = 214
    INVALID_MLINE_VERTEX = 215
    INVALID_MLINESTYLE_ELEMENT_COUNT = 216
    INVALID_SPLINE_DEFINITION = 217
    INVALID_SPLINE_CONTROL_POINT_COUNT = 218
    INVALID_SPLINE_FIT_POINT_COUNT = 219
    INVALID_SPLINE_KNOT_VALUE_COUNT = 220
    INVALID_SPLINE_WEIGHT_COUNT = 221
    INVALID_DIMENSION_GEOMETRY_LOCATION = 222
    INVALID_TRANSPARENCY = 223
    INVALID_CREASE_VALUE_COUNT = 224
    INVALID_ELLIPSE_RATIO = 225
    INVALID_HATCH_BOUNDARY_PATH = 226
    TAG_ATTRIBUTE_MISSING = 227
    INVALID_MESH_DATA = 228


REQUIRED_ROOT_DICT_ENTRIES = ("ACAD_GROUP", "ACAD_PLOTSTYLENAME")


class ErrorEntry:
    def __init__(
        self,
        code: int,
        message: str = "",
        dxf_entity: Optional[DXFEntity] = None,
        data: Any = None,
    ):
        self.code: int = code  # error code AuditError()
        self.entity: Optional[DXFEntity] = dxf_entity  # source entity of error
        self.message: str = message  # error message
        self.data: Any = data  # additional data as an arbitrary object


class Auditor:
    def __init__(self, doc: Drawing) -> None:
        assert doc is not None and doc.rootdict is not None and doc.entitydb is not None
        self.doc = doc
        self._rootdict_handle = doc.rootdict.dxf.handle
        self.errors: list[ErrorEntry] = []
        self.fixes: list[ErrorEntry] = []
        self._trashcan = doc.entitydb.new_trashcan()
        self._post_audit_jobs: list[Callable[[], None]] = []

    def reset(self) -> None:
        self.errors.clear()
        self.fixes.clear()
        self.empty_trashcan()

    def __len__(self) -> int:
        """Returns count of unfixed errors."""
        return len(self.errors)

    def __bool__(self) -> bool:
        """Returns ``True`` if any unfixed errors exist."""
        return self.__len__() > 0

    def __iter__(self) -> Iterable[ErrorEntry]:
        """Iterate over all unfixed errors."""
        return iter(self.errors)

    @property
    def entitydb(self):
        if self.doc:
            return self.doc.entitydb
        else:
            return None

    @property
    def has_errors(self) -> bool:
        """Returns ``True`` if any unrecoverable errors were detected."""
        return bool(self.errors)

    @property
    def has_fixes(self) -> bool:
        """Returns ``True`` if any recoverable errors were fixed while auditing."""
        return bool(self.fixes)

    @property
    def has_issues(self) -> bool:
        """Returns ``True`` if the DXF document has any errors or fixes."""
        return self.has_fixes or self.has_errors

    def print_error_report(
        self,
        errors: Optional[list[ErrorEntry]] = None,
        stream: Optional[TextIO] = None,
    ) -> None:
        def entity_str(count, code, entity):
            if entity is not None and entity.is_alive:
                return f"{count:4d}. Error [{code}] in {str(entity)}."
            else:
                return f"{count:4d}. Error [{code}]."

        if errors is None:
            errors = self.errors
        else:
            errors = list(errors)

        if stream is None:
            stream = sys.stdout

        if len(errors) == 0:
            stream.write("No unrecoverable errors found.\n\n")
        else:
            stream.write(f"{len(errors)} errors found.\n\n")
            for count, error in enumerate(errors):
                stream.write(entity_str(count + 1, error.code, error.entity) + "\n")
                stream.write("   " + error.message + "\n\n")

    def print_fixed_errors(self, stream: Optional[TextIO] = None) -> None:
        def entity_str(count, code, entity):
            if entity is not None and entity.is_alive:
                return f"{count:4d}. Issue [{code}] fixed in {str(entity)}."
            else:
                return f"{count:4d}. Issue [{code}] fixed."

        if stream is None:
            stream = sys.stdout

        if len(self.fixes) == 0:
            stream.write("No issues fixed.\n\n")
        else:
            stream.write(f"{len(self.fixes)} issues fixed.\n\n")
            for count, error in enumerate(self.fixes):
                stream.write(entity_str(count + 1, error.code, error.entity) + "\n")
                stream.write("   " + error.message + "\n\n")

    def add_error(
        self,
        code: int,
        message: str = "",
        dxf_entity: Optional[DXFEntity] = None,
        data: Any = None,
    ) -> None:
        self.errors.append(ErrorEntry(code, message, dxf_entity, data))

    def fixed_error(
        self,
        code: int,
        message: str = "",
        dxf_entity: Optional[DXFEntity] = None,
        data: Any = None,
    ) -> None:
        self.fixes.append(ErrorEntry(code, message, dxf_entity, data))

    def purge(self, codes: set[int]):
        """Remove error messages defined by integer error `codes`.

        This is useful to remove errors which are not important for a specific
        file usage.

        """
        self.errors = [err for err in self.errors if err.code in codes]

    def run(self) -> list[ErrorEntry]:
        if not self.check_root_dict():
            # no root dict found: abort audit process
            return self.errors
        self.doc.entitydb.audit(self)
        self.check_root_dict_entries()
        self.check_modelspace_exist()
        self.check_active_layout_exist()
        self.check_tables()
        self.doc.objects.audit(self)
        self.doc.blocks.audit(self)
        self.doc.groups.audit(self)
        self.doc.layouts.audit(self)
        self.audit_all_database_entities()
        self.check_block_reference_cycles()
        self.empty_trashcan()
        self.doc.objects.purge()
        return self.errors

    def empty_trashcan(self):
        if self.has_trashcan:
            self._trashcan.clear()

    def trash(self, entity: DXFEntity) -> None:
        if entity is None or not entity.is_alive:
            return
        if self.has_trashcan and entity.dxf.handle is not None:
            self._trashcan.add(entity.dxf.handle)
        else:
            entity.destroy()

    @property
    def has_trashcan(self) -> bool:
        return self._trashcan is not None

    def add_post_audit_job(self, job: Callable):
        self._post_audit_jobs.append(job)

    def check_root_dict(self) -> bool:
        rootdict = self.doc.rootdict
        if rootdict.dxftype() != "DICTIONARY":
            self.add_error(
                AuditError.ROOT_DICT_NOT_FOUND,
                f"Critical error - first object in OBJECTS section is not the expected "
                f"root dictionary, found {str(rootdict)}.",
            )
            return False
        if rootdict.dxf.get("owner") != "0":
            rootdict.dxf.owner = "0"
            self.fixed_error(
                code=AuditError.INVALID_OWNER_HANDLE,
                message=f"Fixed invalid owner handle in root {str(rootdict)}.",
            )
        return True

    def check_root_dict_entries(self) -> None:
        rootdict = self.doc.rootdict
        if rootdict.dxftype() != "DICTIONARY":
            return
        for name in REQUIRED_ROOT_DICT_ENTRIES:
            if name not in rootdict:
                self.add_error(
                    code=AuditError.MISSING_REQUIRED_ROOT_DICT_ENTRY,
                    message=f"Missing rootdict entry: {name}",
                    dxf_entity=rootdict,
                )

    def check_modelspace_exist(self) -> None:
        msp = self.doc.modelspace()
        if not msp.is_alive:
            self.add_error(
                code=AuditError.MODELSPACE_NOT_FOUND,
                message=f"Required modelspace layout not found.",
            )

    def check_active_layout_exist(self) -> None:
        try:
            layout = self.doc.active_layout()
        except const.DXFStructureError:
            layout = None  # type: ignore

        if layout is None or layout.is_alive is False:
            self.add_error(
                code=AuditError.ACTIVE_PAPERSPACE_LAYOUT_NOT_FOUND,
                message=f"Required active paperspace layout not found.",
            )

    def check_tables(self) -> None:
        table_section = self.doc.tables
        table_section.viewports.audit(self)
        table_section.linetypes.audit(self)
        table_section.layers.audit(self)
        table_section.styles.audit(self)
        table_section.views.audit(self)
        table_section.ucs.audit(self)
        table_section.appids.audit(self)
        table_section.dimstyles.audit(self)
        table_section.block_records.audit(self)

    def audit_all_database_entities(self) -> None:
        """Audit all entities stored in the entity database."""
        # Destruction of entities can occur while auditing.
        # Best practice to delete entities is to move them into the trashcan:
        # Auditor.trash(entity)
        db = self.doc.entitydb
        db.locked = True
        # To create new entities while auditing, add a post audit job by calling
        # Auditor.app_post_audit_job() with a callable object or function as argument.
        self._post_audit_jobs = []
        for entity in db.values():
            if entity.is_alive:
                entity.audit(self)
        db.locked = False
        self.empty_trashcan()
        self.exec_post_audit_jobs()

    def exec_post_audit_jobs(self):
        for call in self._post_audit_jobs:
            call()
        self._post_audit_jobs = []

    def check_entity_linetype(self, entity: DXFEntity) -> None:
        """Check for usage of undefined line types. AutoCAD does not load
        DXF files with undefined line types.
        """
        assert self.doc is entity.doc, "Entity from different DXF document."
        if not entity.dxf.hasattr("linetype"):
            return
        linetype = validator.make_table_key(entity.dxf.linetype)
        # No table entry in linetypes required:
        if linetype in ("bylayer", "byblock"):
            return

        if linetype not in self.doc.linetypes:
            # Defaults to 'BYLAYER'
            entity.dxf.discard("linetype")
            self.fixed_error(
                code=AuditError.UNDEFINED_LINETYPE,
                message=f"Removed undefined linetype {linetype} in {str(entity)}",
                dxf_entity=entity,
                data=linetype,
            )

    def check_text_style(self, entity: DXFEntity) -> None:
        """Check for usage of undefined text styles."""
        assert self.doc is entity.doc, "Entity from different DXF document."
        if not entity.dxf.hasattr("style"):
            return
        style = entity.dxf.style
        if style not in self.doc.styles:
            # Defaults to 'Standard'
            entity.dxf.discard("style")
            self.fixed_error(
                code=AuditError.UNDEFINED_TEXT_STYLE,
                message=f'Removed undefined text style "{style}" from {str(entity)}.',
                dxf_entity=entity,
                data=style,
            )

    def check_dimension_style(self, entity: DXFGraphic) -> None:
        """Check for usage of undefined dimension styles."""
        assert self.doc is entity.doc, "Entity from different DXF document."
        if not entity.dxf.hasattr("dimstyle"):
            return
        dimstyle = entity.dxf.dimstyle
        if dimstyle not in self.doc.dimstyles:
            # The dimstyle attribute is not optional:
            entity.dxf.dimstyle = "Standard"
            self.fixed_error(
                code=AuditError.UNDEFINED_DIMENSION_STYLE,
                message=f'Replaced undefined dimstyle "{dimstyle}" in '
                f'{str(entity)} by "Standard".',
                dxf_entity=entity,
                data=dimstyle,
            )

    def check_for_valid_layer_name(self, entity: DXFEntity) -> None:
        """Check layer names for invalid characters: <>/\":;?*|='"""
        name = entity.dxf.layer
        if not validator.is_valid_layer_name(name):
            # This error can't be fixed !?
            self.add_error(
                code=AuditError.INVALID_LAYER_NAME,
                message=f'Invalid layer name "{name}" in {str(entity)}',
                dxf_entity=entity,
                data=name,
            )

    def check_entity_color_index(self, entity: DXFGraphic) -> None:
        color = entity.dxf.color
        # 0 == BYBLOCK
        # 256 == BYLAYER
        # 257 == BYOBJECT
        if color < 0 or color > 257:
            entity.dxf.discard("color")
            self.fixed_error(
                code=AuditError.INVALID_COLOR_INDEX,
                message=f"Removed invalid color index of {str(entity)}.",
                dxf_entity=entity,
                data=color,
            )

    def check_entity_lineweight(self, entity: DXFGraphic) -> None:
        weight = entity.dxf.lineweight
        if weight not in const.VALID_DXF_LINEWEIGHT_VALUES:
            entity.dxf.lineweight = validator.fix_lineweight(weight)
            self.fixed_error(
                code=AuditError.INVALID_LINEWEIGHT,
                message=f"Fixed invalid lineweight of {str(entity)}.",
                dxf_entity=entity,
            )

    def check_owner_exist(self, entity: DXFEntity) -> None:
        assert self.doc is entity.doc, "Entity from different DXF document."
        if not entity.dxf.hasattr("owner"):  # important for recover mode
            return
        doc = self.doc
        owner_handle = entity.dxf.owner
        handle = entity.dxf.get("handle", "0")
        if owner_handle == "0":
            # Root-Dictionary or Table-Head:
            if handle == self._rootdict_handle or entity.dxftype() == "TABLE":
                return  # '0' handle as owner is valid
        if owner_handle not in doc.entitydb:
            if handle == self._rootdict_handle:
                entity.dxf.owner = "0"
                self.fixed_error(
                    code=AuditError.INVALID_OWNER_HANDLE,
                    message=f"Fixed invalid owner handle in root {str(entity)}.",
                )
            elif entity.dxftype() == "TABLE":
                name = entity.dxf.get("name", "UNKNOWN")
                entity.dxf.owner = "0"
                self.fixed_error(
                    code=AuditError.INVALID_OWNER_HANDLE,
                    message=f"Fixed invalid owner handle for {name} table.",
                )
            else:
                self.fixed_error(
                    code=AuditError.INVALID_OWNER_HANDLE,
                    message=f"Deleted {str(entity)} entity with invalid owner "
                    f"handle #{owner_handle}.",
                )
                self.trash(doc.entitydb.get(handle))  # type: ignore

    def check_extrusion_vector(self, entity: DXFEntity) -> None:
        if NULLVEC.isclose(entity.dxf.extrusion):
            entity.dxf.discard("extrusion")
            self.fixed_error(
                code=AuditError.INVALID_EXTRUSION_VECTOR,
                message=f"Fixed extrusion vector for entity: {str(entity)}.",
                dxf_entity=entity,
            )

    def check_transparency(self, entity: DXFEntity) -> None:
        value = entity.dxf.transparency
        if value is None:
            return
        if not validator.is_transparency(value):
            entity.dxf.discard("transparency")
            self.fixed_error(
                code=AuditError.INVALID_TRANSPARENCY,
                message=f"Fixed invalid transparency for entity: {str(entity)}.",
                dxf_entity=entity,
            )

    def check_block_reference_cycles(self) -> None:
        cycle_detector = BlockCycleDetector(self.doc)
        for block in self.doc.blocks:
            if cycle_detector.has_cycle(block.name):
                self.add_error(
                    code=AuditError.INVALID_BLOCK_REFERENCE_CYCLE,
                    message=f"Invalid block reference cycle detected in "
                    f'block "{block.name}".',
                    dxf_entity=block.block_record,
                )


class BlockCycleDetector:
    def __init__(self, doc: Drawing):
        self.key = doc.blocks.key
        self.blocks = self._build_block_ledger(doc.blocks)

    def _build_block_ledger(self, blocks: BlocksSection) -> dict[str, set[str]]:
        ledger = {}
        for block in blocks:
            inserts = {
                self.key(insert.dxf.get("name", "")) for insert in block.query("INSERT")
            }
            ledger[self.key(block.name)] = inserts
        return ledger

    def has_cycle(self, block_name: str) -> bool:
        def check(name):
            # block 'name' does not exist: ignore this error, because it is not
            # the task of this method to detect not existing block definitions
            try:
                inserts = self.blocks[name]
            except KeyError:
                return False  # Not existing blocks can't create cycles.
            path.append(name)
            for n in inserts:
                if n in path:
                    return True
                elif check(n):
                    return True
            path.pop()
            return False

        path: list[str] = []
        block_name = self.key(block_name)
        return check(block_name)


def audit(entity: DXFEntity, doc: Drawing) -> Auditor:
    """Setup an :class:`Auditor` object, run the audit process for `entity`
    and return result as :class:`Auditor` object.

    Args:
        entity: DXF entity to validate
        doc: bounded DXF document of `entity`

    """
    if not entity.is_alive:
        raise TypeError("Entity is destroyed.")

    # Validation of unbound entities is possible, but it is not useful
    # to validate entities against a different DXF document:
    if entity.dxf.handle is not None and not factory.is_bound(entity, doc):
        raise ValueError("Entity is bound to different DXF document.")

    auditor = Auditor(doc)
    entity.audit(auditor)
    return auditor
