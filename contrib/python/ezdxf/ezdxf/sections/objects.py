# Copyright (c) 2011-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Optional,
    TYPE_CHECKING,
    Union,
    cast,
)
import logging

from ezdxf.entities.dictionary import Dictionary
from ezdxf.entities import factory, is_dxf_object
from ezdxf.lldxf import const, validator
from ezdxf.entitydb import EntitySpace, EntityDB
from ezdxf.query import EntityQuery
from ezdxf.tools.handle import UnderlayKeyGenerator
from ezdxf.audit import Auditor, AuditError

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import (
        GeoData,
        DictionaryVar,
        DXFTagStorage,
        DXFObject,
        ImageDef,
        UnderlayDefinition,
        DictionaryWithDefault,
        XRecord,
        Placeholder,
    )
    from ezdxf.entities.image import ImageDefReactor

logger = logging.getLogger("ezdxf")


class ObjectsSection:
    def __init__(self, doc: Drawing, entities: Optional[Iterable[DXFObject]] = None):
        self.doc = doc
        self.underlay_key_generator = UnderlayKeyGenerator()
        self._entity_space = EntitySpace()
        if entities is not None:
            self._build(iter(entities))

    @property
    def entitydb(self) -> EntityDB:
        """Returns drawing entity database. (internal API)"""
        return self.doc.entitydb

    def get_entity_space(self) -> EntitySpace:
        """Returns entity space. (internal API)"""
        return self._entity_space

    def next_underlay_key(self, checkfunc=lambda k: True) -> str:
        while True:
            key = self.underlay_key_generator.next()
            if checkfunc(key):
                return key

    def _build(self, entities: Iterator[DXFObject]) -> None:
        section_head = cast("DXFTagStorage", next(entities))

        if section_head.dxftype() != "SECTION" or section_head.base_class[1] != (
            2,
            "OBJECTS",
        ):
            raise const.DXFStructureError(
                "Critical structure error in the OBJECTS section."
            )

        for entity in entities:
            # No check for valid entities here:
            # Use the audit or the recover module to fix invalid DXF files!
            self._entity_space.add(entity)

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        """Export DXF entity by `tagwriter`. (internal API)"""
        tagwriter.write_str("  0\nSECTION\n  2\nOBJECTS\n")
        self._entity_space.export_dxf(tagwriter)
        tagwriter.write_tag2(0, "ENDSEC")

    def new_entity(self, _type: str, dxfattribs: dict) -> DXFObject:
        """Create new DXF object, add it to the entity database and to the
        entity space.

        Args:
             _type: DXF type like `DICTIONARY`
             dxfattribs: DXF attributes as dict

        (internal API)
        """
        dxf_entity = factory.create_db_entry(_type, dxfattribs, self.doc)
        self._entity_space.add(dxf_entity)
        return dxf_entity  # type: ignore

    def delete_entity(self, entity: DXFObject) -> None:
        """Remove `entity` from entity space and destroy object. (internal API)"""
        self._entity_space.remove(entity)
        self.entitydb.delete_entity(entity)

    def delete_all_entities(self) -> None:
        """Delete all DXF objects. (internal API)"""
        db = self.entitydb
        for entity in self._entity_space:
            db.delete_entity(entity)
        self._entity_space.clear()

    def setup_rootdict(self) -> Dictionary:
        """Create a root dictionary. Has to be the first object in the objects
        section. (internal API)"""
        if len(self):
            raise const.DXFStructureError(
                "Can not create root dictionary in none empty objects section."
            )
        logger.debug("Creating ROOT dictionary.")
        # root directory has no owner
        return self.add_dictionary(owner="0", hard_owned=False)

    def setup_object_management_tables(self, rootdict: Dictionary) -> None:
        """Setup required management tables. (internal API)"""

        def setup_plot_style_name_table():
            plot_style_name_dict = self.add_dictionary_with_default(
                owner=rootdict.dxf.handle,
                hard_owned=False,
            )
            placeholder = self.add_placeholder(owner=plot_style_name_dict.dxf.handle)
            plot_style_name_dict.set_default(placeholder)
            plot_style_name_dict["Normal"] = placeholder
            rootdict["ACAD_PLOTSTYLENAME"] = plot_style_name_dict

        def restore_table_handle(table_name: str, handle: str) -> None:
            # The original object table does not exist, but the handle is maybe
            # used by some DXF entities. Try to restore the original table
            # handle.
            new_table = rootdict.get(table_name)
            if isinstance(new_table, Dictionary) and self.doc.entitydb.reset_handle(
                new_table, handle
            ):
                logger.debug(f"reset handle of table {table_name} to #{handle}")

        # check required object tables:
        for name in _OBJECT_TABLE_NAMES:
            table: Union[Dictionary, str, None] = rootdict.get(name)  # type: ignore
            # Dictionary: existing table object
            # str: handle of not existing table object
            # None: no entry in the rootdict for "name"
            if isinstance(table, Dictionary):
                continue  # skip existing tables

            logger.info(f"creating {name} dictionary")
            if name == "ACAD_PLOTSTYLENAME":
                setup_plot_style_name_table()
            else:
                rootdict.add_new_dict(name, hard_owned=False)

            if isinstance(table, str) and validator.is_handle(table):
                restore_table_handle(name, handle=table)

    def add_object(self, entity: DXFObject) -> None:
        """Add `entity` to OBJECTS section. (internal API)"""
        if is_dxf_object(entity):
            self._entity_space.add(entity)
        else:
            raise const.DXFTypeError(
                f"invalid DXF type {entity.dxftype()} for OBJECTS section"
            )

    def add_dxf_object_with_reactor(self, dxftype: str, dxfattribs) -> DXFObject:
        """Add DXF object with reactor. (internal API)"""
        dxfobject = self.new_entity(dxftype, dxfattribs)
        dxfobject.set_reactors([dxfattribs["owner"]])
        return dxfobject

    def purge(self):
        self._entity_space.purge()

    # start of public interface

    @property
    def rootdict(self) -> Dictionary:
        """Returns the root DICTIONARY, or as AutoCAD calls it:
        the named DICTIONARY.
        """
        if len(self):
            return self._entity_space[0]  # type: ignore
        else:
            return self.setup_rootdict()

    def __len__(self) -> int:
        """Returns the count of all DXF objects in the OBJECTS section."""
        return len(self._entity_space)

    def __iter__(self) -> Iterator[DXFObject]:
        """Returns an iterator of all DXF objects in the OBJECTS section."""
        return iter(self._entity_space)  # type: ignore

    def __getitem__(self, index) -> DXFObject:
        """Get entity at `index`.

        The underlying data structure for storing DXF objects is organized like
        a standard Python list, therefore `index` can be any valid list indexing
        or slicing term, like a single index ``objects[-1]`` to get the last
        entity, or an index slice ``objects[:10]`` to get the first 10 or fewer
        objects as ``list[DXFObject]``.

        """
        return self._entity_space[index]  # type: ignore

    def __contains__(self, entity):
        """Returns ``True`` if `entity` stored in OBJECTS section.

        Args:
             entity: :class:`DXFObject` or handle as hex string

        """
        if isinstance(entity, str):
            try:
                entity = self.entitydb[entity]
            except KeyError:
                return False
        return entity in self._entity_space

    def query(self, query: str = "*") -> EntityQuery:
        """Get all DXF objects matching the :ref:`entity query string`."""
        return EntityQuery(iter(self), query)

    def audit(self, auditor: Auditor) -> None:
        """Audit and repair OBJECTS section.

        .. important::

            Do not delete entities while auditing process, because this
            would alter the entity database while iterating, instead use::

                auditor.trash(entity)

            to delete invalid entities after auditing automatically.

        """
        assert self.doc is auditor.doc, "Auditor for different DXF document."
        for entity in self._entity_space:
            if not is_dxf_object(entity):
                auditor.fixed_error(
                    code=AuditError.REMOVED_INVALID_DXF_OBJECT,
                    message=f"Removed invalid DXF entity {str(entity)} "
                    f"from OBJECTS section.",
                )
                auditor.trash(entity)
        self.reorg(auditor)
        self._entity_space.audit(auditor)

    def add_dictionary(self, owner: str = "0", hard_owned: bool = True) -> Dictionary:
        """Add new :class:`~ezdxf.entities.Dictionary` object.

        Args:
            owner: handle to owner as hex string.
            hard_owned: ``True`` to treat entries as hard owned.

        """
        entity = self.new_entity(
            "DICTIONARY",
            dxfattribs={
                "owner": owner,
                "hard_owned": hard_owned,
            },
        )
        return cast(Dictionary, entity)

    def add_dictionary_with_default(
        self, owner="0", default="0", hard_owned: bool = True
    ) -> DictionaryWithDefault:
        """Add new :class:`~ezdxf.entities.DictionaryWithDefault` object.

        Args:
            owner: handle to owner as hex string.
            default: handle to default entry.
            hard_owned: ``True`` to treat entries as hard owned.

        """
        entity = self.new_entity(
            "ACDBDICTIONARYWDFLT",
            dxfattribs={
                "owner": owner,
                "default": default,
                "hard_owned": hard_owned,
            },
        )
        return cast("DictionaryWithDefault", entity)

    def add_dictionary_var(self, owner: str = "0", value: str = "") -> DictionaryVar:
        """Add a new :class:`~ezdxf.entities.DictionaryVar` object.

        Args:
            owner: handle to owner as hex string.
            value: value as string

        """
        return self.new_entity(  # type: ignore
            "DICTIONARYVAR", dxfattribs={"owner": owner, "value": value}
        )

    def add_xrecord(self, owner: str = "0") -> XRecord:
        """Add a new :class:`~ezdxf.entities.XRecord` object.

        Args:
            owner: handle to owner as hex string.

        """
        return self.new_entity("XRECORD", dxfattribs={"owner": owner})  # type: ignore

    def add_placeholder(self, owner: str = "0") -> Placeholder:
        """Add a new :class:`~ezdxf.entities.Placeholder` object.

        Args:
            owner: handle to owner as hex string.

        """
        return self.new_entity(  # type: ignore
            "ACDBPLACEHOLDER", dxfattribs={"owner": owner}
        )

    # end of public interface

    def set_raster_variables(
        self, frame: int = 0, quality: int = 1, units: str = "m"
    ) -> None:
        """Set raster variables.

        Args:
            frame: ``0`` = do not show image frame; ``1`` = show image frame
            quality: ``0`` = draft; ``1`` = high
            units: units for inserting images. This defines the real world unit for one
                drawing unit for the purpose of inserting and scaling images with an
                associated resolution.

                   ===== ===========================
                   mm    Millimeter
                   cm    Centimeter
                   m     Meter (ezdxf default)
                   km    Kilometer
                   in    Inch
                   ft    Foot
                   yd    Yard
                   mi    Mile
                   none  None
                   ===== ===========================

        (internal API), public interface :meth:`~ezdxf.drawing.Drawing.set_raster_variables`

        """
        units_: int = const.RASTER_UNITS.get(units, 0)
        try:
            raster_vars = self.rootdict["ACAD_IMAGE_VARS"]
        except const.DXFKeyError:
            raster_vars = self.add_dxf_object_with_reactor(
                "RASTERVARIABLES",
                dxfattribs={
                    "owner": self.rootdict.dxf.handle,
                    "frame": frame,
                    "quality": quality,
                    "units": units_,
                },
            )
            self.rootdict["ACAD_IMAGE_VARS"] = raster_vars
        else:
            raster_vars.dxf.frame = frame
            raster_vars.dxf.quality = quality
            raster_vars.dxf.units = units_

    def get_raster_variables(self) -> tuple[int, int, str]:
        try:
            raster_vars = self.rootdict["ACAD_IMAGE_VARS"]
        except const.DXFKeyError:
            return 0, 1, "none"
        return (
            raster_vars.dxf.frame,
            raster_vars.dxf.quality,
            const.REVERSE_RASTER_UNITS.get(raster_vars.dxf.units, "none"),
        )

    def set_wipeout_variables(self, frame: int = 0) -> None:
        """Set wipeout variables.

        Args:
            frame: ``0`` = do not show image frame; ``1`` = show image frame

        (internal API)
        """
        try:
            wipeout_vars = self.rootdict["ACAD_WIPEOUT_VARS"]
        except const.DXFKeyError:
            wipeout_vars = self.add_dxf_object_with_reactor(
                "WIPEOUTVARIABLES",
                dxfattribs={
                    "owner": self.rootdict.dxf.handle,
                    "frame": int(frame),
                },
            )
            self.rootdict["ACAD_WIPEOUT_VARS"] = wipeout_vars
        else:
            wipeout_vars.dxf.frame = int(frame)

    def get_wipeout_frame_setting(self) -> int:
        try:
            wipeout_vars = self.rootdict["ACAD_WIPEOUT_VARS"]
        except const.DXFKeyError:
            return 0
        return wipeout_vars.dxf.frame

    def add_image_def(
        self,
        filename: str,
        size_in_pixel: tuple[int, int],
        name: Optional[str] = None,
    ) -> ImageDef:
        """Add an image definition to the objects section.

        Add an :class:`~ezdxf.entities.image.ImageDef` entity to the drawing
        (objects section). `filename` is the image file name as relative or
        absolute path and `size_in_pixel` is the image size in pixel as (x, y)
        tuple. To avoid dependencies to external packages, `ezdxf` can not
        determine the image size by itself. Returns a :class:`~ezdxf.entities.image.ImageDef`
        entity which is needed to create an image reference. `name` is the
        internal image name, if set to ``None``, name is auto-generated.

        Absolute image paths works best for AutoCAD but not really good, you
        have to update external references manually in AutoCAD, which is not
        possible in TrueView. If the drawing units differ from 1 meter, you also
        have to use: :meth:`set_raster_variables`.

        Args:
            filename: image file name (absolute path works best for AutoCAD)
            size_in_pixel: image size in pixel as (x, y) tuple
            name: image name for internal use, None for using filename as name
                (best for AutoCAD)

        """
        if name is None:
            name = filename
        image_dict = self.rootdict.get_required_dict("ACAD_IMAGE_DICT")
        image_def = self.add_dxf_object_with_reactor(
            "IMAGEDEF",
            dxfattribs={
                "owner": image_dict.dxf.handle,
                "filename": filename,
                "image_size": size_in_pixel,
            },
        )
        image_dict[name] = image_def
        return cast("ImageDef", image_def)

    def add_image_def_reactor(self, image_handle: str) -> ImageDefReactor:
        """Add required IMAGEDEF_REACTOR object for IMAGEDEF object.

        (internal API)
        """
        image_def_reactor = self.new_entity(
            "IMAGEDEF_REACTOR",
            dxfattribs={
                "owner": image_handle,
                "image_handle": image_handle,
            },
        )
        return cast("ImageDefReactor", image_def_reactor)

    def add_underlay_def(
        self, filename: str, fmt: str = "pdf", name: Optional[str] = None
    ) -> UnderlayDefinition:
        """Add an :class:`~ezdxf.entities.underlay.UnderlayDefinition` entity
        to the drawing (OBJECTS section). `filename` is the underlay file name
        as relative or absolute path and `fmt` as string (pdf, dwf, dgn).
        The underlay definition is required to create an underlay reference.

        Args:
            filename: underlay file name
            fmt: file format as string ``'pdf'|'dwf'|'dgn'``
            name: pdf format = page number to display; dgn format = ``'default'``; dwf: ????

        """
        fmt = fmt.upper()
        if fmt in ("PDF", "DWF", "DGN"):
            underlay_dict_name = f"ACAD_{fmt}DEFINITIONS"
            underlay_def_entity = f"{fmt}DEFINITION"
        else:
            raise const.DXFValueError(f"Unsupported file format: '{fmt}'")

        if name is None:
            if fmt == "PDF":
                name = "1"  # Display first page by default
            elif fmt == "DGN":
                name = "default"
            else:
                name = "Model"  # Display model space for DWF ???

        underlay_dict = self.rootdict.get_required_dict(underlay_dict_name)
        underlay_def = self.new_entity(
            underlay_def_entity,
            dxfattribs={
                "owner": underlay_dict.dxf.handle,
                "filename": filename,
                "name": name,
            },
        )

        # auto-generated underlay key
        key = self.next_underlay_key(lambda k: k not in underlay_dict)
        underlay_dict[key] = underlay_def
        return cast("UnderlayDefinition", underlay_def)

    def add_geodata(self, owner: str = "0", dxfattribs=None) -> GeoData:
        """Creates a new :class:`GeoData` entity and replaces existing ones.
        The GEODATA entity resides in the OBJECTS section and NOT in the layout
        entity space, and it is linked to the layout by an extension dictionary
        located in BLOCK_RECORD of the layout.

        The GEODATA entity requires DXF version R2010+. The DXF Reference does
        not document if other layouts than model space supports geo referencing,
        so getting/setting geo data may only make sense for the model space
        layout, but it is also available in paper space layouts.

        Args:
            owner: handle to owner as hex string
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.GeoData` entity

        """
        if dxfattribs is None:
            dxfattribs = {}
        dxfattribs["owner"] = owner
        return cast("GeoData", self.add_dxf_object_with_reactor("GEODATA", dxfattribs))

    def reorg(self, auditor: Optional[Auditor] = None) -> Auditor:
        """Validate and recreate the integrity of the OBJECTS section."""
        if auditor is None:
            assert self.doc is not None, "valid document required"
            auditor = Auditor(self.doc)
        sanitizer = _Sanitizer(auditor, self)
        sanitizer.execute()
        return auditor


_OBJECT_TABLE_NAMES = [
    "ACAD_COLOR",
    "ACAD_GROUP",
    "ACAD_LAYOUT",
    "ACAD_MATERIAL",
    "ACAD_MLEADERSTYLE",
    "ACAD_MLINESTYLE",
    "ACAD_PLOTSETTINGS",
    "ACAD_PLOTSTYLENAME",
    "ACAD_SCALELIST",
    "ACAD_TABLESTYLE",
    "ACAD_VISUALSTYLE",
]

KNOWN_DICT_CONTENT: dict[str, str] = {
    "ACAD_COLOR": "DBCOLOR",
    "ACAD_GROUP": "GROUP",
    "ACAD_IMAGE_DICT": "IMAGEDEF",
    "ACAD_DETAILVIEWSTYLE": "ACDBDETAILVIEWSTYLE",
    "ACAD_LAYOUT": "LAYOUT",
    "ACAD_MATERIAL": "MATERIAL",
    "ACAD_MLEADERSTYLE": "MLEADERSTYLE",
    "ACAD_MLINESTYLE": "MLINESTYLE",
    "ACAD_PLOTSETTINGS": "PLOTSETTINGS",
    "ACAD_RENDER_ACTIVE_SETTINGS": "MENTALRAYRENDERSETTINGS",
    "ACAD_SCALELIST": "SCALE",
    "ACAD_SECTIONVIEWSTYLE": "ACDBSECTIONVIEWSTYLE",
    "ACAD_TABLESTYLE": "TABLESTYLE",
    "ACAD_PDFDEFINITIONS": "PDFDEFINITION",
    "ACAD_DWFDEFINITIONS": "DWFDEFINITION",
    "ACAD_DGNDEFINITIONS": "DGNDEFINITION",
    "ACAD_VISUALSTYLE": "VISUALSTYLE",
    "AcDbVariableDictionary": "DICTIONARYVAR",
}


class _Sanitizer:
    def __init__(self, auditor: Auditor, objects: ObjectsSection) -> None:
        self.objects = objects
        self.auditor = auditor
        self.rootdict = objects.rootdict
        self.removed_entity = True

    def dictionaries(self) -> Iterator[Dictionary]:
        for d in self.objects:
            if isinstance(d, Dictionary):
                yield d

    def execute(self, max_loops=100) -> None:
        self.restore_owner_handles_of_dictionary_entries()
        self.validate_known_dictionaries()
        loops = 0
        self.removed_entity = True
        while self.removed_entity and loops < max_loops:
            loops += 1
            self.removed_entity = False
            self.remove_orphaned_dictionaries()
            # Run audit on all entities of the OBJECTS section to take the removed
            # dictionaries into account.
            self.audit_objects()
        self.create_required_structures()

    def restore_owner_handles_of_dictionary_entries(self) -> None:
        def reclaim_entity() -> None:
            entity.dxf.owner = dict_handle
            self.auditor.fixed_error(
                AuditError.INVALID_OWNER_HANDLE,
                f"Fixed invalid owner handle of {entity}.",
            )

        def purge_key():
            purge.append(key)
            self.auditor.fixed_error(
                AuditError.INVALID_OWNER_HANDLE,
                f"Removed invalid key {key} in {str(dictionary)}.",
            )

        entitydb = self.auditor.entitydb
        for dictionary in self.dictionaries():
            purge: list[str] = []  # list of keys to discard
            dict_handle = dictionary.dxf.handle
            for key, entity in dictionary.items():
                if isinstance(entity, str):
                    # handle is not resolved -> entity does not exist
                    purge_key()
                    continue
                owner_handle = entity.dxf.get("owner")
                if owner_handle == dict_handle:
                    continue
                parent_dict = entitydb.get(owner_handle)
                if isinstance(parent_dict, Dictionary) and parent_dict.find_key(entity):
                    # entity belongs to parent_dict, discard key
                    purge_key()
                else:
                    reclaim_entity()
            for key in purge:
                dictionary.discard(key)

    def remove_orphaned_dictionaries(self) -> None:
        def kill_dictionary():
            entitydb.discard(dictionary)
            dictionary._silent_kill()

        entitydb = self.auditor.entitydb
        rootdict = self.rootdict
        for dictionary in self.dictionaries():
            if dictionary is rootdict:
                continue
            owner = entitydb.get(dictionary.dxf.get("owner"))
            if owner is None:
                # owner does not exist:
                # A DICTIONARY without an owner has no purpose and the owner can not be
                # determined, except for searching all dictionaries for an entry that
                # references this DICTIONARY, this is done in the method
                # restore_owner_handles_of_dictionary_entries().
                kill_dictionary()
                continue
            if not isinstance(owner, Dictionary):
                continue
            key = owner.find_key(dictionary)
            if not key:  # owner dictionary has no entry for this dict
                kill_dictionary()

    def validate_known_dictionaries(self) -> None:
        from ezdxf.entities import DXFEntity

        auditor = self.auditor
        for dict_name, expected_type in KNOWN_DICT_CONTENT.items():
            object_dict = self.rootdict.get(dict_name)
            if not isinstance(object_dict, Dictionary):
                continue
            purge_keys: list[str] = []
            for key, entry in object_dict.items():
                if isinstance(entry, DXFEntity) and entry.dxftype() != expected_type:
                    auditor.fixed_error(
                        AuditError.REMOVED_INVALID_DXF_OBJECT,
                        f"Removed invalid type {entry} from {object_dict}<{dict_name}>, "
                        f"expected type {expected_type}",
                    )
                    purge_keys.append(key)
                    auditor.trash(entry)
            for key in purge_keys:
                object_dict.discard(key)

    def create_required_structures(self):
        self.objects.setup_object_management_tables(self.rootdict)
        doc = self.objects.doc
        # update ObjectCollections:
        doc.materials.update_object_dict()
        doc.materials.create_required_entries()
        doc.mline_styles.update_object_dict()
        doc.mline_styles.create_required_entries()
        doc.mleader_styles.update_object_dict()
        doc.mleader_styles.create_required_entries()
        doc.groups.update_object_dict()

    def cleanup_entitydb(self):
        self.auditor.empty_trashcan()
        self.auditor.entitydb.purge()

    def audit_objects(self):
        self.cleanup_entitydb()
        auditor = self.auditor
        current_db_size = len(auditor.entitydb)

        for entity in self.objects:
            if not entity.dxf.hasattr("owner"):
                # check_owner_exist() ignores entities without owner handle
                auditor.fixed_error(
                    code=AuditError.INVALID_OWNER_HANDLE,
                    message=f"Deleted {str(entity)} entity without owner handle.",
                )
                auditor.trash(entity)
                continue
            auditor.check_owner_exist(entity)
            entity.audit(auditor)

        auditor.empty_trashcan()
        if current_db_size != len(auditor.entitydb):
            self.removed_entity = True
