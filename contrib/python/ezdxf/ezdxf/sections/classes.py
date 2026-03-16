# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, Iterable, Union, cast, Optional
from collections import Counter, OrderedDict
import logging

from ezdxf.lldxf.const import DXFStructureError, DXF2004, DXF2000, DXFKeyError
from ezdxf.entities.dxfclass import DXFClass
from ezdxf.entities.dxfentity import DXFEntity, DXFTagStorage

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

logger = logging.getLogger("ezdxf")

# name: cpp_class_name (2), app_name (3), flags(90), was_a_proxy (280),
# is_an_entity (281)
# Multiple entries for 'name' are possible and supported, ClassSection stores
# entries with key: (name, cpp_class_name).
# 0 <ctrl> CLASS
# 1 <str> MPOLYGON
# 2 <str> AcDbMPolygon
# 3 <str> "AcMPolygonObj15|Version(1.0.0.0) Product Desc: Object enabler for the AcDbMPolyg ... odesk.com"
# 90 <int> 3071, b101111111111
# 280 <int> 0
# 281 <int> 1
CLASS_DEFINITIONS = {
    "ACDBDICTIONARYWDFLT": [
        "AcDbDictionaryWithDefault",
        "ObjectDBX Classes",
        0,
        0,
        0,
    ],
    "SUN": ["AcDbSun", "SCENEOE", 1153, 0, 0],
    "DICTIONARYVAR": ["AcDbDictionaryVar", "ObjectDBX Classes", 0, 0, 0],
    "TABLESTYLE": ["AcDbTableStyle", "ObjectDBX Classes", 4095, 0, 0],
    "MATERIAL": ["AcDbMaterial", "ObjectDBX Classes", 1153, 0, 0],
    "VISUALSTYLE": ["AcDbVisualStyle", "ObjectDBX Classes", 4095, 0, 0],
    "SCALE": ["AcDbScale", "ObjectDBX Classes", 1153, 0, 0],
    "MLEADERSTYLE": ["AcDbMLeaderStyle", "ACDB_MLEADERSTYLE_CLASS", 4095, 0, 0],
    "MLEADER": ["AcDbMLeader", "ACDB_MLEADER_CLASS", 3071, 0, 1],
    "MPOLYGON": ["AcDbMPolygon", "AcMPolygonObj15", 1025, 0, 1],
    "CELLSTYLEMAP": ["AcDbCellStyleMap", "ObjectDBX Classes", 1152, 0, 0],
    "EXACXREFPANELOBJECT": ["ExAcXREFPanelObject", "EXAC_ESW", 1025, 0, 0],
    "NPOCOLLECTION": [
        "AcDbImpNonPersistentObjectsCollection",
        "ObjectDBX Classes",
        1153,
        0,
        0,
    ],
    "LAYER_INDEX": ["AcDbLayerIndex", "ObjectDBX Classes", 0, 0, 0],
    "SPATIAL_INDEX": ["AcDbSpatialIndex", "ObjectDBX Classes", 0, 0, 0],
    "IDBUFFER": ["AcDbIdBuffer", "ObjectDBX Classes", 0, 0, 0],
    "DIMASSOC": ["AcDbDimAssoc", "AcDbDimAssoc", 0, 0, 0],
    "ACDBSECTIONVIEWSTYLE": [
        "AcDbSectionViewStyle",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "ACDBDETAILVIEWSTYLE": [
        "AcDbDetailViewStyle",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "IMAGEDEF": ["AcDbRasterImageDef", "ISM", 0, 0, 0],
    "RASTERVARIABLES": ["AcDbRasterVariables", "ISM", 0, 0, 0],
    "IMAGEDEF_REACTOR": ["AcDbRasterImageDefReactor", "ISM", 1, 0, 0],
    "IMAGE": ["AcDbRasterImage", "ISM", 2175, 0, 1],
    "PDFDEFINITION": ["AcDbPdfDefinition", "ObjectDBX Classes", 1153, 0, 0],
    "PDFUNDERLAY": ["AcDbPdfReference", "ObjectDBX Classes", 4095, 0, 1],
    "DWFDEFINITION": ["AcDbDwfDefinition", "ObjectDBX Classes", 1153, 0, 0],
    "DWFUNDERLAY": ["AcDbDwfReference", "ObjectDBX Classes", 1153, 0, 1],
    "DGNDEFINITION": ["AcDbDgnDefinition", "ObjectDBX Classes", 1153, 0, 0],
    "DGNUNDERLAY": ["AcDbDgnReference", "ObjectDBX Classes", 1153, 0, 1],
    "MENTALRAYRENDERSETTINGS": [
        "AcDbMentalRayRenderSettings",
        "SCENEOE",
        1024,
        0,
        0,
    ],
    "ACDBPLACEHOLDER": ["AcDbPlaceHolder", "ObjectDBX Classes", 0, 0, 0],
    "LAYOUT": ["AcDbLayout", "ObjectDBX Classes", 0, 0, 0],
    "SURFACE": ["AcDbSurface", "ObjectDBX Classes", 4095, 0, 1],
    "EXTRUDEDSURFACE": ["AcDbExtrudedSurface", "ObjectDBX Classes", 4095, 0, 1],
    "LOFTEDSURFACE": ["AcDbLoftedSurface", "ObjectDBX Classes", 0, 0, 1],
    "REVOLVEDSURFACE": ["AcDbRevolvedSurface", "ObjectDBX Classes", 0, 0, 1],
    "SWEPTSURFACE": ["AcDbSweptSurface", "ObjectDBX Classes", 0, 0, 1],
    "PLANESURFACE": ["AcDbPlaneSurface", "ObjectDBX Classes", 4095, 0, 1],
    "NURBSSURFACE": ["AcDbNurbSurface", "ObjectDBX Classes", 4095, 0, 1],
    "ACDBASSOCEXTRUDEDSURFACEACTIONBODY": [
        "AcDbAssocExtrudedSurfaceActionBody",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "ACDBASSOCLOFTEDSURFACEACTIONBODY": [
        "AcDbAssocLoftedSurfaceActionBody",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "ACDBASSOCREVOLVEDSURFACEACTIONBODY": [
        "AcDbAssocRevolvedSurfaceActionBody",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "ACDBASSOCSWEPTSURFACEACTIONBODY": [
        "AcDbAssocSweptSurfaceActionBody",
        "ObjectDBX Classes",
        1025,
        0,
        0,
    ],
    "HELIX": ["AcDbHelix", "ObjectDBX Classes", 4095, 0, 1],
    "WIPEOUT": ["AcDbWipeout", "WipeOut", 127, 0, 1],
    "WIPEOUTVARIABLES": ["AcDbWipeoutVariables", "WipeOut", 0, 0, 0],
    "FIELDLIST": ["AcDbFieldList", "ObjectDBX Classes", 1152, 0, 0],
    "GEODATA": ["AcDbGeoData", "ObjectDBX Classes", 4095, 0, 0],
    "SORTENTSTABLE": ["AcDbSortentsTable", "ObjectDBX Classes", 0, 0, 0],
    "ACAD_TABLE": ["AcDbTable", "ObjectDBX Classes", 1025, 0, 1],
    "ARC_DIMENSION": ["AcDbArcDimension", "ObjectDBX Classes", 1025, 0, 1],
    "LARGE_RADIAL_DIMENSION": [
        "AcDbRadialDimensionLarge",
        "ObjectDBX Classes",
        1025,
        0,
        1,
    ],
}

REQ_R2000 = [
    "ACDBDICTIONARYWDFLT",
    "SUN",
    "VISUALSTYLE",
    "MATERIAL",
    "SCALE",
    "TABLESTYLE",
    "MLEADERSTYLE",
    "DICTIONARYVAR",
    "CELLSTYLEMAP",
    "MENTALRAYRENDERSETTINGS",
    "ACDBDETAILVIEWSTYLE",
    "ACDBSECTIONVIEWSTYLE",
    "RASTERVARIABLES",
    "ACDBPLACEHOLDER",
    "LAYOUT",
]

REQ_R2004 = [
    "ACDBDICTIONARYWDFLT",
    "SUN",
    "VISUALSTYLE",
    "MATERIAL",
    "SCALE",
    "TABLESTYLE",
    "MLEADERSTYLE",
    "DICTIONARYVAR",
    "CELLSTYLEMAP",
    "MENTALRAYRENDERSETTINGS",
    "ACDBDETAILVIEWSTYLE",
    "ACDBSECTIONVIEWSTYLE",
    "RASTERVARIABLES",
]

REQUIRED_CLASSES = {
    DXF2000: REQ_R2000,
    DXF2004: REQ_R2004,
}


class ClassesSection:
    def __init__(
        self,
        doc: Optional[Drawing] = None,
        entities: Optional[Iterable[DXFEntity]] = None,
    ):
        # Multiple entries for 'name' possible -> key is (name, cpp_class_name)
        # DXFClasses are not stored in the entities database, because CLASS has
        # no handle.
        self.classes: dict[tuple[str, str], DXFClass] = OrderedDict()
        self.doc = doc
        if entities is not None:
            self.load(iter(entities))

    def __iter__(self) -> Iterator[DXFClass]:
        return (cls for cls in self.classes.values())

    def load(self, entities: Iterator[DXFEntity]) -> None:
        section_head = cast(DXFTagStorage, next(entities))

        if section_head.dxftype() != "SECTION" or section_head.base_class[1] != (
            2,
            "CLASSES",
        ):
            raise DXFStructureError("Critical structure error in CLASSES section.")

        for cls_entity in entities:
            if isinstance(cls_entity, DXFClass):
                self.register(cls_entity)
            else:
                logger.warning(
                    f"Ignored invalid DXF entity type '{cls_entity.dxftype()}'"
                    f" in section CLASSES."
                )

    def register(
        self, classes: Optional[Union[DXFClass, Iterable[DXFClass]]] = None
    ) -> None:
        if classes is None:
            return

        if isinstance(classes, DXFClass):
            classes = (classes,)

        for dxfclass in classes:
            key = dxfclass.key
            if key not in self.classes:
                self.classes[key] = dxfclass

    def add_class(self, name: str):
        """Register a known class by `name`."""
        if name not in CLASS_DEFINITIONS:
            return
        cls_data = CLASS_DEFINITIONS[name]
        cls = DXFClass.new(doc=self.doc)
        cpp, app, flags, proxy, entity = cls_data
        cls.update_dxf_attribs(
            {
                "name": name,
                "cpp_class_name": cpp,
                "app_name": app,
                "flags": flags,
                "was_a_proxy": proxy,
                "is_an_entity": entity,
            }
        )
        self.register(cls)

    def get(self, name: str) -> DXFClass:
        """Returns the first class matching `name`.

        Storage key is the ``(name, cpp_class_name)`` tuple, because there are
        some classes with the same :attr:`name` but different
        :attr:`cpp_class_names`.

        """
        for cls in self.classes.values():
            if cls.dxf.name == name:
                return cls
        raise DXFKeyError(name)

    def add_required_classes(self, dxfversion: str) -> None:
        """Add all required CLASS definitions for the specified DXF version."""
        names = REQUIRED_CLASSES.get(dxfversion, REQ_R2004)
        for name in names:
            self.add_class(name)

        if self.doc is None:  # testing environment SUT
            return

        dxf_types_in_use = self.doc.entitydb.dxf_types_in_use()
        if "IMAGE" in dxf_types_in_use:
            self.add_class("IMAGE")
            self.add_class("IMAGEDEF")
            self.add_class("IMAGEDEF_REACTOR")
        if "PDFUNDERLAY" in dxf_types_in_use:
            self.add_class("PDFDEFINITION")
            self.add_class("PDFUNDERLAY")
        if "DWFUNDERLAY" in dxf_types_in_use:
            self.add_class("DWFDEFINITION")
            self.add_class("DWFUNDERLAY")
        if "DGNUNDERLAY" in dxf_types_in_use:
            self.add_class("DGNDEFINITION")
            self.add_class("DGNUNDERLAY")
        if "EXTRUDEDSURFACE" in dxf_types_in_use:
            self.add_class("EXTRUDEDSURFACE")
            self.add_class("ACDBASSOCEXTRUDEDSURFACEACTIONBODY")
        if "LOFTEDSURFACE" in dxf_types_in_use:
            self.add_class("LOFTEDSURFACE")
            self.add_class("ACDBASSOCLOFTEDSURFACEACTIONBODY")
        if "REVOLVEDSURFACE" in dxf_types_in_use:
            self.add_class("REVOLVEDSURFACE")
            self.add_class("ACDBASSOCREVOLVEDSURFACEACTIONBODY")
        if "SWEPTSURFACE" in dxf_types_in_use:
            self.add_class("SWEPTSURFACE")
            self.add_class("ACDBASSOCSWEPTSURFACEACTIONBODY")

        for dxftype in dxf_types_in_use:
            self.add_class(dxftype)

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        """Export DXF tags. (internal API)"""
        tagwriter.write_str("  0\nSECTION\n  2\nCLASSES\n")
        for dxfclass in self.classes.values():
            dxfclass.export_dxf(tagwriter)
        tagwriter.write_str("  0\nENDSEC\n")

    def update_instance_counters(self) -> None:
        """Update CLASS instance counter for all registered classes, requires
        DXF R2004+.
        """
        assert self.doc is not None
        if self.doc.dxfversion < DXF2004:
            return  # instance counter not supported
        counter: dict[str, int] = Counter()
        # count all entities in the entity database
        for entity in self.doc.entitydb.values():
            counter[entity.dxftype()] += 1

        for dxfclass in self.classes.values():
            dxfclass.dxf.instance_count = counter[dxfclass.dxf.name]
