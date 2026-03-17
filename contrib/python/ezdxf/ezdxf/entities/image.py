# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations

import os
import pathlib
from typing import (
    TYPE_CHECKING,
    Iterable,
    cast,
    Optional,
    Callable,
    Union,
    Type,
)
from typing_extensions import Self

import logging
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.const import SUBCLASS_MARKER, DXF2000, DXF2010
from ezdxf.math import Vec3, Vec2, BoundingBox2d, UVec, Matrix44
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .dxfobj import DXFObject
from .factory import register_entity
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.entities import DXFNamespace, DXFEntity, Dictionary
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.lldxf.types import DXFTag
    from ezdxf.document import Drawing
    from ezdxf import xref

__all__ = ["Image", "ImageDef", "ImageDefReactor", "RasterVariables", "Wipeout"]
logger = logging.getLogger("ezdxf")


class ImageBase(DXFGraphic):
    """DXF IMAGE entity"""

    DXFTYPE = "IMAGEBASE"
    _CLS_GROUP_CODES: dict[int, Union[str, list[str]]] = dict()
    _SUBCLASS_NAME = "dummy"
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    SHOW_IMAGE = 1
    SHOW_IMAGE_WHEN_NOT_ALIGNED = 2
    USE_CLIPPING_BOUNDARY = 4
    USE_TRANSPARENCY = 8

    def __init__(self) -> None:
        super().__init__()
        # Boundary/Clipping path coordinates:
        # 0/0 is in the Left/Top corner of the image!
        # x-coordinates increases in u_pixel vector direction
        # y-coordinates increases against the v_pixel vector!
        # see also WCS coordinate calculation
        self._boundary_path: list[Vec2] = []

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, ImageBase)
        entity._boundary_path = list(self._boundary_path)

    def post_new_hook(self) -> None:
        super().post_new_hook()
        self.reset_boundary_path()

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            path_tags = processor.subclasses[2].pop_tags(codes=(14,))
            self.load_boundary_path(path_tags)
            processor.fast_load_dxfattribs(dxf, self._CLS_GROUP_CODES, 2, recover=True)
            if len(self.boundary_path) < 2:  # something is wrong
                self.dxf = dxf
                self.reset_boundary_path()
        return dxf

    def load_boundary_path(self, tags: Iterable[DXFTag]):
        self._boundary_path = [Vec2(value) for code, value in tags if code == 14]

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, self._SUBCLASS_NAME)
        self.dxf.count_boundary_points = len(self.boundary_path)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "class_version",
                "insert",
                "u_pixel",
                "v_pixel",
                "image_size",
                "image_def_handle",
                "flags",
                "clipping",
                "brightness",
                "contrast",
                "fade",
                "image_def_reactor_handle",
                "clipping_boundary_type",
                "count_boundary_points",
            ],
        )
        self.export_boundary_path(tagwriter)
        if tagwriter.dxfversion >= DXF2010:
            self.dxf.export_dxf_attribs(tagwriter, "clip_mode")

    def export_boundary_path(self, tagwriter: AbstractTagWriter):
        for vertex in self.boundary_path:
            tagwriter.write_vertex(14, vertex)

    @property
    def boundary_path(self):
        """Returns the boundray path in raw form in pixel coordinates.

        A list of vertices as pixel coordinates, Two vertices describe a
        rectangle, lower left corner is (-0.5, -0.5) and upper right corner
        is (ImageSizeX-0.5, ImageSizeY-0.5), more than two vertices is a
        polygon as clipping path. All vertices as pixel coordinates. (read/write)
        """
        return self._boundary_path

    @boundary_path.setter
    def boundary_path(self, vertices: Iterable[UVec]) -> None:
        self.set_boundary_path(vertices)

    def set_boundary_path(self, vertices: Iterable[UVec]) -> None:
        """Set boundary path to `vertices`. Two vertices describe a rectangle
        (lower left and upper right corner), more than two vertices is a polygon
        as clipping path.
        """
        _vertices = Vec2.list(vertices)
        if len(_vertices):
            if len(_vertices) > 2 and not _vertices[-1].isclose(_vertices[0]):
                # Close path, otherwise AutoCAD crashes
                _vertices.append(_vertices[0])
            self._boundary_path = _vertices
            self.set_flag_state(self.USE_CLIPPING_BOUNDARY, state=True)
            self.dxf.clipping = 1
            self.dxf.clipping_boundary_type = 1 if len(_vertices) < 3 else 2
            self.dxf.count_boundary_points = len(self._boundary_path)
        else:
            self.reset_boundary_path()

    def reset_boundary_path(self) -> None:
        """Reset boundary path to the default rectangle [(-0.5, -0.5),
        (ImageSizeX-0.5, ImageSizeY-0.5)].
        """
        lower_left_corner = Vec2(-0.5, -0.5)
        upper_right_corner = Vec2(self.dxf.image_size) + lower_left_corner
        self._boundary_path = [lower_left_corner, upper_right_corner]
        self.set_flag_state(Image.USE_CLIPPING_BOUNDARY, state=False)
        self.dxf.clipping = 0
        self.dxf.clipping_boundary_type = 1
        self.dxf.count_boundary_points = 2

    def transform(self, m: Matrix44) -> Self:
        """Transform IMAGE entity by transformation matrix `m` inplace."""
        self.dxf.insert = m.transform(self.dxf.insert)
        self.dxf.u_pixel = m.transform_direction(self.dxf.u_pixel)
        self.dxf.v_pixel = m.transform_direction(self.dxf.v_pixel)
        self.post_transform(m)
        return self

    def get_wcs_transform(self) -> Matrix44:
        m = Matrix44()
        m.set_row(0, Vec3(self.dxf.u_pixel))
        m.set_row(1, Vec3(self.dxf.v_pixel))
        m.set_row(3, Vec3(self.dxf.insert))
        return m

    def pixel_boundary_path(self) -> list[Vec2]:
        """Returns the boundary path as closed loop in pixel coordinates.  Resolves the 
        simple form of two vertices as a rectangle.  The image coordinate system has an 
        inverted y-axis and the top-left corner is (0, 0).

        .. versionchanged:: 1.2.0

            renamed from :meth:`boundray_path_ocs()`

        """
        boundary_path = self.boundary_path
        if len(boundary_path) == 2:  # rectangle
            p0, p1 = boundary_path
            boundary_path = [p0, Vec2(p1.x, p0.y), p1, Vec2(p0.x, p1.y)]
        if not boundary_path[0].isclose(boundary_path[-1]):
            boundary_path.append(boundary_path[0])
        return boundary_path

    def boundary_path_wcs(self) -> list[Vec3]:
        """Returns the boundary/clipping path in WCS coordinates.

        It's recommended to acquire the clipping path as :class:`~ezdxf.path.Path` object
        by the :func:`~ezdxf.path.make_path` function::

            from ezdxf.path import make_path

            image = ...  # get image entity
            clipping_path = make_path(image)

        """

        u = Vec3(self.dxf.u_pixel)
        v = Vec3(self.dxf.v_pixel)
        origin = Vec3(self.dxf.insert)
        origin += u * 0.5 - v * 0.5
        height = self.dxf.image_size.y
        # Boundary/Clipping path origin 0/0 is in the Left/Top corner of the image!
        vertices = [
            origin + (u * p.x) + (v * (height - p.y))
            for p in self.pixel_boundary_path()
        ]
        return vertices

    def destroy(self) -> None:
        if not self.is_alive:
            return

        del self._boundary_path
        super().destroy()


acdb_image = DefSubclass(
    "AcDbRasterImage",
    {
        "class_version": DXFAttr(90, dxfversion=DXF2000, default=0),
        "insert": DXFAttr(10, xtype=XType.point3d),
        # U-vector of a single pixel (points along the visual bottom of the image,
        # starting at the insertion point)
        "u_pixel": DXFAttr(11, xtype=XType.point3d),
        # V-vector of a single pixel (points along the visual left side of the
        # image, starting at the insertion point)
        "v_pixel": DXFAttr(12, xtype=XType.point3d),
        # Image size in pixels
        "image_size": DXFAttr(13, xtype=XType.point2d),
        # Hard reference to image def object
        "image_def_handle": DXFAttr(340),
        # Image display properties:
        # 1 = Show image
        # 2 = Show image when not aligned with screen
        # 4 = Use clipping boundary
        # 8 = Transparency is on
        "flags": DXFAttr(70, default=3),
        # Clipping state:
        # 0 = Off
        # 1 = On
        "clipping": DXFAttr(
            280,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Brightness value (0-100; default = 50)
        "brightness": DXFAttr(
            281,
            default=50,
            validator=validator.is_in_integer_range(0, 101),
            fixer=validator.fit_into_integer_range(0, 101),
        ),
        # Contrast value (0-100; default = 50)
        "contrast": DXFAttr(
            282,
            default=50,
            validator=validator.is_in_integer_range(0, 101),
            fixer=validator.fit_into_integer_range(0, 101),
        ),
        # Fade value (0-100; default = 0)
        "fade": DXFAttr(
            283,
            default=0,
            validator=validator.is_in_integer_range(0, 101),
            fixer=validator.fit_into_integer_range(0, 101),
        ),
        # Hard reference to image def reactor object, not required by AutoCAD
        "image_def_reactor_handle": DXFAttr(360),
        # Clipping boundary type:
        # 1 = Rectangular
        # 2 = Polygonal
        "clipping_boundary_type": DXFAttr(
            71,
            default=1,
            validator=validator.is_one_of({1, 2}),
            fixer=RETURN_DEFAULT,
        ),
        # Number of clip boundary vertices that follow
        "count_boundary_points": DXFAttr(91),
        # Clip mode:
        # 0 = outside
        # 1 = inside mode
        "clip_mode": DXFAttr(
            290,
            dxfversion=DXF2010,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # boundary path coordinates are pixel coordinates NOT drawing units
    },
)
acdb_image_group_codes = group_code_mapping(acdb_image)


@register_entity
class Image(ImageBase):
    """DXF IMAGE entity"""

    DXFTYPE = "IMAGE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_image)
    _CLS_GROUP_CODES = acdb_image_group_codes
    _SUBCLASS_NAME = acdb_image.name  # type: ignore
    DEFAULT_ATTRIBS = {"layer": "0", "flags": 3}

    def __init__(self) -> None:
        super().__init__()
        self._boundary_path: list[Vec2] = []
        self._image_def: Optional[ImageDef] = None
        self._image_def_reactor: Optional[ImageDefReactor] = None

    @classmethod
    def new(
        cls: Type[Image],
        handle: Optional[str] = None,
        owner: Optional[str] = None,
        dxfattribs: Optional[dict] = None,
        doc: Optional[Drawing] = None,
    ) -> Image:
        dxfattribs = dxfattribs or {}
        # 'image_def' is not a real DXF attribute (image_def_handle)
        image_def = dxfattribs.pop("image_def", None)
        image_size = (1, 1)
        if image_def and image_def.is_alive:
            image_size = image_def.dxf.image_size
        dxfattribs.setdefault("image_size", image_size)

        image = cast("Image", super().new(handle, owner, dxfattribs, doc))
        image.image_def = image_def
        return image

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, Image)
        super().copy_data(entity, copy_strategy=copy_strategy)
        # Each IMAGE has its own ImageDefReactor object, which will be created by
        # binding the copy to the document.
        entity.dxf.discard("image_def_reactor_handle")
        entity._image_def_reactor = None
        # shared IMAGE_DEF
        entity._image_def = self._image_def

    def post_bind_hook(self) -> None:
        # Document in LOAD process -> post_load_hook()
        if self.doc.is_loading:  # type: ignore
            return
        if self._image_def_reactor:  # ImageDefReactor already exist
            return
        # The new Image was created by ezdxf and the ImageDefReactor
        # object does not exist:
        self._create_image_def_reactor()

    def post_load_hook(self, doc: Drawing) -> Optional[Callable]:
        super().post_load_hook(doc)
        db = doc.entitydb
        self._image_def = db.get(self.dxf.get("image_def_handle", None))  # type: ignore
        if self._image_def is None:
            # unrecoverable structure error
            self.destroy()
            return None

        self._image_def_reactor = db.get(  # type: ignore
            self.dxf.get("image_def_reactor_handle", None)
        )
        if self._image_def_reactor is None:
            # Image and ImageDef exist - this is recoverable by creating
            # an ImageDefReactor, but the objects section does not exist yet!
            # Return a post init command:
            return self._fix_missing_image_def_reactor
        return None

    def _fix_missing_image_def_reactor(self):
        try:
            self._create_image_def_reactor()
        except Exception as e:
            logger.exception(
                f"An exception occurred while executing fixing command for "
                f"{str(self)}, destroying entity.",
                exc_info=e,
            )
            self.destroy()
            return
        logger.debug(f"Created missing ImageDefReactor for {str(self)}")

    def _create_image_def_reactor(self):
        # ImageDef -> ImageDefReactor -> Image
        image_def_reactor = self.doc.objects.add_image_def_reactor(self.dxf.handle)
        reactor_handle = image_def_reactor.dxf.handle
        # Link Image to ImageDefReactor:
        self.dxf.image_def_reactor_handle = reactor_handle
        self._image_def_reactor = image_def_reactor
        # Link ImageDef to ImageDefReactor if in same document (XREF mapping!):
        if self.doc is self._image_def.doc:
            self._image_def.append_reactor_handle(reactor_handle)

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        if isinstance(self.image_def, ImageDef):
            registry.add_handle(self.image_def.dxf.handle)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, Image)
        super().map_resources(clone, mapping)
        source_image_def = self.image_def
        if isinstance(source_image_def, ImageDef):
            name = self.get_image_def_name()
            name, clone_image_def = mapping.map_acad_dict_entry(
                "ACAD_IMAGE_DICT", name, source_image_def
            )
            if isinstance(clone_image_def, ImageDef):
                clone.image_def = clone_image_def
                if isinstance(clone._image_def_reactor, ImageDefReactor):
                    clone_image_def.append_reactor_handle(
                        clone._image_def_reactor.dxf.handle
                    )
        # Note:
        # The IMAGEDEF_REACTOR was created automatically at binding the copy to
        # a new document, but the handle of the IMAGEDEF_REACTOR was not add to the
        # IMAGEDEF reactor handles, because at this point the IMAGE had still a reference
        # to the IMAGEDEF of the source document.

    def get_image_def_name(self) -> str:
        """Returns the name of the `image_def` entry in the ACAD_IMAGE_DICT."""
        if self.doc is None:
            return ""
        image_dict = self.doc.rootdict.get_required_dict("ACAD_IMAGE_DICT")
        for name, entry in image_dict.items():
            if entry is self._image_def:
                return name
        return ""

    @property
    def image_def(self) -> Optional[ImageDef]:
        """Returns the associated IMAGEDEF entity, see :class:`ImageDef`."""
        return self._image_def

    @image_def.setter
    def image_def(self, image_def: ImageDef) -> None:
        if image_def and image_def.is_alive:
            self.dxf.image_def_handle = image_def.dxf.handle
            self._image_def = image_def
        else:
            self.dxf.discard("image_def_handle")
            self._image_def = None

    @property
    def image_def_reactor(self) -> Optional[ImageDefReactor]:
        """Returns the associated IMAGEDEF_REACTOR entity."""
        return self._image_def_reactor

    def destroy(self) -> None:
        if not self.is_alive:
            return

        reactor = self._image_def_reactor
        if reactor and reactor.is_alive:
            image_def = self.image_def
            if image_def and image_def.is_alive:
                image_def.discard_reactor_handle(reactor.dxf.handle)
            reactor.destroy()
        super().destroy()

    def audit(self, auditor: Auditor) -> None:
        super().audit(auditor)


# DXF reference error: Subclass marker (AcDbRasterImage)
acdb_wipeout = DefSubclass("AcDbWipeout", dict(acdb_image.attribs))
acdb_wipeout_group_codes = group_code_mapping(acdb_wipeout)


@register_entity
class Wipeout(ImageBase):
    """DXF WIPEOUT entity"""

    DXFTYPE = "WIPEOUT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_wipeout)
    DEFAULT_ATTRIBS = {
        "layer": "0",
        "flags": 7,
        "clipping": 1,
        "brightness": 50,
        "contrast": 50,
        "fade": 0,
        "image_size": (1, 1),
        "image_def_handle": "0",  # has no ImageDef()
        "image_def_reactor_handle": "0",  # has no ImageDefReactor()
        "clip_mode": 0,
    }
    _CLS_GROUP_CODES = acdb_wipeout_group_codes
    _SUBCLASS_NAME = acdb_wipeout.name  # type: ignore

    def set_masking_area(self, vertices: Iterable[UVec]) -> None:
        """Set a new masking area, the area is placed in the layout xy-plane."""
        self.update_dxf_attribs(self.DEFAULT_ATTRIBS)
        vertices = Vec2.list(vertices)
        bounds = BoundingBox2d(vertices)
        x_size, y_size = bounds.size

        dxf = self.dxf
        dxf.insert = Vec3(bounds.extmin)
        dxf.u_pixel = Vec3(x_size, 0, 0)
        dxf.v_pixel = Vec3(0, y_size, 0)

        def boundary_path():
            extmin = bounds.extmin
            for vertex in vertices:
                v = vertex - extmin
                yield Vec2(v.x / x_size - 0.5, 0.5 - v.y / y_size)

        self.set_boundary_path(boundary_path())

    def _reset_handles(self):
        self.dxf.image_def_reactor_handle = "0"
        self.dxf.image_def_handle = "0"

    def audit(self, auditor: Auditor) -> None:
        self._reset_handles()
        super().audit(auditor)

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        self._reset_handles()
        super().export_entity(tagwriter)


# About Image File Paths:
# See notes in knowledge graph: [[IMAGE File Paths]]
# https://ezdxf.mozman.at/notes/#/page/image%20file%20paths

acdb_image_def = DefSubclass(
    "AcDbRasterImageDef",
    {
        "class_version": DXFAttr(90, default=0),
        # File name of image:
        "filename": DXFAttr(1),
        # Image size in pixels:
        "image_size": DXFAttr(10, xtype=XType.point2d),
        # Default size of one pixel in AutoCAD units:
        "pixel_size": DXFAttr(11, xtype=XType.point2d, default=Vec2(0.01, 0.01)),
        "loaded": DXFAttr(280, default=1),
        # Resolution units - this enums differ from the usual drawing units,
        # units.py, same as for RasterVariables.dxf.units, but only these 3 values
        # are valid - confirmed by ODA Specs 20.4.81 IMAGEDEF:
        # 0 = No units
        # 2 = Centimeters
        # 5 = Inch
        "resolution_units": DXFAttr(
            281,
            default=0,
            validator=validator.is_one_of({0, 2, 5}),
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_image_def_group_codes = group_code_mapping(acdb_image_def)


@register_entity
class ImageDef(DXFObject):
    """DXF IMAGEDEF entity"""

    DXFTYPE = "IMAGEDEF"
    DXFATTRIBS = DXFAttributes(base_class, acdb_image_def)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_image_def_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_image_def.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "class_version",
                "filename",
                "image_size",
                "pixel_size",
                "loaded",
                "resolution_units",
            ],
        )


acdb_image_def_reactor = DefSubclass(
    "AcDbRasterImageDefReactor",
    {
        "class_version": DXFAttr(90, default=2),
        # Handle to image:
        "image_handle": DXFAttr(330),
    },
)
acdb_image_def_reactor_group_codes = group_code_mapping(acdb_image_def_reactor)


@register_entity
class ImageDefReactor(DXFObject):
    """DXF IMAGEDEF_REACTOR entity"""

    DXFTYPE = "IMAGEDEF_REACTOR"
    DXFATTRIBS = DXFAttributes(base_class, acdb_image_def_reactor)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_image_def_reactor_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_image_def_reactor.name)
        tagwriter.write_tag2(90, self.dxf.class_version)
        tagwriter.write_tag2(330, self.dxf.image_handle)


acdb_raster_variables = DefSubclass(
    "AcDbRasterVariables",
    {
        "class_version": DXFAttr(90, default=0),
        # Frame:
        # 0 = no frame
        # 1 = show frame
        "frame": DXFAttr(
            70,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Quality:
        # 0 = draft
        # 1 = high
        "quality": DXFAttr(
            71,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Units:
        # 0 = None
        # 1 = mm
        # 2 = cm
        # 3 = m
        # 4 = km
        # 5 = in
        # 6 = ft
        # 7 = yd
        # 8 = mi
        "units": DXFAttr(
            72,
            default=3,
            validator=validator.is_in_integer_range(0, 9),
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_raster_variables_group_codes = group_code_mapping(acdb_raster_variables)


@register_entity
class RasterVariables(DXFObject):
    """DXF RASTERVARIABLES entity"""

    DXFTYPE = "RASTERVARIABLES"
    DXFATTRIBS = DXFAttributes(base_class, acdb_raster_variables)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_raster_variables_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_raster_variables.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "class_version",
                "frame",
                "quality",
                "units",
            ],
        )


acdb_wipeout_variables = DefSubclass(
    "AcDbWipeoutVariables",
    {
        # Display-image-frame flag:
        # 0 = No frame
        # 1 = Display frame
        "frame": DXFAttr(
            70,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_wipeout_variables_group_codes = group_code_mapping(acdb_wipeout_variables)


@register_entity
class WipeoutVariables(DXFObject):
    """DXF WIPEOUTVARIABLES entity"""

    DXFTYPE = "WIPEOUTVARIABLES"
    DXFATTRIBS = DXFAttributes(base_class, acdb_wipeout_variables)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_wipeout_variables_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_wipeout_variables.name)
        self.dxf.export_dxf_attribs(tagwriter, "frame")
