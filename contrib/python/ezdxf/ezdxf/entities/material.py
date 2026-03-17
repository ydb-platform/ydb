# Copyright (c) 2018-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
from ezdxf.lldxf.const import SUBCLASS_MARKER
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    group_code_mapping,
)
from ezdxf.lldxf.tags import Tags
from .dxfentity import base_class, SubclassProcessor
from .dxfobj import DXFObject
from .factory import register_entity
from .objectcollection import ObjectCollection
from ezdxf.math import Matrix44
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace, DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.document import Drawing

__all__ = ["Material", "MaterialCollection"]


def fetch_matrix(tags: Tags, code: int) -> tuple[Tags, Optional[Matrix44]]:
    values = []
    remaining = Tags()
    for tag in tags:
        if tag.code == code:
            values.append(tag.value)
            if len(values) == 16:
                # enough values collected, code 43 is maybe used for two matrices
                code = -1
        else:
            remaining.append(tag)
    if len(values) == 16:
        # only if valid matrix
        return remaining, Matrix44(values)
    else:
        return tags, None


def export_matrix(tagwriter: AbstractTagWriter, code: int, matrix: Matrix44) -> None:
    if matrix is not None:
        for value in matrix:
            tagwriter.write_tag2(code, value)


acdb_material = DefSubclass(
    "AcDbMaterial",
    {
        "name": DXFAttr(1),
        "description": DXFAttr(2, default=""),
        "ambient_color_method": DXFAttr(
            70, default=0
        ),  # 0=use current color; 1=override current color
        "ambient_color_factor": DXFAttr(40, default=1.0),  # valid range is 0.0 to 1.0
        "ambient_color_value": DXFAttr(90),  # integer representing an AcCmEntityColor
        "diffuse_color_method": DXFAttr(
            71, default=0
        ),  # 0=use current color; 1=override current color
        "diffuse_color_factor": DXFAttr(41, default=1.0),  # valid range is 0.0 to 1.0
        "diffuse_color_value": DXFAttr(
            91, default=-1023410177
        ),  # integer representing an AcCmEntityColor
        "diffuse_map_blend_factor": DXFAttr(
            42, default=1.0
        ),  # valid range is 0.0 to 1.0
        "diffuse_map_source": DXFAttr(72, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "diffuse_map_file_name": DXFAttr(3, default=""),
        "diffuse_map_projection_method": DXFAttr(
            73, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "diffuse_map_tiling_method": DXFAttr(74, default=1),  # 1=Tile; 2=Crop; 3=Clamp
        "diffuse_map_auto_transform_method": DXFAttr(75, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 43: Transform matrix of diffuse map mapper (16 reals; row major format; default = identity matrix)
        "specular_gloss_factor": DXFAttr(44, default=0.5),  # valid range is 0.0 to 1.0
        "specular_color_method": DXFAttr(
            73, default=0
        ),  # 0=use current color; 1=override current color
        "specular_color_factor": DXFAttr(45, default=1.0),  # valid range is 0.0 to 1.0
        "specular_color_value": DXFAttr(92),  # integer representing an AcCmEntityColor
        "specular_map_blend_factor": DXFAttr(
            46, default=1.0
        ),  # valid range is 0.0 to 1.0
        "specular_map_source": DXFAttr(77, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "specular_map_file_name": DXFAttr(4, default=""),
        "specular_map_projection_method": DXFAttr(
            78, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "specular_map_tiling_method": DXFAttr(79, default=1),  # 1=Tile; 2=Crop; 3=Clamp
        "specular_map_auto_transform_method": DXFAttr(170, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 47: Transform matrix of specular map mapper (16 reals; row major format; default = identity matrix)
        "reflection_map_blend_factor": DXFAttr(
            48, default=1.0
        ),  # valid range is 0.0 to 1.0
        "reflection_map_source": DXFAttr(171, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "reflection_map_file_name": DXFAttr(6, default=""),
        "reflection_map_projection_method": DXFAttr(
            172, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "reflection_map_tiling_method": DXFAttr(
            173, default=1
        ),  # 1=Tile; 2=Crop; 3=Clamp
        "reflection_map_auto_transform_method": DXFAttr(174, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 49: Transform matrix of reflection map mapper (16 reals; row major format; default = identity matrix)
        "opacity": DXFAttr(140, default=1.0),  # valid range is 0.0 to 1.0
        "opacity_map_blend_factor": DXFAttr(
            141, default=1.0
        ),  # valid range is 0.0 to 1.0
        "opacity_map_source": DXFAttr(175, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "opacity_map_file_name": DXFAttr(7, default=""),
        "opacity_map_projection_method": DXFAttr(
            176, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "opacity_map_tiling_method": DXFAttr(177, default=1),  # 1=Tile; 2=Crop; 3=Clamp
        "opacity_map_auto_transform_method": DXFAttr(178, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 142: Transform matrix of reflection map mapper (16 reals; row major format; default = identity matrix)
        "bump_map_blend_factor": DXFAttr(143, default=1.0),  # valid range is 0.0 to 1.0
        "bump_map_source": DXFAttr(179, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "bump_map_file_name": DXFAttr(8, default=""),
        "bump_map_projection_method": DXFAttr(
            270, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "bump_map_tiling_method": DXFAttr(271, default=1),  # 1=Tile; 2=Crop; 3=Clamp
        "bump_map_auto_transform_method": DXFAttr(272, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 144: Transform matrix of bump map mapper (16 reals; row major format; default = identity matrix)
        "refraction_index": DXFAttr(145, default=1.0),  # valid range is 0.0 to 1.0
        "refraction_map_blend_factor": DXFAttr(
            146, default=1.0
        ),  # valid range is 0.0 to 1.0
        "refraction_map_source": DXFAttr(273, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "refraction_map_file_name": DXFAttr(9, default=""),
        "refraction_map_projection_method": DXFAttr(
            274, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "refraction_map_tiling_method": DXFAttr(
            275, default=1
        ),  # 1=Tile; 2=Crop; 3=Clamp
        "refraction_map_auto_transform_method": DXFAttr(276, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 147: Transform matrix of reflection map mapper (16 reals; row major format; default = identity matrix)
        # normal map shares group codes with diffuse map
        "normal_map_method": DXFAttr(271),
        "normal_map_strength": DXFAttr(465),
        "normal_map_blend_factor": DXFAttr(
            42, default=1.0
        ),  # valid range is 0.0 to 1.0
        "normal_map_source": DXFAttr(72, default=1),
        # 0=use current scene; 1=use image file (specified by file name; null file name specifies no map)
        "normal_map_file_name": DXFAttr(3, default=""),
        "normal_map_projection_method": DXFAttr(
            73, default=1
        ),  # 1=Planar; 2=Box; 3=Cylinder; 4=Sphere
        "normal_map_tiling_method": DXFAttr(74, default=1),  # 1=Tile; 2=Crop; 3=Clamp
        "normal_map_auto_transform_method": DXFAttr(75, default=1),  # bitset;
        # 1 = No auto transform
        # 2 = Scale mapper to current entity extents; translate mapper to entity origin
        # 4 = Include current block transform in mapper transform
        # 16x group code 43: Transform matrix of reflection map mapper (16 reals; row major format; default = identity matrix)
        "color_bleed_scale": DXFAttr(460),
        "indirect_dump_scale": DXFAttr(461),
        "reflectance_scale": DXFAttr(462),
        "transmittance_scale": DXFAttr(463),
        "two_sided_material": DXFAttr(290),
        "luminance": DXFAttr(464),
        "luminance_mode": DXFAttr(270),  # multiple usage of group code 270
        "materials_anonymous": DXFAttr(293),
        "global_illumination_mode": DXFAttr(272),
        "final_gather_mode": DXFAttr(273),
        "gen_proc_name": DXFAttr(300),
        "gen_proc_val_bool": DXFAttr(291),
        "gen_proc_val_int": DXFAttr(271),
        "gen_proc_val_real": DXFAttr(469),
        "gen_proc_val_text": DXFAttr(301),
        "gen_proc_table_end": DXFAttr(292),
        "gen_proc_val_color_index": DXFAttr(62),
        "gen_proc_val_color_rgb": DXFAttr(420),
        "gen_proc_val_color_name": DXFAttr(430),
        "map_utile": DXFAttr(270),  # multiple usage of group code 270
        "translucence": DXFAttr(148),
        "self_illumination": DXFAttr(90),
        "reflectivity": DXFAttr(468),
        "illumination_model": DXFAttr(93),
        "channel_flags": DXFAttr(94, default=63),
    },
)
acdb_material_group_codes = group_code_mapping(acdb_material)


@register_entity
class Material(DXFObject):
    DXFTYPE = "MATERIAL"
    DEFAULT_ATTRIBS = {
        "diffuse_color_method": 1,
        "diffuse_color_value": -1023410177,
    }
    DXFATTRIBS = DXFAttributes(base_class, acdb_material)

    def __init__(self) -> None:
        super().__init__()
        self.diffuse_mapper_matrix: Optional[Matrix44] = None  # code 43
        self.specular_mapper_matrix: Optional[Matrix44] = None  # code 47
        self.reflexion_mapper_matrix: Optional[Matrix44] = None  # code 49
        self.opacity_mapper_matrix: Optional[Matrix44] = None  # group 142
        self.bump_mapper_matrix: Optional[Matrix44] = None  # group 144
        self.refraction_mapper_matrix: Optional[Matrix44] = None  # code 147
        self.normal_mapper_matrix: Optional[Matrix44] = None  # code 43 ???

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy material mapper matrices"""

        def copy(matrix):
            return None if matrix is None else matrix.copy()

        assert isinstance(entity, Material)
        entity.diffuse_mapper_matrix = copy(self.diffuse_mapper_matrix)
        entity.specular_mapper_matrix = copy(self.specular_mapper_matrix)
        entity.reflexion_mapper_matrix = copy(self.reflexion_mapper_matrix)
        entity.opacity_mapper_matrix = copy(self.opacity_mapper_matrix)
        entity.bump_mapper_matrix = copy(self.bump_mapper_matrix)
        entity.refraction_mapper_matrix = copy(self.refraction_mapper_matrix)
        entity.normal_mapper_matrix = copy(self.normal_mapper_matrix)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.fast_load_dxfattribs(
                dxf, acdb_material_group_codes, 1, log=False
            )
            self.load_matrices(tags)
        return dxf

    def load_matrices(self, tags):
        tags, matrix = fetch_matrix(tags, 43)
        if matrix:
            self.diffuse_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 47)
        if matrix:
            self.specular_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 49)
        if matrix:
            self.reflexion_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 142)
        if matrix:
            self.opacity_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 144)
        if matrix:
            self.bump_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 147)
        if matrix:
            self.refraction_mapper_matrix = matrix
        tags, matrix = fetch_matrix(tags, 43)
        if matrix:
            self.normal_mapper_matrix = matrix

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)

        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_material.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "description",
                "ambient_color_method",
                "ambient_color_factor",
                "ambient_color_value",
                "diffuse_color_method",
                "diffuse_color_factor",
                "diffuse_color_value",
                "diffuse_map_blend_factor",
                "diffuse_map_source",
                "diffuse_map_file_name",
                "diffuse_map_projection_method",
                "diffuse_map_tiling_method",
                "diffuse_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 43, self.diffuse_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "specular_gloss_factor",
                "specular_color_method",
                "specular_color_factor",
                "specular_color_value",
                "specular_map_blend_factor",
                "specular_map_source",
                "specular_map_file_name",
                "specular_map_projection_method",
                "specular_map_tiling_method",
                "specular_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 47, self.specular_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "reflection_map_blend_factor",
                "reflection_map_source",
                "reflection_map_file_name",
                "reflection_map_projection_method",
                "reflection_map_tiling_method",
                "reflection_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 49, self.reflexion_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "opacity",
                "opacity_map_blend_factor",
                "opacity_map_source",
                "opacity_map_file_name",
                "opacity_map_projection_method",
                "opacity_map_tiling_method",
                "opacity_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 142, self.opacity_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "bump_map_blend_factor",
                "bump_map_source",
                "bump_map_file_name",
                "bump_map_projection_method",
                "bump_map_tiling_method",
                "bump_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 144, self.bump_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "refraction_index",
                "refraction_map_blend_factor",
                "refraction_map_source",
                "refraction_map_file_name",
                "refraction_map_projection_method",
                "refraction_map_tiling_method",
                "refraction_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 147, self.refraction_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "normal_map_method",
                "normal_map_strength",
                "normal_map_blend_factor",
                "normal_map_source",
                "normal_map_file_name",
                "normal_map_projection_method",
                "normal_map_tiling_method",
                "normal_map_auto_transform_method",
            ],
        )
        export_matrix(tagwriter, 43, self.normal_mapper_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "color_bleed_scale",
                "indirect_dump_scale",
                "reflectance_scale",
                "transmittance_scale",
                "two_sided_material",
                "luminance",
                "luminance_mode",
                "materials_anonymous",
                "global_illumination_mode",
                "final_gather_mode",
                "gen_proc_name",
                "gen_proc_val_bool",
                "gen_proc_val_int",
                "gen_proc_val_real",
                "gen_proc_val_text",
                "gen_proc_table_end",
                "gen_proc_val_color_index",
                "gen_proc_val_color_rgb",
                "gen_proc_val_color_name",
                "map_utile",
                "translucence",
                "self_illumination",
                "reflectivity",
                "illumination_model",
                "channel_flags",
            ],
        )


class MaterialCollection(ObjectCollection[Material]):
    def __init__(self, doc: Drawing):
        super().__init__(doc, dict_name="ACAD_MATERIAL", object_type="MATERIAL")
        self.create_required_entries()

    def create_required_entries(self) -> None:
        for name in ("ByBlock", "ByLayer", "Global"):
            if name not in self:
                self.new(name)
