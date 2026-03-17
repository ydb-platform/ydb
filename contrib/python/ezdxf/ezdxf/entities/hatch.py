# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional

from ezdxf.lldxf import const
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.tags import Tags
from ezdxf.math import NULLVEC, Z_AXIS
from .boundary_paths import AbstractBoundaryPath
from .dxfentity import base_class
from .dxfgfx import acdb_entity
from .factory import register_entity
from .polygon import DXFPolygon

if TYPE_CHECKING:
    from ezdxf.colors import RGB
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter


__all__ = ["Hatch"]


acdb_hatch = DefSubclass(
    "AcDbHatch",
    {
        # This subclass can also represent a MPolygon, whatever this is, never seen
        # such a MPolygon in the wild.
        # x- and y-axis always equal 0, z-axis represents the elevation:
        "elevation": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern name:
        "pattern_name": DXFAttr(2, default="SOLID"),
        # HATCH: Solid fill flag:
        # 0 = pattern fill
        # 1 = solid fill
        "solid_fill": DXFAttr(
            70,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # HATCH: associativity flag
        # 0 = non-associative
        # 1 = associative
        "associative": DXFAttr(
            71,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # 91: Number of boundary paths (loops)
        # following: Boundary path data. Repeats number of times specified by
        # code 91. See Boundary Path Data
        # Hatch style:
        # 0 = Hatch “odd parity” area (Normal style)
        # 1 = Hatch outermost area only (Outer style)
        # 2 = Hatch through entire area (Ignore style)
        "hatch_style": DXFAttr(
            75,
            default=const.HATCH_STYLE_NESTED,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern type:
        # 0 = User-defined
        # 1 = Predefined
        # 2 = Custom
        "pattern_type": DXFAttr(
            76,
            default=const.HATCH_TYPE_PREDEFINED,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern angle (pattern fill only) in degrees:
        "pattern_angle": DXFAttr(52, default=0),
        # Hatch pattern scale or spacing (pattern fill only):
        "pattern_scale": DXFAttr(
            41,
            default=1,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern double flag (pattern fill only):
        # 0 = not double
        # 1 = double
        "pattern_double": DXFAttr(
            77,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # 78: Number of pattern definition lines
        # following: Pattern line data. Repeats number of times specified by
        # code 78. See Pattern Data
        # Pixel size used to determine the density to perform various intersection
        # and ray casting operations in hatch pattern computation for associative
        # hatches and hatches created with the Flood method of hatching
        "pixel_size": DXFAttr(47, optional=True),
        # Number of seed points
        "n_seed_points": DXFAttr(
            98,
            default=0,
            validator=validator.is_greater_or_equal_zero,
            fixer=RETURN_DEFAULT,
        ),
        # 10, 20: Seed point (in OCS) 2D point (multiple entries)
        # 450 Indicates solid hatch or gradient; if solid hatch, the values for the
        # remaining codes are ignored but must be present. Optional;
        #
        # if code 450 is in the file, then the following codes must be in the
        # file: 451, 452, 453, 460, 461, 462, and 470.
        # If code 450 is not in the file, then the following codes must not be
        # in the file: 451, 452, 453, 460, 461, 462, and 470
        #
        #   0 = Solid hatch
        #   1 = Gradient
        #
        # 451 Zero is reserved for future use
        # 452 Records how colors were defined and is used only by dialog code:
        #
        #   0 = Two-color gradient
        #   1 = Single-color gradient
        #
        # 453 Number of colors:
        #
        #   0 = Solid hatch
        #   2 = Gradient
        #
        # 460 Rotation angle in radians for gradients (default = 0, 0)
        # 461 Gradient definition; corresponds to the Centered option on the
        #     Gradient Tab of the Boundary Hatch and Fill dialog box. Each gradient
        #     has two definitions, shifted and non-shifted. A Shift value describes
        #     the blend of the two definitions that should be used. A value of 0.0
        #     means only the non-shifted version should be used, and a value of 1.0
        #     means that only the shifted version should be used.
        #
        # 462 Color tint value used by dialog code (default = 0, 0; range is 0.0 to
        #     1.0). The color tint value is a gradient color and controls the degree
        #     of tint in the dialog when the Hatch group code 452 is set to 1.
        #
        # 463 Reserved for future use:
        #
        #   0 = First value
        #   1 = Second value
        #
        # 470 String (default = LINEAR)
    },
)
acdb_hatch_group_code = group_code_mapping(acdb_hatch)


@register_entity
class Hatch(DXFPolygon):
    """DXF HATCH entity"""

    DXFTYPE = "HATCH"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_hatch)
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000
    LOAD_GROUP_CODES = acdb_hatch_group_code

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_hatch.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "elevation",
                "extrusion",
                "pattern_name",
                "solid_fill",
                "associative",
            ],
        )
        self.paths.export_dxf(tagwriter, self.dxftype())
        self.dxf.export_dxf_attribs(tagwriter, ["hatch_style", "pattern_type"])
        if self.pattern:
            self.dxf.export_dxf_attribs(
                tagwriter, ["pattern_angle", "pattern_scale", "pattern_double"]
            )
            self.pattern.export_dxf(tagwriter)
        self.dxf.export_dxf_attribs(tagwriter, ["pixel_size"])
        self.export_seeds(tagwriter)
        if self.gradient and tagwriter.dxfversion > const.DXF2000:
            self.gradient.export_dxf(tagwriter)

    def load_seeds(self, tags: Tags) -> Tags:
        try:
            start_index = tags.tag_index(98)
        except const.DXFValueError:
            return tags
        seed_data = tags.collect_consecutive_tags(
            {98, 10, 20}, start=start_index
        )

        # Remove seed data from tags:
        del tags[start_index : start_index + len(seed_data) + 1]

        # Just process vertices with group code 10
        self.seeds = [value for code, value in seed_data if code == 10]
        return tags

    def export_seeds(self, tagwriter: AbstractTagWriter):
        tagwriter.write_tag2(98, len(self.seeds))
        for seed in self.seeds:
            tagwriter.write_vertex(10, seed[:2])

    def remove_dependencies(self, other: Optional[Drawing] = None) -> None:
        """Remove all dependencies from actual document. (internal API)"""
        if not self.is_alive:
            return

        super().remove_dependencies()
        self.remove_association()

    def remove_association(self):
        """Remove associated path elements."""
        if self.dxf.associative:
            self.dxf.associative = 0
            for path in self.paths:
                path.source_boundary_objects = []

    def set_solid_fill(
        self, color: int = 7, style: int = 1, rgb: Optional[RGB] = None
    ):
        """Set the solid fill mode and removes all gradient and pattern fill related
        data.

        Args:
            color: :ref:`ACI`, (0 = BYBLOCK; 256 = BYLAYER)
            style: hatch style (0 = normal; 1 = outer; 2 = ignore)
            rgb: true color value as (r, g, b)-tuple - has higher priority
                than `color`. True color support requires DXF R2000.

        """
        # remove existing gradient and pattern fill
        self.gradient = None
        self.pattern = None
        self.dxf.solid_fill = 1

        # if true color is present, the color attribute is ignored
        self.dxf.color = color
        self.dxf.hatch_style = style
        self.dxf.pattern_name = "SOLID"
        self.dxf.pattern_type = const.HATCH_TYPE_PREDEFINED
        if rgb is not None:
            self.rgb = rgb

    def associate(
        self, path: AbstractBoundaryPath, entities: Iterable[DXFEntity]
    ):
        """Set association from hatch boundary `path` to DXF geometry `entities`.

        A HATCH entity can be associative to a base geometry, this association
        is **not** maintained nor verified by `ezdxf`, so if you modify the base
        geometry the geometry of the boundary path is not updated and no
        verification is done to check if the associated geometry matches
        the boundary path, this opens many possibilities to create
        invalid DXF files: USE WITH CARE!

        """
        # I don't see this as a time critical operation, do as much checks as
        # needed to avoid invalid DXF files.
        if not self.is_alive:
            raise const.DXFStructureError("HATCH entity is destroyed")

        doc = self.doc
        owner = self.dxf.owner
        handle = self.dxf.handle
        if doc is None or owner is None or handle is None:
            raise const.DXFStructureError(
                "virtual entity can not have associated entities"
            )

        for entity in entities:
            if not entity.is_alive or entity.is_virtual:
                raise const.DXFStructureError(
                    "associated entity is destroyed or a virtual entity"
                )
            if doc is not entity.doc:
                raise const.DXFStructureError(
                    "associated entity is from a different document"
                )
            if owner != entity.dxf.owner:
                raise const.DXFStructureError(
                    "associated entity is from a different layout"
                )

            path.source_boundary_objects.append(entity.dxf.handle)
            entity.append_reactor_handle(handle)
        self.dxf.associative = 1 if len(path.source_boundary_objects) else 0

    def set_seed_points(self, points: Iterable[tuple[float, float]]) -> None:
        """Set seed points, `points` is an iterable of (x, y)-tuples.
        I don't know why there can be more than one seed point.
        All points in :ref:`OCS` (:attr:`Hatch.dxf.elevation` is the Z value)

        """
        points = list(points)
        if len(points) < 1:
            raise const.DXFValueError(
                "Argument points should be an iterable of 2D points and requires"
                " at least one point."
            )
        self.seeds = list(points)
        self.dxf.n_seed_points = len(self.seeds)
