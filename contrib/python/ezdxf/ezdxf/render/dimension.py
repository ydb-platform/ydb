# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from ezdxf.math import UCS
from ezdxf.lldxf.const import DXFValueError
from ezdxf.entities.dimstyleoverride import DimStyleOverride

from .dim_base import BaseDimensionRenderer
from .dim_curved import AngularDimension, Angular3PDimension, ArcLengthDimension
from .dim_diameter import DiameterDimension
from .dim_linear import LinearDimension
from .dim_ordinate import OrdinateDimension
from .dim_radius import RadiusDimension


if TYPE_CHECKING:
    from ezdxf.entities import Dimension


class DimensionRenderer:
    def dispatch(
        self, override: DimStyleOverride, ucs: Optional[UCS] = None
    ) -> BaseDimensionRenderer:
        dimension = override.dimension
        dim_type = dimension.dimtype
        dxf_type = dimension.dxftype()
        if dxf_type == "ARC_DIMENSION":
            return self.arc_length(dimension, ucs, override)
        elif dxf_type == "LARGE_RADIAL_DIMENSION":
            return self.large_radial(dimension, ucs, override)
        elif dim_type in (0, 1):
            return self.linear(dimension, ucs, override)
        elif dim_type == 2:
            return self.angular(dimension, ucs, override)
        elif dim_type == 3:
            return self.diameter(dimension, ucs, override)
        elif dim_type == 4:
            return self.radius(dimension, ucs, override)
        elif dim_type == 5:
            return self.angular3p(dimension, ucs, override)
        elif dim_type == 6:
            return self.ordinate(dimension, ucs, override)
        else:
            raise DXFValueError(f"Unknown DIMENSION type: {dim_type}")

    def linear(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for linear dimension lines: horizontal, vertical and rotated"""
        return LinearDimension(dimension, ucs, override)

    def angular(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for angular dimension defined by two lines."""
        return AngularDimension(dimension, ucs, override)

    def diameter(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for diameter dimension"""
        return DiameterDimension(dimension, ucs, override)

    def radius(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for radius dimension"""
        return RadiusDimension(dimension, ucs, override)

    def large_radial(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for large radial dimension"""
        raise NotImplementedError()

    def angular3p(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for angular dimension defined by three points."""
        return Angular3PDimension(dimension, ucs, override)

    def ordinate(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for ordinate dimension."""
        return OrdinateDimension(dimension, ucs, override)

    def arc_length(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        """Call renderer for arc length dimension."""
        return ArcLengthDimension(dimension, ucs, override)
