# Copyright (c) 2019-2025 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    List,
    Iterable,
    Sequence,
    cast,
    Iterator,
    Optional,
)
from typing_extensions import TypeAlias, Self
import array
import copy
from ezdxf.audit import AuditError
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.const import (
    SUBCLASS_MARKER,
    DXF2000,
    DXFValueError,
    DXFStructureError,
)
from ezdxf.lldxf.packedtags import VertexArray, Tags
from ezdxf.math import (
    Vec3,
    UVec,
    Matrix44,
    ConstructionEllipse,
    Z_AXIS,
    NULLVEC,
    OCS,
    uniform_knot_vector,
    open_uniform_knot_vector,
    BSpline,
    required_knot_values,
    required_fit_points,
    required_control_points,
    fit_points_to_cad_cv,
    round_knots,
)
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .factory import register_entity
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace, Ellipse
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.audit import Auditor

__all__ = ["Spline"]

# From the Autodesk ObjectARX reference:
# Objects of the AcDbSpline class use an embedded gelib object to maintain the
# actual spline information.
#
# Book recommendations:
#
#  - "Curves and Surfaces for CAGD" by Gerald Farin
#  - "Mathematical Elements for Computer Graphics"
#    by David Rogers and Alan Adams
#  - "An Introduction To Splines For Use In Computer Graphics & Geometric Modeling"
#    by Richard H. Bartels, John C. Beatty, and Brian A Barsky
#
# http://help.autodesk.com/view/OARX/2018/ENU/?guid=OREF-AcDbSpline__setFitData_AcGePoint3dArray__AcGeVector3d__AcGeVector3d__AcGe__KnotParameterization_int_double
# Construction of a AcDbSpline entity from fit points:
# degree has no effect. A spline with degree=3 is always constructed when
# interpolating a series of fit points.

acdb_spline = DefSubclass(
    "AcDbSpline",
    {
        # Spline flags:
        # 1 = Closed spline
        # 2 = Periodic spline
        # 4 = Rational spline
        # 8 = Planar
        # 16 = Linear (planar bit is also set)
        "flags": DXFAttr(70, default=0),
        # degree: The degree can't be higher than 11 according to the Autodesk
        # ObjectARX reference.
        "degree": DXFAttr(71, default=3, validator=validator.is_positive),
        "n_knots": DXFAttr(72, xtype=XType.callback, getter="knot_count"),
        "n_control_points": DXFAttr(
            73, xtype=XType.callback, getter="control_point_count"
        ),
        "n_fit_points": DXFAttr(74, xtype=XType.callback, getter="fit_point_count"),
        "knot_tolerance": DXFAttr(42, default=1e-10, optional=True),
        "control_point_tolerance": DXFAttr(43, default=1e-10, optional=True),
        "fit_tolerance": DXFAttr(44, default=1e-10, optional=True),
        # Start- and end tangents should be normalized, but CAD applications do not
        # crash if they are not normalized.
        "start_tangent": DXFAttr(
            12,
            xtype=XType.point3d,
            optional=True,
            validator=validator.is_not_null_vector,
        ),
        "end_tangent": DXFAttr(
            13,
            xtype=XType.point3d,
            optional=True,
            validator=validator.is_not_null_vector,
        ),
        # Extrusion is the normal vector (omitted if the spline is non-planar)
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # 10: Control points (in WCS); one entry per control point
        # 11: Fit points (in WCS); one entry per fit point
        # 40: Knot value (one entry per knot)
        # 41: Weight (if not 1); with multiple group pairs, they are present if all
        #     are not 1
    },
)
acdb_spline_group_codes = group_code_mapping(acdb_spline)


class SplineData:
    def __init__(self, spline: Spline):
        self.fit_points = spline.fit_points
        self.control_points = spline.control_points
        self.knots = spline.knots
        self.weights = spline.weights


REMOVE_CODES = {10, 11, 40, 41, 72, 73, 74}

Vertices: TypeAlias = List[Sequence[float]]


@register_entity
class Spline(DXFGraphic):
    """DXF SPLINE entity"""

    DXFTYPE = "SPLINE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_spline)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000
    CLOSED = 1  # closed b-spline
    PERIODIC = 2  # uniform b-spline
    RATIONAL = 4  # rational b-spline
    PLANAR = 8  # all spline points in a plane, don't read or set this bit, just ignore like AutoCAD
    LINEAR = 16  # always set with PLANAR, don't read or set this bit, just ignore like AutoCAD

    def __init__(self):
        super().__init__()
        self.fit_points = VertexArray()
        self.control_points = VertexArray()
        self.knots = []
        self.weights = []

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy data: control_points, fit_points, weights, knot_values."""
        assert isinstance(entity, Spline)
        entity._control_points = copy.deepcopy(self._control_points)
        entity._fit_points = copy.deepcopy(self._fit_points)
        entity._knots = copy.deepcopy(self._knots)
        entity._weights = copy.deepcopy(self._weights)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.subclass_by_index(2)
            if tags:
                tags = Tags(self.load_spline_data(tags))
                processor.fast_load_dxfattribs(
                    dxf, acdb_spline_group_codes, subclass=tags, recover=True
                )
            else:
                raise DXFStructureError(
                    f"missing 'AcDbSpline' subclass in SPLINE(#{dxf.handle})"
                )
        return dxf

    def load_spline_data(self, tags) -> Iterator:
        """Load and set spline data (fit points, control points, weights,
        knots) and remove invalid start- and end tangents.
        Yields the remaining unprocessed tags.
        """
        control_points = []
        fit_points = []
        knots = []
        weights = []
        for tag in tags:
            code, value = tag
            if code == 10:
                control_points.append(value)
            elif code == 11:
                fit_points.append(value)
            elif code == 40:
                knots.append(value)
            elif code == 41:
                weights.append(value)
            elif code in (12, 13) and NULLVEC.isclose(value):
                # Tangent values equal to (0, 0, 0) are invalid and ignored at
                # the loading stage!
                pass
            else:
                yield tag
        self.control_points = control_points
        self.fit_points = fit_points
        self.knots = knots
        self.weights = weights

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_spline.name)
        self.dxf.export_dxf_attribs(tagwriter, ["extrusion", "flags", "degree"])
        tagwriter.write_tag2(72, self.knot_count())
        tagwriter.write_tag2(73, self.control_point_count())
        tagwriter.write_tag2(74, self.fit_point_count())
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "knot_tolerance",
                "control_point_tolerance",
                "fit_tolerance",
                "start_tangent",
                "end_tangent",
            ],
        )

        self.export_spline_data(tagwriter)

    def export_spline_data(self, tagwriter: AbstractTagWriter):
        for value in self._knots:
            tagwriter.write_tag2(40, value)

        if len(self._weights):
            for value in self._weights:
                tagwriter.write_tag2(41, value)

        self._control_points.export_dxf(tagwriter, code=10)  # type: ignore
        self._fit_points.export_dxf(tagwriter, code=11)  # type: ignore

    @property
    def closed(self) -> bool:
        """``True`` if spline is closed. A closed spline has a connection from
        the last control point to the first control point. (read/write)
        """
        return self.get_flag_state(self.CLOSED, name="flags")

    @closed.setter
    def closed(self, status: bool) -> None:
        self.set_flag_state(self.CLOSED, state=status, name="flags")

    @property
    def knots(self) -> list[float]:
        """Knot values as :code:`array.array('d')`."""
        return self._knots

    @knots.setter
    def knots(self, values: Iterable[float]) -> None:
        self._knots: list[float] = cast(List[float], array.array("d", values))

    # DXF callback attribute Spline.dxf.n_knots
    def knot_count(self) -> int:
        """Count of knot values."""
        return len(self._knots)

    @property
    def weights(self) -> list[float]:
        """Control point weights as :code:`array.array('d')`."""
        return self._weights

    @weights.setter
    def weights(self, values: Iterable[float]) -> None:
        self._weights: list[float] = cast(List[float], array.array("d", values))

    @property
    def control_points(self) -> Vertices:
        """:class:`~ezdxf.lldxf.packedtags.VertexArray` of control points in
        :ref:`WCS`.
        """
        return self._control_points

    @control_points.setter
    def control_points(self, points: Iterable[UVec]) -> None:
        self._control_points: Vertices = cast(Vertices, VertexArray(Vec3.list(points)))

    # DXF callback attribute Spline.dxf.n_control_points
    def control_point_count(self) -> int:
        """Count of control points."""
        return len(self.control_points)

    @property
    def fit_points(self) -> Vertices:
        """:class:`~ezdxf.lldxf.packedtags.VertexArray` of fit points in
        :ref:`WCS`.
        """
        return self._fit_points

    @fit_points.setter
    def fit_points(self, points: Iterable[UVec]) -> None:
        self._fit_points: Vertices = cast(
            Vertices,
            VertexArray(Vec3.list(points)),
        )

    # DXF callback attribute Spline.dxf.n_fit_points
    def fit_point_count(self) -> int:
        """Count of fit points."""
        return len(self.fit_points)

    def construction_tool(self) -> BSpline:
        """Returns the construction tool :class:`ezdxf.math.BSpline`."""
        if self.control_point_count():
            weights = self.weights if len(self.weights) else None

            if len(self.knots):
                knots = round_knots(self.knots, self.dxf.knot_tolerance)
            else:
                knots = None
            return BSpline(
                control_points=self.control_points,
                order=self.dxf.degree + 1,
                knots=knots,
                weights=weights,
            )
        elif self.fit_point_count():
            tangents = None
            if self.dxf.hasattr("start_tangent") and self.dxf.hasattr("end_tangent"):
                tangents = [self.dxf.start_tangent, self.dxf.end_tangent]
            # SPLINE from fit points has always a degree of 3!
            return fit_points_to_cad_cv(
                self.fit_points,
                tangents=tangents,
            )
        else:
            raise ValueError("Construction tool requires control- or fit points.")

    def apply_construction_tool(self, s) -> Spline:
        """Apply SPLINE data from a :class:`~ezdxf.math.BSpline` construction
        tool or from a :class:`geomdl.BSpline.Curve` object.

        """
        try:
            self.control_points = s.control_points
        except AttributeError:  # maybe a geomdl.BSpline.Curve class
            s = BSpline.from_nurbs_python_curve(s)
            self.control_points = s.control_points

        self.dxf.degree = s.degree
        self.fit_points = []  # remove fit points
        self.knots = s.knots()
        self.weights = s.weights()
        self.set_flag_state(Spline.RATIONAL, state=bool(len(self.weights)))
        return self  # floating interface

    def flattening(self, distance: float, segments: int = 4) -> Iterator[Vec3]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments between two knots, if the
        distance from the center of the approximation segment to the curve is
        bigger than `distance` the segment will be subdivided.

        Args:
            distance: maximum distance from the projected curve point onto the
                segment chord.
            segments: minimum segment count between two knots

        """
        return self.construction_tool().flattening(distance, segments)

    @classmethod
    def from_arc(cls, entity: DXFGraphic) -> Spline:
        """Create a new SPLINE entity from a CIRCLE, ARC or ELLIPSE entity.

        The new SPLINE entity has no owner, no handle, is not stored in
        the entity database nor assigned to any layout!

        """
        dxftype = entity.dxftype()
        if dxftype == "ELLIPSE":
            ellipse = cast("Ellipse", entity).construction_tool()
        elif dxftype == "CIRCLE":
            ellipse = ConstructionEllipse.from_arc(
                center=entity.dxf.get("center", NULLVEC),
                radius=abs(entity.dxf.get("radius", 1.0)),
                extrusion=entity.dxf.get("extrusion", Z_AXIS),
            )
        elif dxftype == "ARC":
            ellipse = ConstructionEllipse.from_arc(
                center=entity.dxf.get("center", NULLVEC),
                radius=abs(entity.dxf.get("radius", 1.0)),
                extrusion=entity.dxf.get("extrusion", Z_AXIS),
                start_angle=entity.dxf.get("start_angle", 0),
                end_angle=entity.dxf.get("end_angle", 360),
            )
        else:
            raise TypeError("CIRCLE, ARC or ELLIPSE entity required.")

        spline = Spline.new(dxfattribs=entity.graphic_properties(), doc=entity.doc)
        s = BSpline.from_ellipse(ellipse)
        spline.dxf.degree = s.degree
        spline.dxf.flags = Spline.RATIONAL
        spline.control_points = s.control_points  # type: ignore
        spline.knots = s.knots()  # type: ignore
        spline.weights = s.weights()  # type: ignore
        return spline

    def set_open_uniform(self, control_points: Sequence[UVec], degree: int = 3) -> None:
        """Open B-spline with a uniform knot vector, start and end at your first
        and last control points.

        """
        self.dxf.flags = 0
        self.dxf.degree = degree
        self.control_points = control_points  # type: ignore
        self.knots = open_uniform_knot_vector(len(control_points), degree + 1)

    def set_uniform(self, control_points: Sequence[UVec], degree: int = 3) -> None:
        """B-spline with a uniform knot vector, does NOT start and end at your
        first and last control points.

        """
        self.dxf.flags = 0
        self.dxf.degree = degree
        self.control_points = control_points  # type: ignore
        self.knots = uniform_knot_vector(len(control_points), degree + 1)

    def set_closed(self, control_points: Sequence[UVec], degree=3) -> None:
        """Closed B-spline with a uniform knot vector, start and end at your
        first control point.

        """
        self.dxf.flags = self.PERIODIC | self.CLOSED
        self.dxf.degree = degree
        self.control_points = control_points  # type: ignore
        self.control_points.extend(control_points[:degree])
        # AutoDesk Developer Docs:
        # If the spline is periodic, the length of knot vector will be greater
        # than length of the control array by 1, but this does not work with
        # BricsCAD.
        self.knots = uniform_knot_vector(len(self.control_points), degree + 1)

    def set_open_rational(
        self,
        control_points: Sequence[UVec],
        weights: Sequence[float],
        degree: int = 3,
    ) -> None:
        """Open rational B-spline with a uniform knot vector, start and end at
        your first and last control points, and has additional control
        possibilities by weighting each control point.

        """
        self.set_open_uniform(control_points, degree=degree)
        self.dxf.flags = self.dxf.flags | self.RATIONAL
        if len(weights) != len(self.control_points):
            raise DXFValueError("Control point count must be equal to weights count.")
        self.weights = weights  # type: ignore

    def set_uniform_rational(
        self,
        control_points: Sequence[UVec],
        weights: Sequence[float],
        degree: int = 3,
    ) -> None:
        """Rational B-spline with a uniform knot vector, does NOT start and end
        at your first and last control points, and has additional control
        possibilities by weighting each control point.

        """
        self.set_uniform(control_points, degree=degree)
        self.dxf.flags = self.dxf.flags | self.RATIONAL
        if len(weights) != len(self.control_points):
            raise DXFValueError("Control point count must be equal to weights count.")
        self.weights = weights  # type: ignore

    def set_closed_rational(
        self,
        control_points: Sequence[UVec],
        weights: Sequence[float],
        degree: int = 3,
    ) -> None:
        """Closed rational B-spline with a uniform knot vector, start and end at
        your first control point, and has additional control possibilities by
        weighting each control point.

        """
        self.set_closed(control_points, degree=degree)
        self.dxf.flags = self.dxf.flags | self.RATIONAL
        weights = list(weights)
        weights.extend(weights[:degree])
        if len(weights) != len(self.control_points):
            raise DXFValueError("Control point count must be equal to weights count.")
        self.weights = weights

    def transform(self, m: Matrix44) -> Spline:
        """Transform the SPLINE entity by transformation matrix `m` inplace."""
        self._control_points.transform(m)  # type: ignore
        self._fit_points.transform(m)  # type: ignore
        # Transform optional attributes if they exist
        dxf = self.dxf
        for name in ("start_tangent", "end_tangent", "extrusion"):
            if dxf.hasattr(name):
                dxf.set(name, m.transform_direction(dxf.get(name)))
        self.post_transform(m)
        return self

    def audit(self, auditor: Auditor) -> None:
        """Audit the SPLINE entity."""
        super().audit(auditor)
        degree = self.dxf.degree
        name = str(self)

        if degree < 1:
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_DEFINITION,
                message=f"Removed {name} with invalid degree: {degree} < 1.",
            )
            auditor.trash(self)
            return

        n_control_points = len(self.control_points)
        n_fit_points = len(self.fit_points)

        if n_control_points == 0 and n_fit_points == 0:
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_DEFINITION,
                message=f"Removed {name} without any points (no geometry).",
            )
            auditor.trash(self)
            return

        if n_control_points > 0:
            self._audit_control_points(auditor)
        # Ignore fit points if defined by control points
        elif n_fit_points > 0:
            self._audit_fit_points(auditor)

    def _audit_control_points(self, auditor: Auditor):
        name = str(self)
        order = self.dxf.degree + 1
        n_control_points = len(self.control_points)

        # Splines with to few control points can't be processed:
        n_control_points_required = required_control_points(order)
        if n_control_points < n_control_points_required:
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_CONTROL_POINT_COUNT,
                message=f"Removed {name} with invalid control point count: "
                f"{n_control_points} < {n_control_points_required}",
            )
            auditor.trash(self)
            return

        n_weights = len(self.weights)
        n_knots = len(self.knots)
        n_knots_required = required_knot_values(n_control_points, order)

        if n_knots < n_knots_required:
            # Can not fix entity: because the knot values are basic
            # values which define the geometry of SPLINE.
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_KNOT_VALUE_COUNT,
                message=f"Removed {name} with invalid knot value count: "
                f"{n_knots} < {n_knots_required}",
            )
            auditor.trash(self)
            return

        if n_weights and n_weights != n_control_points:
            # Can not fix entity: because the weights are basic
            # values which define the geometry of SPLINE.
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_WEIGHT_COUNT,
                message=f"Removed {name} with invalid weight count: "
                f"{n_weights} != {n_control_points}",
            )
            auditor.trash(self)
            return

    def _audit_fit_points(self, auditor: Auditor):
        name = str(self)
        order = self.dxf.degree + 1
        # Assuming end tangents will be estimated if not present,
        # like by ezdxf:
        n_fit_points_required = required_fit_points(order, tangents=True)

        # Splines with to few fit points can't be processed:
        n_fit_points = len(self.fit_points)
        if n_fit_points < n_fit_points_required:
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_FIT_POINT_COUNT,
                message=f"Removed {name} with invalid fit point count: "
                f"{n_fit_points} < {n_fit_points_required}",
            )
            auditor.trash(self)
            return

        # Knot values have no meaning for splines defined by fit points:
        if len(self.knots):
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_KNOT_VALUE_COUNT,
                message=f"Removed unused knot values for {name} "
                f"defined by fit points.",
            )
            self.knots = []

        # Weights have no meaning for splines defined by fit points:
        if len(self.weights):
            auditor.fixed_error(
                code=AuditError.INVALID_SPLINE_WEIGHT_COUNT,
                message=f"Removed unused weights for {name} " f"defined by fit points.",
            )
            self.weights = []

    def ocs(self) -> OCS:
        # WCS entity which supports the "extrusion" attribute in a
        # different way!
        return OCS()
