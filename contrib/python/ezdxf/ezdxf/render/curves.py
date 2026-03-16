# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional
import random
import math
from ezdxf.math import (
    Vec3,
    Vec2,
    UVec,
    Matrix44,
    perlin,
    Bezier4P,
    global_bspline_interpolation,
    BSpline,
    open_uniform_bspline,
    closed_uniform_bspline,
    EulerSpiral as _EulerSpiral,
)

if TYPE_CHECKING:
    from ezdxf.layouts import BaseLayout


def rnd(max_value):
    return max_value / 2.0 - random.random() * max_value


def rnd_perlin(max_value, walker):
    r = perlin.snoise2(walker.x, walker.y)
    return max_value / 2.0 - r * max_value


def random_2d_path(
    steps: int = 100,
    max_step_size: float = 1.0,
    max_heading: float = math.pi / 2,
    retarget: int = 20,
) -> Iterable[Vec2]:
    """Returns a random 2D path as iterable of :class:`~ezdxf.math.Vec2`
    objects.

    Args:
        steps: count of vertices to generate
        max_step_size: max step size
        max_heading: limit heading angle change per step to ± max_heading/2 in
            radians
        retarget: specifies steps before changing global walking target

    """
    max_ = max_step_size * steps

    def next_global_target():
        return Vec2((rnd(max_), rnd(max_)))

    walker = Vec2(0, 0)
    target = next_global_target()
    for i in range(steps):
        if i % retarget == 0:
            target = target + next_global_target()
        angle = (target - walker).angle
        heading = angle + rnd_perlin(max_heading, walker)
        length = max_step_size * random.random()
        walker = walker + Vec2.from_angle(heading, length)
        yield walker


def random_3d_path(
    steps: int = 100,
    max_step_size: float = 1.0,
    max_heading: float = math.pi / 2.0,
    max_pitch: float = math.pi / 8.0,
    retarget: int = 20,
) -> Iterable[Vec3]:
    """Returns a random 3D path as iterable of :class:`~ezdxf.math.Vec3`
    objects.

    Args:
        steps: count of vertices to generate
        max_step_size: max step size
        max_heading: limit heading angle change per step to ± max_heading/2,
            rotation about the z-axis in radians
        max_pitch: limit pitch angle change per step to ± max_pitch/2, rotation
            about the x-axis in radians
        retarget: specifies steps before changing global walking target

    """
    max_ = max_step_size * steps

    def next_global_target():
        return Vec3((rnd(max_), rnd(max_), rnd(max_)))

    walker = Vec3()
    target = next_global_target()
    for i in range(steps):
        if i % retarget == 0:
            target = target + next_global_target()
        angle = (target - walker).angle
        length = max_step_size * random.random()
        heading_angle = angle + rnd_perlin(max_heading, walker)
        next_step = Vec3.from_angle(heading_angle, length)
        pitch_angle = rnd_perlin(max_pitch, walker)
        walker += Matrix44.x_rotate(pitch_angle).transform(next_step)
        yield walker


class Bezier:
    """Render a bezier curve as 2D/3D :class:`~ezdxf.entities.Polyline`.

    The :class:`Bezier` class is implemented with multiple segments, each
    segment is an optimized 4 point bezier curve, the 4 control points of the
    curve are: the start point (1) and the end point (4), point (2) is start
    point + start vector and point (3) is end point + end vector. Each segment
    has its own approximation count.

    .. seealso::

        The new :mod:`ezdxf.path` package provides many advanced construction tools
        based on the :class:`~ezdxf.path.Path` class.

    """

    class Segment:
        def __init__(
            self,
            start: UVec,
            end: UVec,
            start_tangent: UVec,
            end_tangent: UVec,
            segments: int,
        ):
            self.start = Vec3(start)
            self.end = Vec3(end)
            self.start_tangent = Vec3(
                start_tangent
            )  # as vector, from start point
            self.end_tangent = Vec3(end_tangent)  # as vector, from end point
            self.segments = segments

        def approximate(self) -> Iterable[Vec3]:
            control_points = [
                self.start,
                self.start + self.start_tangent,
                self.end + self.end_tangent,
                self.end,
            ]
            bezier = Bezier4P(control_points)
            return bezier.approximate(self.segments)

    def __init__(self) -> None:
        # fit point, first control vector, second control vector, segment count
        self.points: list[
            tuple[Vec3, Optional[Vec3], Optional[Vec3], Optional[int]]
        ] = []

    def start(self, point: UVec, tangent: UVec) -> None:
        """Set start point and start tangent.

        Args:
            point: start point
            tangent: start tangent as vector, example: (5, 0, 0) means a
                     horizontal tangent with a length of 5 drawing units
        """
        self.points.append((Vec3(point), None, tangent, None))

    def append(
        self,
        point: UVec,
        tangent1: UVec,
        tangent2: Optional[UVec] = None,
        segments: int = 20,
    ):
        """Append a control point with two control tangents.

        Args:
            point: control point
            tangent1: first tangent as vector "left" of the control point
            tangent2: second tangent as vector "right" of the control point,
                if omitted `tangent2` = `-tangent1`
            segments: count of line segments for the polyline approximation,
                count of line segments from the previous control point to the
                appended control point.

        """
        tangent1 = Vec3(tangent1)
        if tangent2 is None:
            tangent2 = -tangent1
        else:
            tangent2 = Vec3(tangent2)
        self.points.append((Vec3(point), tangent1, tangent2, int(segments)))

    def _build_bezier_segments(self) -> Iterable[Segment]:
        if len(self.points) > 1:
            for from_point, to_point in zip(self.points[:-1], self.points[1:]):
                start_point = from_point[0]
                start_tangent = from_point[2]  # tangent2
                end_point = to_point[0]
                end_tangent = to_point[1]  # tangent1
                count = to_point[3]
                yield Bezier.Segment(
                    start_point, end_point, start_tangent, end_tangent, count  # type: ignore
                )
        else:
            raise ValueError("Two or more points needed!")

    def render(
        self,
        layout: BaseLayout,
        force3d: bool = False,
        dxfattribs=None,
    ) -> None:
        """Render Bezier curve as 2D/3D :class:`~ezdxf.entities.Polyline`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            force3d: force 3D polyline rendering
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        points: list[Vec3] = []
        for segment in self._build_bezier_segments():
            points.extend(segment.approximate())
        if force3d or any(p[2] for p in points):
            layout.add_polyline3d(points, dxfattribs=dxfattribs)
        else:
            layout.add_polyline2d(points, dxfattribs=dxfattribs)


class Spline:
    """This class can be used to render B-splines into DXF R12 files as
    approximated :class:`~ezdxf.entities.Polyline` entities.
    The advantage of this class over the :class:`R12Spline` class is,
    that this is a real 3D curve, which means that the B-spline vertices do
    have to be located in a flat plane, and no :ref:`UCS` class is needed to
    place the curve in 3D space.

    .. seealso::

        The newer :class:`~ezdxf.math.BSpline` class provides the
        advanced vertex interpolation method :meth:`~ezdxf.math.BSpline.flattening`.

    """

    def __init__(
        self, points: Optional[Iterable[UVec]] = None, segments: int = 100
    ):
        """
        Args:
            points: spline definition points
            segments: count of line segments for approximation, vertex count is
                `segments` + 1

        """
        if points is None:
            points = []
        self.points: list[Vec3] = Vec3.list(points)
        self.segments = int(segments)

    def subdivide(self, segments: int = 4) -> None:
        """Calculate overall segment count, where segments is the sub-segment
        count, `segments` = 4, means 4 line segments between two definition
        points e.g. 4 definition points and 4 segments = 12 overall segments,
        useful for fit point rendering.

        Args:
            segments: sub-segments count between two definition points

        """
        self.segments = (len(self.points) - 1) * segments

    def render_as_fit_points(
        self,
        layout: BaseLayout,
        degree: int = 3,
        method: str = "chord",
        dxfattribs: Optional[dict] = None,
    ) -> None:
        """Render a B-spline as 2D/3D :class:`~ezdxf.entities.Polyline`, where
        the definition points are fit points.

           - 2D spline vertices uses: :meth:`~ezdxf.layouts.BaseLayout.add_polyline2d`
           - 3D spline vertices uses: :meth:`~ezdxf.layouts.BaseLayout.add_polyline3d`

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            degree: degree of B-spline (order = `degree` + 1)
            method: "uniform", "distance"/"chord", "centripetal"/"sqrt_chord" or
                "arc" calculation method for parameter t
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = global_bspline_interpolation(
            self.points, degree=degree, method=method
        )
        vertices = list(spline.approximate(self.segments))
        if any(vertex.z != 0.0 for vertex in vertices):
            layout.add_polyline3d(vertices, dxfattribs=dxfattribs)
        else:
            layout.add_polyline2d(vertices, dxfattribs=dxfattribs)

    render = render_as_fit_points

    def render_open_bspline(
        self, layout: BaseLayout, degree: int = 3, dxfattribs=None
    ) -> None:
        """Render an open uniform B-spline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = BSpline(self.points, order=degree + 1)
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )

    def render_uniform_bspline(
        self, layout: BaseLayout, degree: int = 3, dxfattribs=None
    ) -> None:
        """Render a uniform B-spline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = open_uniform_bspline(self.points, order=degree + 1)
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )

    def render_closed_bspline(
        self, layout: BaseLayout, degree: int = 3, dxfattribs=None
    ) -> None:
        """Render a closed uniform B-spline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = closed_uniform_bspline(self.points, order=degree + 1)
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )

    def render_open_rbspline(
        self,
        layout: BaseLayout,
        weights: Iterable[float],
        degree: int = 3,
        dxfattribs=None,
    ) -> None:
        """Render a rational open uniform BSpline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            weights: list of weights, requires a weight value (float) for each
                definition point.
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = BSpline(self.points, order=degree + 1, weights=weights)
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )

    def render_uniform_rbspline(
        self,
        layout: BaseLayout,
        weights: Iterable[float],
        degree: int = 3,
        dxfattribs=None,
    ) -> None:
        """Render a rational uniform B-spline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            weights: list of weights, requires a weight value (float) for each
                definition point.
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = closed_uniform_bspline(
            self.points, order=degree + 1, weights=weights
        )
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )

    def render_closed_rbspline(
        self,
        layout: BaseLayout,
        weights: Iterable[float],
        degree: int = 3,
        dxfattribs=None,
    ) -> None:
        """Render a rational B-spline as 3D :class:`~ezdxf.entities.Polyline`.
        Definition points are control points.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            weights: list of weights, requires a weight value (float) for each
                definition point.
            degree: degree of B-spline (order = `degree` + 1)
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        spline = closed_uniform_bspline(
            self.points, order=degree + 1, weights=weights
        )
        layout.add_polyline3d(
            list(spline.approximate(self.segments)), dxfattribs=dxfattribs
        )


class EulerSpiral:
    """Render an `euler spiral <https://en.wikipedia.org/wiki/Euler_spiral>`_
    as a 3D :class:`~ezdxf.entities.Polyline` or a
    :class:`~ezdxf.entities.Spline` entity.

    This is a parametric curve, which always starts at the origin (0, 0).

    """

    def __init__(self, curvature: float = 1):
        """
        Args:
            curvature: Radius of curvature

        """
        self.spiral = _EulerSpiral(float(curvature))

    def render_polyline(
        self,
        layout: BaseLayout,
        length: float = 1,
        segments: int = 100,
        matrix: Optional[Matrix44] = None,
        dxfattribs=None,
    ):
        """Render curve as :class:`~ezdxf.entities.Polyline`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            length: length measured along the spiral curve from its initial position
            segments: count of line segments to use, vertex count is `segments` + 1
            matrix: transformation matrix as :class:`~ezdxf.math.Matrix44`
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        Returns:
            :class:`~ezdxf.entities.Polyline`

        """
        points = self.spiral.approximate(length, segments)
        if matrix is not None:
            points = matrix.transform_vertices(points)
        return layout.add_polyline3d(list(points), dxfattribs=dxfattribs)

    def render_spline(
        self,
        layout: BaseLayout,
        length: float = 1,
        fit_points: int = 10,
        degree: int = 3,
        matrix: Optional[Matrix44] = None,
        dxfattribs=None,
    ):
        """
        Render curve as :class:`~ezdxf.entities.Spline`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            length: length measured along the spiral curve from its initial position
            fit_points: count of spline fit points to use
            degree: degree of B-spline
            matrix: transformation matrix as :class:`~ezdxf.math.Matrix44`
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Spline`

        Returns:
            :class:`~ezdxf.entities.Spline`

        """
        spline = self.spiral.bspline(length, fit_points, degree=degree)
        points = spline.control_points
        if matrix is not None:
            points = matrix.transform_vertices(points)
        return layout.add_open_spline(
            control_points=points,
            degree=spline.degree,
            knots=spline.knots(),
            dxfattribs=dxfattribs,
        )
