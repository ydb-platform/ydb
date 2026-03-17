# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
#
# cython: embedsignature=True

"""
Trace pulls together proj, GEOS and ``_crs.pyx`` to implement a function
to project a `~shapely.geometry.LinearRing` / `~shapely.geometry.LineString`.
In general, this should never be called manually, instead leaving the
processing to be done by the :class:`cartopy.crs.Projection` subclasses.
"""
from __future__ import print_function

from functools import lru_cache

cimport cython
from libc.math cimport HUGE_VAL, sqrt, isfinite, isnan
from libcpp cimport bool
from libcpp.list cimport list

cdef bool DEBUG = False

import re
import warnings

import numpy as np
import shapely
import shapely.geometry as sgeom
import shapely.prepared as sprep
from pyproj import Geod, Transformer, proj_version_str
from pyproj.exceptions import ProjError
import shapely.geometry as sgeom

from . import crs as ccrs


ctypedef struct Point:
    double x
    double y

ctypedef list[Point] Line


cdef bool degenerate_line(const Line &value):
    return value.size() < 2


cdef bool close(double a, double b):
    return abs(a - b) <= (1e-8 + 1e-5 * abs(b))


@cython.final
cdef class LineAccumulator:
    cdef list[Line] lines

    def __init__(self):
        self.new_line()

    cdef void new_line(self):
        cdef Line line
        self.lines.push_back(line)

    cdef void add_point(self, const Point &point):
        self.lines.back().push_back(point)

    cdef void add_point_if_empty(self, const Point &point):
        if self.lines.back().empty():
            self.add_point(point)

    cdef object as_geom(self):
        from cython.operator cimport dereference, preincrement

        # self.lines.remove_if(degenerate_line) is not available in Cython.
        cdef list[Line].iterator it = self.lines.begin()
        while it != self.lines.end():
            if degenerate_line(dereference(it)):
                it = self.lines.erase(it)
            else:
                preincrement(it)

        cdef Point first, last
        if self.lines.size() > 1:
            first = self.lines.front().front()
            last = self.lines.back().back()
            if close(first.x, last.x) and close(first.y, last.y):
                self.lines.front().pop_front()
                self.lines.back().splice(self.lines.back().end(),
                                         self.lines.front())
                self.lines.pop_front()

        cdef Line ilines
        cdef Point ipoints
        geoms = []
        for ilines in self.lines:
            coords = [(ipoints.x, ipoints.y) for ipoints in ilines]
            geoms.append(sgeom.LineString(coords))

        geom = sgeom.MultiLineString(geoms)
        return geom

    cdef size_t size(self):
        return self.lines.size()


cdef class Interpolator:
    cdef Point start
    cdef Point end
    cdef readonly transformer
    cdef double src_scale
    cdef double dest_scale
    cdef bint to_180

    def __cinit__(self):
        self.src_scale = 1
        self.dest_scale = 1
        self.to_180 = False

    cdef void init(self, src_crs, dest_crs) except *:
        self.transformer = Transformer.from_crs(src_crs, dest_crs, always_xy=True)
        self.to_180 = (
            self.transformer.name == "noop" and
            src_crs.__class__.__name__ in ("PlateCarree", "RotatedPole")
        )

    cdef void set_line(self, const Point &start, const Point &end):
        self.start = start
        self.end = end

    cdef Point project(self, const Point &src_xy) except *:
        cdef Point dest_xy

        try:
            xx, yy = self.transformer.transform(
                src_xy.x * self.src_scale,
                src_xy.y * self.src_scale,
                errcheck=True
            )
        except ProjError as err:
            msg = str(err).lower()
            if (
                "latitude" in msg or
                "longitude" in msg or
                "outside of projection domain" in msg or
                "tolerance condition error" in msg
            ):
                xx = HUGE_VAL
                yy = HUGE_VAL
            else:
                raise

        if self.to_180 and (xx > 180 or xx < -180) and xx != HUGE_VAL:
            xx = (((xx + 180) % 360) - 180)

        dest_xy.x = xx * self.dest_scale
        dest_xy.y = yy * self.dest_scale
        return dest_xy

    cdef double[:, :] project_points(self, double[:, :] src_xy) except *:
        # Used for fallback to single point updates
        cdef Point xy
        # Make a temporary copy so we don't update the incoming memory view
        new_src_xy = np.asarray(src_xy)*self.src_scale
        try:
            xx, yy = self.transformer.transform(
                new_src_xy[:, 0],
                new_src_xy[:, 1],
                errcheck=True
            )
        except ProjError as err:
            msg = str(err).lower()
            if (
                "latitude" in msg or
                "longitude" in msg or
                "outside of projection domain" in msg or
                "tolerance condition error" in msg
            ):
                # Go back to trying to project a single point at a time
                xx = np.empty(shape=len(src_xy))
                yy = np.empty(shape=len(src_xy))
                for i in range(len(src_xy)):
                    # Update the point object's x/y coords
                    xy.x = src_xy[i, 0]
                    xy.y = src_xy[i, 1]
                    xy = self.project(xy)
                    xx[i] = xy.x
                    yy[i] = xy.y
            else:
                raise

        if self.to_180:
            # Get the places where we should wrap
            wrap_locs = (xx > 180) | (xx < -180) & (xx != HUGE_VAL)
            # Do the wrap at those locations
            xx[wrap_locs] = (((xx[wrap_locs] + 180) % 360) - 180)

        # Destination xy [ncoords, 2]
        return np.stack([xx, yy], axis=-1) * self.dest_scale

    cdef Point interpolate(self, double t) except *:
        raise NotImplementedError


cdef class CartesianInterpolator(Interpolator):
    cdef Point interpolate(self, double t) except *:
        cdef Point xy
        xy.x = self.start.x + (self.end.x - self.start.x) * t
        xy.y = self.start.y + (self.end.y - self.start.y) * t
        return self.project(xy)


cdef class SphericalInterpolator(Interpolator):
    cdef object geod
    cdef double azim
    cdef double s12

    cdef void init(self, src_crs, dest_crs) except *:
        self.transformer = Transformer.from_crs(src_crs, dest_crs, always_xy=True)

        cdef double major_axis = src_crs.ellipsoid.semi_major_metre
        cdef double flattening = 0
        if src_crs.ellipsoid.inverse_flattening > 0:
            flattening = 1 / src_crs.ellipsoid.inverse_flattening
        self.geod = Geod(a=major_axis, f=flattening)

    cdef void set_line(self, const Point &start, const Point &end):
        Interpolator.set_line(self, start, end)
        self.azim, _, self.s12 = self.geod.inv(start.x, start.y, end.x, end.y)

    cdef Point interpolate(self, double t) except *:
        cdef Point lonlat

        lonlat.x, lonlat.y, _ = self.geod.fwd(self.start.x, self.start.y, self.azim, self.s12 * t)
        return self.project(lonlat)


cdef enum State:
    POINT_IN = 1,
    POINT_OUT,
    POINT_NAN


cdef State get_state(const Point &point, object gp_domain, bool geom_fully_inside=False):
    cdef State state
    if geom_fully_inside:
        # Fast-path return because the geometry is fully inside
        return POINT_IN
    if isfinite(point.x) and isfinite(point.y):
        if shapely.__version__ >= "2":
            # Shapely 2.0 doesn't need to create/destroy a point
            state = POINT_IN if shapely.intersects_xy(gp_domain.context, point.x, point.y) else POINT_OUT
        else:
            g_point = sgeom.Point((point.x, point.y))
            state = POINT_IN if gp_domain.covers(g_point) else POINT_OUT
            del g_point
    else:
        state = POINT_NAN
    return state


@cython.cdivision(True)  # Want divide-by-zero to produce NaN.
cdef bool straightAndDomain(double t_start, const Point &p_start,
                            double t_end, const Point &p_end,
                            Interpolator interpolator, double threshold,
                            object gp_domain,
                            bool inside,
                            bool geom_fully_inside=False) except *:
    """
    Return whether the given line segment is suitable as an
    approximation of the projection of the source line.

    t_start: Interpolation parameter for the start point.
    p_start: Projected start point.
    t_end: Interpolation parameter for the end point.
    p_start: Projected end point.
    interpolator: Interpolator for current source line.
    threshold: Lateral tolerance in target projection coordinates.
    gp_domain: Prepared polygon of target map domain.
    inside: Whether the start point is within the map domain.
    geom_fully_inside: Whether all points are within the map domain.

    """
    # Straight and in-domain (de9im[7] == 'F')
    cdef bool valid
    cdef double t_mid
    cdef Point p_mid
    cdef double seg_dx, seg_dy
    cdef double mid_dx, mid_dy
    cdef double seg_hypot_sq
    cdef double along
    cdef double separation
    cdef double hypot

    # This could be optimised out of the loop.
    if not (isfinite(p_start.x) and isfinite(p_start.y)):
        valid = False
    elif not (isfinite(p_end.x) and isfinite(p_end.y)):
        valid = False
    else:
        # Find the projected mid-point
        t_mid = (t_start + t_end) * 0.5
        p_mid = interpolator.interpolate(t_mid)

        # Determine the closest point on the segment to the midpoint, in
        # normalized coordinates.
        #     ○̩ (x1, y1) (assume that this is not necessarily vertical)
        #     │
        #     │   D
        #    ╭├───────○ (x, y)
        #    ┊│┘     ╱
        #    ┊│     ╱
        #    ┊│    ╱
        #    L│   ╱
        #    ┊│  ╱
        #    ┊│θ╱
        #    ┊│╱
        #    ╰̍○̍
        #  (x0, y0)
        # The angle θ can be found by arctan2:
        #     θ = arctan2(y1 - y0, x1 - x0) - arctan2(y - y0, x - x0)
        # and the projection onto the line is simply:
        #     L = hypot(x - x0, y - y0) * cos(θ)
        # with the normalized form being:
        #     along = L / hypot(x1 - x0, y1 - y0)
        #
        # Plugging those into SymPy and .expand().simplify(), we get the
        # following equations (with a slight refactoring to reuse some
        # intermediate values):
        seg_dx = p_end.x - p_start.x
        seg_dy = p_end.y - p_start.y
        mid_dx = p_mid.x - p_start.x
        mid_dy = p_mid.y - p_start.y
        seg_hypot_sq = seg_dx*seg_dx + seg_dy*seg_dy

        along = (seg_dx*mid_dx + seg_dy*mid_dy) / seg_hypot_sq

        if isnan(along):
            valid = True
        else:
            valid = 0.0 < along < 1.0
            if valid:
                # For the distance of the point from the line segment, using
                # the same geometry above, use sin instead of cos:
                #     D = hypot(x - x0, y - y0) * sin(θ)
                # and then simplify with SymPy again:
                separation = (abs(mid_dx*seg_dy - mid_dy*seg_dx) /
                              sqrt(seg_hypot_sq))
                if inside:
                    # Scale the lateral threshold by the distance from
                    # the nearest end. I.e. Near the ends the lateral
                    # threshold is much smaller; it only has its full
                    # value in the middle.
                    valid = (separation <=
                             threshold * 2.0 * (0.5 - abs(0.5 - along)))
                else:
                    # Check if the mid-point makes less than ~11 degree
                    # angle with the straight line.
                    # sin(11') => 0.2
                    # To save the square-root we just use the square of
                    # the lengths, hence:
                    # 0.2 ^ 2 => 0.04
                    hypot = mid_dx*mid_dx + mid_dy*mid_dy
                    valid = ((separation * separation) / hypot) < 0.04

        if valid and not geom_fully_inside:
            # TODO: Re-use geometries, instead of create-destroy!

            # Create a LineString for the current end-point.
            g_segment = sgeom.LineString([
                (p_start.x, p_start.y),
                (p_end.x, p_end.y)])

            if inside:
                valid = gp_domain.covers(g_segment)
            else:
                valid = gp_domain.disjoint(g_segment)

            del g_segment

    return valid


cdef void bisect(double t_start, const Point &p_start, const Point &p_end,
                 object gp_domain, const State &state,
                 Interpolator interpolator, double threshold,
                 double &t_min, Point &p_min, double &t_max, Point &p_max,
                 bool geom_fully_inside=False) except *:
    cdef double t_current
    cdef Point p_current
    cdef bool valid

    # Initialise our bisection range to the start and end points.
    (&t_min)[0] = t_start
    (&p_min)[0] = p_start
    (&t_max)[0] = 1.0
    (&p_max)[0] = p_end

    # Start the search at the end.
    t_current = t_max
    p_current = p_max

    # TODO: See if we can convert the 't' threshold into one based on the
    # projected coordinates - e.g. the resulting line length.

    while abs(t_max - t_min) > 1.0e-6:
        if DEBUG:
            print("t: ", t_current)

        if state == POINT_IN:
            # Straight and entirely-inside-domain
            valid = straightAndDomain(t_start, p_start, t_current, p_current,
                                      interpolator, threshold,
                                      gp_domain, True, geom_fully_inside=geom_fully_inside)

        elif state == POINT_OUT:
            # Straight and entirely-outside-domain
            valid = straightAndDomain(t_start, p_start, t_current, p_current,
                                      interpolator, threshold,
                                      gp_domain, False, geom_fully_inside=geom_fully_inside)
        else:
            valid = not isfinite(p_current.x) or not isfinite(p_current.y)

        if DEBUG:
            print("   => valid: ", valid)

        if valid:
            (&t_min)[0] = t_current
            (&p_min)[0] = p_current
        else:
            (&t_max)[0] = t_current
            (&p_max)[0] = p_current

        t_current = (t_min + t_max) * 0.5
        p_current = interpolator.interpolate(t_current)


cdef void _project_segment(double[:] src_from, double[:] src_to,
                           double[:] dest_from, double[:] dest_to,
                           Interpolator interpolator,
                           object gp_domain,
                           double threshold, LineAccumulator lines,
                           bool geom_fully_inside=False) except *:
    cdef Point p_current, p_min, p_max, p_end
    cdef double t_current, t_min=0, t_max=1
    cdef State state

    p_current.x, p_current.y = src_from
    p_end.x, p_end.y = src_to
    if DEBUG:
        print("Setting line:")
        print("   ", p_current.x, ", ", p_current.y)
        print("   ", p_end.x, ", ", p_end.y)

    interpolator.set_line(p_current, p_end)
    # Now update the current/end with the destination (projected) coords
    p_current.x, p_current.y = dest_from
    p_end.x, p_end.y = dest_to
    if DEBUG:
        print("Projected as:")
        print("   ", p_current.x, ", ", p_current.y)
        print("   ", p_end.x, ", ", p_end.y)

    t_current = 0.0
    state = get_state(p_current, gp_domain, geom_fully_inside)

    cdef size_t old_lines_size = lines.size()
    while t_current < 1.0 and (lines.size() - old_lines_size) < 100:
        if DEBUG:
            print("Bisecting from: ", t_current, " (")
            if state == POINT_IN:
                print("IN")
            elif state == POINT_OUT:
                print("OUT")
            else:
                print("NAN")
            print(")")
            print("   ", p_current.x, ", ", p_current.y)
            print("   ", p_end.x, ", ", p_end.y)

        bisect(t_current, p_current, p_end, gp_domain, state,
               interpolator, threshold,
               t_min, p_min, t_max, p_max, geom_fully_inside=geom_fully_inside)
        if DEBUG:
            print("   => ", t_min, "to", t_max)
            print("   => (", p_min.x, ", ", p_min.y, ") to (",
                  p_max.x, ", ", p_max.y, ")")

        if state == POINT_IN:
            lines.add_point_if_empty(p_current)
            if t_min != t_current:
                lines.add_point(p_min)
                t_current = t_min
                p_current = p_min
            else:
                t_current = t_max
                p_current = p_max
                state = get_state(p_current, gp_domain, geom_fully_inside)
                if state == POINT_IN:
                    lines.new_line()

        elif state == POINT_OUT:
            if t_min != t_current:
                t_current = t_min
                p_current = p_min
            else:
                t_current = t_max
                p_current = p_max
                state = get_state(p_current, gp_domain, geom_fully_inside)
                if state == POINT_IN:
                    lines.new_line()

        else:
            t_current = t_max
            p_current = p_max
            state = get_state(p_current, gp_domain, geom_fully_inside)
            if state == POINT_IN:
                lines.new_line()


@lru_cache(maxsize=4)
def _interpolator(src_crs, dest_projection):
    # Get an Interpolator from the given CRS and projection.
    # Callers must hold a reference to these systems for the lifetime
    # of the interpolator. If they get garbage-collected while interpolator
    # exists you *will* segfault.

    cdef Interpolator interpolator
    if src_crs.is_geodetic():
        interpolator = SphericalInterpolator()
    else:
        interpolator = CartesianInterpolator()
    interpolator.init(src_crs, dest_projection)
    return interpolator


def project_linear(geometry not None, src_crs not None,
                   dest_projection not None):
    """
    Project a geometry from one projection to another.

    Parameters
    ----------
    geometry : `shapely.geometry.LineString` or `shapely.geometry.LinearRing`
        A geometry to be projected.
    src_crs : cartopy.crs.CRS
        The coordinate system of the line to be projected.
    dest_projection : cartopy.crs.Projection
        The projection for the resulting projected line.

    Returns
    -------
    `shapely.geometry.MultiLineString`
        The result of projecting the given geometry from the source projection
        into the destination projection.

    """
    cdef:
        double threshold = dest_projection.threshold
        Interpolator interpolator
        object g_domain
        double[:, :] src_coords, dest_coords
        unsigned int src_size, src_idx
        object gp_domain
        LineAccumulator lines

    g_domain = dest_projection.domain

    interpolator = _interpolator(src_crs, dest_projection)

    src_coords = np.asarray(geometry.coords)
    dest_coords = interpolator.project_points(src_coords)
    gp_domain = sprep.prep(g_domain)

    src_size = len(src_coords)  # check exceptions

    # Test the entire geometry to see if there are any domain crossings
    # If there are none, then we can skip expensive domain checks later
    # TODO: Handle projections other than rectangular
    cdef bool geom_fully_inside = False
    if isinstance(dest_projection, (ccrs._RectangularProjection, ccrs._WarpedRectangularProjection)):
        dest_line = sgeom.LineString([(x[0], x[1]) for x in dest_coords])
        if dest_line.is_valid:
            # We can only check for covers with valid geometries
            # some have nans/infs at this point still
            geom_fully_inside = gp_domain.covers(dest_line)

    lines = LineAccumulator()
    for src_idx in range(1, src_size):
        _project_segment(src_coords[src_idx - 1, :2], src_coords[src_idx, :2],
                         dest_coords[src_idx - 1, :2], dest_coords[src_idx, :2],
                         interpolator, gp_domain, threshold, lines,
                         geom_fully_inside=geom_fully_inside);

    del gp_domain

    multi_line_string = lines.as_geom()

    del lines, interpolator
    return multi_line_string


class _Testing:
    @staticmethod
    def straight_and_within(Point l_start, Point l_end,
                            double t_start, double t_end,
                            Interpolator interpolator, double threshold,
                            object domain):
        # This function is for testing/demonstration only.
        # It is not careful about freeing resources, and it short-circuits
        # optimisations that are made in the real algorithm (in exchange for
        # a convenient signature).

        cdef object gp_domain
        gp_domain = sprep.prep(domain)

        state = get_state(interpolator.project(l_start), gp_domain)
        cdef bool p_start_inside_domain = state == POINT_IN

        # l_end and l_start should be un-projected.
        interpolator.set_line(l_start, l_end)

        cdef Point p0 = interpolator.interpolate(t_start)
        cdef Point p1 = interpolator.interpolate(t_end)

        valid = straightAndDomain(
            t_start, p0, t_end, p1,
            interpolator, threshold,
            gp_domain, p_start_inside_domain)

        del gp_domain
        return valid

    @staticmethod
    def interpolator(source_crs, destination_projection):
        return _interpolator(source_crs, destination_projection)

    @staticmethod
    def interp_prj_pt(Interpolator interp, const Point &lonlat):
        return interp.project(lonlat)

    @staticmethod
    def interp_t_pt(Interpolator interp, const Point &start, const Point &end, double t):
        interp.set_line(start, end)
        return interp.interpolate(t)
