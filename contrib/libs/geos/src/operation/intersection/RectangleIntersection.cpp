/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2014 Mika Heiskanen <mika.heiskanen@fmi.fi>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/Orientation.h>
#include <geos/operation/intersection/RectangleIntersection.h>
#include <geos/operation/intersection/Rectangle.h>
#include <geos/operation/intersection/RectangleIntersectionBuilder.h>
#include <geos/operation/predicate/RectangleIntersects.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Location.h>
#include <geos/util/UnsupportedOperationException.h>
#include <list>
#include <stdexcept>

using geos::operation::intersection::Rectangle;
using geos::operation::intersection::RectangleIntersectionBuilder;
using namespace geos::geom;
using namespace geos::algorithm;
namespace geos {
namespace operation { // geos::operation
namespace intersection { // geos::operation::intersection

/**
 * \brief Test if two coordinates are different
 */

inline
bool
different(double x1, double y1, double x2, double y2)
{
    return !(x1 == x2 && y1 == y2);
}

/**
 * \brief Calculate a line intersection point
 *
 * Note:
 *   - Calling this with x1,y1 and x2,y2 swapped cuts the other end of the line
 *   - Calling this with x and y swapped cuts in y-direction instead
 *   - Calling with 1<->2 and x<->y swapped works too
 */

inline
void
clip_one_edge(double& x1, double& y1, double x2, double y2, double limit)
{
    if(x2 == limit) {
        y1 = y2;
        x1 = x2;
    }

    if(x1 != x2) {
        y1 += (y2 - y1) * (limit - x1) / (x2 - x1);
        x1 = limit;
    }
}

/**
 * \brief Start point is outside, endpoint is definitely inside
 *
 * Note: Even though one might think using >= etc operators would produce
 *       the same result, that is not the case. We rely on the fact
 *       that nothing is clipped unless the point is truly outside the
 *       rectangle! Without this handling lines ending on the edges of
 *       the rectangle would be very difficult.
 */

void
clip_to_edges(double& x1, double& y1,
              double x2, double y2,
              const Rectangle& rect)
{
    if(x1 < rect.xmin()) {
        clip_one_edge(x1, y1, x2, y2, rect.xmin());
    }
    else if(x1 > rect.xmax()) {
        clip_one_edge(x1, y1, x2, y2, rect.xmax());
    }

    if(y1 < rect.ymin()) {
        clip_one_edge(y1, x1, y2, x2, rect.ymin());
    }
    else if(y1 > rect.ymax()) {
        clip_one_edge(y1, x1, y2, x2, rect.ymax());
    }
}


/**
 * \brief Clip  geometry
 *
 * Here outGeom may also be a MultiPoint
 */

void
RectangleIntersection::clip_point(const geom::Point* g,
                                  RectangleIntersectionBuilder& parts,
                                  const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    double x = g->getX();
    double y = g->getY();

    if(rect.position(x, y) == Rectangle::Inside) {
        parts.add(dynamic_cast<geom::Point*>(g->clone().release()));
    }
}

bool
RectangleIntersection::clip_linestring_parts(const geom::LineString* gi,
        RectangleIntersectionBuilder& parts,
        const Rectangle& rect)
{
    auto n = gi->getNumPoints();

    if(gi == nullptr || n < 1) {
        return false;
    }

    // For shorthand code

    std::vector<Coordinate> cs;
    gi->getCoordinatesRO()->toVector(cs);
    //const geom::CoordinateSequence &cs = *(gi->getCoordinatesRO());

    // Keep a record of the point where a line segment entered
    // the rectangle. If the boolean is set, we must insert
    // the point to the beginning of the linestring which then
    // continues inside the rectangle.

    double x0 = 0;
    double y0 = 0;
    bool add_start = false;

    // Start iterating

    size_t i = 0;

    while(i < n) {
        // Establish initial position

        double x = cs[i].x;
        double y = cs[i].y;
        Rectangle::Position pos = rect.position(x, y);

        if(pos == Rectangle::Outside) {
            // Skip points as fast as possible until something has to be checked
            // in more detail.

            ++i;	// we already know it is outside

            if(x < rect.xmin())
                while(i < n && cs[i].x < rect.xmin()) {
                    ++i;
                }

            else if(x > rect.xmax())
                while(i < n && cs[i].x > rect.xmax()) {
                    ++i;
                }

            else if(y < rect.ymin())
                while(i < n && cs[i].y < rect.ymin()) {
                    ++i;
                }

            else if(y > rect.ymax())
                while(i < n && cs[i].y > rect.ymax()) {
                    ++i;
                }

            if(i >= n) {
                return false;
            }

            // Establish new position
            x = cs[i].x;
            y = cs[i].y;
            pos = rect.position(x, y);

            // Handle all possible cases
            x0 = cs[i - 1].x;
            y0 = cs[i - 1].y;
            clip_to_edges(x0, y0, x, y, rect);

            if(pos == Rectangle::Inside) {
                add_start = true;	// x0,y0 must have clipped the rectangle
                // Main loop will enter the Inside/Edge section

            }
            else if(pos == Rectangle::Outside) {
                // From Outside to Outside. We need to check whether
                // we created a line segment inside the box. In any
                // case, we will continue the main loop after this
                // which will then enter the Outside section.

                // Clip the other end too
                clip_to_edges(x, y, x0, y0, rect);

                Rectangle::Position prev_pos = rect.position(x0, y0);
                pos = rect.position(x, y);

                if(different(x0, y0, x, y) &&		// discard corners etc
                        Rectangle::onEdge(prev_pos) &&		// discard if does not intersect rect
                        Rectangle::onEdge(pos) &&
                        !Rectangle::onSameEdge(prev_pos, pos)	// discard if travels along edge
                  ) {
                    std::vector<Coordinate>* coords = new std::vector<Coordinate>(2);
                    (*coords)[0] = Coordinate(x0, y0);
                    (*coords)[1] = Coordinate(x, y);
                    auto seq = _csf->create(coords);
                    geom::LineString* line = _gf->createLineString(seq.release());
                    parts.add(line);
                }

                // Continue main loop outside the rect

            }
            else {
                // From outside to edge. If the edge we hit first when
                // following the line is not the edge we end at, then
                // clearly we must go through the rectangle and hence
                // a start point must be set.

                Rectangle::Position newpos = rect.position(x0, y0);
                if(!Rectangle::onSameEdge(pos, newpos)) {
                    add_start = true;
                }
                else {
                    // we ignore the travel along the edge and continue
                    // the main loop at the last edge point
                }
            }
        }

        else {
            // The point is now stricly inside or on the edge.
            // Keep iterating until the end or the point goes
            // outside. We may have to output partial linestrings
            // while iterating until we go strictly outside

            auto start_index = i;			// 1st valid original point
            bool go_outside = false;

            while(!go_outside && ++i < n) {
                x = cs[i].x;
                y = cs[i].y;

                Rectangle::Position prev_pos = pos;
                pos = rect.position(x, y);

                if(pos == Rectangle::Inside) {
                    // Just keep going
                }
                else if(pos == Rectangle::Outside) {
                    go_outside = true;

                    // Clip the outside point to edges
                    clip_to_edges(x, y, cs[i - 1].x, cs[i - 1].y, rect);
                    pos = rect.position(x, y);

                    // Does the line exit through the inside of the box?

                    bool through_box = (different(x, y, cs[i].x, cs[i].y) &&
                                        !Rectangle::onSameEdge(prev_pos, pos));

                    // Output a LineString if it at least one segment long

                    if(start_index < i - 1 || add_start || through_box) {
                        std::vector<Coordinate>* coords = new std::vector<Coordinate>();
                        if(add_start) {
                            coords->push_back(Coordinate(x0, y0));
                            add_start = false;
                        }
                        //line->addSubLineString(&g, start_index, i-1);
                        coords->insert(coords->end(), cs.begin() + start_index, cs.begin() + i);

                        if(through_box) {
                            coords->push_back(Coordinate(x, y));
                        }

                        auto seq = _csf->create(coords);
                        geom::LineString* line = _gf->createLineString(seq.release());
                        parts.add(line);
                    }
                    // And continue main loop on the outside
                }
                else {
                    // on same edge?
                    if(Rectangle::onSameEdge(prev_pos, pos)) {
                        // Nothing to output if we haven't been elsewhere
                        if(start_index < i - 1 || add_start) {
                            std::vector<Coordinate>* coords = new std::vector<Coordinate>();
                            //geom::LineString * line = new geom::LineString();
                            if(add_start) {
                                //line->addPoint(x0,y0);
                                coords->push_back(Coordinate(x0, y0));
                                add_start = false;
                            }
                            //line->addSubLineString(&g, start_index, i-1);
                            coords->insert(coords->end(), cs.begin() + start_index, cs.begin() + i);

                            auto seq = _csf->create(coords);
                            geom::LineString* line = _gf->createLineString(seq.release());
                            parts.add(line);
                        }
                        start_index = i;
                    }
                    else {
                        // On different edge. Must have gone through the box
                        // then - keep collecting points that generate inside
                        // line segments
                    }
                }
            }

            // Was everything in? If so, generate no output but return true in this case only
            if(start_index == 0 && i >= n) {
                return true;
            }

            // Flush the last line segment if data ended and there is something to flush

            if(!go_outside &&						// meaning data ended
                    (start_index < i - 1 || add_start)) {	// meaning something has to be generated
                std::vector<Coordinate>* coords = new std::vector<Coordinate>();
                //geom::LineString * line = new geom::LineString();
                if(add_start) {
                    //line->addPoint(x0,y0);
                    coords->push_back(Coordinate(x0, y0));
                    add_start = false;
                }
                //line->addSubLineString(&g, start_index, i-1);
                coords->insert(coords->end(), cs.begin() + start_index, cs.begin() + i);

                auto seq = _csf->create(coords);
                geom::LineString* line = _gf->createLineString(seq.release());
                parts.add(line);
            }

        }
    }

    return false;

}

/**
 * \brief Clip polygon, do not close clipped ones
 */

void
RectangleIntersection::clip_polygon_to_linestrings(const geom::Polygon* g,
        RectangleIntersectionBuilder& toParts,
        const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    // Clip the exterior first to see what's going on

    RectangleIntersectionBuilder parts(*_gf);

    // If everything was in, just clone the original

    if(clip_linestring_parts(g->getExteriorRing(), parts, rect)) {
        toParts.add(dynamic_cast<geom::Polygon*>(g->clone().release()));
        return;
    }

    // Now, if parts is empty, our rectangle may be inside the polygon
    // If not, holes are outside too

    if(parts.empty()) {
        // We could now check whether the rectangle is inside the outer
        // ring to avoid checking the holes. However, if holes are much
        // smaller than the exterior ring just checking the holes
        // separately could be faster.

        if(g->getNumInteriorRing() == 0) {
            return;
        }

    }
    else {
        // The exterior must have been clipped into linestrings.
        // Move them to the actual parts collector, clearing parts.

        parts.reconnect();
        parts.release(toParts);
    }

    // Handle the holes now:
    // - Clipped ones become linestrings
    // - Intact ones become new polygons without holes

    for(size_t i = 0, n = g->getNumInteriorRing(); i < n; ++i) {
        if(clip_linestring_parts(g->getInteriorRingN(i), parts, rect)) {
            // clones
            LinearRing* hole = new LinearRing(*(g->getInteriorRingN(i)));
            // becomes exterior
            Polygon* poly = _gf->createPolygon(hole, nullptr);
            toParts.add(poly);
        }
        else if(!parts.empty()) {
            parts.reconnect();
            parts.release(toParts);
        }
    }
}

/**
 * \brief Clip polygon, close clipped ones
 */

void
RectangleIntersection::clip_polygon_to_polygons(const geom::Polygon* g,
        RectangleIntersectionBuilder& toParts,
        const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    // Clip the exterior first to see what's going on

    RectangleIntersectionBuilder parts(*_gf);

    // If everything was in, just clone the original

    const LineString* shell = g->getExteriorRing();
    if(clip_linestring_parts(shell, parts, rect)) {
        toParts.add(dynamic_cast<geom::Polygon*>(g->clone().release()));
        return;
    }

    // If there were no intersections, the outer ring might be
    // completely outside.

    using geos::algorithm::Orientation;
    if(parts.empty()) {
        Coordinate rectCenter(rect.xmin(), rect.ymin());
        rectCenter.x += (rect.xmax() - rect.xmin()) / 2;
        rectCenter.y += (rect.ymax() - rect.ymin()) / 2;
        if(PointLocation::locateInRing(rectCenter,
                                       *g->getExteriorRing()->getCoordinatesRO())
                != Location::INTERIOR) {
            return;
        }
    }
    else {
        // TODO: make CCW checking part of clip_linestring_parts ?
        if(Orientation::isCCW(shell->getCoordinatesRO())) {
            parts.reverseLines();
        }
    }

    // Must do this to make sure all end points are on the edges

    parts.reconnect();

    // Handle the holes now:
    // - Clipped ones become part of the exterior
    // - Intact ones become holes in new polygons formed by exterior parts


    for(size_t i = 0, n = g->getNumInteriorRing(); i < n; ++i) {
        RectangleIntersectionBuilder holeparts(*_gf);
        const LinearRing* hole = g->getInteriorRingN(i);
        if(clip_linestring_parts(hole, holeparts, rect)) {
            // becomes exterior
            LinearRing* cloned = new LinearRing(*hole);
            Polygon* poly = _gf->createPolygon(cloned, nullptr);
            parts.add(poly);
        }
        else {
            if(!holeparts.empty()) {
                // TODO: make CCW checking part of clip_linestring_parts ?
                if(! Orientation::isCCW(hole->getCoordinatesRO())) {
                    holeparts.reverseLines();
                }
                holeparts.reconnect();
                holeparts.release(parts);
            }
            else {

                Coordinate rectCenter(rect.xmin(), rect.ymin());
                rectCenter.x += (rect.xmax() - rect.xmin()) / 2;
                rectCenter.y += (rect.ymax() - rect.ymin()) / 2;
                if(PointLocation::isInRing(rectCenter,
                                           g->getInteriorRingN(i)->getCoordinatesRO())) {
                    // Completely inside the hole
                    return;
                }
            }
        }
    }

    parts.reconnectPolygons(rect);
    parts.release(toParts);

}

/**
 * \brief Clip  geometry
 */

void
RectangleIntersection::clip_polygon(const geom::Polygon* g,
                                    RectangleIntersectionBuilder& parts,
                                    const Rectangle& rect,
                                    bool keep_polygons)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    if(keep_polygons) {
        clip_polygon_to_polygons(g, parts, rect);
    }
    else {
        clip_polygon_to_linestrings(g, parts, rect);
    }
}

/**
 * \brief Clip geometry
 */

void
RectangleIntersection::clip_linestring(const geom::LineString* g,
                                       RectangleIntersectionBuilder& parts,
                                       const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    // If everything was in, just clone the original

    if(clip_linestring_parts(g, parts, rect)) {
        parts.add(dynamic_cast<geom::LineString*>(g->clone().release()));
    }

}

void
RectangleIntersection::clip_multipoint(const geom::MultiPoint* g,
                                       RectangleIntersectionBuilder& parts,
                                       const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }
    for(size_t i = 0, n = g->getNumGeometries(); i < n; ++i) {
        clip_point(g->getGeometryN(i), parts, rect);
    }
}

void
RectangleIntersection::clip_multilinestring(const geom::MultiLineString* g,
        RectangleIntersectionBuilder& parts,
        const Rectangle& rect)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    for(size_t i = 0, n = g->getNumGeometries(); i < n; ++i) {
        clip_linestring(g->getGeometryN(i), parts, rect);
    }
}

void
RectangleIntersection::clip_multipolygon(const geom::MultiPolygon* g,
        RectangleIntersectionBuilder& parts,
        const Rectangle& rect,
        bool keep_polygons)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    for(size_t i = 0, n = g->getNumGeometries(); i < n; ++i) {
        clip_polygon(g->getGeometryN(i), parts, rect, keep_polygons);
    }
}

void
RectangleIntersection::clip_geometrycollection(
    const geom::GeometryCollection* g,
    RectangleIntersectionBuilder& parts,
    const Rectangle& rect,
    bool keep_polygons)
{
    if(g == nullptr || g->isEmpty()) {
        return;
    }

    for(size_t i = 0, n = g->getNumGeometries(); i < n; ++i) {
        clip_geom(g->getGeometryN(i),
                  parts, rect, keep_polygons);
    }
}

void
RectangleIntersection::clip_geom(const geom::Geometry* g,
                                 RectangleIntersectionBuilder& parts,
                                 const Rectangle& rect,
                                 bool keep_polygons)
{
    if(const Point* p1 = dynamic_cast<const geom::Point*>(g)) {
        return clip_point(p1, parts, rect);
    }
    else if(const MultiPoint* p2 = dynamic_cast<const geom::MultiPoint*>(g)) {
        return clip_multipoint(p2, parts, rect);
    }
    else if(const LineString* p3 = dynamic_cast<const geom::LineString*>(g)) {
        return clip_linestring(p3, parts, rect);
    }
    else if(const MultiLineString* p4 = dynamic_cast<const geom::MultiLineString*>(g)) {
        return clip_multilinestring(p4, parts, rect);
    }
    else if(const Polygon* p5 = dynamic_cast<const geom::Polygon*>(g)) {
        return clip_polygon(p5, parts, rect, keep_polygons);
    }
    else if(const MultiPolygon* p6 = dynamic_cast<const geom::MultiPolygon*>(g)) {
        return clip_multipolygon(p6, parts, rect, keep_polygons);
    }
    else if(const GeometryCollection* p7 = dynamic_cast<const geom::GeometryCollection*>(g)) {
        return clip_geometrycollection(p7, parts, rect, keep_polygons);
    }
    else {
        throw util::UnsupportedOperationException("Encountered an unknown geometry component when clipping polygons");
    }
}

/* public static */
std::unique_ptr<geom::Geometry>
RectangleIntersection::clipBoundary(const geom::Geometry& g, const Rectangle& rect)
{
    RectangleIntersection ri(g, rect);
    return ri.clipBoundary();
}

std::unique_ptr<geom::Geometry>
RectangleIntersection::clipBoundary()
{
    RectangleIntersectionBuilder parts(*_gf);

    bool keep_polygons = false;
    clip_geom(&_geom, parts, _rect, keep_polygons);

    return parts.build();
}

/* public static */
std::unique_ptr<geom::Geometry>
RectangleIntersection::clip(const geom::Geometry& g, const Rectangle& rect)
{
    RectangleIntersection ri(g, rect);
    return ri.clip();
}

std::unique_ptr<geom::Geometry>
RectangleIntersection::clip()
{
    RectangleIntersectionBuilder parts(*_gf);

    bool keep_polygons = true;
    clip_geom(&_geom, parts, _rect, keep_polygons);

    return parts.build();
}

RectangleIntersection::RectangleIntersection(const geom::Geometry& geom, const Rectangle& rect)
    : _geom(geom), _rect(rect),
      _gf(geom.getFactory())
{
    _csf = _gf->getCoordinateSequenceFactory();
}

} // namespace geos::operation::intersection
} // namespace geos::operation
} // namespace geos
