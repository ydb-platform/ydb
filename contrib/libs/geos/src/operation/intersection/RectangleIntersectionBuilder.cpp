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

#include <geos/operation/intersection/Rectangle.h>
#include <geos/operation/intersection/RectangleIntersectionBuilder.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/LineString.h>
#include <geos/geom/LinearRing.h>
#include <geos/algorithm/PointLocation.h>
#include <geos/util/IllegalArgumentException.h>

#include <cmath> // for fabs()

namespace geos {
namespace operation { // geos::operation
namespace intersection { // geos::operation::intersection

using namespace geos::geom;

RectangleIntersectionBuilder::~RectangleIntersectionBuilder()
{
    for(std::list<geom::Polygon*>::iterator i = polygons.begin(), e = polygons.end(); i != e; ++i) {
        delete *i;
    }
    for(std::list<geom::LineString*>::iterator i = lines.begin(), e = lines.end(); i != e; ++i) {
        delete *i;
    }
    for(std::list<geom::Point*>::iterator i = points.begin(), e = points.end(); i != e; ++i) {
        delete *i;
    }
}

void
RectangleIntersectionBuilder::reconnect()
{
    // Nothing to reconnect if there aren't at least two lines
    if(lines.size() < 2) {
        return;
    }

    geom::LineString* line1 = lines.front();
    const geom::CoordinateSequence& cs1 = *line1->getCoordinatesRO();

    geom::LineString* line2 = lines.back();
    const geom::CoordinateSequence& cs2 = *line2->getCoordinatesRO();

    const auto n1 = cs1.size();
    const auto n2 = cs2.size();

    // Safety check against bad input to prevent segfaults
    if(n1 == 0 || n2 == 0) {
        return;
    }

    if(cs1[0] != cs2[n2 - 1]) {
        return;
    }

    // Merge the two linestrings
    auto ncs = valid::RepeatedPointRemover::removeRepeatedPoints(&cs2);
    ncs->add(&cs1, false, true);

    delete line1;
    delete line2;

    LineString* nline = _gf.createLineString(ncs.release());
    lines.pop_front();
    lines.pop_back();

    lines.push_front(nline);
}


void
RectangleIntersectionBuilder::release(RectangleIntersectionBuilder& theParts)
{
    for(std::list<geom::Polygon*>::iterator i = polygons.begin(), e = polygons.end(); i != e; ++i) {
        theParts.add(*i);
    }

    for(std::list<geom::LineString*>::iterator i = lines.begin(), e = lines.end(); i != e; ++i) {
        theParts.add(*i);
    }

    for(std::list<geom::Point*>::iterator i = points.begin(), e = points.end(); i != e; ++i) {
        theParts.add(*i);
    }

    clear();
}

/**
 * \brief Clear the parts having transferred ownership elsewhere
 */

void
RectangleIntersectionBuilder::clear()
{
    polygons.clear();
    lines.clear();
    points.clear();
}

/**
 * \brief Test if there are no parts at all
 */

bool
RectangleIntersectionBuilder::empty() const
{
    return polygons.empty() && lines.empty() && points.empty();
}

/**
 * \brief Add intermediate Polygon
 */

void
RectangleIntersectionBuilder::add(geom::Polygon* thePolygon)
{
    polygons.push_back(thePolygon);
}

/**
 * \brief Add intermediate LineString
 */

void
RectangleIntersectionBuilder::add(geom::LineString* theLine)
{
    lines.push_back(theLine);
}

/**
 * \brief Add intermediate Point
 */

void
RectangleIntersectionBuilder::add(geom::Point* thePoint)
{
    points.push_back(thePoint);
}

std::unique_ptr<geom::Geometry>
RectangleIntersectionBuilder::build()
{
    // Total number of objects

    std::size_t n = polygons.size() + lines.size() + points.size();

    if(n == 0) {
        return std::unique_ptr<Geometry>(_gf.createGeometryCollection());
    }

    std::vector<Geometry*>* geoms = new std::vector<Geometry*>;
    geoms->reserve(n);

    for(std::list<geom::Polygon*>::iterator i = polygons.begin(), e = polygons.end(); i != e; ++i) {
        geoms->push_back(*i);
    }
    polygons.clear();

    for(std::list<geom::LineString*>::iterator i = lines.begin(), e = lines.end(); i != e; ++i) {
        geoms->push_back(*i);
    }
    lines.clear();

    for(std::list<geom::Point*>::iterator i = points.begin(), e = points.end(); i != e; ++i) {
        geoms->push_back(*i);
    }
    points.clear();

    // If source geometry is GeometryCollection, SRID for geometry parts may be lost after clipping.
    // And when clipping result is single geometry GeometryFactory::buildGeometry()
    // doesn't set SRID. Factory just returns passed pointer.
    if (geoms->size() == 1)
        (*geoms)[0]->setSRID((*geoms)[0]->getFactory()->getSRID());

    return std::unique_ptr<Geometry>(
               (*geoms)[0]->getFactory()->buildGeometry(geoms)
           );
}

/**
 * Distance of 1st point of linestring to last point of linearring
 * along rectangle edges
 */

double
distance(const Rectangle& rect,
         double x1, double y1,
         double x2, double y2)
{
    double dist = 0;

    Rectangle::Position pos = rect.position(x1, y1);
    Rectangle::Position endpos = rect.position(x2, y2);

    if(pos & Rectangle::Position::Outside ||
            endpos & Rectangle::Position::Outside ||
            pos & Rectangle::Position::Inside ||
            endpos & Rectangle::Position::Inside) {
        throw geos::util::IllegalArgumentException("Can't compute distance to non-boundary position.");
    }

    while(true) {
        // Close up when we have the same edge and the
        // points are in the correct clockwise order
        if((pos & endpos) != 0 &&
                (
                    (x1 == rect.xmin() && y2 >= y1) ||
                    (y1 == rect.ymax() && x2 >= x1) ||
                    (x1 == rect.xmax() && y2 <= y1) ||
                    (y1 == rect.ymin() && x2 <= x1))
          ) {
            dist += fabs(x2 - x1) + fabs(y2 - y1);
            break;
        }

        pos = Rectangle::nextEdge(pos);
        if(pos & Rectangle::Left) {
            dist += x1 - rect.xmin();
            x1 = rect.xmin();
        }
        else if(pos & Rectangle::Top) {
            dist += rect.ymax() - y1;
            y1 = rect.ymax();
        }
        else if(pos & Rectangle::Right) {
            dist += rect.xmax() - x1;
            x1 = rect.xmax();
        }
        else {
            dist += y1 - rect.ymin();
            y1 = rect.ymin();
        }
    }
    return dist;
}

double
distance(const Rectangle& rect,
         const std::vector<Coordinate>& ring,
         const geom::LineString* line)
{
    auto nr = ring.size();
    const Coordinate& c1 = ring[nr - 1];

    const CoordinateSequence* linecs = line->getCoordinatesRO();
    const Coordinate& c2 = linecs->getAt(0);

    return distance(rect, c1.x, c1.y, c2.x, c2.y);
}

double
distance(const Rectangle& rect,
         const std::vector<Coordinate>& ring)
{
    auto nr = ring.size();
    const Coordinate& c1 = ring[nr - 1]; // TODO: ring.back() ?
    const Coordinate& c2 = ring[0]; // TODO: ring.front() ?
    return distance(rect, c1.x, c1.y, c2.x, c2.y);
}

/**
 * \brief Reverse given segment in a coordinate vector
 */
void
reverse_points(std::vector<Coordinate>& v, size_t start, size_t end)
{
    geom::Coordinate p1;
    geom::Coordinate p2;
    while(start < end) {
        p1 = v[start];
        p2 = v[end];
        v[start] = p2;
        v[end] = p1;
        ++start;
        --end;
    }
}

/**
 * \brief Normalize a ring into lexicographic order
 */
void
normalize_ring(std::vector<Coordinate>& ring)
{
    if(ring.empty()) {
        return;
    }

    // Find the "smallest" coordinate

    size_t best_pos = 0;
    auto n = ring.size();
    for(size_t pos = 0; pos < n; ++pos) {
        // TODO: use CoordinateLessThan ?
        if(ring[pos].x < ring[best_pos].x) {
            best_pos = pos;
        }
        else if(ring[pos].x == ring[best_pos].x &&
                ring[pos].y < ring[best_pos].y) {
            best_pos = pos;
        }
    }

    // Quick exit if the ring is already normalized
    if(best_pos == 0) {
        return;
    }

    // Flip hands -algorithm to the part without the
    // duplicate last coordinate at n-1:

    reverse_points(ring, 0, best_pos - 1);
    reverse_points(ring, best_pos, n - 2);
    reverse_points(ring, 0, n - 2);

    // And make sure the ring is valid by duplicating the first coordinate
    // at the end:

    geom::Coordinate c;
    c = ring[0];
    ring[n - 1] = c;
}

void
RectangleIntersectionBuilder::close_boundary(
    const Rectangle& rect,
    std::vector<Coordinate>* ring,
    double x1, double y1,
    double x2, double y2)
{
    Rectangle::Position endpos = rect.position(x2, y2);
    Rectangle::Position pos = rect.position(x1, y1);

    while(true) {
        // Close up when we have the same edge and the
        // points are in the correct clockwise order
        if((pos & endpos) != 0 &&
                (
                    (x1 == rect.xmin() && y2 >= y1) ||
                    (y1 == rect.ymax() && x2 >= x1) ||
                    (x1 == rect.xmax() && y2 <= y1) ||
                    (y1 == rect.ymin() && x2 <= x1))
          ) {
            if(x1 != x2 || y1 != y2) {	// the polygon may have started at a corner
                ring->push_back(Coordinate(x2, y2));
            }
            break;
        }

        pos = Rectangle::nextEdge(pos);
        if(pos & Rectangle::Left) {
            x1 = rect.xmin();
        }
        else if(pos & Rectangle::Top) {
            y1 = rect.ymax();
        }
        else if(pos & Rectangle::Right) {
            x1 = rect.xmax();
        }
        else {
            y1 = rect.ymin();
        }

        ring->push_back(Coordinate(x1, y1));

    }
}

void
RectangleIntersectionBuilder::close_ring(const Rectangle& rect,
        std::vector<Coordinate>* ring)
{
    auto nr = ring->size();
    Coordinate& c2 = (*ring)[0];
    Coordinate& c1 = (*ring)[nr - 1];
    double x2 = c2.x;
    double y2 = c2.y;
    double x1 = c1.x;
    double y1 = c1.y;

    close_boundary(rect, ring, x1, y1, x2, y2);

}


void
RectangleIntersectionBuilder::reconnectPolygons(const Rectangle& rect)
{
    // Build the exterior rings first

    typedef std::vector< geom::LinearRing*> LinearRingVect;
    typedef std::pair< geom::LinearRing*, LinearRingVect* > ShellAndHoles;
    typedef std::list< ShellAndHoles > ShellAndHolesList;

    ShellAndHolesList exterior;

    const CoordinateSequenceFactory& _csf = *_gf.getCoordinateSequenceFactory();

    // If there are no lines, the rectangle must have been
    // inside the exterior ring.

    if(lines.empty()) {
        geom::LinearRing* ring = rect.toLinearRing(_gf);
        exterior.push_back(make_pair(ring, new LinearRingVect()));
    }
    else {
        // Reconnect all lines into one or more linearrings
        // using box boundaries if necessary

        std::vector<Coordinate>* ring = nullptr;

        while(!lines.empty() || ring != nullptr) {
            if(ring == nullptr) {
                ring = new std::vector<Coordinate>();
                LineString* line = lines.front();
                lines.pop_front();
                line->getCoordinatesRO()->toVector(*ring);
                delete line;
            }

            // Distance to own endpoint
            double own_distance = distance(rect, *ring);

            // Find line to connect to
            // TODO: should we use LineMerge op ?
            double best_distance = -1;
            std::list<LineString*>::iterator best_pos = lines.begin();
            for(std::list<LineString*>::iterator iter = lines.begin(); iter != lines.end(); ++iter) {
                double d = distance(rect, *ring, *iter);
                if(best_distance < 0 || d < best_distance) {
                    best_distance = d;
                    best_pos = iter;
                }
            }

            // If own end point is closest, close the ring and continue
            if(best_distance < 0 || own_distance < best_distance) {
                close_ring(rect, ring);
                normalize_ring(*ring);
                auto shell_cs = _csf.create(ring);
                geom::LinearRing* shell = _gf.createLinearRing(shell_cs.release());
                exterior.push_back(make_pair(shell, new LinearRingVect()));
                ring = nullptr;
            }
            else {
                LineString* line = *best_pos;
                auto nr = ring->size();
                const CoordinateSequence& cs = *line->getCoordinatesRO();
                close_boundary(rect, ring,
                               (*ring)[nr - 1].x,
                               (*ring)[nr - 1].y,
                               cs[0].x,
                               cs[0].y);
                // above function adds the 1st point
                for(size_t i = 1; i < cs.size(); ++i) {
                    ring->push_back(cs[i]);
                }
                //ring->addSubLineString(line,1);
                delete line;
                lines.erase(best_pos);
            }
        }
    }

    // Attach holes to polygons

    for(std::list<geom::Polygon*>::iterator i = polygons.begin(), e = polygons.end(); i != e; ++i) {
        geom::Polygon* poly = *i;
        const geom::LinearRing* hole = poly->getExteriorRing();

        if(exterior.size() == 1) {
            exterior.front().second->push_back(new LinearRing(*hole));
        }
        else {
            using geos::algorithm::PointLocation;
            const geom::Coordinate& c = hole->getCoordinatesRO()->getAt(0);
            for(ShellAndHolesList::iterator p_i = exterior.begin(), p_e = exterior.end(); p_i != p_e; ++p_i) {
                ShellAndHoles& p = *p_i;
                const CoordinateSequence* shell_cs = p.first->getCoordinatesRO();
                if(PointLocation::isInRing(c, shell_cs)) {
                    // add hole to shell
                    p.second->push_back(new LinearRing(*hole));
                    break;
                }
            }
        }

        delete poly;
    }

    // Build the result polygons

    std::list<geom::Polygon*> new_polygons;
    for(ShellAndHolesList::iterator i = exterior.begin(), e = exterior.end(); i != e; ++i) {
        ShellAndHoles& p = *i;
        geom::Polygon* poly = _gf.createPolygon(p.first, p.second);
        new_polygons.push_back(poly);
    }

    clear();
    polygons = new_polygons;
}

void
RectangleIntersectionBuilder::reverseLines()
{
    std::list<geom::LineString*> new_lines;
    for(std::list<geom::LineString*>::reverse_iterator i = lines.rbegin(), e = lines.rend(); i != e; ++i) {
        LineString* ol = *i;
        new_lines.push_back(dynamic_cast<LineString*>(ol->reverse().release()));
        delete ol;
    }
    lines = new_lines;
}


} // namespace geos::operation::intersection
} // namespace geos::operation
} // namespace geos
