/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Martin Davis <mtnclimb@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: algorithm/InteriorPointArea.java (JTS-1.17+)
 * https://github.com/locationtech/jts/commit/a140ca30cc51be4f65c950a30b0a8f51a6df75ba
 *
 **********************************************************************/

#include <geos/algorithm/InteriorPointArea.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/util/Interrupt.h>

#include <algorithm>
#include <vector>
#include <typeinfo>
#include <memory> // for unique_ptr

using namespace std;
using namespace geos::geom;

namespace geos {
namespace algorithm { // geos.algorithm

// file statics
namespace {

double
avg(double a, double b)
{
    return (a + b) / 2.0;
}

/**
 * Finds a safe scan line Y ordinate by projecting
 * the polygon segments
 * to the Y axis and finding the
 * Y-axis interval which contains the centre of the Y extent.
 * The centre of
 * this interval is returned as the scan line Y-ordinate.
 * <p>
 * Note that in the case of (degenerate, invalid)
 * zero-area polygons the computed Y value
 * may be equal to a vertex Y-ordinate.
 *
 * @author mdavis
 *
 */
class ScanLineYOrdinateFinder {
public:
    static double
    getScanLineY(const Polygon& poly)
    {
        ScanLineYOrdinateFinder finder(poly);
        return finder.getScanLineY();
    }

    ScanLineYOrdinateFinder(const Polygon& nPoly)
        : poly(nPoly)
    {
        // initialize using extremal values
        hiY = poly.getEnvelopeInternal()->getMaxY();
        loY = poly.getEnvelopeInternal()->getMinY();
        centreY = avg(loY, hiY);
    }

    double
    getScanLineY()
    {
        process(*poly.getExteriorRing());
        for (size_t i = 0; i < poly.getNumInteriorRing(); i++) {
            process(*poly.getInteriorRingN(i));
        }
        double bisectY = avg(hiY, loY);
        return bisectY;
    }

private:
    const Polygon& poly;

    double centreY;
    double hiY;
    double loY;

    void
    process(const LineString& line)
    {
        const CoordinateSequence* seq = line.getCoordinatesRO();
        for (std::size_t i = 0, s = seq->size(); i < s; i++) {
            double y = seq->getY(i);
            updateInterval(y);
        }
    }

    void
    updateInterval(double y)
    {
        if (y <= centreY) {
            if (y > loY) {
                loY = y;
            }
        }
        else if (y > centreY) {
            if (y < hiY) {
                hiY = y;
            }
        }
    }
};

class InteriorPointPolygon {
public:
    InteriorPointPolygon(const Polygon& poly)
        : polygon(poly)
    {
        interiorPointY = ScanLineYOrdinateFinder::getScanLineY(polygon);
    }

    bool
    getInteriorPoint(Coordinate& ret) const
    {
        ret = interiorPoint;
        return true;
    }

    double getWidth()
    {
        return interiorSectionWidth;
    }

    void
    process()
    {
        vector<double> crossings;

        /*
         * This results in returning a null Coordinate
         */
        if (polygon.isEmpty()) return;
        /*
         * set default interior point in case polygon has zero area
         */
        interiorPoint = *polygon.getCoordinate();

        const LinearRing* shell = polygon.getExteriorRing();
        scanRing(*shell, crossings);
        for (size_t i = 0; i < polygon.getNumInteriorRing(); i++) {
            const LinearRing* hole = polygon.getInteriorRingN(i);
            scanRing(*hole, crossings);
        }
        findBestMidpoint(crossings);
    }

private:
    const Polygon& polygon;
    double interiorPointY;
    double interiorSectionWidth = 0.0;
    Coordinate interiorPoint;

    void scanRing(const LinearRing& ring, vector<double>& crossings)
    {
        // skip rings which don't cross scan line
        if (! intersectsHorizontalLine(ring.getEnvelopeInternal(), interiorPointY))
            return;

        const CoordinateSequence* seq = ring.getCoordinatesRO();
        for (size_t i = 1; i < seq->size(); i++) {
            const Coordinate& ptPrev = seq->getAt(i - 1);
            const Coordinate& pt = seq->getAt(i);
            addEdgeCrossing(ptPrev, pt, interiorPointY, crossings);
        }
    }

    void addEdgeCrossing(const Coordinate& p0, const Coordinate& p1, double scanY, vector<double>& crossings)
    {
        // skip non-crossing segments
        if (!intersectsHorizontalLine(p0, p1, scanY))
            return;
        if (! isEdgeCrossingCounted(p0, p1, scanY))
            return;

        // edge intersects scan line, so add a crossing
        double xInt = intersection(p0, p1, scanY);
        crossings.push_back(xInt);
    }

    void findBestMidpoint(vector<double>& crossings)
    {
        // zero-area polygons will have no crossings
        if (crossings.empty()) return;

        //Assert.isTrue(0 == crossings.size() % 2, "Interior Point robustness failure: odd number of scanline crossings");

        sort(crossings.begin(), crossings.end());
        /*
         * Entries in crossings list are expected to occur in pairs representing a
         * section of the scan line interior to the polygon (which may be zero-length)
        */
        for (size_t i = 0; i < crossings.size(); i += 2) {
            double x1 = crossings[i];
            // crossings count must be even so this should be safe
            double x2 = crossings[i + 1];

            double width = x2 - x1;
            if (width > interiorSectionWidth) {
                interiorSectionWidth = width;
                double interiorPointX = avg(x1, x2);
                interiorPoint = Coordinate(interiorPointX, interiorPointY);
            }
        }
    }

    static bool
    isEdgeCrossingCounted(const Coordinate& p0, const Coordinate& p1, double scanY)
    {
        // skip horizontal lines
        if (p0.y == p1.y)
            return false;
        // handle cases where vertices lie on scan-line
        // downward segment does not include start point
        if (p0.y == scanY && p1.y < scanY)
            return false;
        // upward segment does not include endpoint
        if (p1.y == scanY && p0.y < scanY)
            return false;
        return true;
    }

    static double
    intersection(const Coordinate& p0, const Coordinate& p1, double Y)
    {
        double x0 = p0.x;
        double x1 = p1.x;

        if (x0 == x1)
            return x0;

        // Assert: segDX is non-zero, due to previous equality test
        double segDX = x1 - x0;
        double segDY = p1.y - p0.y;
        double m = segDY / segDX;
        double x = x0 + ((Y - p0.y) / m);
        return x;
    }

    static bool
    intersectsHorizontalLine(const Envelope* env, double y)
    {
        if (y < env->getMinY())
            return false;
        if (y > env->getMaxY())
            return false;
        return true;
    }

    static bool
    intersectsHorizontalLine(const Coordinate& p0, const Coordinate& p1, double y)
    {
        // both ends above?
        if (p0.y > y && p1.y > y)
            return false;
        // both ends below?
        if (p0.y < y && p1.y < y)
            return false;
        // segment must intersect line
        return true;
    }
};

} // anonymous namespace

InteriorPointArea::InteriorPointArea(const Geometry* g)
{
    maxWidth = -1;
    process(g);
}

bool
InteriorPointArea::getInteriorPoint(Coordinate& ret) const
{
    // GEOS-specific code
    if (maxWidth < 0)
        return false;

    ret = interiorPoint;
    return true;
}

/*private*/
void
InteriorPointArea::process(const Geometry* geom)
{
    if (geom->isEmpty())
        return;

    const Polygon* poly = dynamic_cast<const Polygon*>(geom);
    if (poly) {
        processPolygon(poly);
        return;
    }

    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(geom);
    if (gc) {
        for (std::size_t i = 0, n = gc->getNumGeometries(); i < n; i++) {
            process(gc->getGeometryN(i));
            GEOS_CHECK_FOR_INTERRUPTS();
        }
    }
}

/*private*/
void
InteriorPointArea::processPolygon(const Polygon* polygon)
{
    InteriorPointPolygon intPtPoly(*polygon);
    intPtPoly.process();
    double width = intPtPoly.getWidth();
    if (width > maxWidth) {
        maxWidth = width;
        intPtPoly.getInteriorPoint(interiorPoint);
    }
}

} // namespace geos.algorithm
} // namespace geos
