/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 ***********************************************************************
 *
 * Last port: operation/overlay/validate/OffsetPointGenerator.java rev. 1.1 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/overlay/validate/OffsetPointGenerator.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/util/LinearComponentExtracter.h>

#include <cassert>
#include <functional>
#include <vector>
#include <memory> // for unique_ptr
#include <cmath>
#include <algorithm> // std::for_each

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

using namespace std;
using namespace geos::geom;
using namespace geos::algorithm;

namespace geos {
namespace operation { // geos.operation
namespace overlay { // geos.operation.overlay
namespace validate { // geos.operation.overlay.validate

/*public*/
OffsetPointGenerator::OffsetPointGenerator(const geom::Geometry& geom,
        double offset)
    :
    g(geom),
    offsetDistance(offset)
{
}

/*public*/
std::unique_ptr< std::vector<geom::Coordinate> >
OffsetPointGenerator::getPoints()
{
    assert(offsetPts.get() == nullptr);
    offsetPts.reset(new vector<Coordinate>());

    vector<const LineString*> lines;
    geos::geom::util::LinearComponentExtracter::getLines(g, lines);

    for (const auto& line : lines) {
        extractPoints(line);
    }

    // NOTE: Apparently, this is 'source' method giving up the object resource.
    return std::move(offsetPts);
}

/*private*/
void
OffsetPointGenerator::extractPoints(const LineString* line)
{
    const CoordinateSequence& pts = *(line->getCoordinatesRO());
    assert(pts.size() > 1);

    for(size_t i = 0, n = pts.size() - 1; i < n; ++i) {
        computeOffsets(pts[i], pts[i + 1]);
    }
}

/*private*/
void
OffsetPointGenerator::computeOffsets(const Coordinate& p0,
                                     const Coordinate& p1)
{
    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    double len = sqrt(dx * dx + dy * dy);

    // u is the vector that is the length of the offset,
    // in the direction of the segment
    double ux = offsetDistance * dx / len;
    double uy = offsetDistance * dy / len;

    double midX = (p1.x + p0.x) / 2;
    double midY = (p1.y + p0.y) / 2;

    Coordinate offsetLeft(midX - uy, midY + ux);
    Coordinate offsetRight(midX + uy, midY - ux);

    offsetPts->push_back(offsetLeft);
    offsetPts->push_back(offsetRight);
}

} // namespace geos.operation.overlay.validate
} // namespace geos.operation.overlay
} // namespace geos.operation
} // namespace geos

