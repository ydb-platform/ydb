/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>
#include <geos/geom/Envelope.h>

#include <vector>
#include <map>


// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class Geometry;
class GeometryCollection;
class Polygon;
class LinearRing;
}
namespace operation {
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

using namespace geos::geom;

/**
 * Computes a robust clipping envelope for a pair of polygonal geometries.
 * The envelope is computed to be large enough to include the full
 * length of all geometry line segments which intersect
 * a given target envelope.
 * This ensures that line segments which might intersect are
 * not perturbed when clipped using {@link RingClipper}.
 * @author mdavis
 */
class GEOS_DLL RobustClipEnvelopeComputer {

private:

    // Members
    const Envelope* targetEnv;
    Envelope clipEnv;

    // Methods
    void add(const Geometry* g);
    void addCollection(const GeometryCollection* gc);
    void addPolygon(const Polygon* poly);
    void addPolygonRing(const LinearRing* ring);
    void addSegment(const Coordinate& p1, const Coordinate& p2);
    bool intersectsSegment(const Envelope* env, const Coordinate& p1, const Coordinate& p2);



public:

    RobustClipEnvelopeComputer(const Envelope* p_targetEnv);

    static Envelope getEnvelope(const Geometry* a, const Geometry* b, const Envelope* targetEnv);

    Envelope getEnvelope();

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

