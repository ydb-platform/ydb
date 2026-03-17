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

#include <geos/operation/overlayng/RobustClipEnvelopeComputer.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>


namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


RobustClipEnvelopeComputer::RobustClipEnvelopeComputer(const Envelope* p_targetEnv)
    : targetEnv(p_targetEnv)
    , clipEnv(*p_targetEnv)
{}


/*public static*/
Envelope
RobustClipEnvelopeComputer::getEnvelope(const Geometry* a, const Geometry* b, const Envelope* targetEnv)
{
    RobustClipEnvelopeComputer cec(targetEnv);
    cec.add(a);
    cec.add(b);
    return cec.getEnvelope();
}

/*public*/
Envelope
RobustClipEnvelopeComputer::getEnvelope()
{
    return clipEnv;
}

/*public*/
void
RobustClipEnvelopeComputer::add(const Geometry* g)
{
    if (g == nullptr || g->isEmpty())
        return;

    if (g->getGeometryTypeId() == GEOS_POLYGON)
        addPolygon(static_cast<const Polygon*>(g));
    else if (g->isCollection())
        addCollection(static_cast<const GeometryCollection*>(g));
}

/*private*/
void
RobustClipEnvelopeComputer::addCollection(const GeometryCollection* gc)
{
    for (std::size_t i = 0; i < gc->getNumGeometries(); i++) {
        const Geometry* g = gc->getGeometryN(i);
        add(g);
    }
}

/*private*/
void
RobustClipEnvelopeComputer::addPolygon(const Polygon* poly)
{
    const LinearRing* shell = poly->getExteriorRing();
    addPolygonRing(shell);

    for (std::size_t i = 0; i < poly->getNumInteriorRing(); i++) {
        const LinearRing* hole = poly->getInteriorRingN(i);
        addPolygonRing(hole);
    }
}

/**
* Adds a polygon ring to the graph. Empty rings are ignored.
*/
/*private*/
void
RobustClipEnvelopeComputer::addPolygonRing(const LinearRing* ring)
{
    // don't add empty lines
    if (ring->isEmpty())
      return;

    const CoordinateSequence* seq = ring->getCoordinatesRO();
    for (std::size_t i = 1; i < seq->size(); i++) {
        addSegment(seq->getAt(i - 1), seq->getAt(i));
    }
}

/*private*/
void
RobustClipEnvelopeComputer::addSegment(const Coordinate& p1, const Coordinate& p2)
{
    if (intersectsSegment(targetEnv, p1, p2)) {
        clipEnv.expandToInclude(p1);
        clipEnv.expandToInclude(p2);
    }
}

/*private*/
bool
RobustClipEnvelopeComputer::intersectsSegment(const Envelope* env, const Coordinate& p1, const Coordinate& p2)
{
    /**
     * This is a crude test of whether segment intersects envelope.
     * It could be refined by checking exact intersection.
     * This could be based on the algorithm in the HotPixel.intersectsScaled method.
     */
    return env->intersects(p1, p2);
}



} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
