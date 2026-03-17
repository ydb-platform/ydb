/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/predicate/RectangleIntersects.java r378 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/operation/predicate/RectangleIntersects.h>
#include <geos/operation/predicate/SegmentIntersectionTester.h>

// for EnvelopeIntersectsVisitor inheritance
#include <geos/geom/util/ShortCircuitedGeometryVisitor.h>
#include <geos/geom/util/LinearComponentExtracter.h>

#include <geos/geom/Envelope.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/LineString.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/Location.h>

#include <geos/algorithm/locate/SimplePointInAreaLocator.h>

#include <memory>

//using namespace geos::geom::util;

namespace geos {
namespace operation { // geos.operation
namespace predicate { // geos.operation.predicate

//----------------------------------------------------------------
// EnvelopeIntersectsVisitor
//----------------------------------------------------------------

class EnvelopeIntersectsVisitor: public geom::util::ShortCircuitedGeometryVisitor {
private:

    const geom::Envelope& rectEnv;
    bool intersectsVar;

    // Declare type as noncopyable
    EnvelopeIntersectsVisitor(const EnvelopeIntersectsVisitor& other) = delete;
    EnvelopeIntersectsVisitor& operator=(const EnvelopeIntersectsVisitor& rhs) = delete;

protected:

    void
    visit(const geom::Geometry& element) override
    {
        const geom::Envelope& elementEnv = *(element.getEnvelopeInternal());

        // skip if envelopes do not intersect
        if(! rectEnv.intersects(elementEnv)) {
            return;
        }

        // fully contained - must intersect
        if(rectEnv.contains(elementEnv)) {
            intersectsVar = true;
            return;
        }

        /*
         * Since the envelopes intersect and the test element is
         * connected, if the test envelope is completely bisected by
         * an edge of the rectangle the element and the rectangle
         * must touch (This is basically an application of the
         * Jordan Curve Theorem).  The alternative situation
         * is that the test envelope is "on a corner" of the
         * rectangle envelope, i.e. is not completely bisected.
         * In this case it is not possible to make a conclusion
         * about the presence of an intersection.
         */
        if(elementEnv.getMinX() >= rectEnv.getMinX()
                && elementEnv.getMaxX() <= rectEnv.getMaxX()) {
            intersectsVar = true;
            return;
        }
        if(elementEnv.getMinY() >= rectEnv.getMinY()
                && elementEnv.getMaxY() <= rectEnv.getMaxY()) {
            intersectsVar = true;
            return;
        }
    }

    bool
    isDone() override
    {
        return intersectsVar == true;
    }

public:

    EnvelopeIntersectsVisitor(const geom::Envelope& env)
        :
        rectEnv(env),
        intersectsVar(false)
    {}

    bool
    intersects()
    {
        return intersectsVar;
    }

};

//----------------------------------------------------------------
// ContainsPointVisitor
//----------------------------------------------------------------

/**
 * Tests whether it can be concluded
 * that a geometry contains a corner point of a rectangle.
 */
class ContainsPointVisitor: public geom::util::ShortCircuitedGeometryVisitor {
private:

    const geom::Envelope& rectEnv;
    bool containsPointVar;
    const geom::CoordinateSequence& rectSeq;

    // Declare type as noncopyable
    ContainsPointVisitor(const ContainsPointVisitor& other);
    ContainsPointVisitor& operator=(const ContainsPointVisitor& rhs);

protected:

    void
    visit(const geom::Geometry& geom) override
    {
        using geos::algorithm::locate::SimplePointInAreaLocator;

        const geom::Polygon* poly;

        // if test geometry is not polygonal this check is not needed
        if(nullptr == (poly = dynamic_cast<const geom::Polygon*>(&geom))) {
            return;
        }

        const geom::Envelope& elementEnv = *(geom.getEnvelopeInternal());

        if(!rectEnv.intersects(elementEnv)) {
            return;
        }

        // test each corner of rectangle for inclusion
        for(int i = 0; i < 4; i++) {

            const geom::Coordinate& rectPt = rectSeq.getAt(i);

            if(!elementEnv.contains(rectPt)) {
                continue;
            }

            // check rect point in poly (rect is known not to
            // touch polygon at this point)
            if(SimplePointInAreaLocator::locatePointInPolygon(rectPt, poly) != geom::Location::EXTERIOR) {
                containsPointVar = true;
                return;
            }
        }
    }

    bool
    isDone() override
    {
        return containsPointVar;
    }

public:

    ContainsPointVisitor(const geom::Polygon& rect)
        :
        rectEnv(*(rect.getEnvelopeInternal())),
        containsPointVar(false),
        rectSeq(*(rect.getExteriorRing()->getCoordinatesRO()))
    {}

    bool
    containsPoint()
    {
        return containsPointVar;
    }

};

//----------------------------------------------------------------
// LineIntersectsVisitor
//----------------------------------------------------------------

class LineIntersectsVisitor: public geom::util::ShortCircuitedGeometryVisitor {
private:

    //const geom::Polygon& rectangle;
    const geom::Envelope& rectEnv;
    const geom::LineString& rectLine;
    bool intersectsVar;
    //const geom::CoordinateSequence &rectSeq;

    void
    computeSegmentIntersection(const geom::Geometry& geom)
    {
        using geos::geom::util::LinearComponentExtracter;

        // check segment intersection
        // get all lines from geom (e.g. if it's a multi-ring polygon)
        geom::LineString::ConstVect lines;
        LinearComponentExtracter::getLines(geom, lines);
        SegmentIntersectionTester si;
        if(si.hasIntersectionWithLineStrings(rectLine, lines)) {
            intersectsVar = true;
            return;
        }
    }

    // Declare type as noncopyable
    LineIntersectsVisitor(const LineIntersectsVisitor& other);
    LineIntersectsVisitor& operator=(const LineIntersectsVisitor& rhs);

protected:

    void
    visit(const geom::Geometry& geom) override
    {
        const geom::Envelope& elementEnv = *(geom.getEnvelopeInternal());

        // check for envelope intersection
        if(! rectEnv.intersects(elementEnv)) {
            return;
        }

        computeSegmentIntersection(geom);
    }

    bool
    isDone() override
    {
        return intersectsVar;
    }

public:

    LineIntersectsVisitor(const geom::Polygon& rect)
        :
        //rectangle(rect),
        rectEnv(*(rect.getEnvelopeInternal())),
        rectLine(*(rect.getExteriorRing())),
        intersectsVar(false)
        //rectSeq(*(rect.getExteriorRing()->getCoordinatesRO()))
    {}

    /**
     * Reports whether any segment intersection exists.
     *
     * @return <code>true</code> if a segment intersection exists
     * <code>false</code> if no segment intersection exists
     */
    bool
    intersects() const
    {
        return intersectsVar;
    }

};

//----------------------------------------------------------------
// RectangleIntersects
//----------------------------------------------------------------

bool
RectangleIntersects::intersects(const geom::Geometry& geom)
{
    if(!rectEnv.intersects(geom.getEnvelopeInternal())) {
        return false;
    }

    // test envelope relationships
    EnvelopeIntersectsVisitor visitor(rectEnv);
    visitor.applyTo(geom);
    if(visitor.intersects()) {
        return true;
    }

    // test if any rectangle corner is contained in the target
    ContainsPointVisitor ecpVisitor(rectangle);
    ecpVisitor.applyTo(geom);
    if(ecpVisitor.containsPoint()) {
        return true;
    }

    // test if any lines intersect
    LineIntersectsVisitor liVisitor(rectangle);
    liVisitor.applyTo(geom);
    if(liVisitor.intersects()) {
        return true;
    }

    return false;
}

} // namespace geos.operation.predicate
} // namespace geos.operation
} // namespace geos



