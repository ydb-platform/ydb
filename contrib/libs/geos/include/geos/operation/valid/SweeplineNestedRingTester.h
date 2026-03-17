/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/valid/SweeplineNestedRingTester.java rev. 1.12 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_SWEEPLINENESTEDRINGTESTER_H
#define GEOS_OP_SWEEPLINENESTEDRINGTESTER_H

#include <geos/export.h>
#include <geos/geom/Envelope.h> // for inline
//#include <geos/indexSweepline.h> // for inline and inheritance
#include <geos/index/sweepline/SweepLineOverlapAction.h> // for inheritance
#include <geos/index/sweepline/SweepLineIndex.h> // for inlines

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class LinearRing;
class Envelope;
class Coordinate;
}
namespace index {
namespace sweepline {
class SweepLineIndex;
}
}
namespace geomgraph {
class GeometryGraph;
}
}

namespace geos {
namespace operation { // geos::operation
namespace valid { // geos::operation::valid

/** \brief
 * Tests whether any of a set of [LinearRings](@ref geom::LinearRing) are
 * nested inside another ring in the set, using an
 * [SweepLineIndex](@ref index::sweepline::SweepLineIndex) to speed up
 * the comparisons.
 */
class GEOS_DLL SweeplineNestedRingTester {

private:
    geomgraph::GeometryGraph* graph;  // used to find non-node vertices
    std::vector<geom::LinearRing*> rings;
    index::sweepline::SweepLineIndex* sweepLine;
    geom::Coordinate* nestedPt;
    void buildIndex();

    SweeplineNestedRingTester(const SweeplineNestedRingTester&) = delete;
    SweeplineNestedRingTester& operator=(const SweeplineNestedRingTester&) = delete;

public:

    SweeplineNestedRingTester(geomgraph::GeometryGraph* newGraph)
        :
        graph(newGraph),
        rings(),
        sweepLine(new index::sweepline::SweepLineIndex()),
        nestedPt(nullptr)
    {}

    ~SweeplineNestedRingTester()
    {
        delete sweepLine;
    }

    /*
     * Be aware that the returned Coordinate (if != NULL)
     * will point to storage owned by one of the LinearRing
     * previously added. If you destroy them, this
     * will point to an invalid memory address.
     */
    geom::Coordinate*
    getNestedPoint()
    {
        return nestedPt;
    }

    void
    add(geom::LinearRing* ring)
    {
        rings.push_back(ring);
    }

    bool isNonNested();
    bool isInside(geom::LinearRing* innerRing, geom::LinearRing* searchRing);
    class OverlapAction: public index::sweepline::SweepLineOverlapAction {
    public:
        bool isNonNested;
        OverlapAction(SweeplineNestedRingTester* p);
        void overlap(index::sweepline::SweepLineInterval* s0,
                     index::sweepline::SweepLineInterval* s1) override;
    private:
        SweeplineNestedRingTester* parent;
    };
};

} // namespace geos::operation::valid
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_SWEEPLINENESTEDRINGTESTER_H
