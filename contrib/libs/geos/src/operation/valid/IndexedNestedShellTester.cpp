/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/algorithm/PointLocation.h>
#include <geos/algorithm/locate/IndexedPointInAreaLocator.h>
#include <geos/geom/Polygon.h>
#include <geos/index/strtree/STRtree.h>
#include <geos/operation/valid/IndexedNestedShellTester.h>
#include <geos/operation/valid/IsValidOp.h>

#include <deque>

namespace geos {
namespace operation {
namespace valid {

class PolygonIndexedLocators {

public:
    using Locator = algorithm::locate::IndexedPointInAreaLocator;

    PolygonIndexedLocators(const geom::Polygon & p) :
        poly(p),
        shellLoc(*poly.getExteriorRing())
    {
        auto n = poly.getNumInteriorRing();
        for (size_t i = 0; i < n; i++) {
            ringLoc.emplace_back(*poly.getInteriorRingN(i));
        }
    }

    Locator& getShellLocator() {
        return shellLoc;
    }

    Locator& getHoleLocator(size_t holeNum) {
        return ringLoc[holeNum];
    }

    const geom::Polygon* getPolygon() const {
        return &poly;
    }

    const geom::LinearRing* getInteriorRingN(size_t n) const {
        return poly.getInteriorRingN(n);
    }

private:
    const geom::Polygon& poly;
    Locator shellLoc;
    std::deque<Locator> ringLoc;
};

IndexedNestedShellTester::IndexedNestedShellTester(const geos::geomgraph::GeometryGraph &g, size_t initialCapacity) :
    graph(g),
    nestedPt(nullptr),
    processed(false)
{
    polys.reserve(initialCapacity);
}

bool
IndexedNestedShellTester::isNonNested() {
    return getNestedPoint() == nullptr;
}

const geom::Coordinate*
IndexedNestedShellTester::getNestedPoint() {
    compute();

    return nestedPt;
}

void
IndexedNestedShellTester::compute() {
    if (processed) {
        return;
    }

    processed = true;

    index::strtree::STRtree tree;
    for (const auto& p : polys) {
        tree.insert(p->getEnvelopeInternal(), (void*) p->getExteriorRing());
    }

    std::vector<void*> hits;
    for (const auto& outerPoly : polys) {
        hits.clear();

        PolygonIndexedLocators locs(*outerPoly);
        const geom::LinearRing* outerShell = outerPoly->getExteriorRing();

        tree.query(outerShell->getEnvelopeInternal(), hits);

        for (const auto& hit : hits) {
            const geom::LinearRing* potentialInnerShell = static_cast<const geom::LinearRing*>(hit);

            if (potentialInnerShell == outerShell) {
                continue;
            }

            // check if p1 can possibly by inside p2
            if (!outerShell->getEnvelopeInternal()->covers(potentialInnerShell->getEnvelopeInternal())) {
                continue;
            }

            checkShellNotNested(potentialInnerShell, locs);

            if (nestedPt != nullptr) {
                return;
            }
        }

    }
}

/*private*/
void
IndexedNestedShellTester::checkShellNotNested(const geom::LinearRing* shell, PolygonIndexedLocators & locs)
{
    const geom::CoordinateSequence* shellPts = shell->getCoordinatesRO();

    // test if shell is inside polygon shell
    const geom::LinearRing* polyShell = locs.getPolygon()->getExteriorRing();
    const geom::Coordinate* shellPt = IsValidOp::findPtNotNode(shellPts, polyShell, &graph);

    // if no point could be found, we can assume that the shell
    // is outside the polygon
    if(shellPt == nullptr) {
        return;
    }

    bool insidePolyShell = locs.getShellLocator().locate(shellPt) != geom::Location::EXTERIOR;
    if(!insidePolyShell) {
        return;
    }

    auto nholes = locs.getPolygon()->getNumInteriorRing();
    if (nholes == 0) {
        nestedPt = shellPt;
        return;
    }

    // Check if the shell is inside one of the holes.
    // This is the case if one of the calls to checkShellInsideHole
    // returns a null coordinate.
    // Otherwise, the shell is not properly contained in a hole, which is
    // an error.
    const geom::Coordinate* badNestedPt = nullptr;
    for (size_t i = 0; i < nholes; i++) {
        const geom::LinearRing* hole = locs.getPolygon()->getInteriorRingN(i);

        if (hole->getEnvelopeInternal()->covers(shell->getEnvelopeInternal())) {
            badNestedPt = checkShellInsideHole(shell, locs.getHoleLocator(i));
            if(badNestedPt == nullptr) {
                return;
            }

        }
    }

    nestedPt = shellPt;
}


const geom::Coordinate*
IndexedNestedShellTester::checkShellInsideHole(const geom::LinearRing* shell,
        algorithm::locate::IndexedPointInAreaLocator & holeLoc) {

    const geom::CoordinateSequence* shellPts = shell->getCoordinatesRO();
    const geom::LinearRing* hole = static_cast<const geom::LinearRing*>(&holeLoc.getGeometry());
    const geom::CoordinateSequence* holePts = hole->getCoordinatesRO();

    const geom::Coordinate* shellPtNotOnHole = IsValidOp::findPtNotNode(shellPts, hole, &graph);

    if (shellPtNotOnHole) {
        // Found a point not on the hole boundary. Is it outside the hole?
        if (holeLoc.locate(shellPtNotOnHole) == geom::Location::EXTERIOR) {
            return shellPtNotOnHole;
        }
    }

    const geom::Coordinate* holePt = IsValidOp::findPtNotNode(holePts, shell, &graph);
    // if point is on hole but not on shell, check that the hole is outside the shell

    if (holePt != nullptr) {
        if (algorithm::PointLocation::isInRing(*holePt, shellPts)) {
            return holePt;
        }

        return nullptr;
    }

    // should never reach here: points in hole and shell appear to be equal
    throw util::GEOSException("Hole and shell appear to be equal in IndexedNestedShellTester");
}

}
}
}
