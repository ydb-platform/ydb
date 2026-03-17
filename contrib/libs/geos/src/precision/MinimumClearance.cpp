/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2016 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: precision/MinimumClearance.java (f6187ee2 JTS-1.14)
 *
 **********************************************************************/

#include <geos/algorithm/Distance.h>
#include <geos/precision/MinimumClearance.h>
#include <geos/index/strtree/STRtree.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/operation/distance/FacetSequenceTreeBuilder.h>
#include <geos/geom/LineSegment.h>

using namespace geos::geom;
using namespace geos::operation::distance;
using namespace geos::index::strtree;

namespace geos {
namespace precision {

MinimumClearance::MinimumClearance(const Geometry* g) : inputGeom(g) {}

double
MinimumClearance::getDistance()
{
    compute();
    return minClearance;
}

std::unique_ptr<LineString>
MinimumClearance::getLine()
{
    compute();

    // return empty line string if no min pts were found
    if(minClearance == std::numeric_limits<double>::infinity()) {
        return inputGeom->getFactory()->createLineString();
    }

    return inputGeom->getFactory()->createLineString(minClearancePts->clone());
}

void
MinimumClearance::compute()
{
    class MinClearanceDistance : public ItemDistance {
    private:
        double minDist;
        std::vector<Coordinate> minPts;

        void
        updatePts(const Coordinate& p, const Coordinate& seg0, const Coordinate& seg1)
        {
            LineSegment seg(seg0, seg1);

            minPts[0] = p;
            seg.closestPoint(p, minPts[1]);
        }

    public:
        MinClearanceDistance() :
            minDist(std::numeric_limits<double>::infinity()),
            minPts(std::vector<Coordinate>(2))
        {}

        const std::vector<Coordinate>*
        getCoordinates()
        {
            return &minPts;
        }

        double
        distance(const ItemBoundable* b1, const ItemBoundable* b2) override
        {
            FacetSequence* fs1 = static_cast<FacetSequence*>(b1->getItem());
            FacetSequence* fs2 = static_cast<FacetSequence*>(b2->getItem());

            minDist = std::numeric_limits<double>::infinity();

            return distance(fs1, fs2);
        }

        double
        distance(const FacetSequence* fs1, const FacetSequence* fs2)
        {
            // Compute MinClearance distance metric

            vertexDistance(fs1, fs2);
            if(fs1->size() == 1 && fs2->size() == 1) {
                return minDist;
            }
            if(minDist <= 0.0) {
                return minDist;
            }

            segmentDistance(fs1, fs2);
            if(minDist <= 0.0) {
                return minDist;
            }

            segmentDistance(fs2, fs1);
            return minDist;
        }

        double
        vertexDistance(const FacetSequence* fs1, const FacetSequence* fs2)
        {
            for(size_t i1 = 0; i1 < fs1->size(); i1++) {
                for(size_t i2 = 0; i2 < fs2->size(); i2++) {
                    const Coordinate* p1 = fs1->getCoordinate(i1);
                    const Coordinate* p2 = fs2->getCoordinate(i2);
                    if(!p1->equals2D(*p2)) {
                        double d = p1->distance(*p2);
                        if(d < minDist) {
                            minDist = d;
                            minPts[0] = *p1;
                            minPts[1] = *p2;
                            if(d == 0.0) {
                                return d;
                            }
                        }
                    }
                }
            }
            return minDist;
        }

        double
        segmentDistance(const FacetSequence* fs1, const FacetSequence* fs2)
        {
            for(size_t i1 = 0; i1 < fs1->size(); i1++) {
                for(size_t i2 = 1; i2 < fs2->size(); i2++) {
                    const Coordinate* p = fs1->getCoordinate(i1);

                    const Coordinate* seg0 = fs2->getCoordinate(i2 - 1);
                    const Coordinate* seg1 = fs2->getCoordinate(i2);

                    if(!(p->equals2D(*seg0) || p->equals2D(*seg1))) {
                        double d = geos::algorithm::Distance::pointToSegment(*p, *seg0, *seg1);
                        if(d < minDist) {
                            minDist = d;
                            updatePts(*p, *seg0, *seg1);
                            if(d == 0.0) {
                                return d;
                            }
                        }
                    }
                }
            }
            return minDist;
        }
    };

    // already computed
    if(minClearancePts.get() != nullptr) {
        return;
    }

    // initialize to "No Distance Exists" state
    minClearancePts = std::unique_ptr<CoordinateSequence>(inputGeom->getFactory()->getCoordinateSequenceFactory()->create(2,
                      2));
    minClearance = std::numeric_limits<double>::infinity();

    // handle empty geometries
    if(inputGeom->isEmpty()) {
        return;
    }

    auto tree = FacetSequenceTreeBuilder::build(inputGeom);
    MinClearanceDistance mcd;
    std::pair<const void*, const void*> nearest = tree->nearestNeighbour(&mcd);

    minClearance = mcd.distance(
                       static_cast<const FacetSequence*>(nearest.first),
                       static_cast<const FacetSequence*>(nearest.second));

    const std::vector<Coordinate>* minClearancePtsVec = mcd.getCoordinates();
    minClearancePts->setAt((*minClearancePtsVec)[0], 0);
    minClearancePts->setAt((*minClearancePtsVec)[1], 1);
}


}
}
