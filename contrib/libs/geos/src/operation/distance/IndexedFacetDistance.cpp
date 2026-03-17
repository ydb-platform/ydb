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
 * Last port: operation/distance/IndexedFacetDistance.java (f6187ee2 JTS-1.14)
 *
 **********************************************************************/

#include <geos/geom/Coordinate.h>
#include <geos/index/strtree/STRtree.h>
#include <geos/operation/distance/IndexedFacetDistance.h>

using namespace geos::geom;
using namespace geos::index::strtree;

namespace geos {
namespace operation {
namespace distance {

/*public static*/
double
IndexedFacetDistance::distance(const Geometry* g1, const Geometry* g2)
{
    IndexedFacetDistance ifd(g1);
    return ifd.distance(g2);
}

/*public static*/
std::vector<geom::Coordinate>
IndexedFacetDistance::nearestPoints(const geom::Geometry* g1, const geom::Geometry* g2)
{
    IndexedFacetDistance dist(g1);
    return dist.nearestPoints(g2);
}

double
IndexedFacetDistance::distance(const Geometry* g) const
{
    struct : public ItemDistance {
        double
        distance(const ItemBoundable* item1, const ItemBoundable* item2) override
        {
            return static_cast<const FacetSequence*>(item1->getItem())->distance(*static_cast<const FacetSequence*>
                    (item2->getItem()));
        }
    } itemDistance;

    std::unique_ptr<STRtree> tree2(FacetSequenceTreeBuilder::build(g));

    std::pair<const void*, const void*> obj = cachedTree->nearestNeighbour(tree2.get(),
            dynamic_cast<ItemDistance*>(&itemDistance));

    const FacetSequence *fs1 = static_cast<const FacetSequence*>(obj.first);
    const FacetSequence *fs2 = static_cast<const FacetSequence*>(obj.second);

    double p_distance = fs1->distance(*fs2);

    return p_distance;
}

std::vector<GeometryLocation>
IndexedFacetDistance::nearestLocations(const geom::Geometry* g) const
{
    struct : public ItemDistance {
        double
        distance(const ItemBoundable* item1, const ItemBoundable* item2) override
        {
            return static_cast<const FacetSequence*>(item1->getItem())->distance(*static_cast<const FacetSequence*>
                    (item2->getItem()));
        }
    } itemDistance;
    std::unique_ptr<STRtree> tree2(FacetSequenceTreeBuilder::build(g));
    std::pair<const void*, const void*> obj = cachedTree->nearestNeighbour(tree2.get(),
            dynamic_cast<ItemDistance*>(&itemDistance));
    const FacetSequence *fs1 = static_cast<const FacetSequence*>(obj.first);
    const FacetSequence *fs2 = static_cast<const FacetSequence*>(obj.second);
    std::vector<GeometryLocation> locs;
    locs = fs1->nearestLocations(*fs2);
    return locs;
}

std::vector<Coordinate>
IndexedFacetDistance::nearestPoints(const geom::Geometry* g) const
{
    std::vector<GeometryLocation> minDistanceLocation = nearestLocations(g);
    std::vector<Coordinate> nearestPts;
    nearestPts.push_back(minDistanceLocation[0].getCoordinate());
    nearestPts.push_back(minDistanceLocation[1].getCoordinate());
    return nearestPts;
}

}
}
}
