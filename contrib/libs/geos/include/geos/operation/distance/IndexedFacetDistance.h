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

#ifndef GEOS_INDEXEDFACETDISTANCE_H
#define GEOS_INDEXEDFACETDISTANCE_H

#include <geos/operation/distance/FacetSequenceTreeBuilder.h>

namespace geos {
namespace operation {
namespace distance {

/// \brief Computes the distance between the facets (segments and vertices)
/// of two [Geometrys](\ref geom::Geometry) using a Branch-and-Bound algorithm.
///
/// The Branch-and-Bound algorithm operates over a traversal of R-trees built
/// on the target and the query geometries.
///
/// This approach provides the following benefits:
///
/// - Performance is dramatically improved due to the use of the R-tree index
///   and the pruning due to the Branch-and-Bound approach
/// - The spatial index on the target geometry is cached which allow reuse in
///   an repeated query situation.
///
/// Using this technique is usually much more performant than using the
/// brute-force \ref geom::Geometry::distance(const Geometry* g) const when one
/// or both input geometries are large, or when evaluating many distance
/// computations against a single geometry.
///
/// \author Martin Davis
class GEOS_DLL IndexedFacetDistance {
public:

    /// \brief Creates a new distance-finding instance for a given target geom::Geometry.
    ///
    /// Distances will be computed to all facets of the input geometry.
    /// The facets of the geometry are the discrete segments and points
    /// contained in its components.
    /// In the case of lineal and puntal inputs, this is equivalent to computing
    /// the conventional distance.
    /// In the case of polygonal inputs, this is equivalent to computing the
    /// distance to the polygon boundaries.
    ///
    /// \param g a Geometry, which may be of any type.
    IndexedFacetDistance(const geom::Geometry* g) :
        cachedTree(FacetSequenceTreeBuilder::build(g))
    {}

    /// \brief Computes the distance between facets of two geometries.
    ///
    /// For geometries with many segments or points, this can be faster than
    /// using a simple distance algorithm.
    ///
    /// \param g1 a geometry
    /// \param g2 a geometry
    /// \return the distance between facets of the geometries
    static double distance(const geom::Geometry* g1, const geom::Geometry* g2);

    /// \brief Computes the nearest points of the facets of two geometries.
    ///
    /// \param g1 a geometry
    /// \param g2 a geometry
    /// \return the nearest points on the facets of the geometries
    static std::vector<geom::Coordinate> nearestPoints(const geom::Geometry* g1, const geom::Geometry* g2);

    /// \brief Computes the distance from the base geometry to the given geometry.
    ///
    /// \param g the geometry to compute the distance to
    ///
    /// \return the computed distance
    double distance(const geom::Geometry* g) const;

    /// \brief Computes the nearest locations on the base geometry and the given geometry.
    ///
    /// \param g the geometry to compute the nearest location to
    /// \return the nearest locations
    std::vector<GeometryLocation> nearestLocations(const geom::Geometry* g) const;

    /// \brief Compute the nearest locations on the target geometry and the given geometry.
    ///
    /// \param g the geometry to compute the nearest point to
    /// \return the nearest points
    std::vector<geom::Coordinate> nearestPoints(const geom::Geometry* g) const;

private:
    std::unique_ptr<geos::index::strtree::STRtree> cachedTree;

};
}
}
}

#endif //GEOS_INDEXEDFACETDISTANCE_H
