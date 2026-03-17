/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2010      Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: original work
 *
 * Developed by Sandro Santilli (strk@kbt.io)
 * for Faunalia (http://www.faunalia.it)
 * with funding from Regione Toscana - Settore SISTEMA INFORMATIVO
 * TERRITORIALE ED AMBIENTALE - for the project: "Sviluppo strumenti
 * software per il trattamento di dati geografici basati su QuantumGIS
 * e Postgis (CIG 0494241492)"
 *
 **********************************************************************/

#ifndef GEOS_OPERATION_SHAREDPATHSOP_H
#define GEOS_OPERATION_SHAREDPATHSOP_H

#include <geos/export.h> // for GEOS_DLL

#include <vector>

// Forward declarations
namespace geos {
namespace geom {
class LineString;
class Geometry;
class GeometryFactory;
}
}


namespace geos {
namespace operation { // geos.operation

/// Find shared paths among two linear Geometry objects.
namespace sharedpaths { // geos.operation.sharedpaths

/** \brief
 * Find shared paths among two linear Geometry objects
 *
 * For each shared path report if it direction is the same
 * or opposite.
 *
 * Paths reported as shared are given in the direction they
 * appear in the first geometry.
 *
 * \remark Developed by Sandro Santilli (strk@kbt.io)
 * for Faunalia (http://www.faunalia.it)
 * with funding from Regione Toscana - Settore SISTEMA INFORMATIVO
 * TERRITORIALE ED AMBIENTALE - for the project: "Sviluppo strumenti
 * software per il trattamento di dati geografici basati su QuantumGIS
 * e Postgis (CIG 0494241492)"
 *
 */
class GEOS_DLL SharedPathsOp {
public:

    /// LineString vector (list of edges)
    typedef std::vector<geom::LineString*> PathList;

    /// Find paths shared between two linear geometries
    ///
    /// @param g1
    ///   First geometry. Must be linear.
    ///
    /// @param g2
    ///   Second geometry. Must be linear.
    ///
    /// @param sameDirection
    ///   Shared edges having the same direction are pushed
    ///   onto this vector. They'll be of type LineString.
    ///   Ownership of the edges is tranferred.
    ///
    /// @param oppositeDirection
    ///   Shared edges having the opposite direction are pushed
    ///   onto this vector. They'll be of type geom::LineString.
    ///   Ownership of the edges is tranferred.
    ///
    static void sharedPathsOp(const geom::Geometry& g1,
                              const geom::Geometry& g2,
                              PathList& sameDirection,
                              PathList& oppositeDirection);

    /// Constructor
    ///
    /// @param g1
    ///   First geometry. Must be linear.
    ///
    /// @param g2
    ///   Second geometry. Must be linear.
    ///
    SharedPathsOp(const geom::Geometry& g1, const geom::Geometry& g2);

    /// Get shared paths
    ///
    /// @param sameDirection
    ///   Shared edges having the same direction are pushed
    ///   onto this vector. They'll be of type geom::LineString.
    ///   Ownership of the edges is tranferred.
    ///
    /// @param oppositeDirection
    ///   Shared edges having the opposite direction are pushed
    ///   onto this vector. They'll be of type geom::LineString.
    ///   Ownership of the edges is tranferred.
    ///
    void getSharedPaths(PathList& sameDirection, PathList& oppositeDirection);

    /// Delete all edges in the list
    static void clearEdges(PathList& from);

private:

    /// Get all the linear intersections
    ///
    /// Ownership of linestring pushed to the given container
    /// is transferred to caller. See clearEdges for a deep
    /// release if you need one.
    ///
    void findLinearIntersections(PathList& to);

    /// Check if the given edge goes forward or backward on the given line.
    ///
    /// PRECONDITION: It is assumed the edge fully lays on the geometry
    ///
    bool isForward(const geom::LineString& edge,
                   const geom::Geometry& geom);

    /// Check if the given edge goes in the same direction over
    /// the two geometries.
    bool
    isSameDirection(const geom::LineString& edge)
    {
        return (isForward(edge, _g1) == isForward(edge, _g2));
    }

    /// Throw an IllegalArgumentException if the geom is not linear
    void checkLinealInput(const geom::Geometry& g);

    const geom::Geometry& _g1;
    const geom::Geometry& _g2;
    const geom::GeometryFactory& _gf;

    // Declare type as noncopyable
    SharedPathsOp(const SharedPathsOp& other) = delete;
    SharedPathsOp& operator=(const SharedPathsOp& rhs) = delete;

};

} // namespace geos.operation.sharedpaths
} // namespace geos.operation
} // namespace geos

#endif

