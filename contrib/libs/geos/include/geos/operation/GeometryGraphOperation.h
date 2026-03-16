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
 * Last port: operation/GeometryGraphOperation.java rev. 1.18 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OPERATION_GEOMETRYGRAPHOPERATION_H
#define GEOS_OPERATION_GEOMETRYGRAPHOPERATION_H

#include <geos/export.h>
#include <geos/algorithm/LineIntersector.h> // for composition

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace algorithm {
class BoundaryNodeRule;
}
namespace geom {
class Geometry;
class PrecisionModel;
}
namespace geomgraph {
class GeometryGraph;
}
}


namespace geos {
namespace operation { // geos.operation

/// The base class for operations that require GeometryGraph
class GEOS_DLL GeometryGraphOperation {

public:

    GeometryGraphOperation(const geom::Geometry* g0,
                           const geom::Geometry* g1);

    GeometryGraphOperation(const geom::Geometry* g0,
                           const geom::Geometry* g1,
                           const algorithm::BoundaryNodeRule& boundaryNodeRule);

    GeometryGraphOperation(const geom::Geometry* g0);

    virtual ~GeometryGraphOperation();

    const geom::Geometry* getArgGeometry(unsigned int i) const;

protected:

    algorithm::LineIntersector li;

    const geom::PrecisionModel* resultPrecisionModel;

    /** \brief
     * The operation args into an array so they can be accessed by index
     */
    std::vector<geomgraph::GeometryGraph*> arg;

    void setComputationPrecision(const geom::PrecisionModel* pm);
};

} // namespace geos.operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif
