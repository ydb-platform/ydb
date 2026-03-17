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

#ifndef GEOS_PRECISION_MINIMUMCLEARANCE_H
#define GEOS_PRECISION_MINIMUMCLEARANCE_H

#include <geos/geom/Geometry.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>

namespace geos {
namespace precision {

/// Computes the Minimum Clearance of a Geometry.
class GEOS_DLL MinimumClearance {
private:
    const geom::Geometry* inputGeom;
    double minClearance;
    std::unique_ptr<geom::CoordinateSequence> minClearancePts;

    void compute();
public:
    MinimumClearance(const geom::Geometry* g);

    /**
     * Gets the Minimum Clearance distance.
     *
     * @return the value of the minimum clearance distance
     * or <tt>DBL_MAX</tt> if no Minimum Clearance distance exists
     */
    double getDistance();

    /**
     * Gets a LineString containing two points
     * which are at the Minimum Clearance distance.
     *
     * @return the value of the minimum clearance distance
     * or <tt>LINESTRING EMPTY</tt> if no Minimum Clearance distance exists
     */
    std::unique_ptr<geom::LineString> getLine();
};
}
}

#endif


