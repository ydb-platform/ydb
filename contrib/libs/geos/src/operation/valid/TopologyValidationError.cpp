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
 * Last port: operation/valid/TopologyValidationError.java rev. 1.16 (JTS-1.10)
 *
 **********************************************************************/

#include <geos/operation/valid/TopologyValidationError.h>
#include <geos/geom/Coordinate.h>

#include <string>

using namespace std;
using namespace geos::geom;

namespace geos {
namespace operation { // geos.operation
namespace valid { // geos.operation.valid

const char* TopologyValidationError::errMsg[] = {
    "Topology Validation Error",
    "Repeated Point",
    "Hole lies outside shell",
    "Holes are nested",
    "Interior is disconnected",
    "Self-intersection",
    "Ring Self-intersection",
    "Nested shells",
    "Duplicate Rings",
    "Too few points in geometry component",
    "Invalid Coordinate",
    "Ring is not closed"
};

TopologyValidationError::TopologyValidationError(int newErrorType,
        const Coordinate& newPt)
    :
    errorType(newErrorType),
    pt(newPt)
{
}

TopologyValidationError::TopologyValidationError(int newErrorType)
    :
    errorType(newErrorType),
    pt(Coordinate::getNull())
{
}

int
TopologyValidationError::getErrorType()
{
    return errorType;
}

Coordinate&
TopologyValidationError::getCoordinate()
{
    return pt;
}

string
TopologyValidationError::getMessage()
{
    return string(errMsg[errorType]);
}

string
TopologyValidationError::toString()
{
    return getMessage().append(" at or near point ").append(pt.toString());
}

} // namespace geos.operation.valid
} // namespace geos.operation
} // namespace geos

