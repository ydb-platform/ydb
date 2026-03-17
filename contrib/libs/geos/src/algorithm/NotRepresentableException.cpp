/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/algorithm/NotRepresentableException.h>

#include <string>

namespace geos {
namespace algorithm { // geos.algorithm

NotRepresentableException::NotRepresentableException()
    :
    GEOSException(
        "NotRepresentableException",
        "Projective point not representable on the Cartesian plane.")

{
}

NotRepresentableException::NotRepresentableException(std::string msg)
    :
    GEOSException(
        "NotRepresentableException", msg)
{
}

} // namespace geos.algorithm
} // namespace geos

