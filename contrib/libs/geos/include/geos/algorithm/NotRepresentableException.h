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
 **********************************************************************/

#ifndef GEOS_ALGORITHM_NOTREPRESENTABLEEXCEPTION_H
#define GEOS_ALGORITHM_NOTREPRESENTABLEEXCEPTION_H

#include <geos/export.h>
#include <string>
#include <geos/util/GEOSException.h>

namespace geos {
namespace algorithm { // geos::algorithm

/**
 * \class NotRepresentableException
 * \brief
 * Indicates that a HCoordinate has been computed which is
 * not representable on the Cartesian plane.
 *
 * @version 1.4
 * @see HCoordinate
 */
class GEOS_DLL NotRepresentableException: public util::GEOSException {
public:
    NotRepresentableException();
    NotRepresentableException(std::string msg);
    ~NotRepresentableException() noexcept override {}
};

} // namespace geos::algorithm
} // namespace geos

#endif

