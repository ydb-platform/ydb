/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Excensus LLC.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: triangulate/quadedge/LocateFailureException.java r524
 *
 **********************************************************************/

#ifndef GEOS_TRIANGULATE_QUADEDGE_LOCATEFAILUREEXCEPTION_H
#define GEOS_TRIANGULATE_QUADEDGE_LOCATEFAILUREEXCEPTION_H

#include <string>

#include <geos/util.h>

namespace geos {
namespace triangulate { //geos.triangulate
namespace quadedge { //geos.triangulate.quadedge

class GEOS_DLL LocateFailureException : public geos::util::GEOSException {
public:
    LocateFailureException(std::string const& msg);
};

} //namespace geos.triangulate.quadedge
} //namespace geos.triangulate
} //namespace goes

#endif //GEOS_TRIANGULATE_QUADEDGE_LOCATEFAILUREEXCEPTION_H

