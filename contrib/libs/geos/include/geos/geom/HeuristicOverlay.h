/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2013-2020 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: ORIGINAL WORK
 *
 **********************************************************************/

#ifndef GEOS_GEOM_HEURISTICOVERLAY_H
#define GEOS_GEOM_HEURISTICOVERLAY_H

#include <geos/export.h>
#include <memory> // for unique_ptr

namespace geos {
namespace geom { // geos::geom

class Geometry;

std::unique_ptr<Geometry> GEOS_DLL
HeuristicOverlay(const Geometry* g0, const Geometry* g1, int opCode);

} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_HEURISTICOVERLAY_H
