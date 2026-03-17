/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2020 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/overlayng/EdgeSourceInfo.h>

namespace geos {      // geos
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


EdgeSourceInfo::EdgeSourceInfo(int p_index, int p_depthDelta, bool p_isHole)
    : index(p_index)
    , dim(geom::Dimension::A)
    , edgeIsHole(p_isHole)
    , depthDelta(p_depthDelta)
    {}

EdgeSourceInfo::EdgeSourceInfo(int p_index)
    : index(p_index)
    , dim(geom::Dimension::L)
    , edgeIsHole(false)
    , depthDelta(0)
    {}


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
