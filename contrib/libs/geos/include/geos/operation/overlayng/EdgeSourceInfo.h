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

#pragma once

#include <geos/geom/Dimension.h>
#include <geos/export.h>


namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng
/**
 * Records topological information about an
 * edge representing a piece of linework (lineString or polygon ring)
 * from a single source geometry.
 * This information is carried through the noding process
 * (which may result in many noded edges sharing the same information object).
 * It is then used to populate the topology info fields
 * in {@link Edge}s (possibly via merging).
 * That information is used to construct the topology graph {@link OverlayLabel}s.
 *
 * @author mdavis
 *
 */

class GEOS_DLL EdgeSourceInfo {

private:

    // Members
    int index;
    int dim;
    bool edgeIsHole;
    int depthDelta;


public:

    EdgeSourceInfo(int p_index, int p_depthDelta, bool p_isHole);
    EdgeSourceInfo(int p_index);

    int getIndex() const { return index; }
    int getDimension() const { return dim; }
    int getDepthDelta() const { return depthDelta; }
    bool isHole() const { return edgeIsHole; }

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

