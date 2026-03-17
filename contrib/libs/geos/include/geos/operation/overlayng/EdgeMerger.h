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

#include <geos/operation/overlayng/OverlayLabel.h>
#include <geos/operation/overlayng/EdgeKey.h>
#include <geos/operation/overlayng/Edge.h>
#include <geos/export.h>

#include <vector>
#include <map>


// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace operation {
namespace overlayng {
class Edge;
class EdgeKey;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

/**
 * Performs merging on the noded edges of the input geometries.
 * Merging takes place on edges which are coincident
 * (i.e. have the same coordinate list, modulo direction).
 * The following situations can occur:
 *
 * - Coincident edges from different input geometries have their labels combined
 * - Coincident edges from the same area geometry indicate a topology collapse.
 * In this case the topology locations are "summed" to provide a final
 * assignment of side location
 * - Coincident edges from the same linear geometry can simply be merged
 * using the same ON location
 *
 * One constraint that is maintained is that the direction of linear
 * edges should be preserved if possible (which is the case if there is
 * no other coincident edge, or if all coincident edges have the same direction).
 * This ensures that the overlay output line direction will be as consistent
 * as possible with input lines.
 *
 * The merger also preserves the order of the edges in the input.
 * This means that for polygon-line overlay
 * the result lines will be in the same order as in the input
 * (possibly with multiple result lines for a single input line).
 *
 * @author mdavis
 *
 */
class GEOS_DLL EdgeMerger {

public:

    static std::vector<Edge*> merge(std::vector<Edge*>& edges);

};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos
