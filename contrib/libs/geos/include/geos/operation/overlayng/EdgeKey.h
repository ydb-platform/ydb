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
#include <geos/export.h>

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace operation {
namespace overlayng {
class Edge;
}
}
}

namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng


// using geos::geom::Location;
// using geos::geom::Position;

/**
 * A key for sorting and comparing edges in a noded arrangement.
 * Relies on the fact that in a correctly noded arrangement
 * edges are identical (up to direction)
 * iff they have their first segment in common.
 *
 * @author mdavis
 *
 */

class GEOS_DLL EdgeKey {

private:

    // Members
    double p0x;
    double p0y;
    double p1x;
    double p1y;

    // Methods
    void initPoints(const Edge* edge);
    void init(const geom::Coordinate& p0, const geom::Coordinate& p1);


public:

    EdgeKey(const Edge* edge);

    int compareTo(const EdgeKey* ek) const;
    bool equals(const EdgeKey* ek) const;

    friend bool operator< (const EdgeKey& ek1, const EdgeKey& ek2);
    friend bool operator== (const EdgeKey& ek1, const EdgeKey& ek2);


};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

