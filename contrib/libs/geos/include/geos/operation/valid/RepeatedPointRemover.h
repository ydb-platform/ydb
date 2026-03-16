/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_OP_VALID_REPEATEDPOINTREMOVER_H
#define GEOS_OP_VALID_REPEATEDPOINTREMOVER_H

#include <geos/geom/CoordinateArraySequence.h>

namespace geos {
namespace operation {
namespace valid {

    /// Removes repeated, consecutive equal, coordinates from a CoordinateSequence.
    class GEOS_DLL RepeatedPointRemover {

    /// \brief
    /// Returns a new CoordinateSequence being a copy of the input
    /// with any consecutive equal Coordinate removed.
    ///
    /// Equality test is 2D based
    ///
    /// Ownership of returned object goes to the caller.
    /// \param seq
    /// \return
    public:
        static std::unique_ptr<geom::CoordinateArraySequence> removeRepeatedPoints(const geom::CoordinateSequence* seq);
    };
}
}
}

#endif
