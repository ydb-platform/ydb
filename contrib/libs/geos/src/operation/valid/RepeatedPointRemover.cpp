/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston <dbaston@gmail.com
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/operation/valid/RepeatedPointRemover.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/util.h>

namespace geos {
namespace operation {
namespace valid {

std::unique_ptr<geom::CoordinateArraySequence>
RepeatedPointRemover::removeRepeatedPoints(const geom::CoordinateSequence* seq) {
    using geom::Coordinate;

    if (seq->isEmpty()) {
        return detail::make_unique<geom::CoordinateArraySequence>(0, seq->getDimension());
    }

    auto pts = detail::make_unique<std::vector<Coordinate>>();
    auto sz = seq->getSize();
    pts->reserve(sz); // assume not many points are repeated

    const Coordinate* prevPt = &(seq->getAt(0));
    pts->push_back(*prevPt) ;

    for (size_t i = 1; i < sz; i++) {
        const Coordinate* nextPt = &(seq->getAt(i));
        if (*nextPt != *prevPt) {
            pts->push_back(*nextPt);
        }
        prevPt = nextPt;
    }

    return detail::make_unique<geom::CoordinateArraySequence>(pts.release(), seq->getDimension());
}

}
}
}
