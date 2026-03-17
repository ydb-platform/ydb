/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/
#ifndef GEOS_INDEX_STRTREE_ENVELOPEUTIL_H
#define GEOS_INDEX_STRTREE_ENVELOPEUTIL_H

#include <geos/geom/Envelope.h>

namespace geos {
namespace index { // geos::index
namespace strtree { // geos::index::strtree

class GEOS_DLL EnvelopeUtil {
public:
    // EnvelopeUtil(const void* newBounds, void* newItem);
    // ~EnvelopeUtil() override = default;

    static double maximumDistance(const geom::Envelope* env1, const geom::Envelope* env2);

};

} // namespace geos::index::strtree
} // namespace geos::index
} // namespace geos

#endif // GEOS_INDEX_STRTREE_ENVELOPEUTIL_H
