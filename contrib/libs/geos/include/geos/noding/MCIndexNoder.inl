/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/MCIndexNoder.java rev. 1.6 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODINGMCINDEXNODER_INL
#define GEOS_NODINGMCINDEXNODER_INL

#include <geos/noding/MCIndexNoder.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/NodedSegmentString.h>

#include <cassert>

namespace geos {
namespace noding { // geos::noding

INLINE index::SpatialIndex&
MCIndexNoder::getIndex()
{
    return index;
}

INLINE std::vector<SegmentString*>*
MCIndexNoder::getNodedSubstrings() const
{
    assert(nodedSegStrings); // must have colled computeNodes before!
    return NodedSegmentString::getNodedSubstrings(*nodedSegStrings);
}

} // namespace geos::noding
} // namespace geos

#endif // GEOS_NODINGMCINDEXNODER_INL

