/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006      Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/SimpleNoder.java rev. 1.7 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SIMPLENODER_H
#define GEOS_NODING_SIMPLENODER_H

#include <geos/export.h>

#include <vector>

#include <geos/inline.h>

#include <geos/noding/SinglePassNoder.h>
#include <geos/noding/NodedSegmentString.h> // for inlined (FIXME)

// Forward declarations
namespace geos {
namespace noding {
//class SegmentString;
}
}

namespace geos {
namespace noding { // geos.noding


/** \brief
 * Nodes a set of {@link SegmentString}s by
 * performing a brute-force comparison of every segment to every other one.
 *
 * This has n^2 performance, so is too slow for use on large numbers
 * of segments.
 *
 * @version 1.7
 */
class GEOS_DLL SimpleNoder: public SinglePassNoder {
private:
    std::vector<SegmentString*>* nodedSegStrings;
    virtual void computeIntersects(SegmentString* e0, SegmentString* e1);

public:
    SimpleNoder(SegmentIntersector* nSegInt = nullptr)
        :
        SinglePassNoder(nSegInt)
    {}

    void computeNodes(std::vector<SegmentString*>* inputSegmentStrings) override;

    std::vector<SegmentString*>*
    getNodedSubstrings() const override
    {
        return NodedSegmentString::getNodedSubstrings(*nodedSegStrings);
    }
};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_SIMPLENODER_H
