/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: noding/FastSegmentSetIntersectionFinder.java r388 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/noding/FastSegmentSetIntersectionFinder.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/SegmentIntersectionDetector.h>
#include <geos/noding/SegmentSetMutualIntersector.h>
#include <geos/noding/MCIndexSegmentSetMutualIntersector.h>
#include <geos/algorithm/LineIntersector.h>

namespace geos {
namespace noding { // geos::noding

/*
 * private:
 */

/*
 * protected:
 */

/*
 * public:
 */
FastSegmentSetIntersectionFinder::
FastSegmentSetIntersectionFinder(noding::SegmentString::ConstVect* baseSegStrings)
    :	segSetMutInt(new MCIndexSegmentSetMutualIntersector()),
      lineIntersector(new algorithm::LineIntersector())
{
    segSetMutInt->setBaseSegments(baseSegStrings);
}

bool
FastSegmentSetIntersectionFinder::
intersects(noding::SegmentString::ConstVect* segStrings)
{
    SegmentIntersectionDetector intFinder(lineIntersector.get());

    return this->intersects(segStrings, &intFinder);
}

bool
FastSegmentSetIntersectionFinder::
intersects(noding::SegmentString::ConstVect* segStrings,
           SegmentIntersectionDetector* intDetector)
{
    segSetMutInt->setSegmentIntersector(intDetector);
    segSetMutInt->process(segStrings);

    return intDetector->hasIntersection();
}

} // geos::noding
} // geos

