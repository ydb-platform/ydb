/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 *
 **********************************************************************
 *
 * Last port: noding/SegmentStringUtil.java rev. 1.2 (JTS-1.9)
 *
 **********************************************************************/

#ifndef GEOS_NODING_SEGMENTSTRINGUTIL_H
#define GEOS_NODING_SEGMENTSTRINGUTIL_H

#include <geos/noding/NodedSegmentString.h>
#include <geos/geom/LineString.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/util/LinearComponentExtracter.h>

namespace geos {
namespace noding { // geos::noding

/** \brief
 * Utility methods for processing {@link SegmentString}s.
 *
 * @author Martin Davis
 *
 */
class SegmentStringUtil {
public:
    /** \brief
     * Extracts all linear components from a given [Geometry](@ref geom::Geometry)
     * to [SegmentStrings](@ref SegmentString)
     *
     * The SegmentString data item is set to be the source Geometry.
     *
     * @param g the geometry to extract from
     * @param segStr a List of SegmentStrings (output parameter).
     *               Ownership of elements pushed to the vector
     *               is transferred to caller.
     */
    static void
    extractSegmentStrings(const geom::Geometry* g,
                          SegmentString::ConstVect& segStr)
    {
        geom::LineString::ConstVect lines;
        geom::util::LinearComponentExtracter::getLines(*g, lines);

        for(std::size_t i = 0, n = lines.size(); i < n; i++) {
            geom::LineString* line = (geom::LineString*)(lines[i]);

            // we take ownership of the coordinates here
            // TODO: check if this can be optimized by getting
            //       the internal CS.
            auto pts = line->getCoordinates();

            segStr.push_back(new NodedSegmentString(pts.release(), g));
        }
    }

};

} // geos::noding
} // geos

#endif // GEOS_NODING_SEGMENTSTRINGUTIL_H
