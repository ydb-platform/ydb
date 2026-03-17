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
 **********************************************************************
 *
 * Last port: noding/snap/SnappingNoder.java
 *
 **********************************************************************/

#pragma once

#include <geos/export.h>

#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/noding/Noder.h>
#include <geos/noding/snap/SnappingPointIndex.h>


// Forward declarations
namespace geos {
namespace geom {
class Envelope;
class PrecisionModel;
}
namespace noding {
class NodedSegmentString;
}
}

namespace geos {
namespace noding { // geos::noding
namespace snap {   // geos::noding::snap

/**
 * Nodes a set of segment strings
 * snapping vertices and intersection points together if
 * they lie within the given snap tolerance distance.
 * Vertices take priority over intersection points for snapping.
 * Input segment strings are generally only split at true node points
 * (i.e. the output segment strings are of maximal length in the output arrangement).
 *
 * The snap tolerance should be chosen to be as small as possible
 * while still producing a correct result.
 * It probably only needs to be small enough to eliminate
 * "nearly-coincident" segments, for which intersection points cannot be computed accurately.
 * This implies a factor of about 10e-12
 * smaller than the magnitude of the segment coordinates.
 *
 * With an appropriate snap tolerance this algorithm appears to be very robust.
 * So far no failure cases have been found,
 * given a small enough snap tolerance.
 *
 * The correctness of the output is not verified by this noder.
 * If required this can be done by {@link noding::ValidatingNoder}.
 */
class GEOS_DLL SnappingNoder : public Noder {

private:

    // Members
    double snapTolerance;
    SnappingPointIndex snapIndex;
    std::vector<SegmentString*>* nodedResult;

    // Methods
    void snapVertices(std::vector<SegmentString*>& segStrings, std::vector<SegmentString*>& nodedStrings);

    SegmentString* snapVertices(SegmentString* ss);

    std::unique_ptr<std::vector<geom::Coordinate>> snap(geom::CoordinateSequence* cs);

    /**
    * Computes all interior intersections in the collection of {@link SegmentString}s,
    * and returns their {@link Coordinate}s.
    *
    * Also adds the intersection nodes to the segments.
    *
    * @return a list of Coordinates for the intersections
    */
    std::unique_ptr<std::vector<SegmentString*>> snapIntersections(std::vector<SegmentString*>& inputSS);


public:

    /**
     * Creates a snapping noder using the given snap distance tolerance.
     * @param p_snapTolerance points are snapped if within this distance
     */
    SnappingNoder(double p_snapTolerance)
        : snapTolerance(p_snapTolerance)
        , snapIndex(p_snapTolerance)
        {}

    /**
    * @return a Collection of NodedSegmentStrings representing the substrings
    */
    std::vector<SegmentString*>* getNodedSubstrings() const override;

    void computeNodes(std::vector<SegmentString*>* inputSegStrings) override;


};


} // namespace geos::noding::snap
} // namespace geos::noding
} // namespace geos



