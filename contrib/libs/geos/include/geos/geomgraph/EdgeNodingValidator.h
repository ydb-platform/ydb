/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geomgraph/EdgeNodingValidator.java rev. 1.6 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_EDGENODINGVALIDATOR_H
#define GEOS_GEOMGRAPH_EDGENODINGVALIDATOR_H

#include <geos/export.h>
#include <vector>

#include <geos/noding/FastNodingValidator.h> // for composition

#include <geos/inline.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class CoordinateSequence;
}
namespace noding {
class SegmentString;
}
namespace geomgraph {
class Edge;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

/** \brief
 * Validates that a collection of SegmentStrings is correctly noded.
 *
 * Throws an appropriate exception if an noding error is found.
 */
class GEOS_DLL EdgeNodingValidator {

private:
    std::vector<noding::SegmentString*>& toSegmentStrings(std::vector<Edge*>& edges);

    // Make sure this member is initialized *before*
    // the NodingValidator, as initialization of
    // NodingValidator will use toSegmentString(), that
    // in turn expects this member to be initialized
    std::vector<noding::SegmentString*> segStr;

    // Make sure this member is initialized *before*
    // the NodingValidator, as initialization of
    // NodingValidator will use toSegmentString(), that
    // in turn expects this member to be initialized
    std::vector<geom::CoordinateSequence*> newCoordSeq;

    noding::FastNodingValidator nv;

public:

    /** \brief
     * Checks whether the supplied {@link Edge}s are correctly noded.
     *
     * Throws a [TopologyException](@ref util::TopologyException) if they are not.
     *
     * @param edges a collection of Edges.
     * @throws TopologyException if the SegmentStrings are not
     *         correctly noded
     */
    static void
    checkValid(std::vector<Edge*>& edges)
    {
        EdgeNodingValidator validator(edges);
        validator.checkValid();
    }

    EdgeNodingValidator(std::vector<Edge*>& edges)
        :
        segStr(),
        newCoordSeq(),
        nv(toSegmentStrings(edges))
    {}

    ~EdgeNodingValidator();

    void
    checkValid()
    {
        nv.checkValid();
    }
};


} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

//#ifdef GEOS_INLINE
//# include "geos/geomgraph/EdgeNodingValidator.inl"
//#endif

#endif // ifndef GEOS_GEOMGRAPH_EDGENODINGVALIDATOR_H
