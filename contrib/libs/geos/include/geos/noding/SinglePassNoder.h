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
 **********************************************************************/

#ifndef GEOS_NODING_SINGLEPASSNODER_H
#define GEOS_NODING_SINGLEPASSNODER_H

#include <geos/export.h>

#include <vector>

#include <geos/inline.h>

#include <geos/noding/Noder.h>

// Forward declarations
namespace geos {
namespace noding {
class SegmentString;
class SegmentIntersector;
}
}

namespace geos {
namespace noding { // geos.noding


/** \brief
 * Base class for {@link Noder}s which make a single
 * pass to find intersections.
 *
 * This allows using a custom SegmentIntersector
 * (which for instance may simply identify intersections, rather than
 * insert them).
 *
 * Last port: noding/SinglePassNoder.java rev. 1.3 (JTS-1.7)
 *
 * TODO: Noder inheritance (that's just an interface!)
 *
 */
class GEOS_DLL SinglePassNoder : public Noder { // implements Noder

protected:

    /// Externally owned
    SegmentIntersector* segInt;

public:

    SinglePassNoder(SegmentIntersector* nSegInt = nullptr): segInt(nSegInt) {}

    ~SinglePassNoder() override {}

    /** \brief
     * Sets the SegmentIntersector to use with this noder.
     *
     * A SegmentIntersector will normally add intersection nodes
     * to the input segment strings, but it may not - it may
     * simply record the presence of intersections.
     * However, some Noders may require that intersections be added.
     *
     * @param newSegInt
     */
    virtual void
    setSegmentIntersector(SegmentIntersector* newSegInt)
    {
        segInt = newSegInt;
    }

    /** \brief
     * Computes the noding for a collection of {@link SegmentString}s.
     *
     * @param segStrings a collection of {@link SegmentString}s to node
     */
    void computeNodes(std::vector<SegmentString*>* segStrings) override = 0;

    /** \brief
     * Returns a Collection of fully noded {@link SegmentString}s.
     *
     * The SegmentStrings have the same context as their parent.
     *
     * @return a Collection of SegmentStrings
     */
    std::vector<SegmentString*>* getNodedSubstrings() const override = 0;

};

} // namespace geos.noding
} // namespace geos

#endif // GEOS_NODING_SINGLEPASSNODER_H
