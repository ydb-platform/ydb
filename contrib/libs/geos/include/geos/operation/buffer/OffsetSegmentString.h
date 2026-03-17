/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2007 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/buffer/OffsetSegmentString.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_BUFFER_OFFSETSEGMENTSTRING_H
#define GEOS_OP_BUFFER_OFFSETSEGMENTSTRING_H

#include <geos/geom/Coordinate.h> // for inlines
#include <geos/geom/CoordinateSequence.h> // for inlines
#include <geos/geom/CoordinateArraySequence.h> // for composition
#include <geos/geom/PrecisionModel.h> // for inlines

#include <vector>
#include <memory>
#include <cassert>

namespace geos {
namespace operation { // geos.operation
namespace buffer { // geos.operation.buffer

/// A dynamic list of the vertices in a constructed offset curve.
///
/// Automatically removes close vertices
/// which are closer than a given tolerance.
///
/// @author Martin Davis
///
class OffsetSegmentString {

private:

    geom::CoordinateArraySequence* ptList;

    const geom::PrecisionModel* precisionModel;

    /** \brief
     * The distance below which two adjacent points on the curve
     * are considered to be coincident.
     *
     * This is chosen to be a small fraction of the offset distance.
     */
    double minimumVertexDistance;

    /** \brief
     * Tests whether the given point is redundant relative to the previous
     * point in the list (up to tolerance)
     *
     * @param pt
     * @return true if the point is redundant
     */
    bool
    isRedundant(const geom::Coordinate& pt) const
    {
        if(ptList->size() < 1) {
            return false;
        }
        const geom::Coordinate& lastPt = ptList->back();
        double ptDist = pt.distance(lastPt);
        if(ptDist < minimumVertexDistance) {
            return true;
        }
        return false;
    }

    OffsetSegmentString(const OffsetSegmentString&) = delete;
    OffsetSegmentString& operator=(const OffsetSegmentString&) = delete;

public:

    friend std::ostream& operator<< (std::ostream& os, const OffsetSegmentString& node);

    OffsetSegmentString()
        :
        ptList(new geom::CoordinateArraySequence()),
        precisionModel(nullptr),
        minimumVertexDistance(0.0)
    {
    }

    ~OffsetSegmentString()
    {
        delete ptList;
    }

    void
    reset()
    {
        if(ptList) {
            ptList->clear();
        }
        else {
            ptList = new geom::CoordinateArraySequence();
        }

        precisionModel = nullptr;
        minimumVertexDistance = 0.0;
    }

    void
    setPrecisionModel(const geom::PrecisionModel* nPrecisionModel)
    {
        precisionModel = nPrecisionModel;
    }

    void
    setMinimumVertexDistance(double nMinVertexDistance)
    {
        minimumVertexDistance = nMinVertexDistance;
    }

    void
    addPt(const geom::Coordinate& pt)
    {
        assert(precisionModel);

        geom::Coordinate bufPt = pt;
        precisionModel->makePrecise(bufPt);
        // don't add duplicate (or near-duplicate) points
        if(isRedundant(bufPt)) {
            return;
        }
        // we ask to allow repeated as we checked this ourself
        // (JTS uses a vector for ptList, not a CoordinateSequence,
        // we should do the same)
        ptList->add(bufPt, true);
    }

    void
    addPts(const geom::CoordinateSequence& pts, bool isForward)
    {
        if(isForward) {
            for(size_t i = 0, n = pts.size(); i < n; ++i) {
                addPt(pts[i]);
            }
        }
        else {
            for(size_t i = pts.size(); i > 0; --i) {
                addPt(pts[i - 1]);
            }
        }
    }

    /// Check that points are a ring
    ///
    /// add the startpoint again if they are not
    void
    closeRing()
    {
        if(ptList->size() < 1) {
            return;
        }
        const geom::Coordinate& startPt = ptList->front();
        const geom::Coordinate& lastPt = ptList->back();
        if(startPt.equals(lastPt)) {
            return;
        }
        // we ask to allow repeated as we checked this ourself
        ptList->add(startPt, true);
    }

    /// Get coordinates by taking ownership of them
    ///
    /// After this call, the coordinates reference in
    /// this object are dropped. Calling twice will
    /// segfault...
    ///
    /// FIXME: refactor memory management of this
    ///
    geom::CoordinateSequence*
    getCoordinates()
    {
        closeRing();
        geom::CoordinateSequence* ret = ptList;
        ptList = nullptr;
        return ret;
    }

    inline size_t
    size() const
    {
        return ptList ? ptList->size() : 0 ;
    }

};

inline std::ostream&
operator<< (std::ostream& os,
            const OffsetSegmentString& lst)
{
    if(lst.ptList) {
        os << *(lst.ptList);
    }
    else {
        os << "empty (consumed?)";
    }
    return os;
}

} // namespace geos.operation.buffer
} // namespace geos.operation
} // namespace geos


#endif // ndef GEOS_OP_BUFFER_OFFSETSEGMENTSTRING_H

