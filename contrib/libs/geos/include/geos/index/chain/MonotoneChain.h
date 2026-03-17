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
 * Last port: index/chain/MonotoneChain.java rev. 1.15 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_CHAIN_MONOTONECHAIN_H
#define GEOS_IDX_CHAIN_MONOTONECHAIN_H

#include <geos/export.h>
#include <geos/geom/Envelope.h> // for inline

#include <memory> // for unique_ptr

// Forward declarations
namespace geos {
namespace geom {
class LineSegment;
class CoordinateSequence;
}
namespace index {
namespace chain {
class MonotoneChainSelectAction;
class MonotoneChainOverlapAction;
}
}
}

namespace geos {
namespace index { // geos::index
namespace chain { // geos::index::chain

/** \brief
 * Monotone Chains are a way of partitioning the segments of a linestring to
 * allow for fast searching of intersections.
 *
 * They have the following properties:
 *
 * - the segments within a monotone chain never intersect each other
 * - the envelope of any contiguous subset of the segments in a monotone
 *   chain is equal to the envelope of the endpoints of the subset.
 *
 * Property 1 means that there is no need to test pairs of segments from
 * within the same monotone chain for intersection.
 * Property 2 allows an efficient binary search to be used to find the
 * intersection points of two monotone chains.
 *
 * For many types of real-world data, these properties eliminate
 * a large number of segment comparisons, producing substantial speed gains.
 *
 * One of the goals of this implementation of MonotoneChains is to be
 * as space and time efficient as possible. One design choice that aids this
 * is that a MonotoneChain is based on a subarray of a list of points.
 * This means that new arrays of points (potentially very large) do not
 * have to be allocated.
 *
 * MonotoneChains support the following kinds of queries:
 *
 * - Envelope select: determine all the segments in the chain which
 *   intersect a given envelope
 * - Overlap: determine all the pairs of segments in two chains whose
 *   envelopes overlap
 *
 * This implementation of MonotoneChains uses the concept of internal iterators
 * to return the resultsets for the above queries.
 * This has time and space advantages, since it
 * is not necessary to build lists of instantiated objects to represent the segments
 * returned by the query.
 * However, it does mean that the queries are not thread-safe.
 *
 */
class GEOS_DLL MonotoneChain {
public:

    /// @param pts
    ///   Ownership left to caller, this class holds a reference.
    ///
    /// @param start
    ///
    /// @param end
    ///
    /// @param context
    ///   Ownership left to caller, this class holds a reference.
    ///
    MonotoneChain(const geom::CoordinateSequence& pts,
                  std::size_t start, std::size_t end, void* context);

    ~MonotoneChain() = default;

    /// Returned envelope is owned by this class
    const geom::Envelope& getEnvelope();
    const geom::Envelope& getEnvelope(double expansionDistance);

    size_t
    getStartIndex() const
    {
        return start;
    }

    size_t
    getEndIndex() const
    {
        return end;
    }

    /** \brief
     *  Set given LineSegment with points of the segment starting
     *  at the given index.
     */
    void getLineSegment(std::size_t index, geom::LineSegment& ls) const;

    /**
     * Return the subsequence of coordinates forming this chain.
     * Allocates a new CoordinateSequence to hold the Coordinates
     *
     */
    std::unique_ptr<geom::CoordinateSequence> getCoordinates() const;

    /**
     * Determine all the line segments in the chain whose envelopes overlap
     * the searchEnvelope, and process them
     */
    void select(const geom::Envelope& searchEnv,
                MonotoneChainSelectAction& mcs);

    void computeOverlaps(MonotoneChain* mc,
                         MonotoneChainOverlapAction* mco);

    void computeOverlaps(MonotoneChain* mc, double overlapTolerance,
                         MonotoneChainOverlapAction* mco);

    void
    setId(int nId)
    {
        id = nId;
    }

    inline int
    getId() const
    {
        return id;
    }

    void*
    getContext()
    {
        return context;
    }

private:

    void computeSelect(const geom::Envelope& searchEnv,
                       size_t start0,
                       size_t end0,
                       MonotoneChainSelectAction& mcs);

    void computeOverlaps(std::size_t start0, std::size_t end0, MonotoneChain& mc,
                         std::size_t start1, std::size_t end1,
                         double overlapTolerance,
                         MonotoneChainOverlapAction& mco);

    bool overlaps(size_t start0, size_t end0,
                  const MonotoneChain& mc, size_t start1, size_t end1,
                  double overlapTolerance) const;

    bool overlaps(const geom::Coordinate& p1, const geom::Coordinate& p2,
                  const geom::Coordinate& q1, const geom::Coordinate& q2,
                  double overlapTolerance) const;

    /// Externally owned
    const geom::CoordinateSequence& pts;

    /// user-defined information
    void* context;

    /// Index of chain start vertex into the CoordinateSequence, 0 based.
    size_t start;

    /// Index of chain end vertex into the CoordinateSequence, 0 based.
    size_t end;

    /// Owned by this class
    geom::Envelope env;
    bool envIsSet;

    /// useful for optimizing chain comparisons
    int id;


    // Declare type as noncopyable
    MonotoneChain(const MonotoneChain& other) = delete;
    MonotoneChain& operator=(const MonotoneChain& rhs) = delete;
};

} // namespace geos::index::chain
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_CHAIN_MONOTONECHAIN_H

