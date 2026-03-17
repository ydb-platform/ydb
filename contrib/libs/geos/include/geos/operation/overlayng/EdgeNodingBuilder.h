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
 * Last port: operation/overlayng/EdgeNodingBuilder.java 6ef89b096
 *
 **********************************************************************/

#pragma once

#include <geos/algorithm/LineIntersector.h>
#include <geos/algorithm/Orientation.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineString.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/Polygon.h>
#include <geos/noding/IntersectionAdder.h>
#include <geos/noding/MCIndexNoder.h>
#include <geos/noding/Noder.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/noding/SegmentString.h>
#include <geos/noding/ValidatingNoder.h>
#include <geos/noding/snapround/SnapRoundingNoder.h>
#include <geos/operation/overlayng/Edge.h>
#include <geos/operation/overlayng/EdgeSourceInfo.h>
#include <geos/operation/overlayng/InputGeometry.h>
#include <geos/operation/overlayng/LineLimiter.h>
#include <geos/operation/overlayng/OverlayUtil.h>
#include <geos/operation/overlayng/RingClipper.h>
#include <geos/operation/valid/RepeatedPointRemover.h>


#include <geos/export.h>
#include <array>
#include <memory>
#include <deque>

using namespace geos::geom;
using namespace geos::noding;
using geos::noding::snapround::SnapRoundingNoder;
using geos::algorithm::LineIntersector;


namespace geos {      // geos.
namespace operation { // geos.operation
namespace overlayng { // geos.operation.overlayng

/**
 * Builds a set of noded, unique, labelled Edges from
 * the edges of the two input geometries.
 *
 * It performs the following steps:
 *
 *  - Extracts input edges, and attaches topological information
 *  - if clipping is enabled, handles clipping or limiting input geometry
 *  - chooses a {@link noding::Noder} based on provided precision model, unless a custom one is supplied
 *  - calls the chosen Noder, with precision model
 *  - removes any fully collapsed noded edges
 *  - builds {@link Edge}s and merges them
 *
 * @author mdavis
 */
class GEOS_DLL EdgeNodingBuilder {

private:

    // Constants
    static constexpr int MIN_LIMIT_PTS = 20;
    static constexpr bool IS_NODING_VALIDATED = true;

    // Members
    const PrecisionModel* pm;
    std::unique_ptr<std::vector<SegmentString*>> inputEdges;
    Noder* customNoder;
    std::array<bool, 2> hasEdges;
    const Envelope* clipEnv;
    std::unique_ptr<RingClipper> clipper;
    std::unique_ptr<LineLimiter> limiter;

    // For use in createFloatingPrecisionNoder()
    LineIntersector lineInt;
    IntersectionAdder intAdder;
    std::unique_ptr<Noder> internalNoder;
    std::unique_ptr<Noder> spareInternalNoder;
    // EdgeSourceInfo*, Edge* owned by EdgeNodingBuilder, stored in deque
    std::deque<EdgeSourceInfo> edgeSourceInfoQue;
    std::deque<Edge> edgeQue;

    /**
    * Gets a noder appropriate for the precision model supplied.
    * This is one of:
    *
    * - Fixed precision: a snap-rounding noder (which should be fully robust)
    * - Floating precision: a conventional nodel (which may be non-robust).
    *   In this case, a validation step is applied to the output from the noder.
    */
    Noder* getNoder();
    std::unique_ptr<Noder> createFixedPrecisionNoder(const PrecisionModel* pm);
    std::unique_ptr<Noder> createFloatingPrecisionNoder(bool doValidation);


    void addCollection(const GeometryCollection* gc, int geomIndex);
    void addGeometryCollection(const GeometryCollection* gc, int geomIndex, int expectedDim);
    void addPolygon(const Polygon* poly, int geomIndex);
    void addPolygonRing(const LinearRing* ring, bool isHole, int geomIndex);
    void addLine(const LineString* line, int geomIndex);
    void addLine(std::unique_ptr<CoordinateArraySequence>& pts, int geomIndex);
    void addEdge(std::unique_ptr<std::vector<Coordinate>> pts, const EdgeSourceInfo* info);
    void addEdge(std::unique_ptr<CoordinateArraySequence>& cas, const EdgeSourceInfo* info);

    // Create a EdgeSourceInfo* owned by EdgeNodingBuilder
    const EdgeSourceInfo* createEdgeSourceInfo(int index, int depthDelta, bool isHole);
    const EdgeSourceInfo* createEdgeSourceInfo(int index);

    /**
    * Tests whether a geometry (represented by its envelope)
    * lies completely outside the clip extent(if any).
    */
    bool isClippedCompletely(const Envelope* env);

    /**
    * Tests whether it is worth limiting a line.
    * Lines that have few vertices or are covered
    * by the clip extent do not need to be limited.
    */
    bool isToBeLimited(const LineString* line) const;

    /**
    * If limiter is provided,
    * limit the line to the clip envelope.
    *
    */
    std::vector<std::unique_ptr<CoordinateArraySequence>>& limit(const LineString* line);

    /**
    * If a clipper is present,
    * clip the line to the clip extent.
    * Otherwise, remove duplicate points from the ring.
    *
    * If clipping is enabled, then every ring MUST
    * be clipped, to ensure that holes are clipped to
    * be inside the shell.
    * This means it is not possible to skip
    * clipping for rings with few vertices.
    *
    * @param ring the line to clip
    * @return the points in the clipped line
    */
    std::unique_ptr<CoordinateArraySequence> clip(const LinearRing* line);

    /**
    * Removes any repeated points from a linear component.
    * This is required so that noding can be computed correctly.
    *
    * @param line the line to process
    * @return the points of the line with repeated points removed
    */
    std::unique_ptr<CoordinateArraySequence> removeRepeatedPoints(const LineString* line);

    int computeDepthDelta(const LinearRing* ring, bool isHole);

    void add(const Geometry* g, int geomIndex);

    /**
    * Nodes a set of segment strings and creates {@link Edge}s from the result.
    * The input segment strings each carry a {@link EdgeSourceInfo} object,
    * which is used to provide source topology info to the constructed Edges
    * (and is then discarded).
    */
    std::vector<Edge*> node(std::vector<SegmentString*>* segStrings);
    std::vector<Edge*> createEdges(std::vector<SegmentString*>* segStrings);


public:

    /**
    * Creates a new builder, with an optional custom noder.
    * If the noder is not provided, a suitable one will
    * be used based on the supplied precision model.
    */
    EdgeNodingBuilder(const PrecisionModel* p_pm, Noder* p_customNoder)
        : pm(p_pm)
        , inputEdges(new std::vector<SegmentString*>)
        , customNoder(p_customNoder)
        , hasEdges({false,false})
        , clipEnv(nullptr)
        , intAdder(lineInt)
        {};

    ~EdgeNodingBuilder()
    {
        for (SegmentString* ss: *inputEdges) {
            delete ss;
        }
    }

    void setClipEnvelope(const Envelope* clipEnv);

    // returns newly allocated vector and segmentstrings
    // std::vector<SegmentString*>* node();

    /**
    * Reports whether there are noded edges
    * for the given input geometry.
    * If there are none, this indicates that either
    * the geometry was empty, or has completely collapsed
    * (because it is smaller than the noding precision).
    *
    * @param geomIndex index of input geometry
    * @return true if there are edges for the geometry
    */
    bool hasEdgesFor(int geomIndex) const;

    /**
    * Creates a set of labelled {Edge}s.
    * representing the fully noded edges of the input geometries.
    * Coincident edges (from the same or both geometries)
    * are merged along with their labels
    * into a single unique, fully labelled edge.
    *
    * @param geom0 the first geometry
    * @param geom1 the second geometry
    * @return the noded, merged, labelled edges
    */
    std::vector<Edge*> build(const Geometry* geom0, const Geometry* geom1);



};


} // namespace geos.operation.overlayng
} // namespace geos.operation
} // namespace geos

