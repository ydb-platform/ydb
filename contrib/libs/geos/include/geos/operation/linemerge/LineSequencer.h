/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: operation/linemerge/LineSequencer.java r378 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_OP_LINEMERGE_LINESEQUENCER_H
#define GEOS_OP_LINEMERGE_LINESEQUENCER_H

#include <geos/export.h>

#include <geos/operation/linemerge/LineMergeGraph.h> // for composition
#include <geos/geom/Geometry.h> // for inlines
#include <geos/geom/LineString.h> // for inlines

#include <vector>
#include <list>
#include <memory> // for unique_ptr

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class GeometryFactory;
class Geometry;
class LineString;
}
namespace planargraph {
class DirectedEdge;
class Subgraph;
class Node;
}
}


namespace geos {
namespace operation { // geos::operation
namespace linemerge { // geos::operation::linemerge

/** \brief
 * Builds a sequence from a set of LineStrings so that
 * they are ordered end to end.
 *
 * A sequence is a complete non-repeating list of the linear
 * components of the input.  Each linestring is oriented
 * so that identical endpoints are adjacent in the list.
 *
 * A typical use case is to convert a set of unoriented geometric links
 * from a linear network (e.g. such as block faces on a bus route)
 * into a continuous oriented path through the network.
 *
 * The input linestrings may form one or more connected sets.
 * The input linestrings should be correctly noded, or the results may
 * not be what is expected.
 * The computed output is a single MultiLineString containing the ordered
 * linestrings in the sequence.
 *
 * The sequencing employs the classic **Eulerian path** graph algorithm.
 * Since Eulerian paths are not uniquely determined, further rules are used
 * to make the computed sequence preserve as much as possible of the input
 * ordering. Within a connected subset of lines, the ordering rules are:
 *
 * - If there is degree-1 node which is the start
 *   node of an linestring, use that node as the start of the sequence
 * - If there is a degree-1 node which is the end
 *   node of an linestring, use that node as the end of the sequence
 * - If the sequence has no degree-1 nodes, use any node as the start
 *
 * @note Not all arrangements of lines can be sequenced. For a connected
 * set of edges in a graph, *Euler's Theorem* states that there is a sequence
 * containing each edge once **if and only if** there are no more than
 * 2 nodes of odd degree. If it is not possible to find a sequence, the
 * `isSequenceable` method will return `false`.
 *
 */
class GEOS_DLL LineSequencer {

private:
    typedef std::list<planargraph::DirectedEdge*> DirEdgeList;
    typedef std::vector< DirEdgeList* > Sequences;

    LineMergeGraph graph;
    const geom::GeometryFactory* factory;
    unsigned int lineCount;
    bool isRun;
    std::unique_ptr<geom::Geometry> sequencedGeometry;
    bool isSequenceableVar;

    void addLine(const geom::LineString* lineString);
    void computeSequence();
    Sequences* findSequences();
    DirEdgeList* findSequence(planargraph::Subgraph& graph);

    void delAll(Sequences&);

    /// return a newly allocated LineString
    static geom::LineString* reverse(const geom::LineString* line);

    /**
     * Builds a geometry ({@link LineString} or {@link MultiLineString} )
     * representing the sequence.
     *
     * @param sequences
     *    a vector of vectors of const planarDirectedEdges
     *    with LineMergeEdges as their parent edges.
     *    Ownership of container _and_ contents retained by caller.
     *
     * @return the sequenced geometry, possibly NULL
     *         if no sequence exists
     */
    geom::Geometry* buildSequencedGeometry(const Sequences& sequences);

    static const planargraph::Node* findLowestDegreeNode(
        const planargraph::Subgraph& graph);

    void addReverseSubpath(const planargraph::DirectedEdge* de,
                           DirEdgeList& deList,
                           DirEdgeList::iterator lit,
                           bool expectedClosed);

    /**
     * Finds an {@link DirectedEdge} for an unvisited edge (if any),
     * choosing the dirEdge which preserves orientation, if possible.
     *
     * @param node the node to examine
     * @return the dirEdge found, or <code>null</code>
     *         if none were unvisited
     */
    static const planargraph::DirectedEdge* findUnvisitedBestOrientedDE(
        const planargraph::Node* node);

    /**
     * Computes a version of the sequence which is optimally
     * oriented relative to the underlying geometry.
     *
     * Heuristics used are:
     *
     * - If the path has a degree-1 node which is the start
     *   node of an linestring, use that node as the start of the sequence
     * - If the path has a degree-1 node which is the end
     *   node of an linestring, use that node as the end of the sequence
     * - If the sequence has no degree-1 nodes, use any node as the start
     *   (NOTE: in this case could orient the sequence according to the
     *   majority of the linestring orientations)
     *
     * @param seq a List of planarDirectedEdges
     * @return the oriented sequence, possibly same as input if already
     *         oriented
     */
    DirEdgeList* orient(DirEdgeList* seq);

    /**
     * Reverse the sequence.
     * This requires reversing the order of the dirEdges, and flipping
     * each dirEdge as well
     *
     * @param seq a List of DirectedEdges, in sequential order
     * @return the reversed sequence
     */
    DirEdgeList* reverse(DirEdgeList& seq);

    /**
     * Tests whether a complete unique path exists in a graph
     * using Euler's Theorem.
     *
     * @param graph the subgraph containing the edges
     * @return <code>true</code> if a sequence exists
     */
    bool hasSequence(planargraph::Subgraph& graph);

public:

    static geom::Geometry*
    sequence(const geom::Geometry& geom)
    {
        LineSequencer sequencer;
        sequencer.add(geom);
        return sequencer.getSequencedLineStrings();
    }

    LineSequencer()
        :
        factory(nullptr),
        lineCount(0),
        isRun(false),
        sequencedGeometry(nullptr),
        isSequenceableVar(false)
    {}

    /** \brief
     * Tests whether a [Geometry](@ref geom::Geometry) is sequenced correctly.
     *
     * [LineStrings](@ref geom::LineString) are trivially sequenced.
     * [MultiLineStrings](@ref geom::MultiLineString) are checked for
     * correct sequencing. Otherwise, `isSequenced` is defined
     * to be `true` for geometries that are not lineal.
     *
     * @param geom the geometry to test
     * @return `true` if the geometry is sequenced or is not lineal
     */
    static bool isSequenced(const geom::Geometry* geom);

    /** \brief
     * Tests whether the arrangement of linestrings has a valid
     * sequence.
     *
     * @return `true` if a valid sequence exists.
     */
    bool
    isSequenceable()
    {
        computeSequence();
        return isSequenceableVar;
    }

    /** \brief
     * Adds a [Geometry](@ref geom::Geometry) to be sequenced.
     *
     * May be called multiple times.
     * Any dimension of Geometry may be added; the constituent
     * linework will be extracted.
     *
     * @param geometry the geometry to add
     */
    void
    add(const geom::Geometry& geometry)
    {
        geometry.applyComponentFilter(*this);
    }

    template <class TargetContainer>
    void
    add(TargetContainer& geoms)
    {
        for(typename TargetContainer::const_iterator i = geoms.begin(),
                e = geoms.end(); i != e; ++i) {
            const geom::Geometry* g = *i;
            add(*g);
        }
    }

    /** \brief
     * Act as a GeometryComponentFilter so to extract
     * the linearworks
     */
    void
    filter(const geom::Geometry* g)
    {
        if(const geom::LineString* ls = dynamic_cast<const geom::LineString*>(g)) {
            addLine(ls);
        }
    }

    /** \brief
     * Returns the LineString or MultiLineString
     * built by the sequencing process, if one exists.
     *
     * @param release release ownership of computed Geometry
     * @return the sequenced linestrings,
     *         or `null` if a valid sequence
     *         does not exist.
     */
    geom::Geometry*
    getSequencedLineStrings(bool release = 1)
    {
        computeSequence();
        if(release) {
            return sequencedGeometry.release();
        }
        else {
            return sequencedGeometry.get();
        }
    }
};

} // namespace geos::operation::linemerge
} // namespace geos::operation
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_OP_LINEMERGE_LINESEQUENCER_H
