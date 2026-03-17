/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_PLANARGRAPH_DIRECTEDEDGESTAR_H
#define GEOS_PLANARGRAPH_DIRECTEDEDGESTAR_H

#include <geos/export.h>

#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace planargraph {
class DirectedEdge;
class Edge;
}
}

namespace geos {
namespace planargraph { // geos.planargraph

/// A sorted collection of DirectedEdge which leave a Node in a PlanarGraph.
class GEOS_DLL DirectedEdgeStar {
protected:

private:
    /**
     * \brief The underlying list of outgoing DirectedEdges
     */
    mutable std::vector<DirectedEdge*> outEdges;
    mutable bool sorted;
    void sortEdges() const;

public:
    /**
     * \brief Constructs a DirectedEdgeStar with no edges.
     */
    DirectedEdgeStar(): sorted(false) {}

    virtual
    ~DirectedEdgeStar() {}

    /**
     * \brief Adds a new member to this DirectedEdgeStar.
     */
    void add(DirectedEdge* de);

    /**
     * \brief Drops a member of this DirectedEdgeStar.
     */
    void remove(DirectedEdge* de);

    /**
     * \brief Returns an Iterator over the DirectedEdges,
     * in ascending order by angle with the positive x-axis.
     */
    std::vector<DirectedEdge*>::iterator
    iterator()
    {
        return begin();
    }
    /// Returns an iterator to first DirectedEdge
    std::vector<DirectedEdge*>::iterator begin();

    /// Returns an iterator to one-past last DirectedEdge
    std::vector<DirectedEdge*>::iterator end();

    /// Returns an const_iterator to first DirectedEdge
    std::vector<DirectedEdge*>::const_iterator begin() const;

    /// Returns an const_iterator to one-past last DirectedEdge
    std::vector<DirectedEdge*>::const_iterator end() const;

    /**
     * \brief Returns the number of edges around the Node associated
     * with this DirectedEdgeStar.
     */
    std::size_t
    getDegree() const
    {
        return outEdges.size();
    }

    /**
     * \brief Returns the coordinate for the node at wich this
     * star is based
     */
    geom::Coordinate& getCoordinate() const;

    /**
     * \brief Returns the DirectedEdges, in ascending order
     * by angle with the positive x-axis.
     */
    std::vector<DirectedEdge*>& getEdges();

    /**
     * \brief Returns the zero-based index of the given Edge,
     * after sorting in ascending order by angle with the
     * positive x-axis.
     */
    int getIndex(const Edge* edge);

    /**
     * \brief Returns the zero-based index of the given DirectedEdge,
     * after sorting in ascending order
     * by angle with the positive x-axis.
     */
    int getIndex(const DirectedEdge* dirEdge);

    /**
     * \brief Returns the remainder when i is divided by the number of
     * edges in this DirectedEdgeStar.
     */
    int getIndex(int i) const;

    /**
     * \brief Returns the DirectedEdge on the left-hand side
     * of the given DirectedEdge (which must be a member of this
     * DirectedEdgeStar).
     */
    DirectedEdge* getNextEdge(DirectedEdge* dirEdge);
};

} // namespace geos::planargraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_PLANARGRAPH_DIRECTEDEDGESTAR_H
