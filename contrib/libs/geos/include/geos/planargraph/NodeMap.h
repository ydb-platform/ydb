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

#ifndef GEOS_PLANARGRAPH_NODEMAP_H
#define GEOS_PLANARGRAPH_NODEMAP_H

#include <geos/export.h>
#include <geos/geom/Coordinate.h> // for use in container

#include <map>
#include <vector>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace planargraph {
class DirectedEdgeStar;
class DirectedEdge;
class Edge;
class Node;
}
}

namespace geos {
namespace planargraph { // geos.planargraph

/**
 * \brief
 * A map of Node, indexed by the coordinate of the node.
 *
 */
class GEOS_DLL NodeMap {
public:
    typedef std::map<geom::Coordinate, Node*, geom::CoordinateLessThen> container;
private:
    container nodeMap;
public:
    /**
     * \brief Constructs a NodeMap without any Nodes.
     */
    NodeMap();

    container& getNodeMap();

    virtual ~NodeMap() = default;

    /**
     * \brief
     * Adds a node to the std::map, replacing any that is already
     * at that location.
     * @return the added node
     */
    Node* add(Node* n);

    /**
     * \brief
     * Removes the Node at the given location, and returns it
     * (or null if no Node was there).
     */
    Node* remove(geom::Coordinate& pt);

    /**
     * \brief
     * Returns the Node at the given location,
     * or null if no Node was there.
     */
    Node* find(const geom::Coordinate& coord);

    /**
     * \brief
     * Returns an Iterator over the Nodes in this NodeMap,
     * sorted in ascending order
     * by angle with the positive x-axis.
     */
    container::iterator
    iterator()
    {
        return nodeMap.begin();
    }

    container::iterator
    begin()
    {
        return nodeMap.begin();
    }
    container::const_iterator
    begin() const
    {
        return nodeMap.begin();
    }

    container::iterator
    end()
    {
        return nodeMap.end();
    }
    container::const_iterator
    end() const
    {
        return nodeMap.end();
    }

    /**
     * \brief
     * Returns the Nodes in this NodeMap, sorted in ascending order
     * by angle with the positive x-axis.
     *
     * @param nodes : the nodes are push_back'ed here
     */
    void getNodes(std::vector<Node*>& nodes);
};


} // namespace geos::planargraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_PLANARGRAPH_NODEMAP_H
