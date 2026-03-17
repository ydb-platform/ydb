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
 * Last port: geomgraph/NodeMap.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_NODEMAP_H
#define GEOS_GEOMGRAPH_NODEMAP_H

#include <geos/export.h>
#include <map>
#include <vector>
#include <string>

#include <geos/geom/Coordinate.h> // for CoordinateLessThen
#include <geos/geomgraph/Node.h> // for testInvariant

#include <geos/inline.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geomgraph {
class Node;
class EdgeEnd;
class NodeFactory;
}
}

namespace geos {
namespace geomgraph { // geos.geomgraph

class GEOS_DLL NodeMap {
public:

    typedef std::map<geom::Coordinate*, Node*, geom::CoordinateLessThen> container;

    typedef container::iterator iterator;

    typedef container::const_iterator const_iterator;

    typedef std::pair<geom::Coordinate*, Node*> pair;

    container nodeMap;

    const NodeFactory& nodeFact;

    /// \brief
    /// NodeMap will keep a reference to the NodeFactory,
    /// keep it alive for the whole NodeMap lifetime
    NodeMap(const NodeFactory& newNodeFact);

    virtual ~NodeMap();

    Node* addNode(const geom::Coordinate& coord);

    Node* addNode(Node* n);

    void add(EdgeEnd* e);

    Node* find(const geom::Coordinate& coord) const;

    const_iterator
    begin() const
    {
        return nodeMap.begin();
    }

    const_iterator
    end() const
    {
        return nodeMap.end();
    }

    iterator
    begin()
    {
        return nodeMap.begin();
    }

    iterator
    end()
    {
        return nodeMap.end();
    }

    void getBoundaryNodes(int geomIndex,
                          std::vector<Node*>& bdyNodes) const;

    std::string print() const;

    void
    testInvariant()
    {
#ifndef NDEBUG
        // Each Coordinate key is a pointer inside the Node value
        for(iterator it = begin(), itEnd = end(); it != itEnd; ++it) {
            pair p = *it;
            geomgraph::Node* n = p.second;
            geom::Coordinate* c = const_cast<geom::Coordinate*>(
                                      &(n->getCoordinate())
                                  );
            assert(p.first == c);
        }
#endif
    }

private:

    // Declare type as noncopyable
    NodeMap(const NodeMap& other) = delete;
    NodeMap& operator=(const NodeMap& rhs) = delete;
};

} // namespace geos.geomgraph
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // ifndef GEOS_GEOMGRAPH_NODEMAP_H
