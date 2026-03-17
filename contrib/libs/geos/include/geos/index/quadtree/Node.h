/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/quadtree/Node.java rev 1.8 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_QUADTREE_NODE_H
#define GEOS_IDX_QUADTREE_NODE_H

#include <geos/export.h>
#include <geos/index/quadtree/NodeBase.h> // for inheritance
#include <geos/geom/Coordinate.h> // for composition
#include <geos/geom/Envelope.h> // for inline

#include <string>
#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
//class Coordinate;
class Envelope;
}
}

namespace geos {
namespace index { // geos::index
namespace quadtree { // geos::index::quadtree

/**
 * \brief
 * Represents a node of a Quadtree.
 *
 * Nodes contain items which have a spatial extent corresponding to
 * the node's position in the quadtree.
 *
 */
class GEOS_DLL Node: public NodeBase {

private:

    /// Owned by this class
    std::unique_ptr<geom::Envelope> env;

    geom::Coordinate centre;

    int level;

    /**
     * Get the subquad for the index.
     * If it doesn't exist, create it.
     *
     * Ownership of the returned object belongs to this class.
     */
    Node* getSubnode(int index);

    std::unique_ptr<Node> createSubnode(int index);

protected:

    bool
    isSearchMatch(const geom::Envelope& searchEnv) const override
    {
        return env->intersects(searchEnv);
    }

public:

    // Create a node computing level from given envelope
    static std::unique_ptr<Node> createNode(const geom::Envelope& env);

    /// Create a node containing the given node and envelope
    //
    /// @param node if not null, will be inserted to the returned node
    /// @param addEnv minimum envelope to use for the node
    ///
    static std::unique_ptr<Node> createExpanded(std::unique_ptr<Node> node,
            const geom::Envelope& addEnv);

    Node(std::unique_ptr<geom::Envelope> nenv, int nlevel)
        :
        env(std::move(nenv)),
        centre((env->getMinX() + env->getMaxX()) / 2,
               (env->getMinY() + env->getMaxY()) / 2),
        level(nlevel)
    {
    }

    ~Node() override {}

    /// Return Envelope associated with this node
    /// ownership retained by this object
    geom::Envelope*
    getEnvelope()
    {
        return env.get();
    }

    /** \brief
     * Returns the subquad containing the envelope.
     * Creates the subquad if
     * it does not already exist.
     */
    Node* getNode(const geom::Envelope* searchEnv);

    /** \brief
     * Returns the smallest <i>existing</i>
     * node containing the envelope.
     */
    NodeBase* find(const geom::Envelope* searchEnv);

    void insertNode(std::unique_ptr<Node> node);

    std::string toString() const override;

};

} // namespace geos::index::quadtree
} // namespace geos::index
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_IDX_QUADTREE_NODE_H
