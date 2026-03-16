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
 * Last port: operation/relate/RelateNodeFactory.java rev. 1.11 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_OP_RELATE_RELATENODEFACTORY_H
#define GEOS_OP_RELATE_RELATENODEFACTORY_H

#include <geos/export.h>

#include <geos/geomgraph/NodeFactory.h> // for RelateNodeFactory inheritance

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
}
namespace geomgraph {
class Node;
}
}


namespace geos {
namespace operation { // geos::operation
namespace relate { // geos::operation::relate

/** \brief
 * Used by the geomgraph::NodeMap in a RelateNodeGraph to create RelateNode objects.
 */
class GEOS_DLL RelateNodeFactory: public geomgraph::NodeFactory {
public:
    geomgraph::Node* createNode(const geom::Coordinate& coord) const override;
    static const geomgraph::NodeFactory& instance();
private:
    RelateNodeFactory() {}
};


} // namespace geos:operation:relate
} // namespace geos:operation
} // namespace geos

#endif // GEOS_OP_RELATE_RELATENODEFACTORY_H
