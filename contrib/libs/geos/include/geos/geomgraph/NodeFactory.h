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
 * Last port: geomgraph/NodeFactory.java rev. 1.3 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOMGRAPH_NODEFACTORY_H
#define GEOS_GEOMGRAPH_NODEFACTORY_H

#include <geos/export.h>
#include <geos/inline.h>

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
namespace geomgraph { // geos.geomgraph

class GEOS_DLL NodeFactory {
public:
    virtual Node* createNode(const geom::Coordinate& coord) const;
    static const NodeFactory& instance();
    virtual
    ~NodeFactory() {}
protected:
    NodeFactory() {}
};


} // namespace geos.geomgraph
} // namespace geos

#endif // ifndef GEOS_GEOMGRAPH_NODEFACTORY_H
