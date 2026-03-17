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
 * Last port: index/kdtree/Node.java rev 1.8 (JTS-1.10)
 *
 **********************************************************************/

#pragma once

#include <geos/index/kdtree/KdNode.h>

namespace geos {
namespace index { // geos::index
namespace kdtree { // geos::index::kdtree

class GEOS_DLL KdNodeVisitor {

private:

protected:

public:

    KdNodeVisitor() {};
    virtual void visit(KdNode *node) = 0;


};


} // namespace geos::index::kdtree
} // namespace geos::index
} // namespace geos

