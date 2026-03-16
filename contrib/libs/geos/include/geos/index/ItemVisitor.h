/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_INDEX_ITEMVISITOR_H
#define GEOS_INDEX_ITEMVISITOR_H

#include <geos/export.h>

namespace geos {
namespace index {

/** \brief
 * A visitor for items in an index.
 *
 * Last port: index/ItemVisitor.java rev. 1.2 (JTS-1.7)
 */
class GEOS_DLL ItemVisitor {
public:
    virtual void visitItem(void*) = 0;

    virtual
    ~ItemVisitor() {}
};

} // namespace geos.index
} // namespace geos

#endif // GEOS_INDEX_ITEMVISITOR_H

