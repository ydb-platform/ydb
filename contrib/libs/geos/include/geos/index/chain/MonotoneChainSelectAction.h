/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: index/chain/MonotoneChainSelectAction.java rev. 1.6 (JTS-1.10)
 *
 **********************************************************************/

#ifndef GEOS_IDX_CHAIN_MONOTONECHAINSELECTACTION_H
#define GEOS_IDX_CHAIN_MONOTONECHAINSELECTACTION_H

#include <geos/export.h>
#include <geos/geom/LineSegment.h> // composition
#include <geos/geom/Envelope.h> // composition


// Forward declarations
namespace geos {
namespace index {
namespace chain {
class MonotoneChain;
}
}
}

namespace geos {
namespace index { // geos::index
namespace chain { // geos::index::chain

/**
 *  The action for the internal iterator for performing
 *  Envelope select queries on a MonotoneChain
 *
 */
class GEOS_DLL MonotoneChainSelectAction {

protected:

    geom::LineSegment selectedSegment;

public:

    MonotoneChainSelectAction() {}

    virtual
    ~MonotoneChainSelectAction() {}

    /// This function can be overridden if the original chain is needed
    virtual void select(MonotoneChain& mc, size_t start);

    /**
     * This is a convenience function which can be overridden
     * to obtain the actual line segment which is selected
     *
     * @param seg
     */
    virtual void select(const geom::LineSegment& seg) = 0;

};


} // namespace geos::index::chain
} // namespace geos::index
} // namespace geos

#endif // GEOS_IDX_CHAIN_MONOTONECHAINSELECTACTION_H

