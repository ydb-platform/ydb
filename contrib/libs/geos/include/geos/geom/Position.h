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
 * Last port: geom/Position.java rev. 1.4 (JTS-1.10)
 *
 **********************************************************************/


#ifndef GEOS_GEOM_POSITION_H
#define GEOS_GEOM_POSITION_H

#include <geos/export.h>
#include <map>
#include <vector>
#include <string>

#include <geos/inline.h>


namespace geos {
namespace geom { // geos.geom

/** \brief
 * A Position indicates the position of a Location relative to a graph
 * component (Node, Edge, or Area).
 */
class GEOS_DLL Position {
public:
    enum {
        /** \brief
         * An indicator that a Location is *on*
         * a GraphComponent
         */
        ON = 0,

        /** \brief
         * An indicator that a Location is to the
         * *left* of a GraphComponent
         */
        LEFT,

        /** \brief
         * An indicator that a Location is to the
         * *right* of a GraphComponent
         */
        RIGHT
    };

    /** \brief
     * Returns LEFT if the position is RIGHT, RIGHT if
     * the position is LEFT, or the position otherwise.
     */
    static int opposite(int position);
};

} // namespace geos.geom
} // namespace geos

#endif // ifndef GEOS_GEOM_POSITION_H

