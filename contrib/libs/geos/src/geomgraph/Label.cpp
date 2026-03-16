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
 * Last port: geomgraph/Label.java r428 (JTS-1.12+)
 *
 **********************************************************************/

#include <geos/geomgraph/Label.h>

#include <string>
#include <sstream>
#include <iostream>

#ifndef GEOS_INLINE
# include <geos/geomgraph/Label.inl>
#endif


using namespace geos::geom;

namespace geos {
namespace geomgraph { // geos.geomgraph

std::string
Label::toString() const
{
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream&
operator<< (std::ostream& os, const Label& l)
{
    os << "A:"
       << l.elt[0]
       << " B:"
       << l.elt[1];
    return os;
}

} // namespace geos.geomgraph
} // namespace geos
