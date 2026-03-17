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
 **********************************************************************/

#include <geos/index/kdtree/KdNode.h>

using namespace geos::geom;

namespace geos {
namespace index { // geos.index
namespace kdtree { // geos.index.kdtree

KdNode::KdNode(double p_x, double p_y, void* p_data) :
    p(p_x, p_y),
    data(p_data),
    left(nullptr),
    right(nullptr),
    count(1) {}

KdNode::KdNode(const Coordinate& p_p, void* p_data) :
    p(p_p),
    data(p_data),
    left(nullptr),
    right(nullptr),
    count(1) {}


} // namespace geos.index.kdtree
} // namespace geos.index
} // namespace geos
