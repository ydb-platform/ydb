/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: simplify/TaggedLinesSimplifier.java rev. 1.4 (JTS-1.7.1)
 *
 **********************************************************************/

#include <geos/simplify/TaggedLinesSimplifier.h>
#include <geos/simplify/LineSegmentIndex.h>
#include <geos/simplify/TaggedLineStringSimplifier.h>
#include <geos/algorithm/LineIntersector.h>

#include <cassert>
#include <algorithm>
#include <memory>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;
using namespace std;

namespace geos {
namespace simplify { // geos::simplify

/*public*/
TaggedLinesSimplifier::TaggedLinesSimplifier()
    :
    inputIndex(new LineSegmentIndex()),
    outputIndex(new LineSegmentIndex()),
    taggedlineSimplifier(new TaggedLineStringSimplifier(inputIndex.get(),
                         outputIndex.get()))
{
}

/*public*/
void
TaggedLinesSimplifier::setDistanceTolerance(double d)
{
    taggedlineSimplifier->setDistanceTolerance(d);
}

/*private*/
void
TaggedLinesSimplifier::simplify(TaggedLineString& tls)
{
    taggedlineSimplifier->simplify(&tls);
}

} // namespace geos::simplify
} // namespace geos
