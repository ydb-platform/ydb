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

#include <geos/profiler.h>
#include <geos/geom/CoordinateSequence.h>
// FIXME: we should probably not be using CoordinateArraySequenceFactory
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/util/IllegalArgumentException.h>

#include <cstdio>
#include <algorithm>
#include <vector>
#include <cassert>
#include <iterator>
#include <sstream>

using namespace std;

namespace geos {
namespace geom { // geos::geom

#if PROFILE
static Profiler* profiler = Profiler::instance();
#endif

double
CoordinateSequence::getOrdinate(size_t index, size_t ordinateIndex) const
{
    switch(ordinateIndex) {
        case CoordinateSequence::X:
            return getAt(index).x;
        case CoordinateSequence::Y:
            return getAt(index).y;
        case CoordinateSequence::Z:
            return getAt(index).z;
        default:
            return DoubleNotANumber;
    }
}

bool
CoordinateSequence::hasRepeatedPoints() const
{
    const std::size_t p_size = getSize();
    for(std::size_t i = 1; i < p_size; i++) {
        if(getAt(i - 1) == getAt(i)) {
            return true;
        }
    }
    return false;
}

/*
 * Returns either the given coordinate array if its length is greater than the
 * given amount, or an empty coordinate array.
 */
CoordinateSequence*
CoordinateSequence::atLeastNCoordinatesOrNothing(size_t n,
        CoordinateSequence* c)
{
    if(c->getSize() >= n) {
        return c;
    }
    else {
        // FIXME: return NULL rather then empty coordinate array
        return CoordinateArraySequenceFactory::instance()->create().release();
    }
}


bool
CoordinateSequence::hasRepeatedPoints(const CoordinateSequence* cl)
{
    const std::size_t size = cl->getSize();
    for(std::size_t i = 1; i < size; i++) {
        if(cl->getAt(i - 1) == cl->getAt(i)) {
            return true;
        }
    }
    return false;
}

const Coordinate*
CoordinateSequence::minCoordinate() const
{
    const Coordinate* minCoord = nullptr;
    const std::size_t p_size = getSize();
    for(std::size_t i = 0; i < p_size; i++) {
        if(minCoord == nullptr || minCoord->compareTo(getAt(i)) > 0) {
            minCoord = &getAt(i);
        }
    }
    return minCoord;
}

size_t
CoordinateSequence::indexOf(const Coordinate* coordinate,
                            const CoordinateSequence* cl)
{
    size_t p_size = cl->size();
    for(size_t i = 0; i < p_size; ++i) {
        if((*coordinate) == cl->getAt(i)) {
            return i;
        }
    }
    return std::numeric_limits<std::size_t>::max();
}

void
CoordinateSequence::scroll(CoordinateSequence* cl,
                           const Coordinate* firstCoordinate)
{
    // FIXME: use a standard algorithm instead
    std::size_t i, j = 0;
    std::size_t ind = indexOf(firstCoordinate, cl);
    if(ind < 1) {
        return;    // not found or already first
    }

    const std::size_t length = cl->getSize();
    vector<Coordinate> v(length);
    for(i = ind; i < length; i++) {
        v[j++] = cl->getAt(i);
    }
    for(i = 0; i < ind; i++) {
        v[j++] = cl->getAt(i);
    }
    cl->setPoints(v);
}

int
CoordinateSequence::increasingDirection(const CoordinateSequence& pts)
{
    size_t ptsize = pts.size();
    for(size_t i = 0, n = ptsize / 2; i < n; ++i) {
        size_t j = ptsize - 1 - i;
        // skip equal points on both ends
        int comp = pts[i].compareTo(pts[j]);
        if(comp != 0) {
            return comp;
        }
    }
    // array must be a palindrome - defined to be in positive direction
    return 1;
}

/* public static */
bool
CoordinateSequence::isRing(const CoordinateSequence *pts)
{
    if (pts->size() < 4) return false;

    if (pts->getAt(0) != pts->getAt(pts->size()-1)) {
        return false;
    }

    return true;
}

void
CoordinateSequence::reverse(CoordinateSequence* cl)
{
    // FIXME: use a standard algorithm
    auto last = cl->size() - 1;
    auto mid = last / 2;
    for(size_t i = 0; i <= mid; i++) {
        const Coordinate tmp = cl->getAt(i);
        cl->setAt(cl->getAt(last - i), i);
        cl->setAt(tmp, last - i);
    }
}

bool
CoordinateSequence::equals(const CoordinateSequence* cl1,
                           const CoordinateSequence* cl2)
{
    // FIXME: use std::equals()

    if(cl1 == cl2) {
        return true;
    }
    if(cl1 == nullptr || cl2 == nullptr) {
        return false;
    }
    size_t npts1 = cl1->getSize();
    if(npts1 != cl2->getSize()) {
        return false;
    }
    for(size_t i = 0; i < npts1; i++) {
        if(!(cl1->getAt(i) == cl2->getAt(i))) {
            return false;
        }
    }
    return true;
}

void
CoordinateSequence::expandEnvelope(Envelope& env) const
{
    const std::size_t p_size = getSize();
    for(std::size_t i = 0; i < p_size; i++) {
        env.expandToInclude(getAt(i));
    }
}

Envelope
CoordinateSequence::getEnvelope() const {
    Envelope e;
    expandEnvelope(e);
    return e;
}

std::ostream&
operator<< (std::ostream& os, const CoordinateSequence& cs)
{
    os << "(";
    for(size_t i = 0, n = cs.size(); i < n; ++i) {
        const Coordinate& c = cs[i];
        if(i) {
            os << ", ";
        }
        os << c;
    }
    os << ")";

    return os;
}

std::string
CoordinateSequence::toString() const
{
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

bool
operator== (const CoordinateSequence& s1, const CoordinateSequence& s2)
{
    return CoordinateSequence::equals(&s1, &s2);
}

bool
operator!= (const CoordinateSequence& s1, const CoordinateSequence& s2)
{
    return ! CoordinateSequence::equals(&s1, &s2);
}

} // namespace geos::geom
} // namespace geos
