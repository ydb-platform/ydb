/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/geom/Envelope.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/util.h>

#include <sstream>
#include <cassert>
#include <algorithm>
#include <vector>
#include <cmath>

namespace geos {
namespace geom { // geos::geom

CoordinateArraySequence::CoordinateArraySequence():
    dimension(0)
{
}

CoordinateArraySequence::CoordinateArraySequence(size_t n,
        size_t dimension_in):
    vect(n),
    dimension(dimension_in)
{
}

CoordinateArraySequence::CoordinateArraySequence(std::vector<Coordinate> && coords, size_t dimension_in):
        vect(std::move(coords)),
        dimension(dimension_in)
{
}

CoordinateArraySequence::CoordinateArraySequence(
    std::vector<Coordinate>* coords, size_t dimension_in)
    : dimension(dimension_in)
{
    std::unique_ptr<std::vector<Coordinate>> coordp(coords);

    if(coordp) {
        vect = std::move(*coords);
    }
}

CoordinateArraySequence::CoordinateArraySequence(
    const CoordinateArraySequence& c)
    :
    CoordinateSequence(c),
    vect(c.vect),
    dimension(c.getDimension())
{
}

CoordinateArraySequence::CoordinateArraySequence(
    const CoordinateSequence& c)
    :
    CoordinateSequence(c),
    vect(c.size()),
    dimension(c.getDimension())
{
    for(size_t i = 0, n = vect.size(); i < n; ++i) {
        vect[i] = c.getAt(i);
    }
}

std::unique_ptr<CoordinateSequence>
CoordinateArraySequence::clone() const
{
    return detail::make_unique<CoordinateArraySequence>(*this);
}

void
CoordinateArraySequence::setPoints(const std::vector<Coordinate>& v)
{
    vect.assign(v.begin(), v.end());
}

std::size_t
CoordinateArraySequence::getDimension() const
{
    if(dimension != 0) {
        return dimension;
    }

    if(vect.empty()) {
        return 3;
    }

    if(std::isnan(vect[0].z)) {
        dimension = 2;
    }
    else {
        dimension = 3;
    }

    return dimension;
}

void
CoordinateArraySequence::toVector(std::vector<Coordinate>& out) const
{
    out.insert(out.end(), vect.begin(), vect.end());
}

void
CoordinateArraySequence::add(const Coordinate& c)
{
    vect.push_back(c);
}

void
CoordinateArraySequence::add(const Coordinate& c, bool allowRepeated)
{
    if(!allowRepeated && ! vect.empty()) {
        const Coordinate& last = vect.back();
        if(last.equals2D(c)) {
            return;
        }
    }
    vect.push_back(c);
}

void
CoordinateArraySequence::add(const CoordinateSequence* cl, bool allowRepeated, bool direction)
{
    // FIXME:  don't rely on negative values for 'j' (the reverse case)

    const auto npts = cl->size();
    if(direction) {
        for(size_t i = 0; i < npts; ++i) {
            add(cl->getAt(i), allowRepeated);
        }
    }
    else {
        for(auto j = npts; j > 0; --j) {
            add(cl->getAt(j - 1), allowRepeated);
        }
    }
}

/*public*/
void
CoordinateArraySequence::add(size_t i, const Coordinate& coord,
                             bool allowRepeated)
{
    // don't add duplicate coordinates
    if(! allowRepeated) {
        size_t sz = size();
        if(sz > 0) {
            if(i > 0) {
                const Coordinate& prev = getAt(i - 1);
                if(prev.equals2D(coord)) {
                    return;
                }
            }
            if(i < sz) {
                const Coordinate& next = getAt(i);
                if(next.equals2D(coord)) {
                    return;
                }
            }
        }
    }

    vect.insert(vect.begin() + i, coord);
}

size_t
CoordinateArraySequence::getSize() const
{
    return vect.size();
}

const Coordinate&
CoordinateArraySequence::getAt(size_t pos) const
{
    return vect[pos];
}

void
CoordinateArraySequence::getAt(size_t pos, Coordinate& c) const
{
    c = vect[pos];
}

void
CoordinateArraySequence::setAt(const Coordinate& c, size_t pos)
{
    vect[pos] = c;
}

void
CoordinateArraySequence::expandEnvelope(Envelope& env) const
{
    for(const auto& coord : vect) {
        env.expandToInclude(coord);
    }
}

void
CoordinateArraySequence::setOrdinate(size_t index, size_t ordinateIndex,
                                     double value)
{
    switch(ordinateIndex) {
    case CoordinateSequence::X:
        vect[index].x = value;
        break;
    case CoordinateSequence::Y:
        vect[index].y = value;
        break;
    case CoordinateSequence::Z:
        vect[index].z = value;
        break;
    default: {
        std::stringstream ss;
        ss << "Unknown ordinate index " << ordinateIndex;
        throw util::IllegalArgumentException(ss.str());
        break;
    }
    }
}

void
CoordinateArraySequence::apply_rw(const CoordinateFilter* filter)
{
    for(auto& coord : vect) {
        filter->filter_rw(&coord);
    }
    dimension = 0; // re-check (see http://trac.osgeo.org/geos/ticket/435)
}

void
CoordinateArraySequence::apply_ro(CoordinateFilter* filter) const
{
    for(const auto& coord : vect) {
        filter->filter_ro(&coord);
    }
}

} // namespace geos::geom
} //namespace geos
