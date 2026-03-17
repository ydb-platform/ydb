/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2005 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/Point.java r320 (JTS-1.12)
 *
 **********************************************************************/

#include <geos/util/UnsupportedOperationException.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/Point.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFilter.h>
#include <geos/geom/CoordinateFilter.h>
#include <geos/geom/GeometryFilter.h>
#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/Envelope.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/GeometryFactory.h>

#include <string>
#include <memory>

using namespace std;

namespace geos {
namespace geom { // geos::geom

const static FixedSizeCoordinateSequence<0> emptyCoords2d(2);
const static FixedSizeCoordinateSequence<0> emptyCoords3d(3);


/*protected*/
Point::Point(CoordinateSequence* newCoords, const GeometryFactory* factory)
    :
    Geometry(factory),
    empty2d(false),
    empty3d(false)
{
    std::unique_ptr<CoordinateSequence> coords(newCoords);

    if(coords == nullptr) {
        empty2d = true;
        return;
    }

    if (coords->getSize() == 1) {
        coordinates.setAt(coords->getAt(0), 0);
    } else if (coords->getSize() > 1) {
        throw util::IllegalArgumentException("Point coordinate list must contain a single element");
    } else if (coords->getDimension() == 3) {
        empty3d = true;
    } else {
        empty2d = true;
    }
}

Point::Point(const Coordinate & c, const GeometryFactory* factory) :
    Geometry(factory),
    empty2d(false),
    empty3d(false)
{
    coordinates.setAt(c, 0);
}

/*protected*/
Point::Point(const Point& p)
    :
    Geometry(p),
    coordinates(p.coordinates),
    empty2d(p.empty2d),
    empty3d(p.empty3d)
{
}

std::unique_ptr<CoordinateSequence>
Point::getCoordinates() const
{
    return getCoordinatesRO()->clone();
}

size_t
Point::getNumPoints() const
{
    return isEmpty() ? 0 : 1;
}

bool
Point::isEmpty() const
{
    return empty2d || empty3d;
}

bool
Point::isSimple() const
{
    return true;
}

Dimension::DimensionType
Point::getDimension() const
{
    return Dimension::P; // point
}

uint8_t
Point::getCoordinateDimension() const
{
    return (uint8_t) getCoordinatesRO()->getDimension();
}

int
Point::getBoundaryDimension() const
{
    return Dimension::False;
}

double
Point::getX() const
{
    if(isEmpty()) {
        throw util::UnsupportedOperationException("getX called on empty Point\n");
    }
    return getCoordinate()->x;
}

double
Point::getY() const
{
    if(isEmpty()) {
        throw util::UnsupportedOperationException("getY called on empty Point\n");
    }
    return getCoordinate()->y;
}

double
Point::getZ() const
{
    if(isEmpty()) {
        throw util::UnsupportedOperationException("getZ called on empty Point\n");
    }
    return getCoordinate()->z;
}

const Coordinate*
Point::getCoordinate() const
{
    return isEmpty() ? nullptr : &coordinates[0];
}

string
Point::getGeometryType() const
{
    return "Point";
}

std::unique_ptr<Geometry>
Point::getBoundary() const
{
    return getFactory()->createGeometryCollection();
}

Envelope::Ptr
Point::computeEnvelopeInternal() const
{
    if(isEmpty()) {
        return Envelope::Ptr(new Envelope());
    }

    return Envelope::Ptr(new Envelope(getCoordinate()->x,
                                      getCoordinate()->x, getCoordinate()->y,
                                      getCoordinate()->y));
}

void
Point::apply_ro(CoordinateFilter* filter) const
{
    if(isEmpty()) {
        return;
    }
    filter->filter_ro(getCoordinate());
}

void
Point::apply_rw(const CoordinateFilter* filter)
{
    if (isEmpty()) {
        return;
    }
    coordinates.apply_rw(filter);
}

void
Point::apply_rw(GeometryFilter* filter)
{
    filter->filter_rw(this);
}

void
Point::apply_ro(GeometryFilter* filter) const
{
    filter->filter_ro(this);
}

void
Point::apply_rw(GeometryComponentFilter* filter)
{
    filter->filter_rw(this);
}

void
Point::apply_ro(GeometryComponentFilter* filter) const
{
    filter->filter_ro(this);
}

void
Point::apply_rw(CoordinateSequenceFilter& filter)
{
    if(isEmpty()) {
        return;
    }
    filter.filter_rw(coordinates, 0);
    if(filter.isGeometryChanged()) {
        geometryChanged();
    }
}

void
Point::apply_ro(CoordinateSequenceFilter& filter) const
{
    if(isEmpty()) {
        return;
    }
    filter.filter_ro(coordinates, 0);
    //if (filter.isGeometryChanged()) geometryChanged();
}

bool
Point::equalsExact(const Geometry* other, double tolerance) const
{
    if(!isEquivalentClass(other)) {
        return false;
    }

    // assume the isEquivalentClass would return false
    // if other is not a point
    assert(dynamic_cast<const Point*>(other));

    if(isEmpty()) {
        return other->isEmpty();
    }
    else if(other->isEmpty()) {
        return false;
    }

    const Coordinate* this_coord = getCoordinate();
    const Coordinate* other_coord = other->getCoordinate();

    // assume the isEmpty checks above worked :)
    assert(this_coord && other_coord);

    return equal(*this_coord, *other_coord, tolerance);
}

int
Point::compareToSameClass(const Geometry* g) const
{
    const Point* p = dynamic_cast<const Point*>(g);
    return getCoordinate()->compareTo(*(p->getCoordinate()));
}

GeometryTypeId
Point::getGeometryTypeId() const
{
    return GEOS_POINT;
}

/*public*/
const CoordinateSequence*
Point::getCoordinatesRO() const
{
    if (empty2d) {
        return &emptyCoords2d;
    } else if (empty3d) {
        return &emptyCoords3d;
    }
    return &coordinates;
}

} // namespace geos::geom
} // namesapce geos
