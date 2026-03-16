/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
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
 * Last port: geom/GeometryCollection.java rev. 1.41
 *
 **********************************************************************/

#include <geos/geom/GeometryCollection.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/geom/CoordinateSequence.h>
#include <geos/geom/CoordinateSequenceFilter.h>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/Dimension.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryFilter.h>
#include <geos/geom/GeometryComponentFilter.h>
#include <geos/geom/Envelope.h>
#include <geos/util.h>

#ifndef GEOS_INLINE
# include <geos/geom/GeometryCollection.inl>
#endif

#include <algorithm>
#include <vector>
#include <memory>

namespace geos {
namespace geom { // geos::geom

/*protected*/
GeometryCollection::GeometryCollection(const GeometryCollection& gc)
    :
    Geometry(gc),
    geometries(gc.geometries.size())
{
    for(size_t i = 0; i < geometries.size(); ++i) {
        geometries[i] = gc.geometries[i]->clone();
    }
}

/*protected*/
GeometryCollection::GeometryCollection(std::vector<Geometry*>* newGeoms, const GeometryFactory* factory):
    Geometry(factory)
{
    if(newGeoms == nullptr) {
        return;
    }
    if(hasNullElements(newGeoms)) {
        throw  util::IllegalArgumentException("geometries must not contain null elements\n");
    }
    for (const auto& geom : *newGeoms) {
        geometries.emplace_back(geom);
    }
    delete newGeoms;

    // Set SRID for inner geoms
    setSRID(getSRID());
}

GeometryCollection::GeometryCollection(std::vector<std::unique_ptr<Geometry>> && newGeoms, const GeometryFactory& factory) :
    Geometry(&factory),
    geometries(std::move(newGeoms)) {

    if (hasNullElements(&geometries)) {
        throw util::IllegalArgumentException("geometries must not contain null elements\n");
    }

    setSRID(getSRID());
}

void
GeometryCollection::setSRID(int newSRID)
{
    Geometry::setSRID(newSRID);
    for(auto& g : geometries) {
        g->setSRID(newSRID);
    }
}

/*
 * Collects all coordinates of all subgeometries into a CoordinateSequence.
 *
 * Returns a newly the collected coordinates
 *
 */
std::unique_ptr<CoordinateSequence>
GeometryCollection::getCoordinates() const
{
    std::vector<Coordinate> coordinates(getNumPoints());

    size_t k = 0;
    for(const auto& g : geometries) {
        auto childCoordinates = g->getCoordinates(); // TODO avoid this copy where getCoordinateRO() exists
        size_t npts = childCoordinates->getSize();
        for(size_t j = 0; j < npts; ++j) {
            coordinates[k] = childCoordinates->getAt(j);
            k++;
        }
    }
    return CoordinateArraySequenceFactory::instance()->create(std::move(coordinates));
}

bool
GeometryCollection::isEmpty() const
{
    for(const auto& g : geometries) {
        if(!g->isEmpty()) {
            return false;
        }
    }
    return true;
}

Dimension::DimensionType
GeometryCollection::getDimension() const
{
    Dimension::DimensionType dimension = Dimension::False;
    for(const auto& g : geometries) {
        dimension = std::max(dimension, g->getDimension());
    }
    return dimension;
}

bool
GeometryCollection::isDimensionStrict(Dimension::DimensionType d) const {
    return std::all_of(geometries.begin(), geometries.end(),
            [&d](const std::unique_ptr<Geometry> & g) {
                return g->getDimension() == d;
            });
}

int
GeometryCollection::getBoundaryDimension() const
{
    int dimension = Dimension::False;
    for(const auto& g : geometries) {
        dimension = std::max(dimension, g->getBoundaryDimension());
    }
    return dimension;
}

uint8_t
GeometryCollection::getCoordinateDimension() const
{
    uint8_t dimension = 2;

    for(const auto& g : geometries) {
        dimension = std::max(dimension, g->getCoordinateDimension());
    }
    return dimension;
}

size_t
GeometryCollection::getNumGeometries() const
{
    return geometries.size();
}

const Geometry*
GeometryCollection::getGeometryN(size_t n) const
{
    return geometries[n].get();
}

size_t
GeometryCollection::getNumPoints() const
{
    size_t numPoints = 0;
    for(const auto& g : geometries) {
        numPoints += g->getNumPoints();
    }
    return numPoints;
}

std::string
GeometryCollection::getGeometryType() const
{
    return "GeometryCollection";
}

std::unique_ptr<Geometry>
GeometryCollection::getBoundary() const
{
    throw util::IllegalArgumentException("Operation not supported by GeometryCollection\n");
}

bool
GeometryCollection::equalsExact(const Geometry* other, double tolerance) const
{
    if(!isEquivalentClass(other)) {
        return false;
    }

    const GeometryCollection* otherCollection = dynamic_cast<const GeometryCollection*>(other);
    if(! otherCollection) {
        return false;
    }

    if(geometries.size() != otherCollection->geometries.size()) {
        return false;
    }
    for(size_t i = 0; i < geometries.size(); ++i) {
        if(!(geometries[i]->equalsExact(otherCollection->geometries[i].get(), tolerance))) {
            return false;
        }
    }
    return true;
}

void
GeometryCollection::apply_rw(const CoordinateFilter* filter)
{
    for(auto& g : geometries) {
        g->apply_rw(filter);
    }
}

void
GeometryCollection::apply_ro(CoordinateFilter* filter) const
{
    for(const auto& g : geometries) {
        g->apply_ro(filter);
    }
}

void
GeometryCollection::apply_ro(GeometryFilter* filter) const
{
    filter->filter_ro(this);
    for(const auto& g : geometries) {
        g->apply_ro(filter);
    }
}

void
GeometryCollection::apply_rw(GeometryFilter* filter)
{
    filter->filter_rw(this);
    for(auto& g : geometries) {
        g->apply_rw(filter);
    }
}

void
GeometryCollection::normalize()
{
    for(auto& g : geometries) {
        g->normalize();
    }
    std::sort(geometries.begin(), geometries.end(), [](const std::unique_ptr<Geometry> & a, const std::unique_ptr<Geometry> & b) {
        return a->compareTo(b.get()) > 0;
    });
}

Envelope::Ptr
GeometryCollection::computeEnvelopeInternal() const
{
    Envelope::Ptr p_envelope(new Envelope());
    for(const auto& g : geometries) {
        const Envelope* env = g->getEnvelopeInternal();
        p_envelope->expandToInclude(env);
    }
    return p_envelope;
}

int
GeometryCollection::compareToSameClass(const Geometry* g) const
{
    const GeometryCollection* gc = dynamic_cast<const GeometryCollection*>(g);
    return compare(geometries, gc->geometries);
}

const Coordinate*
GeometryCollection::getCoordinate() const
{
    for(const auto& g : geometries) {
        if(!g->isEmpty()) {
            return g->getCoordinate();
        }
    }
    return nullptr;
}

/**
 * @return the area of this collection
 */
double
GeometryCollection::getArea() const
{
    double area = 0.0;
    for(const auto& g : geometries) {
        area += g->getArea();
    }
    return area;
}

/**
 * @return the total length of this collection
 */
double
GeometryCollection::getLength() const
{
    double sum = 0.0;
    for(const auto& g : geometries) {
        sum += g->getLength();
    }
    return sum;
}

void
GeometryCollection::apply_rw(GeometryComponentFilter* filter)
{
    filter->filter_rw(this);
    for(auto& g : geometries) {
        if (filter->isDone()) {
            return;
        }
        g->apply_rw(filter);
    }
}

void
GeometryCollection::apply_ro(GeometryComponentFilter* filter) const
{
    filter->filter_ro(this);
    for(const auto& g : geometries) {
        if (filter->isDone()) {
            return;
        }
        g->apply_ro(filter);
    }
}

void
GeometryCollection::apply_rw(CoordinateSequenceFilter& filter)
{
    for(auto& g : geometries) {
        g->apply_rw(filter);
        if(filter.isDone()) {
            break;
        }
    }
    if(filter.isGeometryChanged()) {
        geometryChanged();
    }
}

void
GeometryCollection::apply_ro(CoordinateSequenceFilter& filter) const
{
    for(const auto& g : geometries) {
        g->apply_ro(filter);
        if(filter.isDone()) {
            break;
        }
    }

    assert(!filter.isGeometryChanged()); // read-only filter...
    //if (filter.isGeometryChanged()) geometryChanged();
}

GeometryTypeId
GeometryCollection::getGeometryTypeId() const
{
    return GEOS_GEOMETRYCOLLECTION;
}

std::unique_ptr<Geometry>
GeometryCollection::reverse() const
{
    if(isEmpty()) {
        return clone();
    }

    std::vector<std::unique_ptr<Geometry>> reversed(geometries.size());

    std::transform(geometries.begin(),
                   geometries.end(),
                   reversed.begin(),
    [](const std::unique_ptr<Geometry> & g) {
        return g->reverse();
    });

    return getFactory()->createGeometryCollection(std::move(reversed));
}

} // namespace geos::geom
} // namespace geos
