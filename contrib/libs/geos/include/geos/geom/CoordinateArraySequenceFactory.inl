/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_COORDINATEARRAYSEQUENCEFACTORY_INL
#define GEOS_GEOM_COORDINATEARRAYSEQUENCEFACTORY_INL

#include <cassert>
#include <geos/geom/CoordinateArraySequenceFactory.h>
#include <geos/geom/CoordinateArraySequence.h>

namespace geos {
namespace geom { // geos::geom

INLINE std::unique_ptr<CoordinateSequence>
CoordinateArraySequenceFactory::create(std::vector<Coordinate>* coords,
                                       size_t dimension) const
{
    return std::unique_ptr<CoordinateSequence>(
            new CoordinateArraySequence(coords, dimension));
}

INLINE std::unique_ptr<CoordinateSequence>
CoordinateArraySequenceFactory::create(std::vector<Coordinate> && coords,
        size_t dimension) const {
    return std::unique_ptr<CoordinateSequence>(new CoordinateArraySequence(std::move(coords), dimension));
}

INLINE std::unique_ptr<CoordinateSequence>
CoordinateArraySequenceFactory::create(std::size_t size, std::size_t dimension)
const
{
    return std::unique_ptr<CoordinateSequence>(
            new CoordinateArraySequence(size, dimension));
}

INLINE std::unique_ptr<CoordinateSequence>
CoordinateArraySequenceFactory::create(const CoordinateSequence& seq)
const
{
    return std::unique_ptr<CoordinateSequence>(
            new CoordinateArraySequence(seq));
}


} // namespace geos::geom
} // namespace geos

#endif // GEOS_GEOM_COORDINATEARRAYSEQUENCEFACTORY_INL

