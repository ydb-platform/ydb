/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2019 Daniel Baston
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_GEOM_DEFAULTCOORDINATESEQUENCEFACTORY_H
#define GEOS_GEOM_DEFAULTCOORDINATESEQUENCEFACTORY_H

#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/FixedSizeCoordinateSequence.h>

namespace geos {
namespace geom {

class GEOS_DLL DefaultCoordinateSequenceFactory : public CoordinateSequenceFactory {
public:

    std::unique_ptr<CoordinateSequence> create() const final override {
        return detail::make_unique<CoordinateArraySequence>();
    }

    std::unique_ptr<CoordinateSequence> create(std::vector<Coordinate> *coords, std::size_t dims = 0) const final override {
        return detail::make_unique<CoordinateArraySequence>(coords, dims);
    }

    std::unique_ptr <CoordinateSequence> create(std::vector <Coordinate> &&coords, std::size_t dims = 0) const final override {
        return detail::make_unique<CoordinateArraySequence>(std::move(coords), dims);
    }

    std::unique_ptr <CoordinateSequence> create(std::size_t size, std::size_t dims = 0) const final override {
        switch(size) {
            case 5: return detail::make_unique<FixedSizeCoordinateSequence<5>>(dims);
            case 4: return detail::make_unique<FixedSizeCoordinateSequence<4>>(dims);
            case 3: return detail::make_unique<FixedSizeCoordinateSequence<3>>(dims);
            case 2: return detail::make_unique<FixedSizeCoordinateSequence<2>>(dims);
            case 1: return detail::make_unique<FixedSizeCoordinateSequence<1>>(dims);
            default:
                return detail::make_unique<CoordinateArraySequence>(size, dims);
        }
    }

    std::unique_ptr <CoordinateSequence> create(const CoordinateSequence &coordSeq) const final override {
        auto cs = create(coordSeq.size(), coordSeq.getDimension());
        for (size_t i = 0; i < cs->size(); i++) {
            cs->setAt(coordSeq[i], i);
        }
        return cs;
    }

    static const CoordinateSequenceFactory *instance();
};

}
}

#endif //GEOS_DEFAULTCOORDINATESEQUENCEFACTORY_H
