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

#pragma once


#include <geos/export.h>
#include <geos/noding/Noder.h>

#include <memory> // for unique_ptr

// Forward declarations
namespace geos {
namespace algorithm {
class LineIntersector;
}
namespace geom {
class Geometry;
}
}

namespace geos {
namespace noding { // geos.noding

/**
 * A wrapper for {@link noding::Noder}s which validates
 * the output arrangement is correctly noded.
 * An arrangement of line segments is fully noded if
 * there is no line segment
 * which has another segment intersecting its interior.
 * If the noding is not correct, a {@link util::TopologyException} is thrown
 * with details of the first invalid location found.
 *
 * @author mdavis
 *
 * @see FastNodingValidator
 *
 */
class GEOS_DLL ValidatingNoder : public Noder {

private:

    std::vector<SegmentString*>* nodedSS;
    noding::Noder& noder;


public:

    ValidatingNoder(Noder& noderArg)
        : noder(noderArg)
        {}

    void computeNodes(std::vector<SegmentString*>* segStrings) override;

    void validate();

    std::vector<SegmentString*>* getNodedSubstrings() const override;

};

} // namespace geos.noding
} // namespace geos

