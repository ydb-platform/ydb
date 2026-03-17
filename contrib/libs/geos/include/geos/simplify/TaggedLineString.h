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
 * Last port: simplify/TaggedLineString.java rev. 1.2 (JTS-1.7.1)
 *
 **********************************************************************
 *
 * NOTES: This class can be optimized to work with vector<Coordinate*>
 *        rather then with CoordinateSequence. Also, LineSegment should
 *        be replaced with a class not copying Coordinates.
 *
 **********************************************************************/

#ifndef GEOS_SIMPLIFY_TAGGEDLINESTRING_H
#define GEOS_SIMPLIFY_TAGGEDLINESTRING_H

#include <geos/export.h>
#include <vector>
#include <memory>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4251) // warning C4251: needs to have dll-interface to be used by clients of class
#endif

// Forward declarations
namespace geos {
namespace geom {
class Coordinate;
class CoordinateSequence;
class Geometry;
class LineString;
class LinearRing;
}
namespace simplify {
class TaggedLineSegment;
}
}

namespace geos {
namespace simplify { // geos::simplify


/** \brief
 * Contains and owns a list of TaggedLineSegments.
 */
class GEOS_DLL TaggedLineString {

public:

    typedef std::vector<geom::Coordinate> CoordVect;

    typedef std::unique_ptr<CoordVect> CoordVectPtr;

    typedef geom::CoordinateSequence CoordSeq;

    typedef std::unique_ptr<geom::CoordinateSequence> CoordSeqPtr;

    TaggedLineString(const geom::LineString* nParentLine,
                     std::size_t minimumSize = 2);

    ~TaggedLineString();

    std::size_t getMinimumSize() const;

    const geom::LineString* getParent() const;

    const CoordSeq* getParentCoordinates() const;

    CoordSeqPtr getResultCoordinates() const;

    std::size_t getResultSize() const;

    TaggedLineSegment* getSegment(std::size_t i);

    const TaggedLineSegment* getSegment(std::size_t i) const;

    std::vector<TaggedLineSegment*>& getSegments();

    const std::vector<TaggedLineSegment*>& getSegments() const;

    void addToResult(std::unique_ptr<TaggedLineSegment> seg);

    std::unique_ptr<geom::Geometry> asLineString() const;

    std::unique_ptr<geom::Geometry> asLinearRing() const;

private:

    const geom::LineString* parentLine;

    // TaggedLineSegments owned by this object
    std::vector<TaggedLineSegment*> segs;

    // TaggedLineSegments owned by this object
    std::vector<TaggedLineSegment*> resultSegs;

    std::size_t minimumSize;

    void init();

    static CoordVectPtr extractCoordinates(
        const std::vector<TaggedLineSegment*>& segs);

    // Copying is turned off
    TaggedLineString(const TaggedLineString&);
    TaggedLineString& operator= (const TaggedLineString&);

};

} // namespace geos::simplify
} // namespace geos

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // GEOS_SIMPLIFY_TAGGEDLINESTRING_H
