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
 * Last port: noding/ScaledNoder.java rev. 1.3 (JTS-1.7.1)
 *
 **********************************************************************/

#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateSequence.h> // for apply and delete
#include <geos/geom/CoordinateFilter.h> // for inheritance
#include <geos/noding/ScaledNoder.h>
#include <geos/noding/NodedSegmentString.h>
#include <geos/noding/SegmentString.h>
#include <geos/operation/valid/RepeatedPointRemover.h>
#include <geos/operation/valid/RepeatedPointTester.h>
#include <geos/util/math.h>
#include <geos/util.h>

#include <functional>
#include <vector>
#include <cassert>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#include <string>
#endif

using namespace geos::geom;



namespace geos {
namespace noding { // geos.noding

namespace {

#if GEOS_DEBUG > 1
void
sqlPrint(const std::string& table, std::vector<SegmentString*>& ssv)
{
    std::cerr << "CREATE TABLE \"" << table
              << "\" (id integer, geom geometry);" << std::endl;

    std::cerr << "COPY \"" << table
              << "\" FROM stdin;" << std::endl;

    for(size_t i = 0, n = ssv.size(); i < n; i++) {
        SegmentString* ss = ssv[i];
        geom::CoordinateSequence* cs = ss->getCoordinates();
        assert(cs);

        std::cerr << i << '\t' << "LINESTRING"
                  << *cs
                  << std::endl;
    }
    std::cerr << "\\." << std::endl;
}
#endif // GEOS_DEBUG > 1

} // anonym namespace

class ScaledNoder::Scaler : public geom::CoordinateFilter {
public:
    const ScaledNoder& sn;
    Scaler(const ScaledNoder& n): sn(n)
    {
#if GEOS_DEBUG
        std::cerr << "Scaler: offsetX,Y: " << sn.offsetX << ","
                  << sn.offsetY << " scaleFactor: " << sn.scaleFactor
                  << std::endl;
#endif
    }

    //void filter_ro(const geom::Coordinate* c) { assert(0); }

    void
    filter_rw(geom::Coordinate* c) const override
    {
        c->x = util::round((c->x - sn.offsetX) * sn.scaleFactor);
        c->y = util::round((c->y - sn.offsetY) * sn.scaleFactor);
    }

private:
    // Declare type as noncopyable
    Scaler(const Scaler& other) = delete;
    Scaler& operator=(const Scaler& rhs) = delete;
};

class ScaledNoder::ReScaler: public geom::CoordinateFilter {
public:
    const ScaledNoder& sn;
    ReScaler(const ScaledNoder& n): sn(n)
    {
#if GEOS_DEBUG
        std::cerr << "ReScaler: offsetX,Y: " << sn.offsetX << ","
                  << sn.offsetY << " scaleFactor: " << sn.scaleFactor
                  << std::endl;
#endif
    }

    void
    filter_ro(const geom::Coordinate* c) override
    {
        ::geos::ignore_unused_variable_warning(c);
        assert(0);
    }

    void
    filter_rw(geom::Coordinate* c) const override
    {
        c->x = c->x / sn.scaleFactor + sn.offsetX;
        c->y = c->y / sn.scaleFactor + sn.offsetY;
    }

private:
    // Declare type as noncopyable
    ReScaler(const ReScaler& other);
    ReScaler& operator=(const ReScaler& rhs);
};

/*private*/
void
ScaledNoder::rescale(SegmentString::NonConstVect& segStrings) const
{
    ReScaler rescaler(*this);
    for(SegmentString::NonConstVect::const_iterator
            i0 = segStrings.begin(), i0End = segStrings.end();
            i0 != i0End; ++i0) {

        SegmentString* ss = *i0;

        ss->getCoordinates()->apply_rw(&rescaler);

    }
}


/*private*/
void
ScaledNoder::scale(SegmentString::NonConstVect& segStrings) const
{
    Scaler scaler(*this);
    for(size_t i = 0; i < segStrings.size(); i++) {
        SegmentString* ss = segStrings[i];

        CoordinateSequence* cs = ss->getCoordinates();

#ifndef NDEBUG
        size_t npts = cs->size();
#endif
        cs->apply_rw(&scaler);
        assert(cs->size() == npts);

        operation::valid::RepeatedPointTester rpt;
        if (rpt.hasRepeatedPoint(cs)) {
            auto cs2 = operation::valid::RepeatedPointRemover::removeRepeatedPoints(cs);
            segStrings[i] = new NodedSegmentString(cs2.release(), ss->getData());
            delete ss;
        }
    }
}

ScaledNoder::~ScaledNoder()
{
    for(std::vector<geom::CoordinateSequence*>::const_iterator
            it = newCoordSeq.begin(), end = newCoordSeq.end();
            it != end;
            ++it) {
        delete *it;
    }
}


/*public*/
SegmentString::NonConstVect*
ScaledNoder::getNodedSubstrings() const
{
    SegmentString::NonConstVect* splitSS = noder.getNodedSubstrings();

#if GEOS_DEBUG > 1
    sqlPrint("nodedSegStr", *splitSS);
#endif

    if(isScaled) {
        rescale(*splitSS);
    }

#if GEOS_DEBUG > 1
    sqlPrint("scaledNodedSegStr", *splitSS);
#endif

    return splitSS;

}

/*public*/
void
ScaledNoder::computeNodes(SegmentString::NonConstVect* inputSegStr)
{

#if GEOS_DEBUG > 1
    sqlPrint("inputSegStr", *inputSegStr);
#endif

    if(isScaled) {
        scale(*inputSegStr);
    }

#if GEOS_DEBUG > 1
    sqlPrint("scaledInputSegStr", *inputSegStr);
#endif

    noder.computeNodes(inputSegStr);
}





} // namespace geos.noding
} // namespace geos
