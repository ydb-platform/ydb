/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: precision/EnhancedPrecisionOp.java rev. 1.9 (JTS-1.7)
 *
 **********************************************************************/

#include <geos/precision/EnhancedPrecisionOp.h>
#include <geos/precision/CommonBitsOp.h>
#include <geos/precision/CommonBitsRemover.h> // for unique_ptr composition
#include <geos/geom/Geometry.h>
#include <geos/util/GEOSException.h>

#ifndef GEOS_DEBUG
#define GEOS_DEBUG 0
#endif

#ifdef GEOS_DEBUG
#include <iostream>
#endif

using namespace geos::geom;

namespace geos {
namespace precision { // geos.precision

/*public static*/
std::unique_ptr<Geometry>
EnhancedPrecisionOp::intersection(
    const Geometry* geom0,
    const Geometry* geom1)
{
    util::GEOSException originalEx;

    try {
        return geom0->intersection(geom1);
    }
    catch(const util::GEOSException& ex) {
        originalEx = ex;
    }

    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem). Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
        CommonBitsOp cbo(true);
        auto resultEP = cbo.intersection(geom0, geom1);

        // check that result is a valid geometry after
        // the reshift to orginal precision
        if(! resultEP->isValid()) {
#if GEOS_DEBUG
            std::cerr << "Reduced operation result is invalid"
                      << std::endl;
#endif
            throw originalEx;
        }
        return resultEP;
    }
    catch(const util::GEOSException& ex2) {
#if GEOS_DEBUG
        std::cerr << "Reduced operation exception: "
                  << ex2.what() << std::endl;
#else
        (void)ex2;
#endif
        throw originalEx;
    }
}

/*public static*/
std::unique_ptr<Geometry>
EnhancedPrecisionOp::Union(
    const Geometry* geom0,
    const Geometry* geom1)
{
    util::GEOSException originalEx;
    try {
        return geom0->Union(geom1);
    }
    catch(const util::GEOSException& ex) {
        originalEx = ex;
    }

    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem)->  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
        CommonBitsOp cbo(true);
        auto resultEP = cbo.Union(geom0, geom1);

        // check that result is a valid geometry after
        // the reshift to orginal precision
        if(! resultEP->isValid()) {
            throw originalEx;
        }
        return resultEP;
    }
    catch(const util::GEOSException& /* ex2 */) {
        throw originalEx;
    }
}

/*public static*/
std::unique_ptr<Geometry>
EnhancedPrecisionOp::difference(
    const Geometry* geom0,
    const Geometry* geom1)
{
    util::GEOSException originalEx;

    try {
        return geom0->difference(geom1);
    }
    catch(const util::GEOSException& ex) {
        originalEx = ex;
    }

    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
        CommonBitsOp cbo(true);
        auto resultEP = cbo.difference(geom0, geom1);

        // check that result is a valid geometry after
        // the reshift to orginal precision
        if(! resultEP->isValid()) {
            throw originalEx;
        }
        return resultEP;
    }
    catch(const util::GEOSException& /* ex2 */) {
        throw originalEx;
    }
}

/*public static*/
std::unique_ptr<Geometry>
EnhancedPrecisionOp::symDifference(
    const Geometry* geom0,
    const Geometry* geom1)
{
    util::GEOSException originalEx;
    try {
        return geom0->symDifference(geom1);
    }
    catch(const util::GEOSException& ex) {
        originalEx = ex;
    }

    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem).  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
        CommonBitsOp cbo(true);
        auto resultEP = cbo.symDifference(geom0, geom1);

        // check that result is a valid geometry after
        // the reshift to orginal precision
        if(! resultEP->isValid()) {
            throw originalEx;
        }
        return resultEP;
    }
    catch(const util::GEOSException& /* ex2 */) {
        throw originalEx;
    }
}

/*public static*/
std::unique_ptr<Geometry>
EnhancedPrecisionOp::buffer(const Geometry* geom, double distance)
{
    util::GEOSException originalEx;
    try {
        return geom->buffer(distance);
    }
    catch(const util::GEOSException& ex) {
        originalEx = ex;
    }

    /*
     * If we are here, the original op encountered a precision problem
     * (or some other problem)->  Retry the operation with
     * enhanced precision to see if it succeeds
     */
    try {
        CommonBitsOp cbo(true);
        auto resultEP = cbo.buffer(geom, distance);

        // check that result is a valid geometry
        // after the reshift to orginal precision
        if(! resultEP->isValid()) {
            throw originalEx;
        }
        return resultEP;
    }
    catch(const util::GEOSException& /* ex2 */) {
        throw originalEx;
    }
}

} // namespace geos.precision
} // namespace geos

