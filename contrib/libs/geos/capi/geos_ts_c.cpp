/************************************************************************
 *
 *
 * C-Wrapper for GEOS library
 *
 * Copyright (C) 2005-2006 Refractions Research Inc.
 * Copyright (C) 2010-2012 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2016-2019 Daniel Baston <dbaston@gmail.com>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 * Author: Sandro Santilli <strk@kbt.io>
 * Thread Safety modifications: Chuck Thibert <charles.thibert@ingres.com>
 *
 ***********************************************************************/

#include <geos/geom/Coordinate.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/prep/PreparedGeometry.h>
#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Point.h>
#include <geos/geom/MultiPoint.h>
#include <geos/geom/MultiLineString.h>
#include <geos/geom/MultiPolygon.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/LineSegment.h>
#include <geos/geom/LineString.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/CoordinateSequenceFactory.h>
#include <geos/geom/FixedSizeCoordinateSequence.h>
#include <geos/geom/Coordinate.h>
#include <geos/geom/IntersectionMatrix.h>
#include <geos/geom/Envelope.h>
#include <geos/index/strtree/SimpleSTRtree.h>
#include <geos/index/strtree/GeometryItemDistance.h>
#include <geos/index/ItemVisitor.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/io/WKBWriter.h>
#include <geos/algorithm/BoundaryNodeRule.h>
#include <geos/algorithm/MinimumBoundingCircle.h>
#include <geos/algorithm/MinimumDiameter.h>
#include <geos/algorithm/Orientation.h>
#include <geos/algorithm/construct/MaximumInscribedCircle.h>
#include <geos/algorithm/construct/LargestEmptyCircle.h>
#include <geos/algorithm/distance/DiscreteHausdorffDistance.h>
#include <geos/algorithm/distance/DiscreteFrechetDistance.h>
#include <geos/simplify/DouglasPeuckerSimplifier.h>
#include <geos/simplify/TopologyPreservingSimplifier.h>
#include <geos/noding/GeometryNoder.h>
#include <geos/noding/Noder.h>
#include <geos/operation/buffer/BufferBuilder.h>
#include <geos/operation/buffer/BufferOp.h>
#include <geos/operation/buffer/BufferParameters.h>
#include <geos/operation/distance/DistanceOp.h>
#include <geos/operation/distance/IndexedFacetDistance.h>
#include <geos/operation/linemerge/LineMerger.h>
#include <geos/operation/overlay/OverlayOp.h>
#include <geos/operation/overlay/snap/GeometrySnapper.h>
#include <geos/operation/overlayng/PrecisionReducer.h>
#include <geos/operation/overlayng/OverlayNG.h>
#include <geos/operation/overlayng/OverlayNGRobust.h>
#include <geos/operation/overlayng/UnaryUnionNG.h>
#include <geos/operation/intersection/Rectangle.h>
#include <geos/operation/intersection/RectangleIntersection.h>
#include <geos/operation/polygonize/Polygonizer.h>
#include <geos/operation/polygonize/BuildArea.h>
#include <geos/operation/relate/RelateOp.h>
#include <geos/operation/sharedpaths/SharedPathsOp.h>
#include <geos/operation/union/CascadedPolygonUnion.h>
#include <geos/operation/union/CoverageUnion.h>
#include <geos/operation/valid/IsValidOp.h>
#include <geos/operation/valid/MakeValid.h>
#include <geos/precision/GeometryPrecisionReducer.h>
#include <geos/linearref/LengthIndexedLine.h>
#include <geos/triangulate/DelaunayTriangulationBuilder.h>
#include <geos/triangulate/VoronoiDiagramBuilder.h>
#include <geos/util.h>
#include <geos/util/IllegalArgumentException.h>
#include <geos/util/Interrupt.h>
#include <geos/util/UniqueCoordinateArrayFilter.h>
#include <geos/util/Machine.h>
#include <geos/version.h>

// This should go away
#include <cmath> // finite
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <memory>

#ifdef _MSC_VER
#pragma warning(disable : 4099)
#endif

// Some extra magic to make type declarations in geos_c.h work -
// for cross-checking of types in header.
#define GEOSGeometry geos::geom::Geometry
#define GEOSPreparedGeometry geos::geom::prep::PreparedGeometry
#define GEOSCoordSequence geos::geom::CoordinateSequence
#define GEOSBufferParams geos::operation::buffer::BufferParameters
#define GEOSSTRtree geos::index::strtree::SimpleSTRtree
#define GEOSWKTReader geos::io::WKTReader
#define GEOSWKTWriter geos::io::WKTWriter
#define GEOSWKBReader geos::io::WKBReader
#define GEOSWKBWriter geos::io::WKBWriter

#include "geos_c.h"

// Intentional, to allow non-standard C elements like C99 functions to be
// imported through C++ headers of C library, like <cmath>.
using namespace std;

/// Define this if you want operations triggering Exceptions to
/// be printed.
/// (will use the NOTIFY channel - only implemented for GEOSUnion so far)
///
#undef VERBOSE_EXCEPTIONS

#include <geos/export.h>
#include <geos/precision/MinimumClearance.h>


// import the most frequently used definitions globally
using geos::geom::Geometry;
using geos::geom::LineString;
using geos::geom::LinearRing;
using geos::geom::MultiLineString;
using geos::geom::MultiPolygon;
using geos::geom::Polygon;
using geos::geom::PrecisionModel;
using geos::geom::CoordinateSequence;
using geos::geom::GeometryCollection;
using geos::geom::GeometryFactory;

using geos::io::WKTReader;
using geos::io::WKTWriter;
using geos::io::WKBReader;
using geos::io::WKBWriter;

using geos::algorithm::distance::DiscreteFrechetDistance;
using geos::algorithm::distance::DiscreteHausdorffDistance;

using geos::operation::buffer::BufferBuilder;
using geos::operation::buffer::BufferParameters;
using geos::operation::distance::IndexedFacetDistance;
using geos::operation::geounion::CascadedPolygonUnion;
using geos::operation::overlayng::OverlayNG;
using geos::operation::overlayng::UnaryUnionNG;
using geos::operation::overlayng::OverlayNGRobust;

using geos::precision::GeometryPrecisionReducer;

using geos::util::IllegalArgumentException;

typedef std::unique_ptr<Geometry> GeomPtr;

typedef struct GEOSContextHandle_HS {
    const GeometryFactory* geomFactory;
    char msgBuffer[1024];
    GEOSMessageHandler noticeMessageOld;
    GEOSMessageHandler_r noticeMessageNew;
    void* noticeData;
    GEOSMessageHandler errorMessageOld;
    GEOSMessageHandler_r errorMessageNew;
    void* errorData;
    int WKBOutputDims;
    int WKBByteOrder;
    int initialized;

    GEOSContextHandle_HS()
        :
        geomFactory(nullptr),
        noticeMessageOld(nullptr),
        noticeMessageNew(nullptr),
        noticeData(nullptr),
        errorMessageOld(nullptr),
        errorMessageNew(nullptr),
        errorData(nullptr)
    {
        memset(msgBuffer, 0, sizeof(msgBuffer));
        geomFactory = GeometryFactory::getDefaultInstance();
        WKBOutputDims = 2;
        WKBByteOrder = getMachineByteOrder();
        setNoticeHandler(nullptr);
        setErrorHandler(nullptr);
        initialized = 1;
    }

    GEOSMessageHandler
    setNoticeHandler(GEOSMessageHandler nf)
    {
        GEOSMessageHandler f = noticeMessageOld;
        noticeMessageOld = nf;
        noticeMessageNew = nullptr;
        noticeData = nullptr;

        return f;
    }

    GEOSMessageHandler
    setErrorHandler(GEOSMessageHandler nf)
    {
        GEOSMessageHandler f = errorMessageOld;
        errorMessageOld = nf;
        errorMessageNew = nullptr;
        errorData = nullptr;

        return f;
    }

    GEOSMessageHandler_r
    setNoticeHandler(GEOSMessageHandler_r nf, void* userData)
    {
        GEOSMessageHandler_r f = noticeMessageNew;
        noticeMessageOld = nullptr;
        noticeMessageNew = nf;
        noticeData = userData;

        return f;
    }

    GEOSMessageHandler_r
    setErrorHandler(GEOSMessageHandler_r ef, void* userData)
    {
        GEOSMessageHandler_r f = errorMessageNew;
        errorMessageOld = nullptr;
        errorMessageNew = ef;
        errorData = userData;

        return f;
    }

    void
    NOTICE_MESSAGE(const char *fmt, ...)
    {
        if(nullptr == noticeMessageOld && nullptr == noticeMessageNew) {
            return;
        }

        va_list args;
        va_start(args, fmt);
        int result = vsnprintf(msgBuffer, sizeof(msgBuffer) - 1, fmt, args);
        va_end(args);

        if(result > 0) {
            if(noticeMessageOld) {
                noticeMessageOld("%s", msgBuffer);
            }
            else {
                noticeMessageNew(msgBuffer, noticeData);
            }
        }
    }

    void
    ERROR_MESSAGE(const char *fmt, ...)
    {
        if(nullptr == errorMessageOld && nullptr == errorMessageNew) {
            return;
        }

        va_list args;
        va_start(args, fmt);
        int result = vsnprintf(msgBuffer, sizeof(msgBuffer) - 1, fmt, args);
        va_end(args);

        if(result > 0) {
            if(errorMessageOld) {
                errorMessageOld("%s", msgBuffer);
            }
            else {
                errorMessageNew(msgBuffer, errorData);
            }
        }
    }
} GEOSContextHandleInternal_t;

// CAPI_ItemVisitor is used internally by the CAPI STRtree
// wrappers. It's defined here just to keep it out of the
// extern "C" block.
class CAPI_ItemVisitor : public geos::index::ItemVisitor {
    GEOSQueryCallback callback;
    void* userdata;
public:
    CAPI_ItemVisitor(GEOSQueryCallback cb, void* ud)
        : ItemVisitor(), callback(cb), userdata(ud) {}
    void
    visitItem(void* item) override
    {
        callback(item, userdata);
    }
};


//## PROTOTYPES #############################################

extern "C" const char GEOS_DLL* GEOSjtsport();
extern "C" char GEOS_DLL* GEOSasText(Geometry* g1);


namespace { // anonymous

char*
gstrdup_s(const char* str, const std::size_t size)
{
    char* out = static_cast<char*>(malloc(size + 1));
    if(nullptr != out) {
        // as no strlen call necessary, memcpy may be faster than strcpy
        std::memcpy(out, str, size + 1);
    }

    assert(nullptr != out);

    // we haven't been checking allocation before ticket #371
    if(nullptr == out) {
        throw(std::runtime_error("Failed to allocate memory for duplicate string"));
    }

    return out;
}

char*
gstrdup(std::string const& str)
{
    return gstrdup_s(str.c_str(), str.size());
}

} // namespace anonymous

// Execute a lambda, using the given context handle to process errors.
// Return errval on error.
// Errval should be of the type returned by f, unless f returns a bool in which case we promote to char.
template<typename F>
inline auto execute(
        GEOSContextHandle_t extHandle,
        typename std::conditional<std::is_same<decltype(std::declval<F>()()),bool>::value,
                                  char,
                                  decltype(std::declval<F>()())>::type errval,
        F&& f) -> decltype(errval) {
    if (extHandle == nullptr) {
        return errval;
    }

    GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
    if (!handle->initialized) {
        return errval;
    }

    try {
        return f();
    } catch (const std::exception& e) {
        handle->ERROR_MESSAGE("%s", e.what());
    } catch (...) {
        handle->ERROR_MESSAGE("Unknown exception thrown");
    }

    return errval;
}

// Execute a lambda, using the given context handle to process errors.
// Return nullptr on error.
template<typename F, typename std::enable_if<!std::is_void<decltype(std::declval<F>()())>::value, std::nullptr_t>::type = nullptr>
inline auto execute(GEOSContextHandle_t extHandle, F&& f) -> decltype(f()) {
    if (extHandle == nullptr) {
        return nullptr;
    }

    GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
    if (!handle->initialized) {
        return nullptr;
    }

    try {
        return f();
    } catch (const std::exception& e) {
        handle->ERROR_MESSAGE("%s", e.what());
    } catch (...) {
        handle->ERROR_MESSAGE("Unknown exception thrown");
    }

    return nullptr;
}

// Execute a lambda, using the given context handle to process errors.
// No return value.
template<typename F, typename std::enable_if<std::is_void<decltype(std::declval<F>()())>::value, std::nullptr_t>::type = nullptr>
inline void execute(GEOSContextHandle_t extHandle, F&& f) {
    GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
    try {
        f();
    } catch (const std::exception& e) {
        handle->ERROR_MESSAGE("%s", e.what());
    } catch (...) {
        handle->ERROR_MESSAGE("Unknown exception thrown");
    }
}

extern "C" {

    GEOSContextHandle_t
    initGEOS_r(GEOSMessageHandler nf, GEOSMessageHandler ef)
    {
        GEOSContextHandle_t handle = GEOS_init_r();

        if(nullptr != handle) {
            GEOSContext_setNoticeHandler_r(handle, nf);
            GEOSContext_setErrorHandler_r(handle, ef);
        }

        return handle;
    }

    GEOSContextHandle_t
    GEOS_init_r()
    {
        GEOSContextHandleInternal_t* handle = new GEOSContextHandleInternal_t();

        geos::util::Interrupt::cancel();

        return static_cast<GEOSContextHandle_t>(handle);
    }

    GEOSMessageHandler
    GEOSContext_setNoticeHandler_r(GEOSContextHandle_t extHandle, GEOSMessageHandler nf)
    {
        GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
        if(0 == handle->initialized) {
            return nullptr;
        }

        return handle->setNoticeHandler(nf);
    }

    GEOSMessageHandler
    GEOSContext_setErrorHandler_r(GEOSContextHandle_t extHandle, GEOSMessageHandler nf)
    {
        GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
        if(0 == handle->initialized) {
            return nullptr;
        }

        return handle->setErrorHandler(nf);
    }

    GEOSMessageHandler_r
    GEOSContext_setNoticeMessageHandler_r(GEOSContextHandle_t extHandle, GEOSMessageHandler_r nf, void* userData)
    {
        GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
        if(0 == handle->initialized) {
            return nullptr;
        }

        return handle->setNoticeHandler(nf, userData);
    }

    GEOSMessageHandler_r
    GEOSContext_setErrorMessageHandler_r(GEOSContextHandle_t extHandle, GEOSMessageHandler_r ef, void* userData)
    {
        GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
        if(0 == handle->initialized) {
            return nullptr;
        }

        return handle->setErrorHandler(ef, userData);
    }

    void
    finishGEOS_r(GEOSContextHandle_t extHandle)
    {
        // Fix up freeing handle w.r.t. malloc above
        delete extHandle;
        extHandle = nullptr;
    }

    void
    GEOS_finish_r(GEOSContextHandle_t extHandle)
    {
        finishGEOS_r(extHandle);
    }

    void
    GEOSFree_r(GEOSContextHandle_t extHandle, void* buffer)
    {
        assert(nullptr != extHandle);
        geos::ignore_unused_variable_warning(extHandle);

        free(buffer);
    }

//-----------------------------------------------------------
// relate()-related functions
//  return 0 = false, 1 = true, 2 = error occurred
//-----------------------------------------------------------

    char
    GEOSDisjoint_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->disjoint(g2);
        });
    }

    char
    GEOSTouches_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->touches(g2);
        });
    }

    char
    GEOSIntersects_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->intersects(g2);
        });
    }

    char
    GEOSCrosses_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->crosses(g2);
        });
    }

    char
    GEOSWithin_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->within(g2);
        });
    }

    char
    GEOSContains_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->contains(g2);
        });
    }

    char
    GEOSOverlaps_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->overlaps(g2);
        });
    }

    char
    GEOSCovers_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->covers(g2);
        });
    }

    char
    GEOSCoveredBy_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->coveredBy(g2);
        });
    }


//-------------------------------------------------------------------
// low-level relate functions
//------------------------------------------------------------------

    char
    GEOSRelatePattern_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, const char* pat)
    {
        return execute(extHandle, 2, [&]() {
            std::string s(pat);
            return g1->relate(g2, s);
        });
    }

    char
    GEOSRelatePatternMatch_r(GEOSContextHandle_t extHandle, const char* mat,
                             const char* pat)
    {
        return execute(extHandle, 2, [&]() {
            using geos::geom::IntersectionMatrix;

            std::string m(mat);
            std::string p(pat);
            IntersectionMatrix im(m);

            return im.matches(p);
        });
    }

    char*
    GEOSRelate_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() {
            using geos::geom::IntersectionMatrix;

            auto im = g1->relate(g2);
            if(im == nullptr) {
                return (char*) nullptr;
            }

            return gstrdup(im->toString());
        });
    }

    char*
    GEOSRelateBoundaryNodeRule_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, int bnr)
    {
        using geos::operation::relate::RelateOp;
        using geos::geom::IntersectionMatrix;
        using geos::algorithm::BoundaryNodeRule;

        return execute(extHandle, [&]() -> char* {
            std::unique_ptr<IntersectionMatrix> im;

            switch (bnr) {
                case GEOSRELATE_BNR_MOD2: /* same as OGC */
                    im = RelateOp::relate(g1, g2,
                                          BoundaryNodeRule::getBoundaryRuleMod2());
                    break;
                case GEOSRELATE_BNR_ENDPOINT:
                    im = RelateOp::relate(g1, g2,
                                          BoundaryNodeRule::getBoundaryEndPoint());
                    break;
                case GEOSRELATE_BNR_MULTIVALENT_ENDPOINT:
                    im = RelateOp::relate(g1, g2,
                                          BoundaryNodeRule::getBoundaryMultivalentEndPoint());
                    break;
                case GEOSRELATE_BNR_MONOVALENT_ENDPOINT:
                    im = RelateOp::relate(g1, g2,
                                          BoundaryNodeRule::getBoundaryMonovalentEndPoint());
                    break;
                default:
                    std::ostringstream ss;
                    ss << "Invalid boundary node rule " << bnr;
                    throw std::runtime_error(ss.str());
            }

            if(!im) {
                return nullptr;
            }

            char* result = gstrdup(im->toString());

            return result;
        });
    }



//-----------------------------------------------------------------
// isValid
//-----------------------------------------------------------------


    char
    GEOSisValid_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, 2, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            using geos::operation::valid::IsValidOp;
            using geos::operation::valid::TopologyValidationError;

            IsValidOp ivo(g1);
            TopologyValidationError* err = ivo.getValidationError();

            if(err) {
                handle->NOTICE_MESSAGE("%s", err->toString().c_str());
                return false;
            }
            else {
                return true;
            }
        });
    }

    char*
    GEOSisValidReason_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            using geos::operation::valid::IsValidOp;
            using geos::operation::valid::TopologyValidationError;

            char* result = nullptr;
            char const* const validstr = "Valid Geometry";

            IsValidOp ivo(g1);
            TopologyValidationError* err = ivo.getValidationError();

            if(err) {
                std::ostringstream ss;
                ss.precision(15);
                ss << err->getCoordinate();
                const std::string errloc = ss.str();
                std::string errmsg(err->getMessage());
                errmsg += "[" + errloc + "]";
                result = gstrdup(errmsg);
            }
            else {
                result = gstrdup(std::string(validstr));
            }

            return result;
        });
    }

    char
    GEOSisValidDetail_r(GEOSContextHandle_t extHandle, const Geometry* g,
                        int flags, char** reason, Geometry** location)
    {
        using geos::operation::valid::IsValidOp;
        using geos::operation::valid::TopologyValidationError;

        return execute(extHandle, 2, [&]() {
            IsValidOp ivo(g);
            if(flags & GEOSVALID_ALLOW_SELFTOUCHING_RING_FORMING_HOLE) {
                ivo.setSelfTouchingRingFormingHoleValid(true);
            }
            TopologyValidationError* err = ivo.getValidationError();
            if(err != nullptr) {
                if(location) {
                    *location = g->getFactory()->createPoint(err->getCoordinate());
                }
                if(reason) {
                    std::string errmsg(err->getMessage());
                    *reason = gstrdup(errmsg);
                }
                return false;
            }

            if(location) {
                *location = nullptr;
            }
            if(reason) {
                *reason = nullptr;
            }
            return true; /* valid */

        });
    }

//-----------------------------------------------------------------
// general purpose
//-----------------------------------------------------------------

    char
    GEOSEquals_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, 2, [&]() {
            return g1->equals(g2);
        });
    }

    char
    GEOSEqualsExact_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double tolerance)
    {
        return execute(extHandle, 2, [&]() {
            return g1->equalsExact(g2, tolerance);
        });
    }

    int
    GEOSDistance_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = g1->distance(g2);
            return 1;
        });
    }

    int
    GEOSDistanceIndexed_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = IndexedFacetDistance::distance(g1, g2);
            return 1;
        });
    }

    int
    GEOSHausdorffDistance_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = DiscreteHausdorffDistance::distance(*g1, *g2);
            return 1;
        });
    }

    int
    GEOSHausdorffDistanceDensify_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2,
                                   double densifyFrac, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = DiscreteHausdorffDistance::distance(*g1, *g2, densifyFrac);
            return 1;
        });
    }

    int
    GEOSFrechetDistance_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = DiscreteFrechetDistance::distance(*g1, *g2);
            return 1;
        });
    }

    int
    GEOSFrechetDistanceDensify_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double densifyFrac,
                                 double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = DiscreteFrechetDistance::distance(*g1, *g2, densifyFrac);
            return 1;
        });
    }

    int
    GEOSArea_r(GEOSContextHandle_t extHandle, const Geometry* g, double* area)
    {
        return execute(extHandle, 0, [&]() {
            *area = g->getArea();
            return 1;
        });
    }

    int
    GEOSLength_r(GEOSContextHandle_t extHandle, const Geometry* g, double* length)
    {
        return execute(extHandle, 0, [&]() {
            *length = g->getLength();
            return 1;
        });
    }

    CoordinateSequence*
    GEOSNearestPoints_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() -> CoordinateSequence* {
            if(g1->isEmpty() || g2->isEmpty()) {
                return nullptr;
            }
            return geos::operation::distance::DistanceOp::nearestPoints(g1, g2).release();
        });
    }


    Geometry*
    GEOSGeomFromWKT_r(GEOSContextHandle_t extHandle, const char* wkt)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            const std::string wktstring(wkt);
            WKTReader r(static_cast<GeometryFactory const*>(handle->geomFactory));

            auto g = r.read(wktstring);
            return g.release();
        });
    }

    char*
    GEOSGeomToWKT_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            char* result = gstrdup(g1->toString());
            return result;
        });
    }

    // Remember to free the result!
    unsigned char*
    GEOSGeomToWKB_buf_r(GEOSContextHandle_t extHandle, const Geometry* g, size_t* size)
    {
        using geos::io::WKBWriter;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            int byteOrder = handle->WKBByteOrder;
            WKBWriter w(handle->WKBOutputDims, byteOrder);
            std::ostringstream os(std::ios_base::binary);
            w.write(*g, os);
            std::string wkbstring(os.str());
            const std::size_t len = wkbstring.length();

            unsigned char* result = static_cast<unsigned char*>(malloc(len));
            if(result) {
                std::memcpy(result, wkbstring.c_str(), len);
                *size = len;
            }
            return result;
        });
    }

    Geometry*
    GEOSGeomFromWKB_buf_r(GEOSContextHandle_t extHandle, const unsigned char* wkb, size_t size)
    {
        using geos::io::WKBReader;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            std::string wkbstring(reinterpret_cast<const char*>(wkb), size); // make it binary !
            WKBReader r(*(static_cast<GeometryFactory const*>(handle->geomFactory)));
            std::istringstream is(std::ios_base::binary);
            is.str(wkbstring);
            is.seekg(0, std::ios::beg); // rewind reader pointer
            auto g = r.read(is);
            return g.release();
        });
    }

    /* Read/write wkb hex values.  Returned geometries are
       owned by the caller.*/
    unsigned char*
    GEOSGeomToHEX_buf_r(GEOSContextHandle_t extHandle, const Geometry* g, size_t* size)
    {
        using geos::io::WKBWriter;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            int byteOrder = handle->WKBByteOrder;
            WKBWriter w(handle->WKBOutputDims, byteOrder);
            std::ostringstream os(std::ios_base::binary);
            w.writeHEX(*g, os);
            std::string hexstring(os.str());

            char* result = gstrdup(hexstring);
            if(result) {
                *size = hexstring.length();
            }

            return reinterpret_cast<unsigned char*>(result);
        });
    }

    Geometry*
    GEOSGeomFromHEX_buf_r(GEOSContextHandle_t extHandle, const unsigned char* hex, size_t size)
    {
        using geos::io::WKBReader;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            std::string hexstring(reinterpret_cast<const char*>(hex), size);
            WKBReader r(*(static_cast<GeometryFactory const*>(handle->geomFactory)));
            std::istringstream is(std::ios_base::binary);
            is.str(hexstring);
            is.seekg(0, std::ios::beg); // rewind reader pointer

            auto g = r.readHEX(is);
            return g.release();
        });
    }

    char
    GEOSisEmpty_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, 2, [&]() {
            return g1->isEmpty();
        });
    }

    char
    GEOSisSimple_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, 2, [&]() {
            return g1->isSimple();
        });
    }

    char
    GEOSisRing_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g);
            if(ls) {
                return ls->isRing();
            }
            else {
                return false;
            }
        });
    }

    //free the result of this
    char*
    GEOSGeomType_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            std::string s = g1->getGeometryType();

            char* result = gstrdup(s);
            return result;
        });
    }

    // Return postgis geometry type index
    int
    GEOSGeomTypeId_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, -1, [&]() {
            return static_cast<int>(g1->getGeometryTypeId());
        });
    }

//-------------------------------------------------------------------
// GEOS functions that return geometries
//-------------------------------------------------------------------

    Geometry*
    GEOSEnvelope_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            Geometry* g3 = g1->getEnvelope().release();
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    Geometry*
    GEOSIntersection_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->intersection(g2);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSIntersectionPrec_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double gridSize)
    {
        return execute(extHandle, [&]() {
            using geos::geom::PrecisionModel;

            std::unique_ptr<PrecisionModel> pm;
            if(gridSize != 0) {
                pm.reset(new PrecisionModel(1.0 / gridSize));
            }
            else {
                pm.reset(new PrecisionModel());
            }
            auto g3 = gridSize != 0 ?
              OverlayNG::overlay(g1, g2, OverlayNG::INTERSECTION, pm.get())
              :
              OverlayNGRobust::Overlay(g1, g2, OverlayNG::INTERSECTION);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSBuffer_r(GEOSContextHandle_t extHandle, const Geometry* g1, double width, int quadrantsegments)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->buffer(width, quadrantsegments);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSBufferWithStyle_r(GEOSContextHandle_t extHandle, const Geometry* g1, double width, int quadsegs, int endCapStyle,
                          int joinStyle, double mitreLimit)
    {
        using geos::operation::buffer::BufferParameters;
        using geos::operation::buffer::BufferOp;
        using geos::util::IllegalArgumentException;

        return execute(extHandle, [&]() {
            BufferParameters bp;
            bp.setQuadrantSegments(quadsegs);

            if(endCapStyle > BufferParameters::CAP_SQUARE) {
                throw IllegalArgumentException("Invalid buffer endCap style");
            }
            bp.setEndCapStyle(
                static_cast<BufferParameters::EndCapStyle>(endCapStyle)
            );

            if(joinStyle > BufferParameters::JOIN_BEVEL) {
                throw IllegalArgumentException("Invalid buffer join style");
            }
            bp.setJoinStyle(
                static_cast<BufferParameters::JoinStyle>(joinStyle)
            );
            bp.setMitreLimit(mitreLimit);
            BufferOp op(g1, bp);
            Geometry* g3 = op.getResultGeometry(width);
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    Geometry*
    GEOSOffsetCurve_r(GEOSContextHandle_t extHandle, const Geometry* g1, double width, int quadsegs, int joinStyle,
                      double mitreLimit)
    {
        return execute(extHandle, [&]() {
            BufferParameters bp;
            bp.setEndCapStyle(BufferParameters::CAP_FLAT);
            bp.setQuadrantSegments(quadsegs);

            if(joinStyle > BufferParameters::JOIN_BEVEL) {
                throw IllegalArgumentException("Invalid buffer join style");
            }
            bp.setJoinStyle(
                static_cast<BufferParameters::JoinStyle>(joinStyle)
            );
            bp.setMitreLimit(mitreLimit);

            bool isLeftSide = true;
            if(width < 0) {
                isLeftSide = false;
                width = -width;
            }
            BufferBuilder bufBuilder(bp);
            Geometry* g3 = bufBuilder.bufferLineSingleSided(g1, width, isLeftSide);
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    /* @deprecated in 3.3.0 */
    Geometry*
    GEOSSingleSidedBuffer_r(GEOSContextHandle_t extHandle, const Geometry* g1, double width, int quadsegs, int joinStyle,
                            double mitreLimit, int leftSide)
    {
        return execute(extHandle, [&]() {
            BufferParameters bp;
            bp.setEndCapStyle(BufferParameters::CAP_FLAT);
            bp.setQuadrantSegments(quadsegs);

            if(joinStyle > BufferParameters::JOIN_BEVEL) {
                throw IllegalArgumentException("Invalid buffer join style");
            }
            bp.setJoinStyle(
                static_cast<BufferParameters::JoinStyle>(joinStyle)
            );
            bp.setMitreLimit(mitreLimit);

            bool isLeftSide = leftSide == 0 ? false : true;
            BufferBuilder bufBuilder(bp);
            Geometry* g3 = bufBuilder.bufferLineSingleSided(g1, width, isLeftSide);
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    Geometry*
    GEOSConvexHull_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->convexHull();
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }


    Geometry*
    GEOSMinimumRotatedRectangle_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            geos::algorithm::MinimumDiameter m(g);
            auto g3 = m.getMinimumRectangle();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSMaximumInscribedCircle_r(GEOSContextHandle_t extHandle, const Geometry* g, double tolerance)
    {
        return execute(extHandle, [&]() {
            geos::algorithm::construct::MaximumInscribedCircle mic(g, tolerance);
            auto g3 = mic.getRadiusLine();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSLargestEmptyCircle_r(GEOSContextHandle_t extHandle, const Geometry* g, const GEOSGeometry* boundary, double tolerance)
    {
        return execute(extHandle, [&]() {
            geos::algorithm::construct::LargestEmptyCircle lec(g, boundary, tolerance);
            auto g3 = lec.getRadiusLine();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSMinimumWidth_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            geos::algorithm::MinimumDiameter m(g);
            auto g3 = m.getDiameter();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSMinimumClearanceLine_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            geos::precision::MinimumClearance mc(g);
            auto g3 = mc.getLine();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    int
    GEOSMinimumClearance_r(GEOSContextHandle_t extHandle, const Geometry* g, double* d)
    {
        return execute(extHandle, 2, [&]() {
            geos::precision::MinimumClearance mc(g);
            double res = mc.getDistance();
            *d = res;
            return 0;
        });
    }


    Geometry*
    GEOSDifference_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->difference(g2);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSDifferencePrec_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double gridSize)
    {
        return execute(extHandle, [&]() {

            std::unique_ptr<PrecisionModel> pm;
            if(gridSize != 0) {
                pm.reset(new PrecisionModel(1.0 / gridSize));
            }
            else {
                pm.reset(new PrecisionModel());
            }
            auto g3 = gridSize != 0 ?
                OverlayNG::overlay(g1, g2, OverlayNG::DIFFERENCE, pm.get())
                :
                OverlayNGRobust::Overlay(g1, g2, OverlayNG::DIFFERENCE);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSBoundary_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->getBoundary();
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSSymDifference_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->symDifference(g2);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSSymDifferencePrec_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double gridSize)
    {
        return execute(extHandle, [&]() {

            std::unique_ptr<PrecisionModel> pm;
            if(gridSize != 0) {
                pm.reset(new PrecisionModel(1.0 / gridSize));
            }
            else {
                pm.reset(new PrecisionModel());
            }
            auto g3 = gridSize != 0 ?
              OverlayNG::overlay(g1, g2, OverlayNG::SYMDIFFERENCE, pm.get())
              :
              OverlayNGRobust::Overlay(g1, g2, OverlayNG::SYMDIFFERENCE);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSUnion_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2)
    {
        return execute(extHandle, [&]() {
            auto g3 = g1->Union(g2);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSUnionPrec_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* g2, double gridSize)
    {
        return execute(extHandle, [&]() {

            std::unique_ptr<PrecisionModel> pm;
            if(gridSize != 0) {
                pm.reset(new PrecisionModel(1.0 / gridSize));
            }
            else {
                pm.reset(new PrecisionModel());
            }
            auto g3 = gridSize != 0 ?
              OverlayNG::overlay(g1, g2, OverlayNG::UNION, pm.get())
              :
              OverlayNGRobust::Overlay(g1, g2, OverlayNG::UNION);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSCoverageUnion_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            auto g3 = geos::operation::geounion::CoverageUnion::Union(g);
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSUnaryUnion_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            GeomPtr g3(g->Union());
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSUnaryUnionPrec_r(GEOSContextHandle_t extHandle, const Geometry* g1, double gridSize)
    {
        return execute(extHandle, [&]() {

            std::unique_ptr<PrecisionModel> pm;
            if(gridSize != 0) {
                pm.reset(new PrecisionModel(1.0 / gridSize));
            }
            else {
                pm.reset(new PrecisionModel());
            }
            auto g3 = gridSize != 0 ?
              UnaryUnionNG::Union(g1, *pm)
              :
              OverlayNGRobust::Union(g1);
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSNode_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            auto g3 = geos::noding::GeometryNoder::node(*g);
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSUnionCascaded_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        using geos::operation::geounion::CascadedPolygonUnion;

        return execute(extHandle, [&]() {
            const geos::geom::MultiPolygon *p = dynamic_cast<const geos::geom::MultiPolygon *>(g1);

            if (!p) {
                throw IllegalArgumentException("Invalid argument (must be a MultiPolygon)");
            }

            Geometry* g3 = CascadedPolygonUnion::Union(p);
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    Geometry*
    GEOSPointOnSurface_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            auto ret = g1->getInteriorPoint();
            if(ret == nullptr) {
                const GeometryFactory* gf = g1->getFactory();
                // return an empty point
                ret = gf->createPoint();
            }
            ret->setSRID(g1->getSRID());
            return ret.release();
        });
    }

    Geometry*
    GEOSClipByRect_r(GEOSContextHandle_t extHandle, const Geometry* g, double xmin, double ymin, double xmax, double ymax)
    {
        return execute(extHandle, [&]() {
            using geos::operation::intersection::Rectangle;
            using geos::operation::intersection::RectangleIntersection;
            Rectangle rect(xmin, ymin, xmax, ymax);
            std::unique_ptr<Geometry> g3 = RectangleIntersection::clip(*g, rect);
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

//-------------------------------------------------------------------
// memory management functions
//------------------------------------------------------------------

    void
    GEOSGeom_destroy_r(GEOSContextHandle_t extHandle, Geometry* a)
    {
        execute(extHandle, [&]() {
            // FIXME: mloskot: Does this try-catch around delete means that
            // destructors in GEOS may throw? If it does, this is a serious
            // violation of "never throw an exception from a destructor" principle
            delete a;
        });
    }

    void
    GEOSGeom_setUserData_r(GEOSContextHandle_t extHandle, Geometry* g, void* userData)
    {
        execute(extHandle, [&]() {
            g->setUserData(userData);
        });
    }

    void
    GEOSSetSRID_r(GEOSContextHandle_t extHandle, Geometry* g, int srid)
    {
        execute(extHandle, [&]() {
            g->setSRID(srid);
        });
    }


    int
    GEOSGetNumCoordinates_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, -1, [&]() {
            return static_cast<int>(g->getNumPoints());
        });
    }

    /*
     * Return -1 on exception, 0 otherwise.
     * Converts Geometry to normal form (or canonical form).
     */
    int
    GEOSNormalize_r(GEOSContextHandle_t extHandle, Geometry* g)
    {
        return execute(extHandle, -1, [&]() {
            g->normalize();
            return 0; // SUCCESS
        });
    }

    int
    GEOSGetNumInteriorRings_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, -1, [&]() {
            const Polygon* p = dynamic_cast<const Polygon*>(g1);
            if(!p) {
                throw IllegalArgumentException("Argument is not a Polygon");
            }
            return static_cast<int>(p->getNumInteriorRing());
        });
    }


    // returns -1 on error and 1 for non-multi geometries
    int
    GEOSGetNumGeometries_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, -1, [&]() {
            return static_cast<int>(g1->getNumGeometries());
        });
    }


    /*
     * Call only on GEOMETRYCOLLECTION or MULTI*.
     * Return a pointer to the internal Geometry.
     */
    const Geometry*
    GEOSGetGeometryN_r(GEOSContextHandle_t extHandle, const Geometry* g1, int n)
    {
        return execute(extHandle, [&]() {
            if(n < 0) {
                throw IllegalArgumentException("Index must be non-negative.");
            }
            return g1->getGeometryN(static_cast<size_t>(n));
        });
    }

    /*
     * Call only on LINESTRING
     * Returns NULL on exception
     */
    Geometry*
    GEOSGeomGetPointN_r(GEOSContextHandle_t extHandle, const Geometry* g1, int n)
    {
        using geos::geom::LineString;

        return execute(extHandle, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(!ls) {
                throw IllegalArgumentException("Argument is not a LineString");
            }
            if(n < 0) {
                throw IllegalArgumentException("Index must be non-negative.");
            }
            return ls->getPointN(static_cast<size_t>(n)).release();
        });
    }

    /*
     * Call only on LINESTRING
     */
    Geometry*
    GEOSGeomGetStartPoint_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        using geos::geom::LineString;

        return execute(extHandle, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(!ls) {
                throw IllegalArgumentException("Argument is not a LineString");
            }

            return ls->getStartPoint().release();
        });
    }

    /*
     * Call only on LINESTRING
     */
    Geometry*
    GEOSGeomGetEndPoint_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        using geos::geom::LineString;

        return execute(extHandle, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(!ls) {
                throw IllegalArgumentException("Argument is not a LineString");
            }
            return ls->getEndPoint().release();
        });
    }

    /*
     * Call only on LINESTRING or MULTILINESTRING
     * return 2 on exception, 1 on true, 0 on false
     */
    char
    GEOSisClosed_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        using geos::geom::LineString;
        using geos::geom::MultiLineString;

        return execute(extHandle, 2, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(ls) {
                return ls->isClosed();
            }

            const MultiLineString* mls = dynamic_cast<const MultiLineString*>(g1);
            if(mls) {
                return mls->isClosed();
            }

            throw IllegalArgumentException("Argument is not a LineString or MultiLineString");
        });
    }

    /*
     * Call only on LINESTRING
     * return 0 on exception, otherwise 1
     */
    int
    GEOSGeomGetLength_r(GEOSContextHandle_t extHandle, const Geometry* g1, double* length)
    {
        using geos::geom::LineString;

        return execute(extHandle, 0, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(!ls) {
                throw IllegalArgumentException("Argument is not a LineString");
            }
            *length = ls->getLength();
            return 1;
        });
    }

    /*
     * Call only on LINESTRING
     */
    int
    GEOSGeomGetNumPoints_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        using geos::geom::LineString;

        return execute(extHandle, -1, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g1);
            if(!ls) {
                throw IllegalArgumentException("Argument is not a LineString");
            }
            return static_cast<int>(ls->getNumPoints());
        });
    }

    /*
     * For POINT
     * returns 0 on exception, otherwise 1
     */
    int
    GEOSGeomGetX_r(GEOSContextHandle_t extHandle, const Geometry* g1, double* x)
    {
        using geos::geom::Point;

        return execute(extHandle, 0, [&]() {
            const Point* po = dynamic_cast<const Point*>(g1);
            if(!po) {
                throw IllegalArgumentException("Argument is not a Point");
            }
            *x = po->getX();
            return 1;
        });
    }

    /*
     * For POINT
     * returns 0 on exception, otherwise 1
     */
    int
    GEOSGeomGetY_r(GEOSContextHandle_t extHandle, const Geometry* g1, double* y)
    {
        using geos::geom::Point;

        return execute(extHandle, 0, [&]() {
            const Point* po = dynamic_cast<const Point*>(g1);
            if(!po) {
                throw IllegalArgumentException("Argument is not a Point");
            }
            *y = po->getY();
            return 1;
        });
    }

    /*
     * For POINT
     * returns 0 on exception, otherwise 1
     */
    int
    GEOSGeomGetZ_r(GEOSContextHandle_t extHandle, const Geometry* g1, double* z)
    {
        using geos::geom::Point;

        return execute(extHandle, 0, [&]() {
            const Point* po = dynamic_cast<const Point*>(g1);
            if(!po) {
                throw IllegalArgumentException("Argument is not a Point");
            }
            *z = po->getZ();
            return 1;
        });
    }

    /*
     * Call only on polygon
     * Return a pointer to the internal Geometry.
     */
    const Geometry*
    GEOSGetExteriorRing_r(GEOSContextHandle_t extHandle, const Geometry* g1)
    {
        return execute(extHandle, [&]() {
            const Polygon* p = dynamic_cast<const Polygon*>(g1);
            if(!p) {
                throw IllegalArgumentException("Invalid argument (must be a Polygon)");
            }
            return p->getExteriorRing();
        });
    }

    /*
     * Call only on polygon
     * Return a pointer to internal storage, do not destroy it.
     */
    const Geometry*
    GEOSGetInteriorRingN_r(GEOSContextHandle_t extHandle, const Geometry* g1, int n)
    {
        return execute(extHandle, [&]() {
            const Polygon* p = dynamic_cast<const Polygon*>(g1);
            if(!p) {
                throw IllegalArgumentException("Invalid argument (must be a Polygon)");
            }
            if(n < 0) {
                throw IllegalArgumentException("Index must be non-negative.");
            }
            return p->getInteriorRingN(static_cast<size_t>(n));
        });
    }

    Geometry*
    GEOSGetCentroid_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() -> Geometry* {
            auto ret = g->getCentroid();

            if(ret == nullptr) {
                // TODO check if getCentroid() can really return null
                const GeometryFactory* gf = g->getFactory();
                ret =  gf->createPoint();
            }
            ret->setSRID(g->getSRID());
            return ret.release();
        });
    }

    Geometry*
    GEOSMinimumBoundingCircle_r(GEOSContextHandle_t extHandle, const Geometry* g,
        double* radius, Geometry** center)
    {
        return execute(extHandle, [&]() -> Geometry* {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            geos::algorithm::MinimumBoundingCircle mc(g);
            std::unique_ptr<Geometry> ret = mc.getCircle();
            const GeometryFactory* gf = handle->geomFactory;
            if(!ret) {
                if (center) *center = NULL;
                if (radius) *radius = 0.0;
                return gf->createPolygon().release();
            }
            if (center) *center = static_cast<Geometry*>(gf->createPoint(mc.getCentre()));
            if (radius) *radius = mc.getRadius();
            ret->setSRID(g->getSRID());
            return ret.release();
        });
    }

    Geometry*
    GEOSGeom_createEmptyCollection_r(GEOSContextHandle_t extHandle, int type)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            std::unique_ptr<Geometry> g = 0;
            switch(type) {
            case GEOS_GEOMETRYCOLLECTION:
                g = gf->createGeometryCollection();
                break;
            case GEOS_MULTIPOINT:
                g = gf->createMultiPoint();
                break;
            case GEOS_MULTILINESTRING:
                g = gf->createMultiLineString();
                break;
            case GEOS_MULTIPOLYGON:
                g = gf->createMultiPolygon();
                break;
            default:
                throw IllegalArgumentException("Unsupported type request for GEOSGeom_createEmptyCollection_r");
            }

            return g.release();
        });
    }

    Geometry*
    GEOSGeom_createCollection_r(GEOSContextHandle_t extHandle, int type, Geometry** geoms, unsigned int ngeoms)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            const GeometryFactory* gf = handle->geomFactory;

            std::vector<std::unique_ptr<Geometry>> vgeoms(ngeoms);
            for (size_t i = 0; i < ngeoms; i++) {
                vgeoms[i].reset(geoms[i]);
            }

            std::unique_ptr<Geometry> g;
            switch(type) {
            case GEOS_GEOMETRYCOLLECTION:
                g = gf->createGeometryCollection(std::move(vgeoms));
                break;
            case GEOS_MULTIPOINT:
                g = gf->createMultiPoint(std::move(vgeoms));
                break;
            case GEOS_MULTILINESTRING:
                g = gf->createMultiLineString(std::move(vgeoms));
                break;
            case GEOS_MULTIPOLYGON:
                g = gf->createMultiPolygon(std::move(vgeoms));
                break;
            default:
                handle->ERROR_MESSAGE("Unsupported type request for PostGIS2GEOS_collection");
            }

            return g.release();
        });
    }

    Geometry*
    GEOSPolygonize_r(GEOSContextHandle_t extHandle, const Geometry* const* g, unsigned int ngeoms)
    {
        using geos::operation::polygonize::Polygonizer;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            // Polygonize
            Polygonizer plgnzr;
            for(std::size_t i = 0; i < ngeoms; ++i) {
                plgnzr.add(g[i]);
            }

            auto polys = plgnzr.getPolygons();
            const GeometryFactory* gf = handle->geomFactory;
            return gf->createGeometryCollection(std::move(polys)).release();
        });
    }

    Geometry*
    GEOSPolygonize_valid_r(GEOSContextHandle_t extHandle, const Geometry* const* g, unsigned int ngeoms)
    {
        using geos::operation::polygonize::Polygonizer;

        return execute(extHandle, [&]() -> Geometry* {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            Geometry* out;

            // Polygonize
            Polygonizer plgnzr(true);
            int srid = 0;
            for(std::size_t i = 0; i < ngeoms; ++i) {
                plgnzr.add(g[i]);
                srid = g[i]->getSRID();
            }

            auto polys = plgnzr.getPolygons();
            if (polys.empty()) {
                out = handle->geomFactory->createGeometryCollection().release();
            } else if (polys.size() == 1) {
                return polys[0].release();
            } else {
                return handle->geomFactory->createMultiPolygon(std::move(polys)).release();
            }

            out->setSRID(srid);
            return out;
        });
    }

    Geometry*
    GEOSBuildArea_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        using geos::operation::polygonize::BuildArea;

        return execute(extHandle, [&]() {
            BuildArea builder;
            auto out = builder.build(g);
            out->setSRID(g->getSRID());
            return out.release();
        });
    }

    Geometry*
    GEOSMakeValid_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        using geos::operation::valid::MakeValid;

        return execute(extHandle, [&]() {
            MakeValid makeValid;
            auto out = makeValid.build(g);
            out->setSRID(g->getSRID());
            return out.release();
        });
    }

    Geometry*
    GEOSPolygonizer_getCutEdges_r(GEOSContextHandle_t extHandle, const Geometry* const* g, unsigned int ngeoms)
    {
        using geos::operation::polygonize::Polygonizer;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;
            Geometry* out;

            // Polygonize
            Polygonizer plgnzr;
            int srid = 0;
            for(std::size_t i = 0; i < ngeoms; ++i) {
                plgnzr.add(g[i]);
                srid = g[i]->getSRID();
            }

            const std::vector<const LineString*>& lines = plgnzr.getCutEdges();

            // We need a vector of Geometry pointers, not Polygon pointers.
            // STL vector doesn't allow transparent upcast of this
            // nature, so we explicitly convert.
            // (it's just a waste of processor and memory, btw)
            // XXX mloskot: See comment for GEOSPolygonize_r

            // TODO avoid "new" here
            std::vector<Geometry*>* linevec = new std::vector<Geometry*>(lines.size());

            for(std::size_t i = 0, n = lines.size(); i < n; ++i) {
                (*linevec)[i] = lines[i]->clone().release();
            }

            // The below takes ownership of the passed vector,
            // so we must *not* delete it

            out = gf->createGeometryCollection(linevec);
            out->setSRID(srid);

            return out;
        });
    }

    Geometry*
    GEOSPolygonize_full_r(GEOSContextHandle_t extHandle, const Geometry* g,
                          Geometry** cuts, Geometry** dangles, Geometry** invalid)
    {
        using geos::operation::polygonize::Polygonizer;

        return execute(extHandle, [&]() {
            // Polygonize
            Polygonizer plgnzr;
            for(std::size_t i = 0; i < g->getNumGeometries(); ++i) {
                plgnzr.add(g->getGeometryN(i));
            }

            const GeometryFactory* gf = g->getFactory();

            if(cuts) {
                const std::vector<const LineString*>& lines = plgnzr.getCutEdges();
                std::vector<std::unique_ptr<Geometry>> linevec(lines.size());
                for(std::size_t i = 0, n = lines.size(); i < n; ++i) {
                    linevec[i] = lines[i]->clone();
                }

                *cuts = gf->createGeometryCollection(std::move(linevec)).release();
            }

            if(dangles) {
                const std::vector<const LineString*>& lines = plgnzr.getDangles();
                std::vector<std::unique_ptr<Geometry>> linevec(lines.size());
                for(std::size_t i = 0, n = lines.size(); i < n; ++i) {
                    linevec[i] = lines[i]->clone();
                }

                *dangles = gf->createGeometryCollection(std::move(linevec)).release();
            }

            if(invalid) {
                const std::vector<std::unique_ptr<LineString>>& lines = plgnzr.getInvalidRingLines();
                std::vector<std::unique_ptr<Geometry>> linevec(lines.size());
                for(std::size_t i = 0, n = lines.size(); i < n; ++i) {
                    linevec[i] = lines[i]->clone();
                }

                *invalid = gf->createGeometryCollection(std::move(linevec)).release();
            }

            auto polys = plgnzr.getPolygons();
            Geometry* out = gf->createGeometryCollection(std::move(polys)).release();
            out->setSRID(g->getSRID());
            return out;
        });
    }

    Geometry*
    GEOSLineMerge_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        using geos::operation::linemerge::LineMerger;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;
            LineMerger lmrgr;
            lmrgr.add(g);

            auto lines = lmrgr.getMergedLineStrings();

            auto out = gf->buildGeometry(std::move(lines));
            out->setSRID(g->getSRID());

            return out.release();
        });
    }

    Geometry*
    GEOSReverse_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            auto g3 = g->reverse();
            g3->setSRID(g->getSRID());
            return g3.release();
        });
    }

    void*
    GEOSGeom_getUserData_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            return g->getUserData();
        });
    }

    int
    GEOSGetSRID_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, 0, [&]() {
            return g->getSRID();
        });
    }

    const char* GEOSversion()
    {
        static char version[256];
        snprintf(version, sizeof(version), "%s", GEOS_CAPI_VERSION);
        return version;
    }

    const char* GEOSjtsport()
    {
        return GEOS_JTS_PORT;
    }

    char
    GEOSHasZ_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, -1, [&]() {
            if(g->isEmpty()) {
                return false;
            }

            double az = g->getCoordinate()->z;

            return std::isfinite(az);
        });
    }

    int
    GEOS_getWKBOutputDims_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, -1, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            return handle->WKBOutputDims;
        });
    }

    int
    GEOS_setWKBOutputDims_r(GEOSContextHandle_t extHandle, int newdims)
    {
        return execute(extHandle, -1, [&]() {
            GEOSContextHandleInternal_t *handle = reinterpret_cast<GEOSContextHandleInternal_t *>(extHandle);

            if (newdims < 2 || newdims > 3) {
                handle->ERROR_MESSAGE("WKB output dimensions out of range 2..3");
            }

            const int olddims = handle->WKBOutputDims;
            handle->WKBOutputDims = newdims;

            return olddims;
        });
    }

    int
    GEOS_getWKBByteOrder_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, -1, [&]() {
            GEOSContextHandleInternal_t *handle = reinterpret_cast<GEOSContextHandleInternal_t *>(extHandle);
            return handle->WKBByteOrder;
        });
    }

    int
    GEOS_setWKBByteOrder_r(GEOSContextHandle_t extHandle, int byteOrder)
    {
        return execute(extHandle, -1, [&]() {
            GEOSContextHandleInternal_t *handle = reinterpret_cast<GEOSContextHandleInternal_t *>(extHandle);
            const int oldByteOrder = handle->WKBByteOrder;
            handle->WKBByteOrder = byteOrder;

            return oldByteOrder;
        });
    }


    CoordinateSequence*
    GEOSCoordSeq_create_r(GEOSContextHandle_t extHandle, unsigned int size, unsigned int dims)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            switch (size) {
                case 1:
                    return static_cast<CoordinateSequence*>(new geos::geom::FixedSizeCoordinateSequence<1>(dims));
                case 2:
                    return static_cast<CoordinateSequence*>(new geos::geom::FixedSizeCoordinateSequence<2>(dims));
                default: {
                    const GeometryFactory *gf = handle->geomFactory;
                    return gf->getCoordinateSequenceFactory()->create(size, dims).release();
                }
            }
        });
    }

    int
    GEOSCoordSeq_setOrdinate_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs,
                               unsigned int idx, unsigned int dim, double val)
    {
        return execute(extHandle, 0, [&]() {
            cs->setOrdinate(idx, dim, val);
            return 1;
        });
    }

    int
    GEOSCoordSeq_setX_r(GEOSContextHandle_t extHandle, CoordinateSequence* s, unsigned int idx, double val)
    {
        return GEOSCoordSeq_setOrdinate_r(extHandle, s, idx, 0, val);
    }

    int
    GEOSCoordSeq_setY_r(GEOSContextHandle_t extHandle, CoordinateSequence* s, unsigned int idx, double val)
    {
        return GEOSCoordSeq_setOrdinate_r(extHandle, s, idx, 1, val);
    }

    int
    GEOSCoordSeq_setZ_r(GEOSContextHandle_t extHandle, CoordinateSequence* s, unsigned int idx, double val)
    {
        return GEOSCoordSeq_setOrdinate_r(extHandle, s, idx, 2, val);
    }

    int
    GEOSCoordSeq_setXY_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs, unsigned int idx, double x, double y)
    {
        return execute(extHandle, 0, [&]() {
            cs->setAt({x, y}, idx);
            return 1;
        });
    }

    int
    GEOSCoordSeq_setXYZ_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs, unsigned int idx, double x, double y, double z)
    {
        return execute(extHandle, 0, [&]() {
            cs->setAt({x, y, z}, idx);
            return 1;
        });
    }

    CoordinateSequence*
    GEOSCoordSeq_clone_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs)
    {
        return execute(extHandle, [&]() {
            return cs->clone().release();
        });
    }

    int
    GEOSCoordSeq_getOrdinate_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs,
                               unsigned int idx, unsigned int dim, double* val)
    {
        return execute(extHandle, 0, [&]() {
            *val = cs->getOrdinate(idx, dim);
            return 1;
        });
    }

    int
    GEOSCoordSeq_getX_r(GEOSContextHandle_t extHandle, const CoordinateSequence* s, unsigned int idx, double* val)
    {
        return GEOSCoordSeq_getOrdinate_r(extHandle, s, idx, 0, val);
    }

    int
    GEOSCoordSeq_getY_r(GEOSContextHandle_t extHandle, const CoordinateSequence* s, unsigned int idx, double* val)
    {
        return GEOSCoordSeq_getOrdinate_r(extHandle, s, idx, 1, val);
    }

    int
    GEOSCoordSeq_getZ_r(GEOSContextHandle_t extHandle, const CoordinateSequence* s, unsigned int idx, double* val)
    {
        return GEOSCoordSeq_getOrdinate_r(extHandle, s, idx, 2, val);
    }

    int
    GEOSCoordSeq_getXY_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs, unsigned int idx, double* x, double* y)
    {
        return execute(extHandle, 0, [&]() {
            auto& c = cs->getAt(idx);
            *x = c.x;
            *y = c.y;
            return 1;
        });
    }

    int
    GEOSCoordSeq_getXYZ_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs, unsigned int idx, double* x, double* y, double* z)
    {
        return execute(extHandle, 0, [&]() {
            auto& c = cs->getAt(idx);
            *x = c.x;
            *y = c.y;
            *z = c.z;
            return 1;
        });
    }

    int
    GEOSCoordSeq_getSize_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs, unsigned int* size)
    {
        return execute(extHandle, 0, [&]() {
            const std::size_t sz = cs->getSize();
            *size = static_cast<unsigned int>(sz);
            return 1;
        });
    }

    int
    GEOSCoordSeq_getDimensions_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs, unsigned int* dims)
    {
        return execute(extHandle, 0, [&]() {
            const std::size_t dim = cs->getDimension();
            *dims = static_cast<unsigned int>(dim);

            return 1;
        });
    }

    int
    GEOSCoordSeq_isCCW_r(GEOSContextHandle_t extHandle, const CoordinateSequence* cs, char* val)
    {
        return execute(extHandle, 0, [&]() {
            *val = geos::algorithm::Orientation::isCCW(cs);
            return 1;
        });
    }

    void
    GEOSCoordSeq_destroy_r(GEOSContextHandle_t extHandle, CoordinateSequence* s)
    {
        return execute(extHandle, [&]() {
            delete s;
        });
    }

    const CoordinateSequence*
    GEOSGeom_getCoordSeq_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        using geos::geom::Point;

        return execute(extHandle, [&]() {
            const LineString* ls = dynamic_cast<const LineString*>(g);
            if(ls) {
                return ls->getCoordinatesRO();
            }

            const Point* p = dynamic_cast<const Point*>(g);
            if(p) {
                return p->getCoordinatesRO();
            }

            throw IllegalArgumentException("Geometry must be a Point or LineString");
        });
    }

    Geometry*
    GEOSGeom_createEmptyPoint_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;
            return gf->createPoint().release();
        });
    }

    Geometry*
    GEOSGeom_createPoint_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            return gf->createPoint(cs);
        });
    }

    Geometry*
    GEOSGeom_createPointFromXY_r(GEOSContextHandle_t extHandle, double x, double y)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            geos::geom::Coordinate c(x, y);
            return gf->createPoint(c);
        });
    }

    Geometry*
    GEOSGeom_createLinearRing_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            return gf->createLinearRing(cs);
        });
    }

    Geometry*
    GEOSGeom_createEmptyLineString_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            return gf->createLineString().release();
        });
    }

    Geometry*
    GEOSGeom_createLineString_r(GEOSContextHandle_t extHandle, CoordinateSequence* cs)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;

            return gf->createLineString(cs);
        });
    }

    Geometry*
    GEOSGeom_createEmptyPolygon_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;
            return gf->createPolygon().release();
        });
    }

    Geometry*
    GEOSGeom_createPolygon_r(GEOSContextHandle_t extHandle, Geometry* shell, Geometry** holes, unsigned int nholes)
    {
        using geos::geom::LinearRing;

        // FIXME: holes must be non-nullptr or may be nullptr?
        //assert(0 != holes);

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            const GeometryFactory* gf = handle->geomFactory;
            bool good_holes = true, good_shell = true;

            // Validate input before taking ownership
            for (std::size_t i = 0; i < nholes; i++) {
                if ((!holes) || (!dynamic_cast<LinearRing*>(holes[i]))) {
                    good_holes = false;
                    break;
                }
            }
            if (!dynamic_cast<LinearRing*>(shell)) {
                good_shell = false;
            }

            // Contract for GEOSGeom_createPolygon is to take ownership of arguments
            // which implies freeing them on exception,
            // see https://trac.osgeo.org/geos/ticket/1111
            if (!(good_holes && good_shell)) {
                if (shell) delete shell;
                for (std::size_t i = 0; i < nholes; i++) {
                    if (holes && holes[i])
                        delete holes[i];
                }
                if (!good_shell)
                    throw IllegalArgumentException("Shell is not a LinearRing");
                else
                    throw IllegalArgumentException("Hole is not a LinearRing");
            }

            std::unique_ptr<LinearRing> tmpshell(static_cast<LinearRing*>(shell));
            if (nholes) {
                std::vector<std::unique_ptr<LinearRing>> tmpholes(nholes);
                for (size_t i = 0; i < nholes; i++) {
                    tmpholes[i].reset(static_cast<LinearRing*>(holes[i]));
                }
                return gf->createPolygon(std::move(tmpshell), std::move(tmpholes)).release();
            }
            return gf->createPolygon(std::move(tmpshell)).release();

        });
    }

    Geometry*
    GEOSGeom_clone_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            return g->clone().release();
        });
    }

    Geometry*
    GEOSGeom_setPrecision_r(GEOSContextHandle_t extHandle, const GEOSGeometry* g,
                            double gridSize, int flags)
    {
        using namespace geos::geom;

        return execute(extHandle, [&]() {
            const PrecisionModel* pm = g->getPrecisionModel();
            double cursize = pm->isFloating() ? 0 : 1.0 / pm->getScale();
            std::unique_ptr<PrecisionModel> newpm;
            if(gridSize != 0) {
                newpm.reset(new PrecisionModel(1.0 / std::abs(gridSize)));
            }
            else {
                newpm.reset(new PrecisionModel());
            }
            Geometry* ret;
            GeometryFactory::Ptr gf =
                GeometryFactory::create(newpm.get(), g->getSRID());
            if(gridSize != 0 && cursize != gridSize) {
                GeometryPrecisionReducer reducer(*gf);
                reducer.setChangePrecisionModel(true);
                reducer.setUseAreaReducer(!(flags & GEOS_PREC_NO_TOPO));
                reducer.setPointwise(flags & GEOS_PREC_NO_TOPO);
                reducer.setRemoveCollapsedComponents(!(flags & GEOS_PREC_KEEP_COLLAPSED));
                ret = reducer.reduce(*g).release();
            }
            else {
                // No need or willing to snap, just change the factory
                ret = gf->createGeometry(g);
            }
            return ret;
        });
    }

    double
    GEOSGeom_getPrecision_r(GEOSContextHandle_t extHandle, const GEOSGeometry* g)
    {
        using namespace geos::geom;

        return execute(extHandle, -1.0, [&]() {
            const PrecisionModel* pm = g->getPrecisionModel();
            double cursize = pm->isFloating() ? 0 : 1.0 / pm->getScale();
            return cursize;
        });
    }

    int
    GEOSGeom_getDimensions_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, 0, [&]() {
            return (int) g->getDimension();
        });
    }

    int
    GEOSGeom_getCoordinateDimension_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, 0, [&]() {
            return (int)(g->getCoordinateDimension());
        });
    }

    int
    GEOSGeom_getXMin_r(GEOSContextHandle_t extHandle, const Geometry* g, double* value)
    {
        return execute(extHandle, 0, [&]() {
            if(g->isEmpty()) {
                return 0;
            }

            *value = g->getEnvelopeInternal()->getMinX();
            return 1;
        });
    }

    int
    GEOSGeom_getXMax_r(GEOSContextHandle_t extHandle, const Geometry* g, double* value)
    {
        return execute(extHandle, 0, [&]() {
            if(g->isEmpty()) {
                return 0;
            }

            *value = g->getEnvelopeInternal()->getMaxX();
            return 1;
        });
    }

    int
    GEOSGeom_getYMin_r(GEOSContextHandle_t extHandle, const Geometry* g, double* value)
    {
        return execute(extHandle, 0, [&]() {
            if(g->isEmpty()) {
                return 0;
            }

            *value = g->getEnvelopeInternal()->getMinY();
            return 1;
        });
    }

    int
    GEOSGeom_getYMax_r(GEOSContextHandle_t extHandle, const Geometry* g, double* value)
    {
        return execute(extHandle, 0, [&]() {
            if(g->isEmpty()) {
                return 0;
            }

            *value = g->getEnvelopeInternal()->getMaxY();
            return 1;
        });
    }

    Geometry*
    GEOSSimplify_r(GEOSContextHandle_t extHandle, const Geometry* g1, double tolerance)
    {
        using namespace geos::simplify;

        return execute(extHandle, [&]() {
            Geometry::Ptr g3(DouglasPeuckerSimplifier::simplify(g1, tolerance));
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }

    Geometry*
    GEOSTopologyPreserveSimplify_r(GEOSContextHandle_t extHandle, const Geometry* g1, double tolerance)
    {
        using namespace geos::simplify;

        return execute(extHandle, [&]() {
            Geometry::Ptr g3(TopologyPreservingSimplifier::simplify(g1, tolerance));
            g3->setSRID(g1->getSRID());
            return g3.release();
        });
    }


    /* WKT Reader */
    WKTReader*
    GEOSWKTReader_create_r(GEOSContextHandle_t extHandle)
    {
        using geos::io::WKTReader;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t *handle = reinterpret_cast<GEOSContextHandleInternal_t *>(extHandle);
            return new WKTReader((GeometryFactory *) handle->geomFactory);
        });
    }

    void
    GEOSWKTReader_destroy_r(GEOSContextHandle_t extHandle, WKTReader* reader)
    {
        return execute(extHandle, [&]() {
            delete reader;
        });
    }

    Geometry*
    GEOSWKTReader_read_r(GEOSContextHandle_t extHandle, WKTReader* reader, const char* wkt)
    {
        return execute(extHandle, [&]() {
            const std::string wktstring(wkt);
            return reader->read(wktstring).release();
        });
    }

    /* WKT Writer */
    WKTWriter*
    GEOSWKTWriter_create_r(GEOSContextHandle_t extHandle)
    {
        using geos::io::WKTWriter;

        return execute(extHandle, [&]() {
            return new WKTWriter();
        });
    }

    void
    GEOSWKTWriter_destroy_r(GEOSContextHandle_t extHandle, WKTWriter* Writer)
    {
        execute(extHandle, [&]() {
            delete Writer;
        });
    }


    char*
    GEOSWKTWriter_write_r(GEOSContextHandle_t extHandle, WKTWriter* writer, const Geometry* geom)
    {
        return execute(extHandle, [&]() {
            std::string sgeom(writer->write(geom));
            char* result = gstrdup(sgeom);
            return result;
        });
    }

    void
    GEOSWKTWriter_setTrim_r(GEOSContextHandle_t extHandle, WKTWriter* writer, char trim)
    {
        execute(extHandle, [&]() {
            writer->setTrim(0 != trim);
        });
    }

    void
    GEOSWKTWriter_setRoundingPrecision_r(GEOSContextHandle_t extHandle, WKTWriter* writer, int precision)
    {
        execute(extHandle, [&]() {
            writer->setRoundingPrecision(precision);
        });
    }

    void
    GEOSWKTWriter_setOutputDimension_r(GEOSContextHandle_t extHandle, WKTWriter* writer, int dim)
    {
        execute(extHandle, [&]() {
            writer->setOutputDimension(dim);
        });
    }

    int
    GEOSWKTWriter_getOutputDimension_r(GEOSContextHandle_t extHandle, WKTWriter* writer)
    {
        return execute(extHandle, -1, [&]() {
            return writer->getOutputDimension();
        });
    }

    void
    GEOSWKTWriter_setOld3D_r(GEOSContextHandle_t extHandle, WKTWriter* writer, int useOld3D)
    {
        execute(extHandle, [&]() {
            writer->setOld3D(0 != useOld3D);
        });
    }

    /* WKB Reader */
    WKBReader*
    GEOSWKBReader_create_r(GEOSContextHandle_t extHandle)
    {
        using geos::io::WKBReader;

        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
            return new WKBReader(*(GeometryFactory*)handle->geomFactory);
        });
    }

    void
    GEOSWKBReader_destroy_r(GEOSContextHandle_t extHandle, WKBReader* reader)
    {
        execute(extHandle, [&]() {
            delete reader;
        });
    }

    struct membuf : public std::streambuf {
        membuf(char* s, std::size_t n)
        {
            setg(s, s, s + n);
        }
    };

    Geometry*
    GEOSWKBReader_read_r(GEOSContextHandle_t extHandle, WKBReader* reader, const unsigned char* wkb, size_t size)
    {
        return execute(extHandle, [&]() {
            // http://stackoverflow.com/questions/2079912/simpler-way-to-create-a-c-memorystream-from-char-size-t-without-copying-t
            membuf mb((char*)wkb, size);
            istream is(&mb);

            return reader->read(is).release();
        });
    }

    Geometry*
    GEOSWKBReader_readHEX_r(GEOSContextHandle_t extHandle, WKBReader* reader, const unsigned char* hex, size_t size)
    {
        return execute(extHandle, [&]() {
            std::string hexstring(reinterpret_cast<const char*>(hex), size);
            std::istringstream is(std::ios_base::binary);
            is.str(hexstring);
            is.seekg(0, std::ios::beg); // rewind reader pointer

            return reader->readHEX(is).release();
        });
    }

    /* WKB Writer */
    WKBWriter*
    GEOSWKBWriter_create_r(GEOSContextHandle_t extHandle)
    {
        using geos::io::WKBWriter;

        return execute(extHandle, [&]() {
            return new WKBWriter();
        });
    }

    void
    GEOSWKBWriter_destroy_r(GEOSContextHandle_t extHandle, WKBWriter* Writer)
    {
        execute(extHandle, [&]() {
            delete Writer;
        });
    }


    /* The caller owns the result */
    unsigned char*
    GEOSWKBWriter_write_r(GEOSContextHandle_t extHandle, WKBWriter* writer, const Geometry* geom, size_t* size)
    {
        return execute(extHandle, [&]() {
            std::ostringstream os(std::ios_base::binary);
            writer->write(*geom, os);

            const std::string& wkbstring = os.str();
            const std::size_t len = wkbstring.length();

            unsigned char* result = (unsigned char*) malloc(len);
            std::memcpy(result, wkbstring.c_str(), len);
            *size = len;
            return result;
        });
    }

    /* The caller owns the result */
    unsigned char*
    GEOSWKBWriter_writeHEX_r(GEOSContextHandle_t extHandle, WKBWriter* writer, const Geometry* geom, size_t* size)
    {
        return execute(extHandle, [&]() {
            std::ostringstream os(std::ios_base::binary);
            writer->writeHEX(*geom, os);
            std::string wkbstring(os.str());
            const std::size_t len = wkbstring.length();

            unsigned char* result = (unsigned char*) malloc(len);
            std::memcpy(result, wkbstring.c_str(), len);
            *size = len;
            return result;
        });
    }

    int
    GEOSWKBWriter_getOutputDimension_r(GEOSContextHandle_t extHandle, const GEOSWKBWriter* writer)
    {
        return execute(extHandle, 0, [&]() {
            return writer->getOutputDimension();
        });
    }

    void
    GEOSWKBWriter_setOutputDimension_r(GEOSContextHandle_t extHandle, GEOSWKBWriter* writer, int newDimension)
    {
        execute(extHandle, [&]() {
            writer->setOutputDimension(newDimension);
        });
    }

    int
    GEOSWKBWriter_getByteOrder_r(GEOSContextHandle_t extHandle, const GEOSWKBWriter* writer)
    {
        return execute(extHandle, 0, [&]() {
            return writer->getByteOrder();
        });
    }

    void
    GEOSWKBWriter_setByteOrder_r(GEOSContextHandle_t extHandle, GEOSWKBWriter* writer, int newByteOrder)
    {
        execute(extHandle, [&]() {
            writer->setByteOrder(newByteOrder);
        });
    }

    char
    GEOSWKBWriter_getIncludeSRID_r(GEOSContextHandle_t extHandle, const GEOSWKBWriter* writer)
    {
        return execute(extHandle, -1, [&]{
            return writer->getIncludeSRID();
        });
    }

    void
    GEOSWKBWriter_setIncludeSRID_r(GEOSContextHandle_t extHandle, GEOSWKBWriter* writer, const char newIncludeSRID)
    {
        execute(extHandle, [&]{
            writer->setIncludeSRID(newIncludeSRID);
        });
    }


//-----------------------------------------------------------------
// Prepared Geometry
//-----------------------------------------------------------------

    const geos::geom::prep::PreparedGeometry*
    GEOSPrepare_r(GEOSContextHandle_t extHandle, const Geometry* g)
    {
        return execute(extHandle, [&]() {
            return geos::geom::prep::PreparedGeometryFactory::prepare(g).release();
        });
    }

    void
    GEOSPreparedGeom_destroy_r(GEOSContextHandle_t extHandle, const geos::geom::prep::PreparedGeometry* a)
    {
        execute(extHandle, [&]() {
            delete a;
        });
    }

    char
    GEOSPreparedContains_r(GEOSContextHandle_t extHandle,
                           const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->contains(g);
        });
    }

    char
    GEOSPreparedContainsProperly_r(GEOSContextHandle_t extHandle,
                                   const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->containsProperly(g);
        });
    }

    char
    GEOSPreparedCoveredBy_r(GEOSContextHandle_t extHandle,
                            const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->coveredBy(g);
        });
    }

    char
    GEOSPreparedCovers_r(GEOSContextHandle_t extHandle,
                         const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->covers(g);
        });
    }

    char
    GEOSPreparedCrosses_r(GEOSContextHandle_t extHandle,
                          const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->crosses(g);
        });
    }

    char
    GEOSPreparedDisjoint_r(GEOSContextHandle_t extHandle,
                           const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->disjoint(g);
        });
    }

    char
    GEOSPreparedIntersects_r(GEOSContextHandle_t extHandle,
                             const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->intersects(g);
        });
    }

    char
    GEOSPreparedOverlaps_r(GEOSContextHandle_t extHandle,
                           const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->overlaps(g);
        });
    }

    char
    GEOSPreparedTouches_r(GEOSContextHandle_t extHandle,
                          const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->touches(g);
        });
    }

    char
    GEOSPreparedWithin_r(GEOSContextHandle_t extHandle,
                         const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        return execute(extHandle, 2, [&]() {
            return pg->within(g);
        });
    }

    CoordinateSequence*
    GEOSPreparedNearestPoints_r(GEOSContextHandle_t extHandle,
                         const geos::geom::prep::PreparedGeometry* pg, const Geometry* g)
    {
        using namespace geos::geom;

        return execute(extHandle, [&]() -> CoordinateSequence* {
            return pg->nearestPoints(g).release();
        });
    }

    int
    GEOSPreparedDistance_r(GEOSContextHandle_t extHandle,
                         const geos::geom::prep::PreparedGeometry* pg,
                         const Geometry* g, double* dist)
    {
        return execute(extHandle, 0, [&]() {
            *dist = pg->distance(g);
            return 1;
        });
    }

//-----------------------------------------------------------------
// STRtree
//-----------------------------------------------------------------

    GEOSSTRtree*
    GEOSSTRtree_create_r(GEOSContextHandle_t extHandle,
                         size_t nodeCapacity)
    {
        return execute(extHandle, [&]() {
            return new GEOSSTRtree(nodeCapacity);
        });
    }

    void
    GEOSSTRtree_insert_r(GEOSContextHandle_t extHandle,
                         GEOSSTRtree* tree,
                         const geos::geom::Geometry* g,
                         void* item)
    {
        execute(extHandle, [&]() {
            tree->insert(g->getEnvelopeInternal(), item);
        });
    }

    void
    GEOSSTRtree_query_r(GEOSContextHandle_t extHandle,
                        GEOSSTRtree* tree,
                        const geos::geom::Geometry* g,
                        GEOSQueryCallback callback,
                        void* userdata)
    {
        execute(extHandle, [&]() {
            CAPI_ItemVisitor visitor(callback, userdata);
            tree->query(g->getEnvelopeInternal(), visitor);
        });
    }

    const GEOSGeometry*
    GEOSSTRtree_nearest_r(GEOSContextHandle_t extHandle,
                          GEOSSTRtree* tree,
                          const geos::geom::Geometry* geom)
    {
        return (const GEOSGeometry*) GEOSSTRtree_nearest_generic_r(extHandle, tree, geom, geom, nullptr, nullptr);
    }

    const void*
    GEOSSTRtree_nearest_generic_r(GEOSContextHandle_t extHandle,
                                  GEOSSTRtree* tree,
                                  const void* item,
                                  const geos::geom::Geometry* itemEnvelope,
                                  GEOSDistanceCallback distancefn,
                                  void* userdata)
    {
        using namespace geos::index::strtree;

        struct CustomItemDistance : public ItemDistance {
            CustomItemDistance(GEOSDistanceCallback p_distancefn, void* p_userdata)
                : m_distancefn(p_distancefn), m_userdata(p_userdata) {}

            GEOSDistanceCallback m_distancefn;
            void* m_userdata;

            double
            distance(const ItemBoundable* item1, const ItemBoundable* item2) override
            {
                const void* a = item1->getItem();
                const void* b = item2->getItem();
                double d;

                if(!m_distancefn(a, b, &d, m_userdata)) {
                    throw std::runtime_error(std::string("Failed to compute distance."));
                }

                return d;
            }
        };

        return execute(extHandle, [&]() {
            if(distancefn) {
                CustomItemDistance itemDistance(distancefn, userdata);
                return tree->nearestNeighbour(itemEnvelope->getEnvelopeInternal(), item, &itemDistance);
            }
            else {
                GeometryItemDistance itemDistance = GeometryItemDistance();
                return tree->nearestNeighbour(itemEnvelope->getEnvelopeInternal(), item, &itemDistance);
            }
        });
    }

    void
    GEOSSTRtree_iterate_r(GEOSContextHandle_t extHandle,
                          GEOSSTRtree* tree,
                          GEOSQueryCallback callback,
                          void* userdata)
    {
        return execute(extHandle, [&]() {
            CAPI_ItemVisitor visitor(callback, userdata);
            tree->iterate(visitor);
        });
    }

    char
    GEOSSTRtree_remove_r(GEOSContextHandle_t extHandle,
                         GEOSSTRtree* tree,
                         const geos::geom::Geometry* g,
                         void* item) {
        return execute(extHandle, 2, [&]() {
            return tree->remove(g->getEnvelopeInternal(), item);
        });
    }

    void
    GEOSSTRtree_destroy_r(GEOSContextHandle_t extHandle,
                          GEOSSTRtree* tree)
    {
        return execute(extHandle, [&]() {
            delete tree;
        });
    }

    double
    GEOSProject_r(GEOSContextHandle_t extHandle,
                  const Geometry* g,
                  const Geometry* p)
    {
        return execute(extHandle, -1.0, [&]() {
            const geos::geom::Point* point = dynamic_cast<const geos::geom::Point*>(p);
            if(!point) {
                throw std::runtime_error("third argument of GEOSProject_r must be Point");
            }
            const geos::geom::Coordinate* inputPt = p->getCoordinate();
            return geos::linearref::LengthIndexedLine(g).project(*inputPt);
        });
    }


    Geometry*
    GEOSInterpolate_r(GEOSContextHandle_t extHandle, const Geometry* g, double d)
    {
        return execute(extHandle, [&]() {
            GEOSContextHandleInternal_t* handle = reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);

            geos::linearref::LengthIndexedLine lil(g);
            geos::geom::Coordinate coord = lil.extractPoint(d);
            const GeometryFactory* gf = handle->geomFactory;
            Geometry* point = gf->createPoint(coord);
            point->setSRID(g->getSRID());
            return point;
        });
    }


    double
    GEOSProjectNormalized_r(GEOSContextHandle_t extHandle, const Geometry* g,
                            const Geometry* p)
    {

        double length;
        double distance;
        if(GEOSLength_r(extHandle, g, &length) != 1) {
            return -1.0;
        };
        distance = GEOSProject_r(extHandle, g, p);
        if (distance == -1.0) {
            return -1.0;
        } else {
            return distance / length;
        }
    }


    Geometry*
    GEOSInterpolateNormalized_r(GEOSContextHandle_t extHandle, const Geometry* g,
                                double d)
    {
        double length;
        if (GEOSLength_r(extHandle, g, &length) != 1) {
            return 0;
        }
        return GEOSInterpolate_r(extHandle, g, d * length);
    }

    GEOSGeometry*
    GEOSGeom_extractUniquePoints_r(GEOSContextHandle_t extHandle,
                                   const GEOSGeometry* g)
    {
        using namespace geos::geom;
        using namespace geos::util;

        return execute(extHandle, [&]() {
            /* 1: extract points */
            std::vector<const Coordinate*> coords;
            UniqueCoordinateArrayFilter filter(coords);
            g->apply_ro(&filter);

            /* 2: for each point, create a geometry and put into a vector */
            std::vector<Geometry*>* points = new std::vector<Geometry*>();
            points->reserve(coords.size());
            const GeometryFactory* factory = g->getFactory();
            for(std::vector<const Coordinate*>::iterator it = coords.begin(),
                    itE = coords.end();
                    it != itE; ++it) {
                Geometry* point = factory->createPoint(*(*it));
                points->push_back(point);
            }

            /* 3: create a multipoint */
            Geometry* out = factory->createMultiPoint(points);
            out->setSRID(g->getSRID());
            return out;

        });
    }

    int GEOSOrientationIndex_r(GEOSContextHandle_t extHandle,
                               double Ax, double Ay, double Bx, double By, double Px, double Py)
    {
        using geos::geom::Coordinate;
        using geos::algorithm::Orientation;

        return execute(extHandle, 2, [&]() {
            Coordinate A(Ax, Ay);
            Coordinate B(Bx, By);
            Coordinate P(Px, Py);
            return Orientation::index(A, B, P);
        });
    }

    GEOSGeometry*
    GEOSSharedPaths_r(GEOSContextHandle_t extHandle, const GEOSGeometry* g1, const GEOSGeometry* g2)
    {
        using namespace geos::operation::sharedpaths;

        if(nullptr == extHandle) {
            return nullptr;
        }
        GEOSContextHandleInternal_t* handle =
            reinterpret_cast<GEOSContextHandleInternal_t*>(extHandle);
        if(handle->initialized == 0) {
            return nullptr;
        }

        SharedPathsOp::PathList forw, back;
        try {
            SharedPathsOp::sharedPathsOp(*g1, *g2, forw, back);
        }
        catch(const std::exception& e) {
            SharedPathsOp::clearEdges(forw);
            SharedPathsOp::clearEdges(back);
            handle->ERROR_MESSAGE("%s", e.what());
            return nullptr;
        }
        catch(...) {
            SharedPathsOp::clearEdges(forw);
            SharedPathsOp::clearEdges(back);
            handle->ERROR_MESSAGE("Unknown exception thrown");
            return nullptr;
        }

        // Now forw and back have the geoms we want to use to construct
        // our output GeometryCollections...

        const GeometryFactory* factory = g1->getFactory();
        size_t count;

        std::unique_ptr< std::vector<Geometry*> > out1(
            new std::vector<Geometry*>()
        );
        count = forw.size();
        out1->reserve(count);
        for(size_t i = 0; i < count; ++i) {
            out1->push_back(forw[i]);
        }
        std::unique_ptr<Geometry> out1g(
            factory->createMultiLineString(out1.release())
        );

        std::unique_ptr< std::vector<Geometry*> > out2(
            new std::vector<Geometry*>()
        );
        count = back.size();
        out2->reserve(count);
        for(size_t i = 0; i < count; ++i) {
            out2->push_back(back[i]);
        }
        std::unique_ptr<Geometry> out2g(
            factory->createMultiLineString(out2.release())
        );

        std::unique_ptr< std::vector<Geometry*> > out(
            new std::vector<Geometry*>()
        );
        out->reserve(2);
        out->push_back(out1g.release());
        out->push_back(out2g.release());

        std::unique_ptr<Geometry> outg(
            factory->createGeometryCollection(out.release())
        );

        outg->setSRID(g1->getSRID());
        return outg.release();
    }

    GEOSGeometry*
    GEOSSnap_r(GEOSContextHandle_t extHandle, const GEOSGeometry* g1,
               const GEOSGeometry* g2, double tolerance)
    {
        using namespace geos::operation::overlay::snap;

        return execute(extHandle, [&]() {
            GeometrySnapper snapper(*g1);
            std::unique_ptr<Geometry> ret = snapper.snapTo(*g2, tolerance);
            ret->setSRID(g1->getSRID());
            return ret.release();
        });
    }

    BufferParameters*
    GEOSBufferParams_create_r(GEOSContextHandle_t extHandle)
    {
        return execute(extHandle, [&]() {
            return new BufferParameters();
        });
    }

    void
    GEOSBufferParams_destroy_r(GEOSContextHandle_t extHandle, BufferParameters* p)
    {
        (void)extHandle;
        delete p;
    }

    int
    GEOSBufferParams_setEndCapStyle_r(GEOSContextHandle_t extHandle,
                                      GEOSBufferParams* p, int style)
    {
        return execute(extHandle, 0, [&]() {
            if(style > BufferParameters::CAP_SQUARE) {
                throw IllegalArgumentException("Invalid buffer endCap style");
            }
            p->setEndCapStyle(static_cast<BufferParameters::EndCapStyle>(style));
            return 1;
        });
    }

    int
    GEOSBufferParams_setJoinStyle_r(GEOSContextHandle_t extHandle,
                                    GEOSBufferParams* p, int style)
    {
        return execute(extHandle, 0, [&]() {
            if(style > BufferParameters::JOIN_BEVEL) {
                throw IllegalArgumentException("Invalid buffer join style");
            }
            p->setJoinStyle(static_cast<BufferParameters::JoinStyle>(style));

            return 1;
        });
    }

    int
    GEOSBufferParams_setMitreLimit_r(GEOSContextHandle_t extHandle,
                                     GEOSBufferParams* p, double limit)
    {
        return execute(extHandle, 0, [&]() {
            p->setMitreLimit(limit);
            return 1;
        });
    }

    int
    GEOSBufferParams_setQuadrantSegments_r(GEOSContextHandle_t extHandle,
                                           GEOSBufferParams* p, int segs)
    {
        return execute(extHandle, 0, [&]() {
            p->setQuadrantSegments(segs);
            return 1;
        });
    }

    int
    GEOSBufferParams_setSingleSided_r(GEOSContextHandle_t extHandle,
                                      GEOSBufferParams* p, int ss)
    {
        return execute(extHandle, 0, [&]() {
            p->setSingleSided((ss != 0));
            return 1;
        });
    }

    Geometry*
    GEOSBufferWithParams_r(GEOSContextHandle_t extHandle, const Geometry* g1, const BufferParameters* bp, double width)
    {
        using geos::operation::buffer::BufferOp;

        return execute(extHandle, [&]() {
            BufferOp op(g1, *bp);
            Geometry* g3 = op.getResultGeometry(width);
            g3->setSRID(g1->getSRID());
            return g3;
        });
    }

    Geometry*
    GEOSDelaunayTriangulation_r(GEOSContextHandle_t extHandle, const Geometry* g1, double tolerance, int onlyEdges)
    {
        using geos::triangulate::DelaunayTriangulationBuilder;

        return execute(extHandle, [&]() -> Geometry* {
            DelaunayTriangulationBuilder builder;
            builder.setTolerance(tolerance);
            builder.setSites(*g1);

            if(onlyEdges) {
                Geometry* out = builder.getEdges(*g1->getFactory()).release();
                out->setSRID(g1->getSRID());
                return out;
            }
            else {
                Geometry* out = builder.getTriangles(*g1->getFactory()).release();
                out->setSRID(g1->getSRID());
                return out;
            }
        });
    }

    Geometry*
    GEOSVoronoiDiagram_r(GEOSContextHandle_t extHandle, const Geometry* g1, const Geometry* env, double tolerance,
                         int onlyEdges)
    {
        using geos::triangulate::VoronoiDiagramBuilder;

        return execute(extHandle, [&]() -> Geometry* {
            VoronoiDiagramBuilder builder;
            builder.setSites(*g1);
            builder.setTolerance(tolerance);
            if(env) {
                builder.setClipEnvelope(env->getEnvelopeInternal());
            }
            if(onlyEdges) {
                Geometry* out = builder.getDiagramEdges(*g1->getFactory()).release();
                out->setSRID(g1->getSRID());
                return out;
            }
            else {
                Geometry* out = builder.getDiagram(*g1->getFactory()).release();
                out->setSRID(g1->getSRID());
                return out;
            }
        });
    }

    int
    GEOSSegmentIntersection_r(GEOSContextHandle_t extHandle,
                              double ax0, double ay0, double ax1, double ay1,
                              double bx0, double by0, double bx1, double by1,
                              double* cx, double* cy)
    {
        return execute(extHandle, 0, [&]() {
            geos::geom::LineSegment a(ax0, ay0, ax1, ay1);
            geos::geom::LineSegment b(bx0, by0, bx1, by1);
            geos::geom::Coordinate isect = a.intersection(b);

            if(isect.isNull()) {
                return -1;
            }

            *cx = isect.x;
            *cy = isect.y;

            return 1;
        });
    }

} /* extern "C" */

