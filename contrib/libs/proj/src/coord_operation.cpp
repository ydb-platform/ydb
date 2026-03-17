/******************************************************************************
 * Project:  PROJ
 * Purpose:  PJCoordOperation methods
 *
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2018-2024, Even Rouault, <even.rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#define FROM_PROJ_CPP

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

#include "proj/internal/internal.hpp"

using namespace NS_PROJ::internal;

//! @cond Doxygen_Suppress
/**************************************************************************************/
PJCoordOperation::~PJCoordOperation() {
    /**************************************************************************************/
    proj_destroy(pj);
    proj_destroy(pjSrcGeocentricToLonLat);
    proj_destroy(pjDstGeocentricToLonLat);
}

/**************************************************************************************/
bool PJCoordOperation::isInstantiable() const {
    /**************************************************************************************/
    if (isInstantiableCached == INSTANTIABLE_STATUS_UNKNOWN)
        isInstantiableCached = proj_coordoperation_is_instantiable(pj->ctx, pj);
    return (isInstantiableCached == 1);
}

// Returns true if the passed operation uses NADCON5 grids for NAD83 to
// NAD83(HARN)
static bool isSpecialCaseForNAD83_to_NAD83HARN(const std::string &opName) {
    return opName.find("NAD83 to NAD83(HARN) (47)") != std::string::npos ||
           opName.find("NAD83 to NAD83(HARN) (48)") != std::string::npos ||
           opName.find("NAD83 to NAD83(HARN) (49)") != std::string::npos ||
           opName.find("NAD83 to NAD83(HARN) (50)") != std::string::npos;
}

// Returns true if the passed operation uses "GDA94 to WGS 84 (1)", which
// is the null transformation
static bool isSpecialCaseForGDA94_to_WGS84(const std::string &opName) {
    return opName.find("GDA94 to WGS 84 (1)") != std::string::npos;
}

// Returns true if the passed operation uses "GDA2020 to WGS 84 (2)", which
// is the null transformation
static bool isSpecialCaseForWGS84_to_GDA2020(const std::string &opName) {
    return opName.find("GDA2020 to WGS 84 (2)") != std::string::npos;
}

PJCoordOperation::PJCoordOperation(
    int idxInOriginalListIn, double minxSrcIn, double minySrcIn,
    double maxxSrcIn, double maxySrcIn, double minxDstIn, double minyDstIn,
    double maxxDstIn, double maxyDstIn, PJ *pjIn, const std::string &nameIn,
    double accuracyIn, double pseudoAreaIn, const char *areaNameIn,
    const PJ *pjSrcGeocentricToLonLatIn, const PJ *pjDstGeocentricToLonLatIn)
    : idxInOriginalList(idxInOriginalListIn), minxSrc(minxSrcIn),
      minySrc(minySrcIn), maxxSrc(maxxSrcIn), maxySrc(maxySrcIn),
      minxDst(minxDstIn), minyDst(minyDstIn), maxxDst(maxxDstIn),
      maxyDst(maxyDstIn), pj(pjIn), name(nameIn), accuracy(accuracyIn),
      pseudoArea(pseudoAreaIn), areaName(areaNameIn ? areaNameIn : ""),
      isOffshore(areaName.find("- offshore") != std::string::npos),
      isUnknownAreaName(areaName.empty() || areaName == "unknown"),
      isPriorityOp(isSpecialCaseForNAD83_to_NAD83HARN(name) ||
                   isSpecialCaseForGDA94_to_WGS84(name) ||
                   isSpecialCaseForWGS84_to_GDA2020(name)),
      pjSrcGeocentricToLonLat(pjSrcGeocentricToLonLatIn
                                  ? proj_clone(pjSrcGeocentricToLonLatIn->ctx,
                                               pjSrcGeocentricToLonLatIn)
                                  : nullptr),
      pjDstGeocentricToLonLat(pjDstGeocentricToLonLatIn
                                  ? proj_clone(pjDstGeocentricToLonLatIn->ctx,
                                               pjDstGeocentricToLonLatIn)
                                  : nullptr) {
    const auto IsLonLatOrLatLon = [](const PJ *crs, bool &isLonLatDegreeOut,
                                     bool &isLatLonDegreeOut) {
        const auto eType = proj_get_type(crs);
        if (eType == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
            eType == PJ_TYPE_GEOGRAPHIC_3D_CRS) {
            const auto cs = proj_crs_get_coordinate_system(crs->ctx, crs);
            const char *direction = "";
            double conv_factor = 0;
            constexpr double EPS = 1e-14;
            if (proj_cs_get_axis_info(crs->ctx, cs, 0, nullptr, nullptr,
                                      &direction, &conv_factor, nullptr,
                                      nullptr, nullptr) &&
                ci_equal(direction, "East")) {
                isLonLatDegreeOut = fabs(conv_factor - M_PI / 180) < EPS;
            } else if (proj_cs_get_axis_info(crs->ctx, cs, 1, nullptr, nullptr,
                                             &direction, &conv_factor, nullptr,
                                             nullptr, nullptr) &&
                       ci_equal(direction, "East")) {
                isLatLonDegreeOut = fabs(conv_factor - M_PI / 180) < EPS;
            }
            proj_destroy(cs);
        }
    };

    const auto source = proj_get_source_crs(pj->ctx, pj);
    if (source) {
        IsLonLatOrLatLon(source, srcIsLonLatDegree, srcIsLatLonDegree);
        proj_destroy(source);
    }

    const auto target = proj_get_target_crs(pj->ctx, pj);
    if (target) {
        IsLonLatOrLatLon(target, dstIsLonLatDegree, dstIsLatLonDegree);
        proj_destroy(target);
    }
}

//! @endcond
