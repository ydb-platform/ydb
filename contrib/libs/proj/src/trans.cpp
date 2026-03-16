/******************************************************************************
 * Project:  PROJ
 * Purpose:  proj_trans(), proj_trans_array(), proj_trans_generic(),
 *proj_roundtrip()
 *
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2016-06-09/2016-11-06
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017 Thomas Knudsen/SDFE
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

#include <cmath>
#include <limits>

#include "proj/internal/io_internal.hpp"

inline bool pj_coord_has_nans(PJ_COORD coo) {
    return std::isnan(coo.v[0]) || std::isnan(coo.v[1]) ||
           std::isnan(coo.v[2]) || std::isnan(coo.v[3]);
}

/**************************************************************************************/
int pj_get_suggested_operation(PJ_CONTEXT *,
                               const std::vector<PJCoordOperation> &opList,
                               const int iExcluded[2], bool skipNonInstantiable,
                               PJ_DIRECTION direction, PJ_COORD coord)
/**************************************************************************************/
{
    const auto normalizeLongitude = [](double x) {
        if (x > 180.0) {
            x -= 360.0;
            if (x > 180.0)
                x = fmod(x + 180.0, 360.0) - 180.0;
        } else if (x < -180.0) {
            x += 360.0;
            if (x < -180.0)
                x = fmod(x + 180.0, 360.0) - 180.0;
        }
        return x;
    };

    // Select the operations that match the area of use
    // and has the best accuracy.
    int iBest = -1;
    double bestAccuracy = std::numeric_limits<double>::max();
    const int nOperations = static_cast<int>(opList.size());
    for (int i = 0; i < nOperations; i++) {
        if (i == iExcluded[0] || i == iExcluded[1]) {
            continue;
        }
        const auto &alt = opList[i];
        bool spatialCriterionOK = false;
        if (direction == PJ_FWD) {
            if (alt.pjSrcGeocentricToLonLat) {
                if (alt.minxSrc == -180 && alt.minySrc == -90 &&
                    alt.maxxSrc == 180 && alt.maxySrc == 90) {
                    spatialCriterionOK = true;
                } else {
                    PJ_COORD tmp = coord;
                    pj_fwd4d(tmp, alt.pjSrcGeocentricToLonLat);
                    if (tmp.xyzt.x >= alt.minxSrc &&
                        tmp.xyzt.y >= alt.minySrc &&
                        tmp.xyzt.x <= alt.maxxSrc &&
                        tmp.xyzt.y <= alt.maxySrc) {
                        spatialCriterionOK = true;
                    }
                }
            } else if (coord.xyzt.x >= alt.minxSrc &&
                       coord.xyzt.y >= alt.minySrc &&
                       coord.xyzt.x <= alt.maxxSrc &&
                       coord.xyzt.y <= alt.maxySrc) {
                spatialCriterionOK = true;
            } else if (alt.srcIsLonLatDegree && coord.xyzt.y >= alt.minySrc &&
                       coord.xyzt.y <= alt.maxySrc) {
                const double normalizedLon = normalizeLongitude(coord.xyzt.x);
                if (normalizedLon >= alt.minxSrc &&
                    normalizedLon <= alt.maxxSrc) {
                    spatialCriterionOK = true;
                }
            } else if (alt.srcIsLatLonDegree && coord.xyzt.x >= alt.minxSrc &&
                       coord.xyzt.x <= alt.maxxSrc) {
                const double normalizedLon = normalizeLongitude(coord.xyzt.y);
                if (normalizedLon >= alt.minySrc &&
                    normalizedLon <= alt.maxySrc) {
                    spatialCriterionOK = true;
                }
            }
        } else {
            if (alt.pjDstGeocentricToLonLat) {
                if (alt.minxDst == -180 && alt.minyDst == -90 &&
                    alt.maxxDst == 180 && alt.maxyDst == 90) {
                    spatialCriterionOK = true;
                } else {
                    PJ_COORD tmp = coord;
                    pj_fwd4d(tmp, alt.pjDstGeocentricToLonLat);
                    if (tmp.xyzt.x >= alt.minxDst &&
                        tmp.xyzt.y >= alt.minyDst &&
                        tmp.xyzt.x <= alt.maxxDst &&
                        tmp.xyzt.y <= alt.maxyDst) {
                        spatialCriterionOK = true;
                    }
                }
            } else if (coord.xyzt.x >= alt.minxDst &&
                       coord.xyzt.y >= alt.minyDst &&
                       coord.xyzt.x <= alt.maxxDst &&
                       coord.xyzt.y <= alt.maxyDst) {
                spatialCriterionOK = true;
            } else if (alt.dstIsLonLatDegree && coord.xyzt.y >= alt.minyDst &&
                       coord.xyzt.y <= alt.maxyDst) {
                const double normalizedLon = normalizeLongitude(coord.xyzt.x);
                if (normalizedLon >= alt.minxDst &&
                    normalizedLon <= alt.maxxDst) {
                    spatialCriterionOK = true;
                }
            } else if (alt.dstIsLatLonDegree && coord.xyzt.x >= alt.minxDst &&
                       coord.xyzt.x <= alt.maxxDst) {
                const double normalizedLon = normalizeLongitude(coord.xyzt.y);
                if (normalizedLon >= alt.minyDst &&
                    normalizedLon <= alt.maxyDst) {
                    spatialCriterionOK = true;
                }
            }
        }

        if (spatialCriterionOK) {
            // The offshore test is for the "Test bug 245 (use +datum=carthage)"
            // of test_cs2cs_various.yaml. The long=10 lat=34 point belongs
            // both to the onshore and offshore Tunisia area of uses, but is
            // slightly onshore. So in a general way, prefer a onshore area
            // to a offshore one.
            if (iBest < 0 ||
                (((alt.accuracy >= 0 && alt.accuracy < bestAccuracy) ||
                  // If two operations have the same accuracy, use
                  // the one that has the smallest area
                  (alt.accuracy == bestAccuracy &&
                   alt.pseudoArea < opList[iBest].pseudoArea &&
                   !(alt.isUnknownAreaName &&
                     !opList[iBest].isUnknownAreaName) &&
                   !opList[iBest].isPriorityOp)) &&
                 !alt.isOffshore)) {

                if (skipNonInstantiable && !alt.isInstantiable()) {
                    continue;
                }
                iBest = i;
                bestAccuracy = alt.accuracy;
            }
        }
    }

    return iBest;
}

/**************************************************************************************/
void pj_warn_about_missing_grid(PJ *P)
/**************************************************************************************/
{
    std::string msg("Attempt to use coordinate operation ");
    msg += proj_get_name(P);
    msg += " failed.";
    int gridUsed = proj_coordoperation_get_grid_used_count(P->ctx, P);
    for (int i = 0; i < gridUsed; ++i) {
        const char *gridName = "";
        int available = FALSE;
        if (proj_coordoperation_get_grid_used(P->ctx, P, i, &gridName, nullptr,
                                              nullptr, nullptr, nullptr,
                                              nullptr, &available) &&
            !available) {
            msg += " Grid ";
            msg += gridName;
            msg += " is not available. "
                   "Consult https://proj.org/resource_files.html for guidance.";
        }
    }
    if (!P->errorIfBestTransformationNotAvailable &&
        P->warnIfBestTransformationNotAvailable) {
        msg += " This might become an error in a future PROJ major release. "
               "Set the ONLY_BEST option to YES or NO. "
               "This warning will no longer be emitted (for the current "
               "transformation instance).";
        P->warnIfBestTransformationNotAvailable = false;
    }
    pj_log(P->ctx,
           P->errorIfBestTransformationNotAvailable ? PJ_LOG_ERROR
                                                    : PJ_LOG_DEBUG,
           msg.c_str());
}

/**************************************************************************************/
PJ_COORD proj_trans(PJ *P, PJ_DIRECTION direction, PJ_COORD coord) {
    /***************************************************************************************
    Apply the transformation P to the coordinate coord, preferring the 4D
    interfaces if available.

    See also pj_approx_2D_trans and pj_approx_3D_trans in pj_internal.c, which
    work similarly, but prefers the 2D resp. 3D interfaces if available.
    ***************************************************************************************/
    if (nullptr == P || direction == PJ_IDENT)
        return coord;
    if (P->inverted)
        direction = pj_opposite_direction(direction);

    if (P->iso_obj != nullptr && !P->iso_obj_is_coordinate_operation) {
        pj_log(P->ctx, PJ_LOG_ERROR, "Object is not a coordinate operation");
        proj_errno_set(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        return proj_coord_error();
    }

    if (!P->alternativeCoordinateOperations.empty()) {
        constexpr int N_MAX_RETRY = 2;
        int iExcluded[N_MAX_RETRY] = {-1, -1};

        bool skipNonInstantiable = P->skipNonInstantiable &&
                                   !P->warnIfBestTransformationNotAvailable &&
                                   !P->errorIfBestTransformationNotAvailable;
        const int nOperations =
            static_cast<int>(P->alternativeCoordinateOperations.size());

        // We may need several attempts. For example the point at
        // long=-111.5 lat=45.26 falls into the bounding box of the Canadian
        // ntv2_0.gsb grid, except that it is not in any of the subgrids, being
        // in the US. We thus need another retry that will select the conus
        // grid.
        for (int iRetry = 0; iRetry <= N_MAX_RETRY; iRetry++) {
            // Do a first pass and select the operations that match the area of
            // use and has the best accuracy.
            int iBest = pj_get_suggested_operation(
                P->ctx, P->alternativeCoordinateOperations, iExcluded,
                skipNonInstantiable, direction, coord);
            if (iBest < 0) {
                break;
            }
            if (iRetry > 0) {
                const int oldErrno = proj_errno_reset(P);
                if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_DEBUG) {
                    pj_log(P->ctx, PJ_LOG_DEBUG,
                           proj_context_errno_string(P->ctx, oldErrno));
                }
                pj_log(P->ctx, PJ_LOG_DEBUG,
                       "Did not result in valid result. "
                       "Attempting a retry with another operation.");
            }

            const auto &alt = P->alternativeCoordinateOperations[iBest];
            if (P->iCurCoordOp != iBest) {
                if (proj_log_level(P->ctx, PJ_LOG_TELL) >= PJ_LOG_DEBUG) {
                    std::string msg("Using coordinate operation ");
                    msg += alt.name;
                    pj_log(P->ctx, PJ_LOG_DEBUG, msg.c_str());
                }
                P->iCurCoordOp = iBest;
            }
            PJ_COORD res = coord;
            if (alt.pj->hasCoordinateEpoch)
                coord.xyzt.t = alt.pj->coordinateEpoch;
            if (direction == PJ_FWD)
                pj_fwd4d(res, alt.pj);
            else
                pj_inv4d(res, alt.pj);
            if (proj_errno(alt.pj) == PROJ_ERR_OTHER_NETWORK_ERROR) {
                return proj_coord_error();
            }
            if (res.xyzt.x != HUGE_VAL) {
                return res;
            } else if (P->errorIfBestTransformationNotAvailable ||
                       P->warnIfBestTransformationNotAvailable) {
                pj_warn_about_missing_grid(alt.pj);
                if (P->errorIfBestTransformationNotAvailable) {
                    proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_NO_OPERATION);
                    return res;
                }
                P->warnIfBestTransformationNotAvailable = false;
                skipNonInstantiable = true;
            }
            if (iRetry == N_MAX_RETRY) {
                break;
            }
            iExcluded[iRetry] = iBest;
        }

        // In case we did not find an operation whose area of use is compatible
        // with the input coordinate, then goes through again the list, and
        // use the first operation that does not require grids.
        NS_PROJ::io::DatabaseContextPtr dbContext;
        try {
            if (P->ctx->cpp_context) {
                dbContext =
                    P->ctx->cpp_context->getDatabaseContext().as_nullable();
            }
        } catch (const std::exception &) {
        }
        for (int i = 0; i < nOperations; i++) {
            const auto &alt = P->alternativeCoordinateOperations[i];
            auto coordOperation =
                dynamic_cast<NS_PROJ::operation::CoordinateOperation *>(
                    alt.pj->iso_obj.get());
            if (coordOperation) {
                if (coordOperation->gridsNeeded(dbContext, true).empty()) {
                    if (P->iCurCoordOp != i) {
                        if (proj_log_level(P->ctx, PJ_LOG_TELL) >=
                            PJ_LOG_DEBUG) {
                            std::string msg("Using coordinate operation ");
                            msg += alt.name;
                            msg += " as a fallback due to lack of more "
                                   "appropriate operations";
                            pj_log(P->ctx, PJ_LOG_DEBUG, msg.c_str());
                        }
                        P->iCurCoordOp = i;
                    }
                    if (direction == PJ_FWD) {
                        pj_fwd4d(coord, alt.pj);
                    } else {
                        pj_inv4d(coord, alt.pj);
                    }
                    return coord;
                }
            }
        }

        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_NO_OPERATION);
        return proj_coord_error();
    }

    P->iCurCoordOp =
        0; // dummy value, to be used by proj_trans_get_last_used_operation()
    if (P->hasCoordinateEpoch)
        coord.xyzt.t = P->coordinateEpoch;
    if (pj_coord_has_nans(coord))
        coord.v[0] = coord.v[1] = coord.v[2] = coord.v[3] =
            std::numeric_limits<double>::quiet_NaN();
    else if (direction == PJ_FWD)
        pj_fwd4d(coord, P);
    else
        pj_inv4d(coord, P);
    return coord;
}

/*****************************************************************************/
PJ *proj_trans_get_last_used_operation(PJ *P)
/******************************************************************************
    Return the operation used during the last invocation of proj_trans().
    This is especially useful when P has been created with
proj_create_crs_to_crs() and has several alternative operations. The returned
object must be freed with proj_destroy().
******************************************************************************/
{
    if (nullptr == P || P->iCurCoordOp < 0)
        return nullptr;
    if (P->alternativeCoordinateOperations.empty())
        return proj_clone(P->ctx, P);
    return proj_clone(P->ctx,
                      P->alternativeCoordinateOperations[P->iCurCoordOp].pj);
}

/*****************************************************************************/
int proj_trans_array(PJ *P, PJ_DIRECTION direction, size_t n, PJ_COORD *coord) {
    /******************************************************************************
        Batch transform an array of PJ_COORD.

        Performs transformation on all points, even if errors occur on some
    points.

        Individual points that fail to transform will have their components set
    to HUGE_VAL

        Returns 0 if all coordinates are transformed without error, otherwise
        returns a precise error number if all coordinates that fail to transform
        for the same reason, or a generic error code if they fail for different
        reasons.
    ******************************************************************************/
    size_t i;
    int retErrno = 0;
    bool hasSetRetErrno = false;
    bool sameRetErrno = true;

    for (i = 0; i < n; i++) {
        proj_context_errno_set(P->ctx, 0);
        coord[i] = proj_trans(P, direction, coord[i]);
        int thisErrno = proj_errno(P);
        if (thisErrno != 0) {
            if (!hasSetRetErrno) {
                retErrno = thisErrno;
                hasSetRetErrno = true;
            } else if (sameRetErrno && retErrno != thisErrno) {
                sameRetErrno = false;
                retErrno = PROJ_ERR_COORD_TRANSFM;
            }
        }
    }

    proj_context_errno_set(P->ctx, retErrno);

    return retErrno;
}

/*************************************************************************************/
size_t proj_trans_generic(PJ *P, PJ_DIRECTION direction, double *x, size_t sx,
                          size_t nx, double *y, size_t sy, size_t ny, double *z,
                          size_t sz, size_t nz, double *t, size_t st,
                          size_t nt) {
    /**************************************************************************************

        Transform a series of coordinates, where the individual coordinate
    dimension may be represented by an array that is either

            1. fully populated
            2. a null pointer and/or a length of zero, which will be treated as
    a fully populated array of zeroes
            3. of length one, i.e. a constant, which will be treated as a fully
               populated array of that constant value

        The strides, sx, sy, sz, st, represent the step length, in bytes,
    between consecutive elements of the corresponding array. This makes it
    possible for proj_transform to handle transformation of a large class of
    application specific data structures, without necessarily understanding the
    data structure format, as in:

            typedef struct {double x, y; int quality_level; char
    surveyor_name[134];} XYQS; XYQS survey[345]; double height = 23.45; PJ *P =
    {...}; size_t stride = sizeof (XYQS);
            ...
            proj_transform (
                P, PJ_INV, sizeof(XYQS),
                &(survey[0].x), stride, 345,  (*  We have 345 eastings  *)
                &(survey[0].y), stride, 345,  (*  ...and 345 northings. *)
                &height, 1,                   (*  The height is the
    constant  23.45 m *) 0, 0                          (*  and the time is the
    constant 0.00 s *)
            );

        This is similar to the inner workings of the pj_transform function, but
    the stride functionality has been generalized to work for any size of basic
    unit, not just a fixed number of doubles.

        In most cases, the stride will be identical for x, y,z, and t, since
    they will typically be either individual arrays (stride = sizeof(double)),
    or strided views into an array of application specific data structures
    (stride = sizeof (...)).

        But in order to support cases where x, y, z, and t come from
    heterogeneous sources, individual strides, sx, sy, sz, st, are used.

        Caveat: Since proj_transform does its work *in place*, this means that
    even the supposedly constants (i.e. length 1 arrays) will return from the
    call in altered state. Hence, remember to reinitialize between repeated
    calls.

        Return value: Number of transformations completed.

    **************************************************************************************/
    PJ_COORD coord = {{0, 0, 0, 0}};
    size_t i, nmin;
    double null_broadcast = 0;
    double invalid_time = HUGE_VAL;

    if (nullptr == P)
        return 0;

    if (P->inverted)
        direction = pj_opposite_direction(direction);

    /* ignore lengths of null arrays */
    if (nullptr == x)
        nx = 0;
    if (nullptr == y)
        ny = 0;
    if (nullptr == z)
        nz = 0;
    if (nullptr == t)
        nt = 0;

    /* and make the nullities point to some real world memory for broadcasting
     * nulls */
    if (0 == nx)
        x = &null_broadcast;
    if (0 == ny)
        y = &null_broadcast;
    if (0 == nz)
        z = &null_broadcast;
    if (0 == nt)
        t = &invalid_time;

    /* nothing to do? */
    if (0 == nx + ny + nz + nt)
        return 0;

    /* arrays of length 1 are constants, which we broadcast along the longer
     * arrays */
    /* so we need to find the length of the shortest non-unity array to figure
     * out
     */
    /* how many coordinate pairs we must transform */
    nmin = (nx > 1) ? nx : (ny > 1) ? ny : (nz > 1) ? nz : (nt > 1) ? nt : 1;
    if ((nx > 1) && (nx < nmin))
        nmin = nx;
    if ((ny > 1) && (ny < nmin))
        nmin = ny;
    if ((nz > 1) && (nz < nmin))
        nmin = nz;
    if ((nt > 1) && (nt < nmin))
        nmin = nt;

    /* Check validity of direction flag */
    switch (direction) {
    case PJ_FWD:
    case PJ_INV:
        break;
    case PJ_IDENT:
        return nmin;
    }

    /* Arrays of length==0 are broadcast as the constant 0               */
    /* Arrays of length==1 are broadcast as their single value           */
    /* Arrays of length >1 are iterated over (for the first nmin values) */
    /* The slightly convolved incremental indexing is used due           */
    /* to the stride, which may be any size supported by the platform    */
    for (i = 0; i < nmin; i++) {
        coord.xyzt.x = *x;
        coord.xyzt.y = *y;
        coord.xyzt.z = *z;
        coord.xyzt.t = *t;

        coord = proj_trans(P, direction, coord);

        /* in all full length cases, we overwrite the input with the output,  */
        /* and step on to the next element.                                   */
        /* The casts are somewhat funky, but they compile down to no-ops and  */
        /* they tell compilers and static analyzers that we know what we do   */
        if (nx > 1) {
            *x = coord.xyzt.x;
            x = reinterpret_cast<double *>((reinterpret_cast<char *>(x) + sx));
        }
        if (ny > 1) {
            *y = coord.xyzt.y;
            y = reinterpret_cast<double *>((reinterpret_cast<char *>(y) + sy));
        }
        if (nz > 1) {
            *z = coord.xyzt.z;
            z = reinterpret_cast<double *>((reinterpret_cast<char *>(z) + sz));
        }
        if (nt > 1) {
            *t = coord.xyzt.t;
            t = reinterpret_cast<double *>((reinterpret_cast<char *>(t) + st));
        }
    }

    /* Last time around, we update the length 1 cases with their transformed
     * alter egos */
    if (nx == 1)
        *x = coord.xyzt.x;
    if (ny == 1)
        *y = coord.xyzt.y;
    if (nz == 1)
        *z = coord.xyzt.z;
    if (nt == 1)
        *t = coord.xyzt.t;

    return i;
}

static bool inline coord_is_all_nans(PJ_COORD coo) {
    return std::isnan(coo.v[0]) && std::isnan(coo.v[1]) &&
           std::isnan(coo.v[2]) && std::isnan(coo.v[3]);
}

/* Measure numerical deviation after n roundtrips fwd-inv (or inv-fwd) */
double proj_roundtrip(PJ *P, PJ_DIRECTION direction, int n, PJ_COORD *coord) {
    int i;
    PJ_COORD t, org;

    if (nullptr == P)
        return HUGE_VAL;

    if (n < 1) {
        proj_log_error(P, _("n should be >= 1"));
        proj_errno_set(P, PROJ_ERR_OTHER_API_MISUSE);
        return HUGE_VAL;
    }

    /* in the first half-step, we generate the output value */
    org = *coord;
    *coord = proj_trans(P, direction, org);
    t = *coord;

    /* now we take n-1 full steps in inverse direction: We are */
    /* out of phase due to the half step already taken */
    for (i = 0; i < n - 1; i++)
        t = proj_trans(P, direction,
                       proj_trans(P, pj_opposite_direction(direction), t));

    /* finally, we take the last half-step */
    t = proj_trans(P, pj_opposite_direction(direction), t);

    /* if we start with any NaN, we expect all NaN as output */
    if (pj_coord_has_nans(org) && coord_is_all_nans(t)) {
        return 0.0;
    }

    /* checking for angular *input* since we do a roundtrip, and end where we
     * begin */
    if (proj_angular_input(P, direction))
        return proj_lpz_dist(P, org, t);

    return proj_xyz_dist(org, t);
}
