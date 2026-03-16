/******************************************************************************
 * Project:  PROJ
 * Purpose:  Implements proj_create_crs_to_crs() and the like
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

#include <algorithm>
#include <limits>

#include "proj/internal/internal.hpp"

using namespace NS_PROJ::internal;

/** Adds a " +type=crs" suffix to a PROJ string (if it is a PROJ string) */
std::string pj_add_type_crs_if_needed(const std::string &str) {
    std::string ret(str);
    if ((starts_with(str, "proj=") || starts_with(str, "+proj=") ||
         starts_with(str, "+init=") || starts_with(str, "+title=")) &&
        str.find("type=crs") == std::string::npos) {
        ret += " +type=crs";
    }
    return ret;
}

/*****************************************************************************/
static void reproject_bbox(PJ *pjGeogToCrs, double west_lon, double south_lat,
                           double east_lon, double north_lat, double &minx,
                           double &miny, double &maxx, double &maxy) {
    /*****************************************************************************/

    minx = -std::numeric_limits<double>::max();
    miny = -std::numeric_limits<double>::max();
    maxx = std::numeric_limits<double>::max();
    maxy = std::numeric_limits<double>::max();

    if (!(west_lon == -180.0 && east_lon == 180.0 && south_lat == -90.0 &&
          north_lat == 90.0)) {
        minx = -minx;
        miny = -miny;
        maxx = -maxx;
        maxy = -maxy;

        constexpr int N_STEPS = 20;
        constexpr int N_STEPS_P1 = N_STEPS + 1;
        constexpr int XY_SIZE = N_STEPS_P1 * 4;
        std::vector<double> x(XY_SIZE);
        std::vector<double> y(XY_SIZE);
        const double step_lon = (east_lon - west_lon) / N_STEPS;
        const double step_lat = (north_lat - south_lat) / N_STEPS;
        for (int j = 0; j <= N_STEPS; j++) {
            x[j] = west_lon + j * step_lon;
            y[j] = south_lat;
            x[N_STEPS_P1 + j] = x[j];
            y[N_STEPS_P1 + j] = north_lat;
            x[N_STEPS_P1 * 2 + j] = west_lon;
            y[N_STEPS_P1 * 2 + j] = south_lat + j * step_lat;
            x[N_STEPS_P1 * 3 + j] = east_lon;
            y[N_STEPS_P1 * 3 + j] = y[N_STEPS_P1 * 2 + j];
        }
        proj_trans_generic(pjGeogToCrs, PJ_FWD, &x[0], sizeof(double), XY_SIZE,
                           &y[0], sizeof(double), XY_SIZE, nullptr, 0, 0,
                           nullptr, 0, 0);
        for (int j = 0; j < XY_SIZE; j++) {
            if (x[j] != HUGE_VAL && y[j] != HUGE_VAL) {
                minx = std::min(minx, x[j]);
                miny = std::min(miny, y[j]);
                maxx = std::max(maxx, x[j]);
                maxy = std::max(maxy, y[j]);
            }
        }
    }
}

/*****************************************************************************/
static PJ *add_coord_op_to_list(
    int idxInOriginalList, PJ *op, double west_lon, double south_lat,
    double east_lon, double north_lat, PJ *pjGeogToSrc, PJ *pjGeogToDst,
    const PJ *pjSrcGeocentricToLonLat, const PJ *pjDstGeocentricToLonLat,
    const char *areaName, std::vector<PJCoordOperation> &altCoordOps) {
    /*****************************************************************************/

    double minxSrc;
    double minySrc;
    double maxxSrc;
    double maxySrc;
    double minxDst;
    double minyDst;
    double maxxDst;
    double maxyDst;

    double w = west_lon / 180 * M_PI;
    double s = south_lat / 180 * M_PI;
    double e = east_lon / 180 * M_PI;
    double n = north_lat / 180 * M_PI;
    if (w > e) {
        e += 2 * M_PI;
    }
    // Integrate cos(lat) between south_lat and north_lat
    const double pseudoArea = (e - w) * (std::sin(n) - std::sin(s));

    if (pjSrcGeocentricToLonLat) {
        minxSrc = west_lon;
        minySrc = south_lat;
        maxxSrc = east_lon;
        maxySrc = north_lat;
    } else {
        reproject_bbox(pjGeogToSrc, west_lon, south_lat, east_lon, north_lat,
                       minxSrc, minySrc, maxxSrc, maxySrc);
    }

    if (pjDstGeocentricToLonLat) {
        minxDst = west_lon;
        minyDst = south_lat;
        maxxDst = east_lon;
        maxyDst = north_lat;
    } else {
        reproject_bbox(pjGeogToDst, west_lon, south_lat, east_lon, north_lat,
                       minxDst, minyDst, maxxDst, maxyDst);
    }

    if (minxSrc <= maxxSrc && minxDst <= maxxDst) {
        const char *c_name = proj_get_name(op);
        std::string name(c_name ? c_name : "");

        const double accuracy = proj_coordoperation_get_accuracy(op->ctx, op);
        altCoordOps.emplace_back(
            idxInOriginalList, minxSrc, minySrc, maxxSrc, maxySrc, minxDst,
            minyDst, maxxDst, maxyDst, op, name, accuracy, pseudoArea, areaName,
            pjSrcGeocentricToLonLat, pjDstGeocentricToLonLat);
        op = nullptr;
    }
    return op;
}

namespace {
struct ObjectKeeper {
    PJ *m_obj = nullptr;
    explicit ObjectKeeper(PJ *obj) : m_obj(obj) {}
    ~ObjectKeeper() { proj_destroy(m_obj); }
    ObjectKeeper(const ObjectKeeper &) = delete;
    ObjectKeeper &operator=(const ObjectKeeper &) = delete;
};
} // namespace

/*****************************************************************************/
static PJ *create_operation_to_geog_crs(PJ_CONTEXT *ctx, const PJ *crs) {
    /*****************************************************************************/

    std::unique_ptr<ObjectKeeper> keeper;
    if (proj_get_type(crs) == PJ_TYPE_COORDINATE_METADATA) {
        auto tmp = proj_get_source_crs(ctx, crs);
        assert(tmp);
        keeper.reset(new ObjectKeeper(tmp));
        crs = tmp;
    }
    (void)keeper;

    // Create a geographic 2D long-lat degrees CRS that is related to the
    // CRS
    auto geodetic_crs = proj_crs_get_geodetic_crs(ctx, crs);
    if (!geodetic_crs) {
        proj_context_log_debug(ctx, "Cannot find geodetic CRS matching CRS");
        return nullptr;
    }

    auto geodetic_crs_type = proj_get_type(geodetic_crs);
    if (geodetic_crs_type == PJ_TYPE_GEOCENTRIC_CRS ||
        geodetic_crs_type == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
        geodetic_crs_type == PJ_TYPE_GEOGRAPHIC_3D_CRS) {
        auto datum = proj_crs_get_datum_forced(ctx, geodetic_crs);
        assert(datum);
        auto cs = proj_create_ellipsoidal_2D_cs(
            ctx, PJ_ELLPS2D_LONGITUDE_LATITUDE, nullptr, 0);
        auto ellps = proj_get_ellipsoid(ctx, datum);
        proj_destroy(datum);
        double semi_major_metre = 0;
        double inv_flattening = 0;
        proj_ellipsoid_get_parameters(ctx, ellps, &semi_major_metre, nullptr,
                                      nullptr, &inv_flattening);
        // It is critical to set the prime meridian to 0
        auto temp = proj_create_geographic_crs(
            ctx, "unnamed crs", "unnamed datum", proj_get_name(ellps),
            semi_major_metre, inv_flattening, "Reference prime meridian", 0,
            nullptr, 0, cs);
        proj_destroy(ellps);
        proj_destroy(cs);
        proj_destroy(geodetic_crs);
        geodetic_crs = temp;
        geodetic_crs_type = proj_get_type(geodetic_crs);
    }
    if (geodetic_crs_type != PJ_TYPE_GEOGRAPHIC_2D_CRS) {
        // Shouldn't happen
        proj_context_log_debug(ctx, "Cannot find geographic CRS matching CRS");
        proj_destroy(geodetic_crs);
        return nullptr;
    }

    // Create the transformation from this geographic 2D CRS to the source CRS
    auto operation_ctx = proj_create_operation_factory_context(ctx, nullptr);
    proj_operation_factory_context_set_spatial_criterion(
        ctx, operation_ctx, PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION);
    proj_operation_factory_context_set_grid_availability_use(
        ctx, operation_ctx,
        PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID);
    auto target_crs_2D = proj_crs_demote_to_2D(ctx, nullptr, crs);
    auto op_list_to_geodetic =
        proj_create_operations(ctx, geodetic_crs, target_crs_2D, operation_ctx);
    proj_destroy(target_crs_2D);
    proj_operation_factory_context_destroy(operation_ctx);
    proj_destroy(geodetic_crs);

    const int nOpCount = op_list_to_geodetic == nullptr
                             ? 0
                             : proj_list_get_count(op_list_to_geodetic);
    if (nOpCount == 0) {
        proj_context_log_debug(
            ctx, "Cannot compute transformation from geographic CRS to CRS");
        proj_list_destroy(op_list_to_geodetic);
        return nullptr;
    }
    PJ *opGeogToCrs = nullptr;
    // Use in priority operations *without* grids
    for (int i = 0; i < nOpCount; i++) {
        auto op = proj_list_get(ctx, op_list_to_geodetic, i);
        assert(op);
        if (proj_coordoperation_get_grid_used_count(ctx, op) == 0) {
            opGeogToCrs = op;
            break;
        }
        proj_destroy(op);
    }
    if (opGeogToCrs == nullptr) {
        opGeogToCrs = proj_list_get(ctx, op_list_to_geodetic, 0);
        assert(opGeogToCrs);
    }
    proj_list_destroy(op_list_to_geodetic);
    return opGeogToCrs;
}

/*****************************************************************************/
static PJ *create_operation_geocentric_crs_to_geog_crs(PJ_CONTEXT *ctx,
                                                       const PJ *geocentric_crs)
/*****************************************************************************/
{
    assert(proj_get_type(geocentric_crs) == PJ_TYPE_GEOCENTRIC_CRS);

    auto datum = proj_crs_get_datum_forced(ctx, geocentric_crs);
    assert(datum);
    auto cs = proj_create_ellipsoidal_2D_cs(ctx, PJ_ELLPS2D_LONGITUDE_LATITUDE,
                                            nullptr, 0);
    auto ellps = proj_get_ellipsoid(ctx, datum);
    proj_destroy(datum);
    double semi_major_metre = 0;
    double inv_flattening = 0;
    proj_ellipsoid_get_parameters(ctx, ellps, &semi_major_metre, nullptr,
                                  nullptr, &inv_flattening);
    // It is critical to set the prime meridian to 0
    auto lon_lat_crs = proj_create_geographic_crs(
        ctx, "unnamed crs", "unnamed datum", proj_get_name(ellps),
        semi_major_metre, inv_flattening, "Reference prime meridian", 0,
        nullptr, 0, cs);
    proj_destroy(ellps);
    proj_destroy(cs);

    // Create the transformation from this geocentric CRS to the lon-lat one
    auto operation_ctx = proj_create_operation_factory_context(ctx, nullptr);
    proj_operation_factory_context_set_spatial_criterion(
        ctx, operation_ctx, PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION);
    proj_operation_factory_context_set_grid_availability_use(
        ctx, operation_ctx,
        PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID);
    auto op_list =
        proj_create_operations(ctx, geocentric_crs, lon_lat_crs, operation_ctx);
    proj_operation_factory_context_destroy(operation_ctx);
    proj_destroy(lon_lat_crs);

    const int nOpCount = op_list == nullptr ? 0 : proj_list_get_count(op_list);
    if (nOpCount != 1) {
        proj_context_log_debug(ctx, "Cannot compute transformation from "
                                    "geocentric CRS to geographic CRS");
        proj_list_destroy(op_list);
        return nullptr;
    }

    auto op = proj_list_get(ctx, op_list, 0);
    assert(op);
    proj_list_destroy(op_list);
    return op;
}

/*****************************************************************************/
PJ *proj_create_crs_to_crs(PJ_CONTEXT *ctx, const char *source_crs,
                           const char *target_crs, PJ_AREA *area) {
    /******************************************************************************
        Create a transformation pipeline between two known coordinate reference
        systems.

        See docs/source/development/reference/functions.rst

    ******************************************************************************/
    if (!ctx) {
        ctx = pj_get_default_ctx();
    }

    PJ *src;
    PJ *dst;
    try {
        std::string source_crs_modified(pj_add_type_crs_if_needed(source_crs));
        std::string target_crs_modified(pj_add_type_crs_if_needed(target_crs));

        src = proj_create(ctx, source_crs_modified.c_str());
        if (!src) {
            proj_context_log_debug(ctx, "Cannot instantiate source_crs");
            return nullptr;
        }

        dst = proj_create(ctx, target_crs_modified.c_str());
        if (!dst) {
            proj_context_log_debug(ctx, "Cannot instantiate target_crs");
            proj_destroy(src);
            return nullptr;
        }
    } catch (const std::exception &) {
        return nullptr;
    }

    auto ret = proj_create_crs_to_crs_from_pj(ctx, src, dst, area, nullptr);
    proj_destroy(src);
    proj_destroy(dst);
    return ret;
}

/*****************************************************************************/
std::vector<PJCoordOperation>
pj_create_prepared_operations(PJ_CONTEXT *ctx, const PJ *source_crs,
                              const PJ *target_crs, PJ_OBJ_LIST *op_list)
/*****************************************************************************/
{
    PJ *pjGeogToSrc = nullptr;
    PJ *pjSrcGeocentricToLonLat = nullptr;
    if (proj_get_type(source_crs) == PJ_TYPE_GEOCENTRIC_CRS) {
        pjSrcGeocentricToLonLat =
            create_operation_geocentric_crs_to_geog_crs(ctx, source_crs);
        if (!pjSrcGeocentricToLonLat) {
            // shouldn't happen
            return {};
        }
    } else {
        pjGeogToSrc = create_operation_to_geog_crs(ctx, source_crs);
        if (!pjGeogToSrc) {
            proj_context_log_debug(
                ctx, "Cannot create transformation from geographic "
                     "CRS of source CRS to source CRS");
            return {};
        }
    }

    PJ *pjGeogToDst = nullptr;
    PJ *pjDstGeocentricToLonLat = nullptr;
    if (proj_get_type(target_crs) == PJ_TYPE_GEOCENTRIC_CRS) {
        pjDstGeocentricToLonLat =
            create_operation_geocentric_crs_to_geog_crs(ctx, target_crs);
        if (!pjDstGeocentricToLonLat) {
            // shouldn't happen
            proj_destroy(pjSrcGeocentricToLonLat);
            proj_destroy(pjGeogToSrc);
            return {};
        }
    } else {
        pjGeogToDst = create_operation_to_geog_crs(ctx, target_crs);
        if (!pjGeogToDst) {
            proj_context_log_debug(
                ctx, "Cannot create transformation from geographic "
                     "CRS of target CRS to target CRS");
            proj_destroy(pjSrcGeocentricToLonLat);
            proj_destroy(pjGeogToSrc);
            return {};
        }
    }

    try {
        std::vector<PJCoordOperation> preparedOpList;

        // Iterate over source->target candidate transformations and reproject
        // their long-lat bounding box into the source CRS.
        const auto op_count = proj_list_get_count(op_list);
        for (int i = 0; i < op_count; i++) {
            auto op = proj_list_get(ctx, op_list, i);
            assert(op);
            double west_lon = 0.0;
            double south_lat = 0.0;
            double east_lon = 0.0;
            double north_lat = 0.0;

            const char *areaName = nullptr;
            if (!proj_get_area_of_use(ctx, op, &west_lon, &south_lat, &east_lon,
                                      &north_lat, &areaName)) {
                west_lon = -180;
                south_lat = -90;
                east_lon = 180;
                north_lat = 90;
            }

            if (west_lon <= east_lon) {
                op = add_coord_op_to_list(
                    i, op, west_lon, south_lat, east_lon, north_lat,
                    pjGeogToSrc, pjGeogToDst, pjSrcGeocentricToLonLat,
                    pjDstGeocentricToLonLat, areaName, preparedOpList);
            } else {
                auto op_clone = proj_clone(ctx, op);

                op = add_coord_op_to_list(
                    i, op, west_lon, south_lat, 180, north_lat, pjGeogToSrc,
                    pjGeogToDst, pjSrcGeocentricToLonLat,
                    pjDstGeocentricToLonLat, areaName, preparedOpList);
                op_clone = add_coord_op_to_list(
                    i, op_clone, -180, south_lat, east_lon, north_lat,
                    pjGeogToSrc, pjGeogToDst, pjSrcGeocentricToLonLat,
                    pjDstGeocentricToLonLat, areaName, preparedOpList);
                proj_destroy(op_clone);
            }

            proj_destroy(op);
        }

        proj_destroy(pjGeogToSrc);
        proj_destroy(pjGeogToDst);
        proj_destroy(pjSrcGeocentricToLonLat);
        proj_destroy(pjDstGeocentricToLonLat);
        return preparedOpList;
    } catch (const std::exception &) {
        proj_destroy(pjGeogToSrc);
        proj_destroy(pjGeogToDst);
        proj_destroy(pjSrcGeocentricToLonLat);
        proj_destroy(pjDstGeocentricToLonLat);
        return {};
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static const char *getOptionValue(const char *option,
                                  const char *keyWithEqual) noexcept {
    if (ci_starts_with(option, keyWithEqual)) {
        return option + strlen(keyWithEqual);
    }
    return nullptr;
}
//! @endcond

/*****************************************************************************/
PJ *proj_create_crs_to_crs_from_pj(PJ_CONTEXT *ctx, const PJ *source_crs,
                                   const PJ *target_crs, PJ_AREA *area,
                                   const char *const *options) {
    /******************************************************************************
        Create a transformation pipeline between two known coordinate reference
        systems.

        See docs/source/development/reference/functions.rst

    ******************************************************************************/
    if (!ctx) {
        ctx = pj_get_default_ctx();
    }
    pj_load_ini(
        ctx); // to set ctx->errorIfBestTransformationNotAvailableDefault

    const char *authority = nullptr;
    double accuracy = -1;
    bool allowBallparkTransformations = true;
    bool forceOver = false;
    bool warnIfBestTransformationNotAvailable =
        ctx->warnIfBestTransformationNotAvailableDefault;
    bool errorIfBestTransformationNotAvailable =
        ctx->errorIfBestTransformationNotAvailableDefault;
    for (auto iter = options; iter && iter[0]; ++iter) {
        const char *value;
        if ((value = getOptionValue(*iter, "AUTHORITY="))) {
            authority = value;
        } else if ((value = getOptionValue(*iter, "ACCURACY="))) {
            accuracy = pj_atof(value);
        } else if ((value = getOptionValue(*iter, "ALLOW_BALLPARK="))) {
            if (ci_equal(value, "yes"))
                allowBallparkTransformations = true;
            else if (ci_equal(value, "no"))
                allowBallparkTransformations = false;
            else {
                ctx->logger(ctx->logger_app_data, PJ_LOG_ERROR,
                            "Invalid value for ALLOW_BALLPARK option.");
                return nullptr;
            }
        } else if ((value = getOptionValue(*iter, "ONLY_BEST="))) {
            warnIfBestTransformationNotAvailable = false;
            if (ci_equal(value, "yes"))
                errorIfBestTransformationNotAvailable = true;
            else if (ci_equal(value, "no"))
                errorIfBestTransformationNotAvailable = false;
            else {
                ctx->logger(ctx->logger_app_data, PJ_LOG_ERROR,
                            "Invalid value for ONLY_BEST option.");
                return nullptr;
            }
        } else if ((value = getOptionValue(*iter, "FORCE_OVER="))) {
            if (ci_equal(value, "yes")) {
                forceOver = true;
            }
        } else {
            std::string msg("Unknown option :");
            msg += *iter;
            ctx->logger(ctx->logger_app_data, PJ_LOG_ERROR, msg.c_str());
            return nullptr;
        }
    }

    auto operation_ctx = proj_create_operation_factory_context(ctx, authority);
    if (!operation_ctx) {
        return nullptr;
    }

    proj_operation_factory_context_set_allow_ballpark_transformations(
        ctx, operation_ctx, allowBallparkTransformations);

    if (accuracy >= 0) {
        proj_operation_factory_context_set_desired_accuracy(ctx, operation_ctx,
                                                            accuracy);
    }

    if (area && area->bbox_set) {
        proj_operation_factory_context_set_area_of_interest(
            ctx, operation_ctx, area->west_lon_degree, area->south_lat_degree,
            area->east_lon_degree, area->north_lat_degree);

        if (!area->name.empty()) {
            proj_operation_factory_context_set_area_of_interest_name(
                ctx, operation_ctx, area->name.c_str());
        }
    }

    proj_operation_factory_context_set_spatial_criterion(
        ctx, operation_ctx, PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION);
    proj_operation_factory_context_set_grid_availability_use(
        ctx, operation_ctx,
        (errorIfBestTransformationNotAvailable ||
         warnIfBestTransformationNotAvailable ||
         proj_context_is_network_enabled(ctx))
            ? PROJ_GRID_AVAILABILITY_KNOWN_AVAILABLE
            : PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID);

    auto op_list =
        proj_create_operations(ctx, source_crs, target_crs, operation_ctx);
    proj_operation_factory_context_destroy(operation_ctx);

    if (!op_list) {
        return nullptr;
    }

    auto op_count = proj_list_get_count(op_list);
    if (op_count == 0) {
        proj_list_destroy(op_list);

        proj_context_log_debug(ctx, "No operation found matching criteria");
        return nullptr;
    }

    ctx->forceOver = forceOver;

    const int old_debug_level = ctx->debug_level;
    if (errorIfBestTransformationNotAvailable ||
        warnIfBestTransformationNotAvailable)
        ctx->debug_level = PJ_LOG_NONE;
    PJ *P = proj_list_get(ctx, op_list, 0);
    ctx->debug_level = old_debug_level;
    assert(P);

    if (P != nullptr) {
        P->errorIfBestTransformationNotAvailable =
            errorIfBestTransformationNotAvailable;
        P->warnIfBestTransformationNotAvailable =
            warnIfBestTransformationNotAvailable;
        P->skipNonInstantiable = warnIfBestTransformationNotAvailable;
    }

    const bool mayNeedToReRunWithDiscardMissing =
        (errorIfBestTransformationNotAvailable ||
         warnIfBestTransformationNotAvailable) &&
        !proj_context_is_network_enabled(ctx);
    int singleOpIsInstanciable = -1;
    if (P != nullptr && op_count == 1 && mayNeedToReRunWithDiscardMissing) {
        singleOpIsInstanciable = proj_coordoperation_is_instantiable(ctx, P);
    }

    const auto backup_errno = proj_context_errno(ctx);
    if (P == nullptr ||
        (op_count == 1 && (!mayNeedToReRunWithDiscardMissing ||
                           errorIfBestTransformationNotAvailable ||
                           singleOpIsInstanciable == static_cast<int>(true)))) {
        proj_list_destroy(op_list);
        ctx->forceOver = false;

        if (P != nullptr && (errorIfBestTransformationNotAvailable ||
                             warnIfBestTransformationNotAvailable)) {
            if (singleOpIsInstanciable < 0) {
                singleOpIsInstanciable =
                    proj_coordoperation_is_instantiable(ctx, P);
            }
            if (!singleOpIsInstanciable) {
                pj_warn_about_missing_grid(P);
                if (errorIfBestTransformationNotAvailable) {
                    proj_destroy(P);
                    return nullptr;
                }
            }
        }

        if (P != nullptr) {
            P->over = forceOver;
        }
        return P;
    } else if (op_count == 1 && mayNeedToReRunWithDiscardMissing &&
               !singleOpIsInstanciable) {
        pj_warn_about_missing_grid(P);
    }

    if (errorIfBestTransformationNotAvailable ||
        warnIfBestTransformationNotAvailable)
        ctx->debug_level = PJ_LOG_NONE;
    auto preparedOpList =
        pj_create_prepared_operations(ctx, source_crs, target_crs, op_list);
    ctx->debug_level = old_debug_level;

    ctx->forceOver = false;
    proj_list_destroy(op_list);

    if (preparedOpList.empty()) {
        proj_destroy(P);
        return nullptr;
    }

    bool foundInstanciableAndNonBallpark = false;

    for (auto &op : preparedOpList) {
        op.pj->over = forceOver;
        op.pj->errorIfBestTransformationNotAvailable =
            errorIfBestTransformationNotAvailable;
        op.pj->warnIfBestTransformationNotAvailable =
            warnIfBestTransformationNotAvailable;
        if (mayNeedToReRunWithDiscardMissing &&
            !foundInstanciableAndNonBallpark) {
            if (!proj_coordoperation_has_ballpark_transformation(op.pj->ctx,
                                                                 op.pj) &&
                op.isInstantiable()) {
                foundInstanciableAndNonBallpark = true;
            }
        }
    }
    if (mayNeedToReRunWithDiscardMissing && !foundInstanciableAndNonBallpark) {
        // Re-run proj_create_operations with
        // PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID
        // Can happen for example for NAD27->NAD83 transformation when we
        // have no grid and thus have fallback to Helmert transformation and
        // a WGS84 intermediate.
        operation_ctx = proj_create_operation_factory_context(ctx, authority);
        if (operation_ctx) {
            proj_operation_factory_context_set_allow_ballpark_transformations(
                ctx, operation_ctx, allowBallparkTransformations);

            if (accuracy >= 0) {
                proj_operation_factory_context_set_desired_accuracy(
                    ctx, operation_ctx, accuracy);
            }

            if (area && area->bbox_set) {
                proj_operation_factory_context_set_area_of_interest(
                    ctx, operation_ctx, area->west_lon_degree,
                    area->south_lat_degree, area->east_lon_degree,
                    area->north_lat_degree);

                if (!area->name.empty()) {
                    proj_operation_factory_context_set_area_of_interest_name(
                        ctx, operation_ctx, area->name.c_str());
                }
            }

            proj_operation_factory_context_set_spatial_criterion(
                ctx, operation_ctx,
                PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION);
            proj_operation_factory_context_set_grid_availability_use(
                ctx, operation_ctx,
                PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID);

            op_list = proj_create_operations(ctx, source_crs, target_crs,
                                             operation_ctx);
            proj_operation_factory_context_destroy(operation_ctx);

            if (op_list) {
                ctx->forceOver = forceOver;
                ctx->debug_level = PJ_LOG_NONE;
                auto preparedOpList2 = pj_create_prepared_operations(
                    ctx, source_crs, target_crs, op_list);
                ctx->debug_level = old_debug_level;
                ctx->forceOver = false;
                proj_list_destroy(op_list);

                if (!preparedOpList2.empty()) {
                    // Append new lists of operations to previous one
                    std::vector<PJCoordOperation> newOpList;
                    for (auto &&op : preparedOpList) {
                        if (!proj_coordoperation_has_ballpark_transformation(
                                op.pj->ctx, op.pj)) {
                            newOpList.emplace_back(std::move(op));
                        }
                    }
                    for (auto &&op : preparedOpList2) {
                        op.pj->over = forceOver;
                        op.pj->errorIfBestTransformationNotAvailable =
                            errorIfBestTransformationNotAvailable;
                        op.pj->warnIfBestTransformationNotAvailable =
                            warnIfBestTransformationNotAvailable;
                        newOpList.emplace_back(std::move(op));
                    }
                    preparedOpList = std::move(newOpList);
                } else {
                    // We get there in "cs2cs --only-best --no-ballpark
                    // EPSG:4326+3855 EPSG:4979" use case, where the initial
                    // create_operations returned 1 operation, and the retry
                    // with
                    // PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID
                    // returned 0.
                    if (op_count == 1 &&
                        errorIfBestTransformationNotAvailable) {
                        if (singleOpIsInstanciable < 0) {
                            singleOpIsInstanciable =
                                proj_coordoperation_is_instantiable(ctx, P);
                        }
                        if (!singleOpIsInstanciable) {
                            proj_destroy(P);
                            proj_context_errno_set(ctx, backup_errno);
                            return nullptr;
                        }
                    }
                }
            }
        }
    }

    // If there's finally juste a single result, return it directly
    if (preparedOpList.size() == 1) {
        auto retP = preparedOpList[0].pj;
        preparedOpList[0].pj = nullptr;
        proj_destroy(P);
        return retP;
    }

    P->alternativeCoordinateOperations = std::move(preparedOpList);
    // The returned P is rather dummy
    P->descr = "Set of coordinate operations";
    P->over = forceOver;
    P->iso_obj = nullptr;
    P->fwd = nullptr;
    P->inv = nullptr;
    P->fwd3d = nullptr;
    P->inv3d = nullptr;
    P->fwd4d = nullptr;
    P->inv4d = nullptr;

    return P;
}
