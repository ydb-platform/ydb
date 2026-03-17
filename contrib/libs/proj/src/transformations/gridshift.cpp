/******************************************************************************
 * Project:  PROJ
 * Purpose:  Generic grid shifting, in particular Geographic 3D offsets
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2022, Even Rouault, <even.rouault at spatialys.com>
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

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include <errno.h>
#include <mutex>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include "grids.hpp"
#include "proj/internal/internal.hpp"
#include "proj_internal.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <utility>

PROJ_HEAD(gridshift, "Generic grid shift");

static std::mutex gMutex{};
// Map of (name, isProjected)
static std::map<std::string, bool> gKnownGrids{};

using namespace NS_PROJ;

namespace { // anonymous namespace

struct IXY {
    int32_t x, y;

    inline bool operator!=(const IXY &other) const {
        return x != other.x || y != other.y;
    }
};

struct GridInfo {
    int idxSampleX = -1;
    int idxSampleY = -1;
    int idxSampleZ = -1;
    bool eastingNorthingOffset = false;
    bool bilinearInterpolation = true;
    std::vector<float> shifts{};
    bool swapXYInRes = false;
    std::vector<int> idxSampleXYZ{-1, -1, -1};
    IXY lastIdxXY = IXY{-1, -1};
};

// ---------------------------------------------------------------------------

struct gridshiftData {
    ListOfGenericGrids m_grids{};
    bool m_defer_grid_opening = false;
    int m_error_code_in_defer_grid_opening = 0;
    bool m_bHasHorizontalOffset = false;
    bool m_bHasGeographic3DOffset = false;
    bool m_bHasEllipsoidalHeightOffset = false;
    bool m_bHasVerticalToVertical = false;
    bool m_bHasGeographicToVertical = false;
    bool m_mainGridTypeIsGeographic3DOffset = false;
    bool m_skip_z_transform = false;
    std::string m_mainGridType{};
    std::string m_auxGridType{};
    std::string m_interpolation{};
    std::map<const GenericShiftGrid *, GridInfo> m_cacheGridInfo{};

    //! Offset in X to add in the forward direction, after the correction
    // has been applied. (and reciprocally to subtract in the inverse direction
    // before reading the grid). Used typically for the S-JTSK --> S-JTSK/05
    // grid
    double m_offsetX = 0;

    //! Offset in Y to add in the forward direction, after the correction
    // has been applied. (and reciprocally to subtract in the inverse direction
    // before reading the grid). Used typically for the S-JTSK --> S-JTSK/05
    // grid
    double m_offsetY = 0;

    bool checkGridTypes(PJ *P, bool &isProjectedCoord);
    bool loadGridsIfNeeded(PJ *P);
    const GenericShiftGrid *findGrid(const std::string &type,
                                     const PJ_XYZ &input,
                                     GenericShiftGridSet *&gridSetOut) const;
    PJ_XYZ grid_interpolate(PJ_CONTEXT *ctx, const std::string &type, PJ_XY xy,
                            const GenericShiftGrid *grid,
                            bool &biquadraticInterpolationOut);
    PJ_XYZ grid_apply_internal(PJ_CONTEXT *ctx, const std::string &type,
                               bool isVerticalOnly, const PJ_XYZ in,
                               PJ_DIRECTION direction,
                               const GenericShiftGrid *grid,
                               GenericShiftGridSet *gridset, bool &shouldRetry);

    PJ_XYZ apply(PJ *P, PJ_DIRECTION dir, PJ_XYZ xyz);
};

// ---------------------------------------------------------------------------

bool gridshiftData::checkGridTypes(PJ *P, bool &isProjectedCoord) {
    std::string offsetX, offsetY;
    int gridCount = 0;
    isProjectedCoord = false;
    for (const auto &gridset : m_grids) {
        for (const auto &grid : gridset->grids()) {
            ++gridCount;
            const auto &type = grid->metadataItem("TYPE");
            if (type == "HORIZONTAL_OFFSET") {
                m_bHasHorizontalOffset = true;
                if (offsetX.empty()) {
                    offsetX = grid->metadataItem("constant_offset", 0);
                }
                if (offsetY.empty()) {
                    offsetY = grid->metadataItem("constant_offset", 1);
                }
            } else if (type == "GEOGRAPHIC_3D_OFFSET")
                m_bHasGeographic3DOffset = true;
            else if (type == "ELLIPSOIDAL_HEIGHT_OFFSET")
                m_bHasEllipsoidalHeightOffset = true;
            else if (type == "VERTICAL_OFFSET_VERTICAL_TO_VERTICAL")
                m_bHasVerticalToVertical = true;
            else if (type == "VERTICAL_OFFSET_GEOGRAPHIC_TO_VERTICAL")
                m_bHasGeographicToVertical = true;
            else if (type.empty()) {
                proj_log_error(P, _("Missing TYPE metadata item in grid(s)."));
                return false;
            } else {
                proj_log_error(
                    P, _("Unhandled value for TYPE metadata item in grid(s)."));
                return false;
            }

            isProjectedCoord = !grid->extentAndRes().isGeographic;
        }
    }

    if (!offsetX.empty() || !offsetY.empty()) {
        if (gridCount > 1) {
            // Makes life easier...
            proj_log_error(P, _("Shift offset found in one grid. Only one grid "
                                "with shift offset is supported at a time."));
            return false;
        }
        try {
            m_offsetX = NS_PROJ::internal::c_locale_stod(offsetX);
        } catch (const std::exception &) {
            proj_log_error(P, _("Invalid offset value"));
            return false;
        }
        try {
            m_offsetY = NS_PROJ::internal::c_locale_stod(offsetY);
        } catch (const std::exception &) {
            proj_log_error(P, _("Invalid offset value"));
            return false;
        }
    }

    if (((m_bHasEllipsoidalHeightOffset ? 1 : 0) +
         (m_bHasVerticalToVertical ? 1 : 0) +
         (m_bHasGeographicToVertical ? 1 : 0)) > 1) {
        proj_log_error(P, _("Unsupported mix of grid types."));
        return false;
    }

    if (m_bHasGeographic3DOffset) {
        m_mainGridTypeIsGeographic3DOffset = true;
        m_mainGridType = "GEOGRAPHIC_3D_OFFSET";
    } else if (!m_bHasHorizontalOffset) {
        if (m_bHasEllipsoidalHeightOffset)
            m_mainGridType = "ELLIPSOIDAL_HEIGHT_OFFSET";
        else if (m_bHasGeographicToVertical)
            m_mainGridType = "VERTICAL_OFFSET_GEOGRAPHIC_TO_VERTICAL";
        else {
            assert(m_bHasVerticalToVertical);
            m_mainGridType = "VERTICAL_OFFSET_VERTICAL_TO_VERTICAL";
        }
    } else {
        assert(m_bHasHorizontalOffset);
        m_mainGridType = "HORIZONTAL_OFFSET";
    }

    if (m_bHasHorizontalOffset) {
        if (m_bHasEllipsoidalHeightOffset)
            m_auxGridType = "ELLIPSOIDAL_HEIGHT_OFFSET";
        else if (m_bHasGeographicToVertical)
            m_auxGridType = "VERTICAL_OFFSET_GEOGRAPHIC_TO_VERTICAL";
        else if (m_bHasVerticalToVertical) {
            m_auxGridType = "VERTICAL_OFFSET_VERTICAL_TO_VERTICAL";
        }
    }

    return true;
}

// ---------------------------------------------------------------------------

const GenericShiftGrid *
gridshiftData::findGrid(const std::string &type, const PJ_XYZ &input,
                        GenericShiftGridSet *&gridSetOut) const {
    for (const auto &gridset : m_grids) {
        auto grid = gridset->gridAt(type, input.x, input.y);
        if (grid) {
            gridSetOut = gridset.get();
            return grid;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------

#define REL_TOLERANCE_HGRIDSHIFT 1e-5

PJ_XYZ gridshiftData::grid_interpolate(PJ_CONTEXT *ctx, const std::string &type,
                                       PJ_XY xy, const GenericShiftGrid *grid,
                                       bool &biquadraticInterpolationOut) {
    PJ_XYZ val;

    val.x = val.y = HUGE_VAL;
    val.z = 0;

    const bool isProjectedCoord = !grid->extentAndRes().isGeographic;
    auto iterCache = m_cacheGridInfo.find(grid);
    if (iterCache == m_cacheGridInfo.end()) {
        bool eastingNorthingOffset = false;
        const auto samplesPerPixel = grid->samplesPerPixel();
        int idxSampleY = -1;
        int idxSampleX = -1;
        int idxSampleZ = -1;
        for (int i = 0; i < samplesPerPixel; i++) {
            const auto desc = grid->description(i);
            if (!isProjectedCoord && desc == "latitude_offset") {
                idxSampleY = i;
                const auto unit = grid->unit(idxSampleY);
                if (!unit.empty() && unit != "arc-second") {
                    pj_log(ctx, PJ_LOG_ERROR,
                           "gridshift: Only unit=arc-second currently handled");
                    return val;
                }
            } else if (!isProjectedCoord && desc == "longitude_offset") {
                idxSampleX = i;
                const auto unit = grid->unit(idxSampleX);
                if (!unit.empty() && unit != "arc-second") {
                    pj_log(ctx, PJ_LOG_ERROR,
                           "gridshift: Only unit=arc-second currently handled");
                    return val;
                }
            } else if (isProjectedCoord && desc == "easting_offset") {
                eastingNorthingOffset = true;
                idxSampleX = i;
                const auto unit = grid->unit(idxSampleX);
                if (!unit.empty() && unit != "metre") {
                    pj_log(ctx, PJ_LOG_ERROR,
                           "gridshift: Only unit=metre currently handled");
                    return val;
                }
            } else if (isProjectedCoord && desc == "northing_offset") {
                eastingNorthingOffset = true;
                idxSampleY = i;
                const auto unit = grid->unit(idxSampleY);
                if (!unit.empty() && unit != "metre") {
                    pj_log(ctx, PJ_LOG_ERROR,
                           "gridshift: Only unit=metre currently handled");
                    return val;
                }
            } else if (desc == "ellipsoidal_height_offset" ||
                       desc == "geoid_undulation" || desc == "hydroid_height" ||
                       desc == "vertical_offset") {
                idxSampleZ = i;
                const auto unit = grid->unit(idxSampleZ);
                if (!unit.empty() && unit != "metre") {
                    pj_log(ctx, PJ_LOG_ERROR,
                           "gridshift: Only unit=metre currently handled");
                    return val;
                }
            }
        }
        if (samplesPerPixel >= 2 && idxSampleY < 0 && idxSampleX < 0 &&
            type == "HORIZONTAL_OFFSET") {
            if (isProjectedCoord) {
                eastingNorthingOffset = true;
                idxSampleX = 0;
                idxSampleY = 1;
            } else {
                // X=longitude assumed to be the second component if metadata
                // lacking
                idxSampleX = 1;
                // Y=latitude assumed to be the first component if metadata
                // lacking
                idxSampleY = 0;
            }
        }
        if (type == "HORIZONTAL_OFFSET" || type == "GEOGRAPHIC_3D_OFFSET") {
            if (idxSampleY < 0 || idxSampleX < 0) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "gridshift: grid has not expected samples");
                return val;
            }
        }
        if (type == "ELLIPSOIDAL_HEIGHT_OFFSET" ||
            type == "VERTICAL_OFFSET_GEOGRAPHIC_TO_VERTICAL" ||
            type == "VERTICAL_OFFSET_VERTICAL_TO_VERTICAL" ||
            type == "GEOGRAPHIC_3D_OFFSET") {
            if (idxSampleZ < 0) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "gridshift: grid has not expected samples");
                return val;
            }
        }

        std::string interpolation(m_interpolation);
        if (interpolation.empty())
            interpolation = grid->metadataItem("interpolation_method");
        if (interpolation.empty())
            interpolation = "bilinear";

        if (interpolation != "bilinear" && interpolation != "biquadratic") {
            pj_log(ctx, PJ_LOG_ERROR,
                   "gridshift: Unsupported interpolation_method in grid");
            return val;
        }

        GridInfo gridInfo;
        gridInfo.idxSampleX = idxSampleX;
        gridInfo.idxSampleY = idxSampleY;
        gridInfo.idxSampleZ = m_skip_z_transform ? -1 : idxSampleZ;
        gridInfo.eastingNorthingOffset = eastingNorthingOffset;
        gridInfo.bilinearInterpolation =
            (interpolation == "bilinear" || grid->width() < 3 ||
             grid->height() < 3);
        gridInfo.shifts.resize(3 * 3 * 3);
        if (idxSampleX == 1 && idxSampleY == 0) {
            // Little optimization for the common of grids storing shifts in
            // latitude, longitude, in that order.
            // We want to request data in the order it is stored in the grid,
            // which triggers a read optimization.
            // But we must compensate for that by switching the role of x and y
            // after computation.
            gridInfo.swapXYInRes = true;
            gridInfo.idxSampleXYZ[0] = 0;
            gridInfo.idxSampleXYZ[1] = 1;
        } else {
            gridInfo.idxSampleXYZ[0] = idxSampleX;
            gridInfo.idxSampleXYZ[1] = idxSampleY;
        }
        gridInfo.idxSampleXYZ[2] = idxSampleZ;
        iterCache = m_cacheGridInfo.emplace(grid, std::move(gridInfo)).first;
    }
    // cppcheck-suppress derefInvalidIteratorRedundantCheck
    GridInfo &gridInfo = iterCache->second;
    const int idxSampleX = gridInfo.idxSampleX;
    const int idxSampleY = gridInfo.idxSampleY;
    const int idxSampleZ = gridInfo.idxSampleZ;
    const bool bilinearInterpolation = gridInfo.bilinearInterpolation;
    biquadraticInterpolationOut = !bilinearInterpolation;

    IXY indxy;
    const auto &extent = grid->extentAndRes();
    double x = (xy.x - extent.west) / extent.resX;
    indxy.x = std::isnan(x) ? 0 : (int32_t)lround(floor(x));
    double y = (xy.y - extent.south) / extent.resY;
    indxy.y = std::isnan(y) ? 0 : (int32_t)lround(floor(y));

    PJ_XY frct;
    frct.x = x - indxy.x;
    frct.y = y - indxy.y;
    int tmpInt;
    if (indxy.x < 0) {
        if (indxy.x == -1 && frct.x > 1 - 10 * REL_TOLERANCE_HGRIDSHIFT) {
            ++indxy.x;
            frct.x = 0.;
        } else
            return val;
    } else if ((tmpInt = indxy.x + 1) >= grid->width()) {
        if (tmpInt == grid->width() && frct.x < 10 * REL_TOLERANCE_HGRIDSHIFT) {
            --indxy.x;
            frct.x = 1.;
        } else
            return val;
    }
    if (indxy.y < 0) {
        if (indxy.y == -1 && frct.y > 1 - 10 * REL_TOLERANCE_HGRIDSHIFT) {
            ++indxy.y;
            frct.y = 0.;
        } else
            return val;
    } else if ((tmpInt = indxy.y + 1) >= grid->height()) {
        if (tmpInt == grid->height() &&
            frct.y < 10 * REL_TOLERANCE_HGRIDSHIFT) {
            --indxy.y;
            frct.y = 1.;
        } else
            return val;
    }

    bool nodataFound = false;
    if (bilinearInterpolation) {
        double m10 = frct.x;
        double m11 = m10;
        double m01 = 1. - frct.x;
        double m00 = m01;
        m11 *= frct.y;
        m01 *= frct.y;
        frct.y = 1. - frct.y;
        m00 *= frct.y;
        m10 *= frct.y;
        if (idxSampleX >= 0 && idxSampleY >= 0) {
            if (gridInfo.lastIdxXY != indxy) {
                if (!grid->valuesAt(indxy.x, indxy.y, 2, 2,
                                    idxSampleZ >= 0 ? 3 : 2,
                                    gridInfo.idxSampleXYZ.data(),
                                    gridInfo.shifts.data(), nodataFound) ||
                    nodataFound) {
                    return val;
                }
                gridInfo.lastIdxXY = indxy;
            }
            if (idxSampleZ >= 0) {
                val.x = (m00 * gridInfo.shifts[0] + m10 * gridInfo.shifts[3] +
                         m01 * gridInfo.shifts[6] + m11 * gridInfo.shifts[9]);
                val.y = (m00 * gridInfo.shifts[1] + m10 * gridInfo.shifts[4] +
                         m01 * gridInfo.shifts[7] + m11 * gridInfo.shifts[10]);
                val.z = m00 * gridInfo.shifts[2] + m10 * gridInfo.shifts[5] +
                        m01 * gridInfo.shifts[8] + m11 * gridInfo.shifts[11];
            } else {
                val.x = (m00 * gridInfo.shifts[0] + m10 * gridInfo.shifts[2] +
                         m01 * gridInfo.shifts[4] + m11 * gridInfo.shifts[6]);
                val.y = (m00 * gridInfo.shifts[1] + m10 * gridInfo.shifts[3] +
                         m01 * gridInfo.shifts[5] + m11 * gridInfo.shifts[7]);
            }
        } else {
            val.x = 0;
            val.y = 0;
            if (idxSampleZ >= 0) {
                if (gridInfo.lastIdxXY != indxy) {
                    if (!grid->valuesAt(indxy.x, indxy.y, 2, 2, 1, &idxSampleZ,
                                        gridInfo.shifts.data(), nodataFound) ||
                        nodataFound) {
                        return val;
                    }
                    gridInfo.lastIdxXY = indxy;
                }
                val.z = m00 * gridInfo.shifts[0] + m10 * gridInfo.shifts[1] +
                        m01 * gridInfo.shifts[2] + m11 * gridInfo.shifts[3];
            }
        }
    } else // biquadratic
    {
        // Cf https://geodesy.noaa.gov/library/pdfs/NOAA_TM_NOS_NGS_0084.pdf
        // Depending if we are before or after half-pixel, shift the 3x3 window
        // of interpolation
        if ((frct.x <= 0.5 && indxy.x > 0) || (indxy.x + 2 == grid->width())) {
            indxy.x -= 1;
            frct.x += 1;
        }
        if ((frct.y <= 0.5 && indxy.y > 0) || (indxy.y + 2 == grid->height())) {
            indxy.y -= 1;
            frct.y += 1;
        }

        // Port of qterp() Fortran function from NOAA
        // xToInterp must be in [0,2] range
        // f0 must be f(0), f1 must be f(1), f2 must be f(2)
        // Returns f(xToInterp) interpolated value along the parabolic function
        const auto quadraticInterpol = [](double xToInterp, double f0,
                                          double f1, double f2) {
            const double df0 = f1 - f0;
            const double df1 = f2 - f1;
            const double d2f0 = df1 - df0;
            return f0 + xToInterp * df0 +
                   0.5 * xToInterp * (xToInterp - 1.0) * d2f0;
        };

        if (idxSampleX >= 0 && idxSampleY >= 0) {
            if (gridInfo.lastIdxXY != indxy) {
                if (!grid->valuesAt(indxy.x, indxy.y, 3, 3,
                                    idxSampleZ >= 0 ? 3 : 2,
                                    gridInfo.idxSampleXYZ.data(),
                                    gridInfo.shifts.data(), nodataFound) ||
                    nodataFound) {
                    return val;
                }
                gridInfo.lastIdxXY = indxy;
            }
            const auto *shifts_ptr = gridInfo.shifts.data();
            if (idxSampleZ >= 0) {
                double xyz_shift[3][4];
                for (int j = 0; j <= 2; ++j) {
                    xyz_shift[j][0] = quadraticInterpol(
                        frct.x, shifts_ptr[0], shifts_ptr[3], shifts_ptr[6]);
                    xyz_shift[j][1] = quadraticInterpol(
                        frct.x, shifts_ptr[1], shifts_ptr[4], shifts_ptr[7]);
                    xyz_shift[j][2] = quadraticInterpol(
                        frct.x, shifts_ptr[2], shifts_ptr[5], shifts_ptr[8]);
                    shifts_ptr += 9;
                }
                val.x = quadraticInterpol(frct.y, xyz_shift[0][0],
                                          xyz_shift[1][0], xyz_shift[2][0]);
                val.y = quadraticInterpol(frct.y, xyz_shift[0][1],
                                          xyz_shift[1][1], xyz_shift[2][1]);
                val.z = quadraticInterpol(frct.y, xyz_shift[0][2],
                                          xyz_shift[1][2], xyz_shift[2][2]);
            } else {
                double xy_shift[3][2];
                for (int j = 0; j <= 2; ++j) {
                    xy_shift[j][0] = quadraticInterpol(
                        frct.x, shifts_ptr[0], shifts_ptr[2], shifts_ptr[4]);
                    xy_shift[j][1] = quadraticInterpol(
                        frct.x, shifts_ptr[1], shifts_ptr[3], shifts_ptr[5]);
                    shifts_ptr += 6;
                }
                val.x = quadraticInterpol(frct.y, xy_shift[0][0],
                                          xy_shift[1][0], xy_shift[2][0]);
                val.y = quadraticInterpol(frct.y, xy_shift[0][1],
                                          xy_shift[1][1], xy_shift[2][1]);
            }
        } else {
            val.x = 0;
            val.y = 0;
            if (idxSampleZ >= 0) {
                if (gridInfo.lastIdxXY != indxy) {
                    if (!grid->valuesAt(indxy.x, indxy.y, 3, 3, 1, &idxSampleZ,
                                        gridInfo.shifts.data(), nodataFound) ||
                        nodataFound) {
                        return val;
                    }
                    gridInfo.lastIdxXY = indxy;
                }
                double z_shift[3];
                const auto *shifts_ptr = gridInfo.shifts.data();
                for (int j = 0; j <= 2; ++j) {
                    z_shift[j] = quadraticInterpol(
                        frct.x, shifts_ptr[0], shifts_ptr[1], shifts_ptr[2]);
                    shifts_ptr += 3;
                }
                val.z = quadraticInterpol(frct.y, z_shift[0], z_shift[1],
                                          z_shift[2]);
            }
        }
    }

    if (idxSampleX >= 0 && idxSampleY >= 0 && !gridInfo.eastingNorthingOffset) {
        constexpr double convFactorXY = 1. / 3600 / 180 * M_PI;
        val.x *= convFactorXY;
        val.y *= convFactorXY;
    }

    if (gridInfo.swapXYInRes) {
        std::swap(val.x, val.y);
    }

    return val;
}

// ---------------------------------------------------------------------------

static PJ_XY normalizeX(const GenericShiftGrid *grid, const PJ_XYZ in,
                        const NS_PROJ::ExtentAndRes *&extentOut) {
    PJ_XY normalized;
    normalized.x = in.x;
    normalized.y = in.y;
    extentOut = &(grid->extentAndRes());
    if (extentOut->isGeographic) {
        const double epsilon =
            (extentOut->resX + extentOut->resY) * REL_TOLERANCE_HGRIDSHIFT;
        if (normalized.x < extentOut->west - epsilon)
            normalized.x += 2 * M_PI;
        else if (normalized.x > extentOut->east + epsilon)
            normalized.x -= 2 * M_PI;
    }
    return normalized;
}

// ---------------------------------------------------------------------------

#define MAX_ITERATIONS 10
#define TOL 1e-12

PJ_XYZ gridshiftData::grid_apply_internal(
    PJ_CONTEXT *ctx, const std::string &type, bool isVerticalOnly,
    const PJ_XYZ in, PJ_DIRECTION direction, const GenericShiftGrid *grid,
    GenericShiftGridSet *gridset, bool &shouldRetry) {

    shouldRetry = false;
    if (in.x == HUGE_VAL)
        return in;

    /* normalized longitude of input */
    const NS_PROJ::ExtentAndRes *extent;
    PJ_XY normalized_in = normalizeX(grid, in, extent);

    bool biquadraticInterpolationOut = false;
    PJ_XYZ shift = grid_interpolate(ctx, type, normalized_in, grid,
                                    biquadraticInterpolationOut);
    if (grid->hasChanged()) {
        shouldRetry = gridset->reopen(ctx);
        PJ_XYZ out;
        out.x = out.y = out.z = HUGE_VAL;
        return out;
    }
    if (shift.x == HUGE_VAL)
        return shift;

    if (direction == PJ_FWD) {
        PJ_XYZ out = in;
        out.x += shift.x;
        out.y += shift.y;
        out.z += shift.z;
        return out;
    }

    if (isVerticalOnly) {
        PJ_XYZ out = in;
        out.z -= shift.z;
        return out;
    }

    PJ_XY guess;
    guess.x = normalized_in.x - shift.x;
    guess.y = normalized_in.y - shift.y;

    // NOAA NCAT transformer tool doesn't do iteration in the reverse path.
    // Do the same (only for biquadratic, although NCAT applies this logic to
    // bilinear too)
    // Cf
    // https://github.com/noaa-ngs/ncat-lib/blob/77bcff1ce4a78fe06d0312102ada008aefcc2c62/src/gov/noaa/ngs/grid/Transformer.java#L374
    // When trying to do iterative reverse path with biquadratic, we can
    // get convergence failures on points that are close to the boundary of
    // cells or half-cells. For example with
    // echo -122.4250009683  37.8286740788 0 | bin/cct -I +proj=gridshift
    // +grids=tests/us_noaa_nadcon5_nad83_1986_nad83_harn_conus_extract_sanfrancisco.tif
    // +interpolation=biquadratic
    if (!biquadraticInterpolationOut) {
        int i = MAX_ITERATIONS;
        const double toltol = TOL * TOL;
        PJ_XY diff;
        do {
            shift = grid_interpolate(ctx, type, guess, grid,
                                     biquadraticInterpolationOut);
            if (grid->hasChanged()) {
                shouldRetry = gridset->reopen(ctx);
                PJ_XYZ out;
                out.x = out.y = out.z = HUGE_VAL;
                return out;
            }

            /* We can possibly go outside of the initial guessed grid, so try */
            /* to fetch a new grid into which iterate... */
            if (shift.x == HUGE_VAL) {
                PJ_XYZ lp;
                lp.x = guess.x;
                lp.y = guess.y;
                auto newGrid = findGrid(type, lp, gridset);
                if (newGrid == nullptr || newGrid == grid ||
                    newGrid->isNullGrid())
                    break;
                pj_log(ctx, PJ_LOG_TRACE, "Switching from grid %s to grid %s",
                       grid->name().c_str(), newGrid->name().c_str());
                grid = newGrid;
                normalized_in = normalizeX(grid, in, extent);
                diff.x = std::numeric_limits<double>::max();
                diff.y = std::numeric_limits<double>::max();
                continue;
            }

            diff.x = guess.x + shift.x - normalized_in.x;
            diff.y = guess.y + shift.y - normalized_in.y;
            guess.x -= diff.x;
            guess.y -= diff.y;

        } while (--i && (diff.x * diff.x + diff.y * diff.y >
                         toltol)); /* prob. slightly faster than hypot() */

        if (i == 0) {
            pj_log(ctx, PJ_LOG_TRACE,
                   "Inverse grid shift iterator failed to converge.");
            proj_context_errno_set(ctx, PROJ_ERR_COORD_TRANSFM_NO_CONVERGENCE);
            PJ_XYZ out;
            out.x = out.y = out.z = HUGE_VAL;
            return out;
        }

        if (shift.x == HUGE_VAL) {
            pj_log(
                ctx, PJ_LOG_TRACE,
                "Inverse grid shift iteration failed, presumably at grid edge. "
                "Using first approximation.");
        }
    }

    PJ_XYZ out;
    out.x = extent->isGeographic ? adjlon(guess.x) : guess.x;
    out.y = guess.y;
    out.z = in.z - shift.z;
    return out;
}

// ---------------------------------------------------------------------------

bool gridshiftData::loadGridsIfNeeded(PJ *P) {
    if (m_error_code_in_defer_grid_opening) {
        proj_errno_set(P, m_error_code_in_defer_grid_opening);
        return false;
    } else if (m_defer_grid_opening) {
        m_defer_grid_opening = false;
        m_grids = pj_generic_grid_init(P, "grids");
        m_error_code_in_defer_grid_opening = proj_errno(P);
        if (m_error_code_in_defer_grid_opening) {
            return false;
        }
        bool isProjectedCoord;
        if (!checkGridTypes(P, isProjectedCoord)) {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

static const std::string sHORIZONTAL_OFFSET("HORIZONTAL_OFFSET");

PJ_XYZ gridshiftData::apply(PJ *P, PJ_DIRECTION direction, PJ_XYZ xyz) {
    PJ_XYZ out;

    out.x = HUGE_VAL;
    out.y = HUGE_VAL;
    out.z = HUGE_VAL;

    std::string &type = m_mainGridType;
    bool bFoundGeog3DOffset = false;
    while (true) {
        GenericShiftGridSet *gridset = nullptr;
        const GenericShiftGrid *grid = findGrid(type, xyz, gridset);
        if (!grid) {
            if (m_mainGridTypeIsGeographic3DOffset && m_bHasHorizontalOffset) {
                // If we have a mix of grids with GEOGRAPHIC_3D_OFFSET
                // and HORIZONTAL_OFFSET+ELLIPSOIDAL_HEIGHT_OFFSET
                type = sHORIZONTAL_OFFSET;
                grid = findGrid(type, xyz, gridset);
            }
            if (!grid) {
                proj_context_errno_set(P->ctx,
                                       PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
                return out;
            }
        } else {
            if (m_mainGridTypeIsGeographic3DOffset)
                bFoundGeog3DOffset = true;
        }

        if (grid->isNullGrid()) {
            out = xyz;
            break;
        }
        bool shouldRetry = false;
        out = grid_apply_internal(
            P->ctx, type, !(m_bHasGeographic3DOffset || m_bHasHorizontalOffset),
            xyz, direction, grid, gridset, shouldRetry);
        if (!shouldRetry) {
            break;
        }
    }

    if (out.x == HUGE_VAL || out.y == HUGE_VAL) {
        if (proj_context_errno(P->ctx) == 0) {
            proj_context_errno_set(P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
        }
        return out;
    }

    // Second pass to apply vertical transformation, if it is in a
    // separate grid than lat-lon offsets.
    if (!bFoundGeog3DOffset && !m_auxGridType.empty()) {
        xyz = out;
        while (true) {
            GenericShiftGridSet *gridset = nullptr;
            const auto grid = findGrid(m_auxGridType, xyz, gridset);
            if (!grid) {
                proj_context_errno_set(P->ctx,
                                       PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
                return out;
            }
            if (grid->isNullGrid()) {
                break;
            }

            bool shouldRetry = false;
            out = grid_apply_internal(P->ctx, m_auxGridType, true, xyz,
                                      direction, grid, gridset, shouldRetry);
            if (!shouldRetry) {
                break;
            }
        }

        if (out.x == HUGE_VAL || out.y == HUGE_VAL) {
            proj_context_errno_set(P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_GRID);
            return out;
        }
    }

    return out;
}

} // anonymous namespace

// ---------------------------------------------------------------------------

static PJ_XYZ pj_gridshift_forward_3d(PJ_LPZ lpz, PJ *P) {
    auto Q = static_cast<gridshiftData *>(P->opaque);

    if (!Q->loadGridsIfNeeded(P)) {
        return proj_coord_error().xyz;
    }

    PJ_XYZ xyz;
    xyz.x = lpz.lam;
    xyz.y = lpz.phi;
    xyz.z = lpz.z;

    xyz = Q->apply(P, PJ_FWD, xyz);

    xyz.x += Q->m_offsetX;
    xyz.y += Q->m_offsetY;

    return xyz;
}

// ---------------------------------------------------------------------------

static PJ_LPZ pj_gridshift_reverse_3d(PJ_XYZ xyz, PJ *P) {
    auto Q = static_cast<gridshiftData *>(P->opaque);

    // Must be done before using m_offsetX !
    if (!Q->loadGridsIfNeeded(P)) {
        return proj_coord_error().lpz;
    }

    xyz.x -= Q->m_offsetX;
    xyz.y -= Q->m_offsetY;

    PJ_XYZ xyz_out = Q->apply(P, PJ_INV, xyz);
    PJ_LPZ lpz;
    lpz.lam = xyz_out.x;
    lpz.phi = xyz_out.y;
    lpz.z = xyz_out.z;
    return lpz;
}

// ---------------------------------------------------------------------------

static PJ *pj_gridshift_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    delete static_cast<struct gridshiftData *>(P->opaque);
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

// ---------------------------------------------------------------------------

static void pj_gridshift_reassign_context(PJ *P, PJ_CONTEXT *ctx) {
    auto Q = (struct gridshiftData *)P->opaque;
    for (auto &grid : Q->m_grids) {
        grid->reassign_context(ctx);
    }
}

// ---------------------------------------------------------------------------

PJ *PJ_TRANSFORMATION(gridshift, 0) {
    auto Q = new gridshiftData;
    P->opaque = (void *)Q;
    P->destructor = pj_gridshift_destructor;
    P->reassign_context = pj_gridshift_reassign_context;

    P->fwd3d = pj_gridshift_forward_3d;
    P->inv3d = pj_gridshift_reverse_3d;
    P->fwd = nullptr;
    P->inv = nullptr;

    if (0 == pj_param(P->ctx, P->params, "tgrids").i) {
        proj_log_error(P, _("+grids parameter missing."));
        return pj_gridshift_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    bool isKnownGrid = false;
    bool isProjectedCoord = false;

    if (!P->ctx->defer_grid_opening ||
        !pj_param(P->ctx, P->params, "tcoord_type").i) {
        const char *gridnames = pj_param(P->ctx, P->params, "sgrids").s;
        gMutex.lock();
        const auto iter = gKnownGrids.find(gridnames);
        isKnownGrid = iter != gKnownGrids.end();
        if (isKnownGrid) {
            Q->m_defer_grid_opening = true;
            isProjectedCoord = iter->second;
        }
        gMutex.unlock();
    }

    if (P->ctx->defer_grid_opening || isKnownGrid) {
        Q->m_defer_grid_opening = true;
    } else {
        const char *gridnames = pj_param(P->ctx, P->params, "sgrids").s;
        Q->m_grids = pj_generic_grid_init(P, "grids");
        /* Was gridlist compiled properly? */
        if (proj_errno(P)) {
            proj_log_error(P, _("could not find required grid(s)."));
            return pj_gridshift_destructor(
                P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        }
        if (!Q->checkGridTypes(P, isProjectedCoord)) {
            return pj_gridshift_destructor(
                P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        }

        gMutex.lock();
        gKnownGrids[gridnames] = isProjectedCoord;
        gMutex.unlock();
    }

    if (pj_param(P->ctx, P->params, "tinterpolation").i) {
        const char *interpolation =
            pj_param(P->ctx, P->params, "sinterpolation").s;
        if (strcmp(interpolation, "bilinear") == 0 ||
            strcmp(interpolation, "biquadratic") == 0) {
            Q->m_interpolation = interpolation;
        } else {
            proj_log_error(P, _("Unsupported value for +interpolation."));
            return pj_gridshift_destructor(
                P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    if (pj_param(P->ctx, P->params, "tno_z_transform").i) {
        Q->m_skip_z_transform = true;
    }

    // +coord_type not advertised in documentation on purpose for now.
    // It is probably useless to do it, as the only potential use case of it
    // would be for PROJ itself when generating pipelines with deferred grid
    // opening.
    if (pj_param(P->ctx, P->params, "tcoord_type").i) {
        // Check the coordinate type (projected/geographic) from the explicit
        // +coord_type switch. This is mostly only useful in deferred grid
        // opening, otherwise we have figured it out above in checkGridTypes()
        const char *coord_type = pj_param(P->ctx, P->params, "scoord_type").s;
        if (coord_type) {
            if (strcmp(coord_type, "projected") == 0) {
                if (!P->ctx->defer_grid_opening && !isProjectedCoord) {
                    proj_log_error(P,
                                   _("+coord_type=projected specified, but the "
                                     "grid is known to not be projected"));
                    return pj_gridshift_destructor(
                        P, PROJ_ERR_INVALID_OP_MISSING_ARG);
                }
                isProjectedCoord = true;
            } else if (strcmp(coord_type, "geographic") == 0) {
                if (!P->ctx->defer_grid_opening && isProjectedCoord) {
                    proj_log_error(P, _("+coord_type=geographic specified, but "
                                        "the grid is known to be projected"));
                    return pj_gridshift_destructor(
                        P, PROJ_ERR_INVALID_OP_MISSING_ARG);
                }
            } else {
                proj_log_error(P, _("Unsupported value for +coord_type: valid "
                                    "values are 'geographic' or 'projected'"));
                return pj_gridshift_destructor(P,
                                               PROJ_ERR_INVALID_OP_MISSING_ARG);
            }
        }
    }

    if (isKnownGrid || pj_param(P->ctx, P->params, "tcoord_type").i) {
        if (isProjectedCoord) {
            P->left = PJ_IO_UNITS_PROJECTED;
            P->right = PJ_IO_UNITS_PROJECTED;
        } else {
            P->left = PJ_IO_UNITS_RADIANS;
            P->right = PJ_IO_UNITS_RADIANS;
        }
    } else {
        P->left = PJ_IO_UNITS_WHATEVER;
        P->right = PJ_IO_UNITS_WHATEVER;
    }

    return P;
}

// ---------------------------------------------------------------------------

void pj_clear_gridshift_knowngrids_cache() {
    std::lock_guard<std::mutex> lock(gMutex);
    gKnownGrids.clear();
}
