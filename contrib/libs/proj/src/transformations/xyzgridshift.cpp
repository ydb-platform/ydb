/******************************************************************************
 * Project:  PROJ
 * Purpose:  Geocentric translation using a grid
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2019, Even Rouault, <even.rouault at spatialys.com>
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

#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include "grids.hpp"
#include "proj_internal.h"

#include <algorithm>

PROJ_HEAD(xyzgridshift, "Geocentric grid shift");

using namespace NS_PROJ;

namespace { // anonymous namespace
struct xyzgridshiftData {
    PJ *cart = nullptr;
    bool grid_ref_is_input = true;
    ListOfGenericGrids grids{};
    bool defer_grid_opening = false;
    int error_code_in_defer_grid_opening = 0;
    double multiplier = 1.0;
};
} // anonymous namespace

// ---------------------------------------------------------------------------

static bool get_grid_values(PJ *P, xyzgridshiftData *Q, const PJ_LP &lp,
                            double &dx, double &dy, double &dz) {
    if (Q->defer_grid_opening) {
        Q->defer_grid_opening = false;
        Q->grids = pj_generic_grid_init(P, "grids");
        Q->error_code_in_defer_grid_opening = proj_errno(P);
    }
    if (Q->error_code_in_defer_grid_opening) {
        proj_errno_set(P, Q->error_code_in_defer_grid_opening);
        return false;
    }

    GenericShiftGridSet *gridset = nullptr;
    auto grid = pj_find_generic_grid(Q->grids, lp, gridset);
    if (!grid) {
        return false;
    }
    if (grid->isNullGrid()) {
        dx = 0;
        dy = 0;
        dz = 0;
        return true;
    }
    const auto samplesPerPixel = grid->samplesPerPixel();
    if (samplesPerPixel < 3) {
        proj_log_error(P, "xyzgridshift: grid has not enough samples");
        return false;
    }
    int sampleX = 0;
    int sampleY = 1;
    int sampleZ = 2;
    for (int i = 0; i < samplesPerPixel; i++) {
        const auto desc = grid->description(i);
        if (desc == "x_translation") {
            sampleX = i;
        } else if (desc == "y_translation") {
            sampleY = i;
        } else if (desc == "z_translation") {
            sampleZ = i;
        }
    }
    const auto unit = grid->unit(sampleX);
    if (!unit.empty() && unit != "metre") {
        proj_log_error(P, "xyzgridshift: Only unit=metre currently handled");
        return false;
    }

    bool must_retry = false;
    if (!pj_bilinear_interpolation_three_samples(P->ctx, grid, lp, sampleX,
                                                 sampleY, sampleZ, dx, dy, dz,
                                                 must_retry)) {
        if (must_retry)
            return get_grid_values(P, Q, lp, dx, dy, dz);
        return false;
    }

    dx *= Q->multiplier;
    dy *= Q->multiplier;
    dz *= Q->multiplier;
    return true;
}

// ---------------------------------------------------------------------------

#define SQUARE(x) ((x) * (x))

// ---------------------------------------------------------------------------

static PJ_COORD iterative_adjustment(PJ *P, xyzgridshiftData *Q,
                                     const PJ_COORD &pointInit, double factor) {
    PJ_COORD point = pointInit;
    for (int i = 0; i < 10; i++) {
        PJ_COORD geodetic;
        geodetic.lpz = pj_inv3d(point.xyz, Q->cart);

        double dx, dy, dz;
        if (!get_grid_values(P, Q, geodetic.lp, dx, dy, dz)) {
            return proj_coord_error();
        }

        dx *= factor;
        dy *= factor;
        dz *= factor;

        const double err = SQUARE((point.xyz.x - pointInit.xyz.x) - dx) +
                           SQUARE((point.xyz.y - pointInit.xyz.y) - dy) +
                           SQUARE((point.xyz.z - pointInit.xyz.z) - dz);

        point.xyz.x = pointInit.xyz.x + dx;
        point.xyz.y = pointInit.xyz.y + dy;
        point.xyz.z = pointInit.xyz.z + dz;
        if (err < 1e-10) {
            break;
        }
    }
    return point;
}

// ---------------------------------------------------------------------------

static PJ_COORD direct_adjustment(PJ *P, xyzgridshiftData *Q, PJ_COORD point,
                                  double factor) {
    PJ_COORD geodetic;
    geodetic.lpz = pj_inv3d(point.xyz, Q->cart);

    double dx, dy, dz;
    if (!get_grid_values(P, Q, geodetic.lp, dx, dy, dz)) {
        return proj_coord_error();
    }
    point.xyz.x += factor * dx;
    point.xyz.y += factor * dy;
    point.xyz.z += factor * dz;
    return point;
}

// ---------------------------------------------------------------------------

static PJ_XYZ pj_xyzgridshift_forward_3d(PJ_LPZ lpz, PJ *P) {
    auto Q = static_cast<xyzgridshiftData *>(P->opaque);
    PJ_COORD point = {{0, 0, 0, 0}};
    point.lpz = lpz;

    if (Q->grid_ref_is_input) {
        point = direct_adjustment(P, Q, point, 1.0);
    } else {
        point = iterative_adjustment(P, Q, point, 1.0);
    }

    return point.xyz;
}

static PJ_LPZ pj_xyzgridshift_reverse_3d(PJ_XYZ xyz, PJ *P) {
    auto Q = static_cast<xyzgridshiftData *>(P->opaque);
    PJ_COORD point = {{0, 0, 0, 0}};
    point.xyz = xyz;

    if (Q->grid_ref_is_input) {
        point = iterative_adjustment(P, Q, point, -1.0);
    } else {
        point = direct_adjustment(P, Q, point, -1.0);
    }

    return point.lpz;
}

static PJ *pj_xyzgridshift_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    auto Q = static_cast<struct xyzgridshiftData *>(P->opaque);
    if (Q) {
        if (Q->cart)
            Q->cart->destructor(Q->cart, errlev);
        delete Q;
    }
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

static void pj_xyzgridshift_reassign_context(PJ *P, PJ_CONTEXT *ctx) {
    auto Q = (struct xyzgridshiftData *)P->opaque;
    for (auto &grid : Q->grids) {
        grid->reassign_context(ctx);
    }
}

PJ *PJ_TRANSFORMATION(xyzgridshift, 0) {
    auto Q = new xyzgridshiftData;
    P->opaque = (void *)Q;
    P->destructor = pj_xyzgridshift_destructor;
    P->reassign_context = pj_xyzgridshift_reassign_context;

    P->fwd4d = nullptr;
    P->inv4d = nullptr;
    P->fwd3d = pj_xyzgridshift_forward_3d;
    P->inv3d = pj_xyzgridshift_reverse_3d;
    P->fwd = nullptr;
    P->inv = nullptr;

    P->left = PJ_IO_UNITS_CARTESIAN;
    P->right = PJ_IO_UNITS_CARTESIAN;

    // Pass a dummy ellipsoid definition that will be overridden just afterwards
    Q->cart = proj_create(P->ctx, "+proj=cart +a=1");
    if (Q->cart == nullptr)
        return pj_xyzgridshift_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    /* inherit ellipsoid definition from P to Q->cart */
    pj_inherit_ellipsoid_def(P, Q->cart);

    const char *grid_ref = pj_param(P->ctx, P->params, "sgrid_ref").s;
    if (grid_ref) {
        if (strcmp(grid_ref, "input_crs") == 0) {
            // default
        } else if (strcmp(grid_ref, "output_crs") == 0) {
            // Convention use for example for NTF->RGF93 grid that contains
            // delta x,y,z from NTF to RGF93, but the grid itself is referenced
            // in RGF93
            Q->grid_ref_is_input = false;
        } else {
            proj_log_error(P, _("unusupported value for grid_ref"));
            return pj_xyzgridshift_destructor(
                P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
    }

    if (0 == pj_param(P->ctx, P->params, "tgrids").i) {
        proj_log_error(P, _("+grids parameter missing."));
        return pj_xyzgridshift_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* multiplier for delta x,y,z */
    if (pj_param(P->ctx, P->params, "tmultiplier").i) {
        Q->multiplier = pj_param(P->ctx, P->params, "dmultiplier").f;
    }

    if (P->ctx->defer_grid_opening) {
        Q->defer_grid_opening = true;
    } else {
        Q->grids = pj_generic_grid_init(P, "grids");
        /* Was gridlist compiled properly? */
        if (proj_errno(P)) {
            proj_log_error(P, _("could not find required grid(s)."));
            return pj_xyzgridshift_destructor(
                P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
        }
    }

    return P;
}
