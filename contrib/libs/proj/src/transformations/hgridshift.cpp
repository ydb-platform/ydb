

#include <errno.h>
#include <mutex>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include "grids.hpp"
#include "proj_internal.h"

PROJ_HEAD(hgridshift, "Horizontal grid shift");

static std::mutex gMutexHGridShift{};
static std::set<std::string> gKnownGridsHGridShift{};

using namespace NS_PROJ;

namespace { // anonymous namespace
struct hgridshiftData {
    double t_final = 0;
    double t_epoch = 0;
    ListOfHGrids grids{};
    bool defer_grid_opening = false;
    int error_code_in_defer_grid_opening = 0;
};
} // anonymous namespace

static PJ_XYZ pj_hgridshift_forward_3d(PJ_LPZ lpz, PJ *P) {
    auto Q = static_cast<hgridshiftData *>(P->opaque);
    PJ_COORD point = {{0, 0, 0, 0}};
    point.lpz = lpz;

    if (Q->defer_grid_opening) {
        Q->defer_grid_opening = false;
        Q->grids = pj_hgrid_init(P, "grids");
        Q->error_code_in_defer_grid_opening = proj_errno(P);
    }
    if (Q->error_code_in_defer_grid_opening) {
        proj_errno_set(P, Q->error_code_in_defer_grid_opening);
        return proj_coord_error().xyz;
    }

    if (!Q->grids.empty()) {
        /* Only try the gridshift if at least one grid is loaded,
         * otherwise just pass the coordinate through unchanged. */
        point.lp = pj_hgrid_apply(P->ctx, Q->grids, point.lp, PJ_FWD);
    }

    return point.xyz;
}

static PJ_LPZ pj_hgridshift_reverse_3d(PJ_XYZ xyz, PJ *P) {
    auto Q = static_cast<hgridshiftData *>(P->opaque);
    PJ_COORD point = {{0, 0, 0, 0}};
    point.xyz = xyz;

    if (Q->defer_grid_opening) {
        Q->defer_grid_opening = false;
        Q->grids = pj_hgrid_init(P, "grids");
        Q->error_code_in_defer_grid_opening = proj_errno(P);
    }
    if (Q->error_code_in_defer_grid_opening) {
        proj_errno_set(P, Q->error_code_in_defer_grid_opening);
        return proj_coord_error().lpz;
    }

    if (!Q->grids.empty()) {
        /* Only try the gridshift if at least one grid is loaded,
         * otherwise just pass the coordinate through unchanged. */
        point.lp = pj_hgrid_apply(P->ctx, Q->grids, point.lp, PJ_INV);
    }

    return point.lpz;
}

static void pj_hgridshift_forward_4d(PJ_COORD &coo, PJ *P) {
    struct hgridshiftData *Q = (struct hgridshiftData *)P->opaque;

    /* If transformation is not time restricted, we always call it */
    if (Q->t_final == 0 || Q->t_epoch == 0) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf
        // https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto xyz = pj_hgridshift_forward_3d(coo.lpz, P);
        coo.xyz = xyz;
        return;
    }

    /* Time restricted - only apply transform if within time bracket */
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf
        // https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto xyz = pj_hgridshift_forward_3d(coo.lpz, P);
        coo.xyz = xyz;
    }
}

static void pj_hgridshift_reverse_4d(PJ_COORD &coo, PJ *P) {
    struct hgridshiftData *Q = (struct hgridshiftData *)P->opaque;

    /* If transformation is not time restricted, we always call it */
    if (Q->t_final == 0 || Q->t_epoch == 0) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf
        // https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto lpz = pj_hgridshift_reverse_3d(coo.xyz, P);
        coo.lpz = lpz;
        return;
    }

    /* Time restricted - only apply transform if within time bracket */
    if (coo.lpzt.t < Q->t_epoch && Q->t_final > Q->t_epoch) {
        // Assigning in 2 steps avoids cppcheck warning
        // "Overlapping read/write of union is undefined behavior"
        // Cf
        // https://github.com/OSGeo/PROJ/pull/3527#pullrequestreview-1233332710
        const auto lpz = pj_hgridshift_reverse_3d(coo.xyz, P);
        coo.lpz = lpz;
    }
}

static PJ *pj_hgridshift_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    delete static_cast<struct hgridshiftData *>(P->opaque);
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

static void pj_hgridshift_reassign_context(PJ *P, PJ_CONTEXT *ctx) {
    auto Q = (struct hgridshiftData *)P->opaque;
    for (auto &grid : Q->grids) {
        grid->reassign_context(ctx);
    }
}

PJ *PJ_TRANSFORMATION(hgridshift, 0) {
    auto Q = new hgridshiftData;
    P->opaque = (void *)Q;
    P->destructor = pj_hgridshift_destructor;
    P->reassign_context = pj_hgridshift_reassign_context;

    P->fwd4d = pj_hgridshift_forward_4d;
    P->inv4d = pj_hgridshift_reverse_4d;
    P->fwd3d = pj_hgridshift_forward_3d;
    P->inv3d = pj_hgridshift_reverse_3d;
    P->fwd = nullptr;
    P->inv = nullptr;

    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;

    if (0 == pj_param(P->ctx, P->params, "tgrids").i) {
        proj_log_error(P, _("+grids parameter missing."));
        return pj_hgridshift_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* TODO: Refactor into shared function that can be used  */
    /*       by both vgridshift and hgridshift               */
    if (pj_param(P->ctx, P->params, "tt_final").i) {
        Q->t_final = pj_param(P->ctx, P->params, "dt_final").f;
        if (Q->t_final == 0) {
            /* a number wasn't passed to +t_final, let's see if it was "now" */
            /* and set the time accordingly.                                 */
            if (!strcmp("now", pj_param(P->ctx, P->params, "st_final").s)) {
                time_t now;
                struct tm *date;
                time(&now);
                date = localtime(&now);
                Q->t_final = 1900.0 + date->tm_year + date->tm_yday / 365.0;
            }
        }
    }

    if (pj_param(P->ctx, P->params, "tt_epoch").i)
        Q->t_epoch = pj_param(P->ctx, P->params, "dt_epoch").f;

    if (P->ctx->defer_grid_opening) {
        Q->defer_grid_opening = true;
    } else {
        const char *gridnames = pj_param(P->ctx, P->params, "sgrids").s;
        gMutexHGridShift.lock();
        const bool isKnownGrid = gKnownGridsHGridShift.find(gridnames) !=
                                 gKnownGridsHGridShift.end();
        gMutexHGridShift.unlock();
        if (isKnownGrid) {
            Q->defer_grid_opening = true;
        } else {
            Q->grids = pj_hgrid_init(P, "grids");
            /* Was gridlist compiled properly? */
            if (proj_errno(P)) {
                proj_log_error(P, _("could not find required grid(s)."));
                return pj_hgridshift_destructor(
                    P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
            }

            gMutexHGridShift.lock();
            gKnownGridsHGridShift.insert(gridnames);
            gMutexHGridShift.unlock();
        }
    }

    return P;
}

void pj_clear_hgridshift_knowngrids_cache() {
    std::lock_guard<std::mutex> lock(gMutexHGridShift);
    gKnownGridsHGridShift.clear();
}
