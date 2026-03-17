
#include <errno.h>
#include <math.h>
#include <stddef.h>
#include <string.h>

#include "proj.h"
#include "proj_internal.h"

namespace { // anonymous namespace
struct pj_ob_tran_data {
    struct PJconsts *link;
    double lamp;
    double cphip, sphip;
};
} // anonymous namespace

PROJ_HEAD(ob_tran, "General Oblique Transformation")
"\n\tMisc Sph"
    "\n\to_proj= plus parameters for projection"
    "\n\to_lat_p= o_lon_p= (new pole) or"
    "\n\to_alpha= o_lon_c= o_lat_c= or"
    "\n\to_lon_1= o_lat_1= o_lon_2= o_lat_2=";

#define TOL 1e-10

static PJ_XY o_forward(PJ_LP lp, PJ *P) { /* spheroid */
    struct pj_ob_tran_data *Q =
        static_cast<struct pj_ob_tran_data *>(P->opaque);
    double coslam, sinphi, cosphi;

    coslam = cos(lp.lam);
    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    /* Formula (5-8b) of Snyder's "Map projections: a working manual" */
    lp.lam = adjlon(aatan2(cosphi * sin(lp.lam),
                           Q->sphip * cosphi * coslam + Q->cphip * sinphi) +
                    Q->lamp);
    /* Formula (5-7) */
    lp.phi = aasin(P->ctx, Q->sphip * sinphi - Q->cphip * cosphi * coslam);

    return Q->link->fwd(lp, Q->link);
}

static PJ_XY t_forward(PJ_LP lp, PJ *P) { /* spheroid */
    struct pj_ob_tran_data *Q =
        static_cast<struct pj_ob_tran_data *>(P->opaque);
    double cosphi, coslam;

    cosphi = cos(lp.phi);
    coslam = cos(lp.lam);
    lp.lam = adjlon(aatan2(cosphi * sin(lp.lam), sin(lp.phi)) + Q->lamp);
    lp.phi = aasin(P->ctx, -cosphi * coslam);

    return Q->link->fwd(lp, Q->link);
}

static PJ_LP o_inverse(PJ_XY xy, PJ *P) { /* spheroid */

    struct pj_ob_tran_data *Q =
        static_cast<struct pj_ob_tran_data *>(P->opaque);
    double coslam, sinphi, cosphi;

    PJ_LP lp = Q->link->inv(xy, Q->link);
    if (lp.lam != HUGE_VAL) {
        lp.lam -= Q->lamp;
        coslam = cos(lp.lam);
        sinphi = sin(lp.phi);
        cosphi = cos(lp.phi);
        /* Formula (5-9) */
        lp.phi = aasin(P->ctx, Q->sphip * sinphi + Q->cphip * cosphi * coslam);
        /* Formula (5-10b) */
        lp.lam = aatan2(cosphi * sin(lp.lam),
                        Q->sphip * cosphi * coslam - Q->cphip * sinphi);
    }
    return lp;
}

static PJ_LP t_inverse(PJ_XY xy, PJ *P) { /* spheroid */

    struct pj_ob_tran_data *Q =
        static_cast<struct pj_ob_tran_data *>(P->opaque);
    double cosphi, t;

    PJ_LP lp = Q->link->inv(xy, Q->link);
    if (lp.lam != HUGE_VAL) {
        cosphi = cos(lp.phi);
        t = lp.lam - Q->lamp;
        lp.lam = aatan2(cosphi * sin(t), -sin(lp.phi));
        lp.phi = aasin(P->ctx, cosphi * cos(t));
    }
    return lp;
}

static PJ *destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;
    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    if (static_cast<struct pj_ob_tran_data *>(P->opaque)->link)
        static_cast<struct pj_ob_tran_data *>(P->opaque)->link->destructor(
            static_cast<struct pj_ob_tran_data *>(P->opaque)->link, errlev);

    return pj_default_destructor(P, errlev);
}

/***********************************************************************

These functions are modified versions of the functions "argc_params"
and "argv_params" from PJ_pipeline.c

Basically, they do the somewhat backwards stunt of turning the paralist
representation of the +args back into the original +argv, +argc
representation accepted by pj_init_ctx().

This, however, also begs the question of whether we really need the
paralist linked list representation, or if we could do with a simpler
null-terminated argv style array? This would simplify some code, and
keep memory allocations more localized.

***********************************************************************/

typedef struct {
    int argc;
    char **argv;
} ARGS;

/* count the number of args in the linked list <params> */
static size_t paralist_params_argc(paralist *params) {
    size_t argc = 0;
    for (; params != nullptr; params = params->next)
        argc++;
    return argc;
}

/* turn paralist into argc/argv style argument list */
static ARGS ob_tran_target_params(paralist *params) {
    int i = 0;
    ARGS args = {0, nullptr};
    size_t argc = paralist_params_argc(params);
    if (argc < 2)
        return args;

    /* all args except the proj=ob_tran */
    args.argv = static_cast<char **>(calloc(argc - 1, sizeof(char *)));
    if (nullptr == args.argv)
        return args;

    /* Copy all args *except* the proj=ob_tran or inv arg to the argv array */
    for (i = 0; params != nullptr; params = params->next) {
        if (0 == strcmp(params->param, "proj=ob_tran") ||
            0 == strcmp(params->param, "inv"))
            continue;
        args.argv[i++] = params->param;
    }
    args.argc = i;

    /* Then convert the o_proj=xxx element to proj=xxx */
    for (i = 0; i < args.argc; i++) {
        if (0 != strncmp(args.argv[i], "o_proj=", 7))
            continue;
        args.argv[i] += 2;
        if (strcmp(args.argv[i], "proj=ob_tran") == 0) {
            free(args.argv);
            args.argc = 0;
            args.argv = nullptr;
        }
        break;
    }

    return args;
}

PJ *PJ_PROJECTION(ob_tran) {
    double phip;
    ARGS args;
    PJ *R; /* projection to rotate */

    struct pj_ob_tran_data *Q = static_cast<struct pj_ob_tran_data *>(
        calloc(1, sizeof(struct pj_ob_tran_data)));
    if (nullptr == Q)
        return destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->opaque = Q;
    P->destructor = destructor;

    /* get name of projection to be translated */
    if (pj_param(P->ctx, P->params, "so_proj").s == nullptr) {
        proj_log_error(P, _("Missing parameter: o_proj"));
        return destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    /* Create the target projection object to rotate */
    args = ob_tran_target_params(P->params);
    /* avoid endless recursion */
    if (args.argv == nullptr) {
        proj_log_error(P, _("Failed to find projection to be rotated"));
        return destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }
    R = pj_create_argv_internal(P->ctx, args.argc, args.argv);
    free(args.argv);

    if (nullptr == R) {
        proj_log_error(P, _("Projection to be rotated is unknown"));
        return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    // Transfer the used flag from the R object to the P object
    for (auto p = P->params; p; p = p->next) {
        if (!p->used) {
            for (auto r = R->params; r; r = r->next) {
                if (r->used && strcmp(r->param, p->param) == 0) {
                    p->used = 1;
                    break;
                }
            }
        }
    }

    Q->link = R;

    if (pj_param(P->ctx, P->params, "to_alpha").i) {
        double lamc, phic, alpha;

        lamc = pj_param(P->ctx, P->params, "ro_lon_c").f;
        phic = pj_param(P->ctx, P->params, "ro_lat_c").f;
        alpha = pj_param(P->ctx, P->params, "ro_alpha").f;

        if (fabs(fabs(phic) - M_HALFPI) <= TOL) {
            proj_log_error(
                P, _("Invalid value for lat_c: |lat_c| should be < 90°"));
            return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        Q->lamp = lamc + aatan2(-cos(alpha), -sin(alpha) * sin(phic));
        phip = aasin(P->ctx, cos(phic) * sin(alpha));
    } else if (pj_param(P->ctx, P->params, "to_lat_p")
                   .i) { /* specified new pole */
        Q->lamp = pj_param(P->ctx, P->params, "ro_lon_p").f;
        phip = pj_param(P->ctx, P->params, "ro_lat_p").f;
    } else { /* specified new "equator" points */
        double lam1, lam2, phi1, phi2, con;

        lam1 = pj_param(P->ctx, P->params, "ro_lon_1").f;
        phi1 = pj_param(P->ctx, P->params, "ro_lat_1").f;
        lam2 = pj_param(P->ctx, P->params, "ro_lon_2").f;
        phi2 = pj_param(P->ctx, P->params, "ro_lat_2").f;
        con = fabs(phi1);

        if (fabs(phi1) > M_HALFPI - TOL) {
            proj_log_error(
                P, _("Invalid value for lat_1: |lat_1| should be < 90°"));
            return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        if (fabs(phi2) > M_HALFPI - TOL) {
            proj_log_error(
                P, _("Invalid value for lat_2: |lat_2| should be < 90°"));
            return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        if (fabs(phi1 - phi2) < TOL) {
            proj_log_error(
                P, _("Invalid value for lat_1 and lat_2: lat_1 should be "
                     "different from lat_2"));
            return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        if (con < TOL) {
            proj_log_error(P, _("Invalid value for lat_1: lat_1 should be "
                                "different from zero"));
            return destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }

        Q->lamp = atan2(cos(phi1) * sin(phi2) * cos(lam1) -
                            sin(phi1) * cos(phi2) * cos(lam2),
                        sin(phi1) * cos(phi2) * sin(lam2) -
                            cos(phi1) * sin(phi2) * sin(lam1));
        phip = atan(-cos(Q->lamp - lam1) / tan(phi1));
    }

    if (fabs(phip) > TOL) { /* oblique */
        Q->cphip = cos(phip);
        Q->sphip = sin(phip);
        P->fwd = Q->link->fwd ? o_forward : nullptr;
        P->inv = Q->link->inv ? o_inverse : nullptr;
    } else { /* transverse */
        P->fwd = Q->link->fwd ? t_forward : nullptr;
        P->inv = Q->link->inv ? t_inverse : nullptr;
    }

    /* Support some rather speculative test cases, where the rotated projection
     */
    /* is actually latlong. We do not want scaling in that case... */
    if (Q->link->right == PJ_IO_UNITS_RADIANS)
        P->right = PJ_IO_UNITS_WHATEVER;

    return P;
}

#undef TOL
