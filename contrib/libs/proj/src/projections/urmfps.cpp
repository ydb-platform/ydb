

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(urmfps, "Urmaev Flat-Polar Sinusoidal") "\n\tPCyl, Sph\n\tn=";
PROJ_HEAD(wag1, "Wagner I (Kavrayskiy VI)") "\n\tPCyl, Sph";

namespace { // anonymous namespace
struct pj_urmfps {
    double n, C_y;
};
} // anonymous namespace

#define C_x 0.8773826753
#define Cy 1.139753528477

static PJ_XY urmfps_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    lp.phi = aasin(P->ctx,
                   static_cast<struct pj_urmfps *>(P->opaque)->n * sin(lp.phi));
    xy.x = C_x * lp.lam * cos(lp.phi);
    xy.y = static_cast<struct pj_urmfps *>(P->opaque)->C_y * lp.phi;
    return xy;
}

static PJ_LP urmfps_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    xy.y /= static_cast<struct pj_urmfps *>(P->opaque)->C_y;
    lp.phi = aasin(P->ctx,
                   sin(xy.y) / static_cast<struct pj_urmfps *>(P->opaque)->n);
    lp.lam = xy.x / (C_x * cos(xy.y));
    return lp;
}

static PJ *urmfps_setup(PJ *P) {
    static_cast<struct pj_urmfps *>(P->opaque)->C_y =
        Cy / static_cast<struct pj_urmfps *>(P->opaque)->n;
    P->es = 0.;
    P->inv = urmfps_s_inverse;
    P->fwd = urmfps_s_forward;
    return P;
}

PJ *PJ_PROJECTION(urmfps) {
    struct pj_urmfps *Q =
        static_cast<struct pj_urmfps *>(calloc(1, sizeof(struct pj_urmfps)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    P->opaque = Q;

    if (!pj_param(P->ctx, P->params, "tn").i) {
        proj_log_error(P, _("Missing parameter n."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    Q->n = pj_param(P->ctx, P->params, "dn").f;
    if (Q->n <= 0. || Q->n > 1.) {
        proj_log_error(P,
                       _("Invalid value for n: it should be in ]0,1] range."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    return urmfps_setup(P);
}

PJ *PJ_PROJECTION(wag1) {
    struct pj_urmfps *Q =
        static_cast<struct pj_urmfps *>(calloc(1, sizeof(struct pj_urmfps)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    static_cast<struct pj_urmfps *>(P->opaque)->n =
        0.8660254037844386467637231707;
    return urmfps_setup(P);
}

#undef C_x
#undef Cy
