
#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(tpeqd, "Two Point Equidistant")
"\n\tMisc Sph\n\tlat_1= lon_1= lat_2= lon_2=";

namespace { // anonymous namespace
struct pj_tpeqd {
    double cp1, sp1, cp2, sp2, ccs, cs, sc, r2z0, z02, dlam2;
    double hz0, thz0, rhshz0, ca, sa, lp, lamc;
};
} // anonymous namespace

static PJ_XY tpeqd_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_tpeqd *Q = static_cast<struct pj_tpeqd *>(P->opaque);
    double t, z1, z2, dl1, dl2, sp, cp;

    sp = sin(lp.phi);
    cp = cos(lp.phi);
    z1 =
        aacos(P->ctx, Q->sp1 * sp + Q->cp1 * cp * cos(dl1 = lp.lam + Q->dlam2));
    z2 =
        aacos(P->ctx, Q->sp2 * sp + Q->cp2 * cp * cos(dl2 = lp.lam - Q->dlam2));
    z1 *= z1;
    z2 *= z2;

    t = z1 - z2;
    xy.x = Q->r2z0 * t;
    t = Q->z02 - t;
    xy.y = Q->r2z0 * asqrt(4. * Q->z02 * z2 - t * t);
    if ((Q->ccs * sp - cp * (Q->cs * sin(dl1) - Q->sc * sin(dl2))) < 0.)
        xy.y = -xy.y;
    return xy;
}

static PJ_LP tpeqd_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_tpeqd *Q = static_cast<struct pj_tpeqd *>(P->opaque);
    double cz1, cz2, s, d, cp, sp;

    cz1 = cos(hypot(xy.y, xy.x + Q->hz0));
    cz2 = cos(hypot(xy.y, xy.x - Q->hz0));
    s = cz1 + cz2;
    d = cz1 - cz2;
    lp.lam = -atan2(d, (s * Q->thz0));
    lp.phi = aacos(P->ctx, hypot(Q->thz0 * s, d) * Q->rhshz0);
    if (xy.y < 0.)
        lp.phi = -lp.phi;
    /* lam--phi now in system relative to P1--P2 base equator */
    sp = sin(lp.phi);
    cp = cos(lp.phi);
    lp.lam -= Q->lp;
    s = cos(lp.lam);
    lp.phi = aasin(P->ctx, Q->sa * sp + Q->ca * cp * s);
    lp.lam = atan2(cp * sin(lp.lam), Q->sa * cp * s - Q->ca * sp) + Q->lamc;
    return lp;
}

PJ *PJ_PROJECTION(tpeqd) {
    double lam_1, lam_2, phi_1, phi_2, A12;
    struct pj_tpeqd *Q =
        static_cast<struct pj_tpeqd *>(calloc(1, sizeof(struct pj_tpeqd)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* get control point locations */
    phi_1 = pj_param(P->ctx, P->params, "rlat_1").f;
    lam_1 = pj_param(P->ctx, P->params, "rlon_1").f;
    phi_2 = pj_param(P->ctx, P->params, "rlat_2").f;
    lam_2 = pj_param(P->ctx, P->params, "rlon_2").f;

    if (phi_1 == phi_2 && lam_1 == lam_2) {
        proj_log_error(P, _("Invalid value for lat_1/lon_1/lat_2/lon_2: the 2 "
                            "points should be distinct."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }

    P->lam0 = adjlon(0.5 * (lam_1 + lam_2));
    Q->dlam2 = adjlon(lam_2 - lam_1);

    Q->cp1 = cos(phi_1);
    Q->cp2 = cos(phi_2);
    Q->sp1 = sin(phi_1);
    Q->sp2 = sin(phi_2);
    Q->cs = Q->cp1 * Q->sp2;
    Q->sc = Q->sp1 * Q->cp2;
    Q->ccs = Q->cp1 * Q->cp2 * sin(Q->dlam2);

    const auto SQ = [](double x) { return x * x; };
    // Numerically stable formula for computing the central angle from
    // (phi_1, lam_1) to (phi_2, lam_2).
    // Special case of Vincenty formula on the sphere.
    // https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulae
    const double cs_minus_sc_cos_dlam = Q->cs - Q->sc * cos(Q->dlam2);
    Q->z02 = atan2(sqrt(SQ(Q->cp2 * sin(Q->dlam2)) + SQ(cs_minus_sc_cos_dlam)),
                   Q->sp1 * Q->sp2 + Q->cp1 * Q->cp2 * cos(Q->dlam2));
    if (Q->z02 == 0.0) {
        // Actually happens when both lat_1 = lat_2 and |lat_1| = 90
        proj_log_error(P, _("Invalid value for lat_1 and lat_2: their absolute "
                            "value should be < 90°."));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    Q->hz0 = .5 * Q->z02;
    A12 = atan2(Q->cp2 * sin(Q->dlam2), cs_minus_sc_cos_dlam);
    const double pp = aasin(P->ctx, Q->cp1 * sin(A12));
    Q->ca = cos(pp);
    Q->sa = sin(pp);
    Q->lp = adjlon(atan2(Q->cp1 * cos(A12), Q->sp1) - Q->hz0);
    Q->dlam2 *= .5;
    Q->lamc = M_HALFPI - atan2(sin(A12) * Q->sp1, cos(A12)) - Q->dlam2;
    Q->thz0 = tan(Q->hz0);
    Q->rhshz0 = .5 / sin(Q->hz0);
    Q->r2z0 = 0.5 / Q->z02;
    Q->z02 *= Q->z02;

    P->inv = tpeqd_s_inverse;
    P->fwd = tpeqd_s_forward;
    P->es = 0.;

    return P;
}
