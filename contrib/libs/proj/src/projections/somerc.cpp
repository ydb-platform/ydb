

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(somerc, "Swiss. Obl. Mercator") "\n\tCyl, Ell\n\tFor CH1903";

namespace { // anonymous namespace
struct pj_somerc {
    double K, c, hlf_e, kR, cosp0, sinp0;
};
} // anonymous namespace

#define EPS 1.e-10
#define NITER 6

static PJ_XY somerc_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double phip, lamp, phipp, lampp, sp, cp;
    struct pj_somerc *Q = static_cast<struct pj_somerc *>(P->opaque);

    sp = P->e * sin(lp.phi);
    phip = 2. * atan(exp(Q->c * (log(tan(M_FORTPI + 0.5 * lp.phi)) -
                                 Q->hlf_e * log((1. + sp) / (1. - sp))) +
                         Q->K)) -
           M_HALFPI;
    lamp = Q->c * lp.lam;
    cp = cos(phip);
    phipp = aasin(P->ctx, Q->cosp0 * sin(phip) - Q->sinp0 * cp * cos(lamp));
    lampp = aasin(P->ctx, cp * sin(lamp) / cos(phipp));
    xy.x = Q->kR * lampp;
    xy.y = Q->kR * log(tan(M_FORTPI + 0.5 * phipp));
    return xy;
}

static PJ_LP somerc_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_somerc *Q = static_cast<struct pj_somerc *>(P->opaque);
    double phip, lamp, phipp, lampp, cp, esp, con, delp;
    int i;

    phipp = 2. * (atan(exp(xy.y / Q->kR)) - M_FORTPI);
    lampp = xy.x / Q->kR;
    cp = cos(phipp);
    phip = aasin(P->ctx, Q->cosp0 * sin(phipp) + Q->sinp0 * cp * cos(lampp));
    lamp = aasin(P->ctx, cp * sin(lampp) / cos(phip));
    con = (Q->K - log(tan(M_FORTPI + 0.5 * phip))) / Q->c;
    for (i = NITER; i; --i) {
        esp = P->e * sin(phip);
        delp = (con + log(tan(M_FORTPI + 0.5 * phip)) -
                Q->hlf_e * log((1. + esp) / (1. - esp))) *
               (1. - esp * esp) * cos(phip) * P->rone_es;
        phip -= delp;
        if (fabs(delp) < EPS)
            break;
    }
    if (i) {
        lp.phi = phip;
        lp.lam = lamp / Q->c;
    } else {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    return (lp);
}

PJ *PJ_PROJECTION(somerc) {
    double cp, phip0, sp;
    struct pj_somerc *Q =
        static_cast<struct pj_somerc *>(calloc(1, sizeof(struct pj_somerc)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->hlf_e = 0.5 * P->e;
    cp = cos(P->phi0);
    cp *= cp;
    Q->c = sqrt(1 + P->es * cp * cp * P->rone_es);
    sp = sin(P->phi0);
    Q->sinp0 = sp / Q->c;
    phip0 = aasin(P->ctx, Q->sinp0);
    Q->cosp0 = cos(phip0);
    sp *= P->e;
    Q->K = log(tan(M_FORTPI + 0.5 * phip0)) -
           Q->c * (log(tan(M_FORTPI + 0.5 * P->phi0)) -
                   Q->hlf_e * log((1. + sp) / (1. - sp)));
    Q->kR = P->k0 * sqrt(P->one_es) / (1. - sp * sp);
    P->inv = somerc_e_inverse;
    P->fwd = somerc_e_forward;
    return P;
}

#undef EPS
#undef NITER
