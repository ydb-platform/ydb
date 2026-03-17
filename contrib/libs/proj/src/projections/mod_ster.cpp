/* based upon Snyder and Linck, USGS-NMD */

#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>

PROJ_HEAD(mil_os, "Miller Oblated Stereographic") "\n\tAzi(mod)";
PROJ_HEAD(lee_os, "Lee Oblated Stereographic") "\n\tAzi(mod)";
PROJ_HEAD(gs48, "Modified Stereographic of 48 U.S.") "\n\tAzi(mod)";
PROJ_HEAD(alsk, "Modified Stereographic of Alaska") "\n\tAzi(mod)";
PROJ_HEAD(gs50, "Modified Stereographic of 50 U.S.") "\n\tAzi(mod)";

#define EPSLN 1e-12

namespace { // anonymous namespace
struct pj_mod_ster_data {
    const COMPLEX *zcoeff;
    double cchio, schio;
    int n;
};
} // anonymous namespace

static PJ_XY mod_ster_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_mod_ster_data *Q =
        static_cast<struct pj_mod_ster_data *>(P->opaque);
    double sinlon, coslon, esphi, chi, schi, cchi, s;
    COMPLEX p;

    sinlon = sin(lp.lam);
    coslon = cos(lp.lam);
    esphi = P->e * sin(lp.phi);
    chi = 2. * atan(tan((M_HALFPI + lp.phi) * .5) *
                    pow((1. - esphi) / (1. + esphi), P->e * .5)) -
          M_HALFPI;
    schi = sin(chi);
    cchi = cos(chi);
    const double denom = 1. + Q->schio * schi + Q->cchio * cchi * coslon;
    if (denom == 0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    s = 2. / denom;
    p.r = s * cchi * sinlon;
    p.i = s * (Q->cchio * schi - Q->schio * cchi * coslon);
    p = pj_zpoly1(p, Q->zcoeff, Q->n);
    xy.x = p.r;
    xy.y = p.i;

    return xy;
}

static PJ_LP mod_ster_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_mod_ster_data *Q =
        static_cast<struct pj_mod_ster_data *>(P->opaque);
    int nn;
    COMPLEX p, fxy, fpxy, dp;
    double den, rh = 0.0, z, sinz = 0.0, cosz = 0.0, chi, phi = 0.0, esphi;

    p.r = xy.x;
    p.i = xy.y;
    for (nn = 20; nn; --nn) {
        fxy = pj_zpolyd1(p, Q->zcoeff, Q->n, &fpxy);
        fxy.r -= xy.x;
        fxy.i -= xy.y;
        den = fpxy.r * fpxy.r + fpxy.i * fpxy.i;
        dp.r = -(fxy.r * fpxy.r + fxy.i * fpxy.i) / den;
        dp.i = -(fxy.i * fpxy.r - fxy.r * fpxy.i) / den;
        p.r += dp.r;
        p.i += dp.i;
        if ((fabs(dp.r) + fabs(dp.i)) <= EPSLN)
            break;
    }
    if (nn) {
        rh = hypot(p.r, p.i);
        z = 2. * atan(.5 * rh);
        sinz = sin(z);
        cosz = cos(z);
        if (fabs(rh) <= EPSLN) {
            /* if we end up here input coordinates were (0,0).
             * pj_inv() adds P->lam0 to lp.lam, this way we are
             * sure to get the correct offset */
            lp.lam = 0.0;
            lp.phi = P->phi0;
            return lp;
        }
        chi = aasin(P->ctx, cosz * Q->schio + p.i * sinz * Q->cchio / rh);
        phi = chi;
        for (nn = 20; nn; --nn) {
            double dphi;
            esphi = P->e * sin(phi);
            dphi = 2. * atan(tan((M_HALFPI + chi) * .5) *
                             pow((1. + esphi) / (1. - esphi), P->e * .5)) -
                   M_HALFPI - phi;
            phi += dphi;
            if (fabs(dphi) <= EPSLN)
                break;
        }
    }
    if (nn) {
        lp.phi = phi;
        lp.lam =
            atan2(p.r * sinz, rh * Q->cchio * cosz - p.i * Q->schio * sinz);
    } else
        lp.lam = lp.phi = HUGE_VAL;
    return lp;
}

static PJ *mod_ster_setup(PJ *P) { /* general initialization */
    struct pj_mod_ster_data *Q =
        static_cast<struct pj_mod_ster_data *>(P->opaque);
    double esphi, chio;

    if (P->es != 0.0) {
        esphi = P->e * sin(P->phi0);
        chio = 2. * atan(tan((M_HALFPI + P->phi0) * .5) *
                         pow((1. - esphi) / (1. + esphi), P->e * .5)) -
               M_HALFPI;
    } else
        chio = P->phi0;
    Q->schio = sin(chio);
    Q->cchio = cos(chio);
    P->inv = mod_ster_e_inverse;
    P->fwd = mod_ster_e_forward;

    return P;
}

/* Miller Oblated Stereographic */
PJ *PJ_PROJECTION(mil_os) {
    static const COMPLEX AB[] = {{0.924500, 0.}, {0., 0.}, {0.019430, 0.}};

    struct pj_mod_ster_data *Q = static_cast<struct pj_mod_ster_data *>(
        calloc(1, sizeof(struct pj_mod_ster_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = 2;
    P->lam0 = DEG_TO_RAD * 20.;
    P->phi0 = DEG_TO_RAD * 18.;
    Q->zcoeff = AB;
    P->es = 0.;

    return mod_ster_setup(P);
}

/* Lee Oblated Stereographic */
PJ *PJ_PROJECTION(lee_os) {
    static const COMPLEX AB[] = {
        {0.721316, 0.}, {0., 0.}, {-0.0088162, -0.00617325}};

    struct pj_mod_ster_data *Q = static_cast<struct pj_mod_ster_data *>(
        calloc(1, sizeof(struct pj_mod_ster_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = 2;
    P->lam0 = DEG_TO_RAD * -165.;
    P->phi0 = DEG_TO_RAD * -10.;
    Q->zcoeff = AB;
    P->es = 0.;

    return mod_ster_setup(P);
}

PJ *PJ_PROJECTION(gs48) {
    static const COMPLEX /* 48 United States */
        AB[] = {
            {0.98879, 0.}, {0., 0.}, {-0.050909, 0.}, {0., 0.}, {0.075528, 0.}};

    struct pj_mod_ster_data *Q = static_cast<struct pj_mod_ster_data *>(
        calloc(1, sizeof(struct pj_mod_ster_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = 4;
    P->lam0 = DEG_TO_RAD * -96.;
    P->phi0 = DEG_TO_RAD * 39.;
    Q->zcoeff = AB;
    P->es = 0.;
    P->a = 6370997.;

    return mod_ster_setup(P);
}

PJ *PJ_PROJECTION(alsk) {
    static const COMPLEX ABe[] = {
        /* Alaska ellipsoid */
        {.9945303, 0.},         {.0052083, -.0027404}, {.0072721, .0048181},
        {-.0151089, -.1932526}, {.0642675, -.1381226}, {.3582802, -.2884586},
    };

    static const COMPLEX ABs[] = {/* Alaska sphere */
                                  {.9972523, 0.},        {.0052513, -.0041175},
                                  {.0074606, .0048125},  {-.0153783, -.1968253},
                                  {.0636871, -.1408027}, {.3660976, -.2937382}};

    struct pj_mod_ster_data *Q = static_cast<struct pj_mod_ster_data *>(
        calloc(1, sizeof(struct pj_mod_ster_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = 5;
    P->lam0 = DEG_TO_RAD * -152.;
    P->phi0 = DEG_TO_RAD * 64.;
    if (P->es != 0.0) { /* fixed ellipsoid/sphere */
        Q->zcoeff = ABe;
        P->a = 6378206.4;
        P->e = sqrt(P->es = 0.00676866);
    } else {
        Q->zcoeff = ABs;
        P->a = 6370997.;
    }

    return mod_ster_setup(P);
}

PJ *PJ_PROJECTION(gs50) {
    static const COMPLEX ABe[] = {
        /* GS50 ellipsoid */
        {.9827497, 0.},         {.0210669, .0053804},  {-.1031415, -.0571664},
        {-.0323337, -.0322847}, {.0502303, .1211983},  {.0251805, .0895678},
        {-.0012315, -.1416121}, {.0072202, -.1317091}, {-.0194029, .0759677},
        {-.0210072, .0834037}};

    static const COMPLEX ABs[] = {
        /* GS50 sphere */
        {.9842990, 0.},         {.0211642, .0037608},  {-.1036018, -.0575102},
        {-.0329095, -.0320119}, {.0499471, .1223335},  {.0260460, .0899805},
        {.0007388, -.1435792},  {.0075848, -.1334108}, {-.0216473, .0776645},
        {-.0225161, .0853673}};

    struct pj_mod_ster_data *Q = static_cast<struct pj_mod_ster_data *>(
        calloc(1, sizeof(struct pj_mod_ster_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->n = 9;
    P->lam0 = DEG_TO_RAD * -120.;
    P->phi0 = DEG_TO_RAD * 45.;
    if (P->es != 0.0) { /* fixed ellipsoid/sphere */
        Q->zcoeff = ABe;
        P->a = 6378206.4;
        P->e = sqrt(P->es = 0.00676866);
    } else {
        Q->zcoeff = ABs;
        P->a = 6370997.;
    }

    return mod_ster_setup(P);
}

#undef EPSLN
