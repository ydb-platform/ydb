
#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"
#include <math.h>

PROJ_HEAD(bipc, "Bipolar conic of western hemisphere") "\n\tConic Sph";

#define EPS 1e-10
#define EPS10 1e-10
#define ONEEPS 1.000000001
#define NITER 10
#define lamB -.34894976726250681539
#define n .63055844881274687180
#define F 1.89724742567461030582
#define Azab .81650043674686363166
#define Azba 1.82261843856185925133
#define T 1.27246578267089012270
#define rhoc 1.20709121521568721927
#define cAzc .69691523038678375519
#define sAzc .71715351331143607555
#define C45 .70710678118654752469
#define S45 .70710678118654752410
#define C20 .93969262078590838411
#define S20 -.34202014332566873287
#define R110 1.91986217719376253360
#define R104 1.81514242207410275904

namespace { // anonymous namespace
struct pj_bipc_data {
    int noskew;
};
} // anonymous namespace

static PJ_XY bipc_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_bipc_data *Q = static_cast<struct pj_bipc_data *>(P->opaque);
    double cphi, sphi, tphi, t, al, Az, z, Av, cdlam, sdlam, r;
    int tag;

    cphi = cos(lp.phi);
    sphi = sin(lp.phi);
    cdlam = cos(sdlam = lamB - lp.lam);
    sdlam = sin(sdlam);
    if (fabs(fabs(lp.phi) - M_HALFPI) < EPS10) {
        Az = lp.phi < 0. ? M_PI : 0.;
        tphi = HUGE_VAL;
    } else {
        tphi = sphi / cphi;
        Az = atan2(sdlam, C45 * (tphi - cdlam));
    }
    if ((tag = (Az > Azba))) {
        sdlam = lp.lam + R110;
        cdlam = cos(sdlam);
        sdlam = sin(sdlam);
        z = S20 * sphi + C20 * cphi * cdlam;
        if (fabs(z) > 1.) {
            if (fabs(z) > ONEEPS) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            } else
                z = z < 0. ? -1. : 1.;
        } else
            z = acos(z);
        if (tphi != HUGE_VAL)
            Az = atan2(sdlam, (C20 * tphi - S20 * cdlam));
        Av = Azab;
        xy.y = rhoc;
    } else {
        z = S45 * (sphi + cphi * cdlam);
        if (fabs(z) > 1.) {
            if (fabs(z) > ONEEPS) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return xy;
            } else
                z = z < 0. ? -1. : 1.;
        } else
            z = acos(z);
        Av = Azba;
        xy.y = -rhoc;
    }
    if (z < 0.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    t = pow(tan(.5 * z), n);
    r = F * t;
    if ((al = .5 * (R104 - z)) < 0.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    al = (t + pow(al, n)) / T;
    if (fabs(al) > 1.) {
        if (fabs(al) > ONEEPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        } else
            al = al < 0. ? -1. : 1.;
    } else
        al = acos(al);
    t = n * (Av - Az);
    if (fabs(t) < al)
        r /= cos(al + (tag ? t : -t));
    xy.x = r * sin(t);
    xy.y += (tag ? -r : r) * cos(t);
    if (Q->noskew) {
        t = xy.x;
        xy.x = -xy.x * cAzc - xy.y * sAzc;
        xy.y = -xy.y * cAzc + t * sAzc;
    }
    return (xy);
}

static PJ_LP bipc_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_bipc_data *Q = static_cast<struct pj_bipc_data *>(P->opaque);
    double t, r, rp, rl, al, z = 0.0, fAz, Az, s, c, Av;
    int neg, i;

    if (Q->noskew) {
        t = xy.x;
        xy.x = -xy.x * cAzc + xy.y * sAzc;
        xy.y = -xy.y * cAzc - t * sAzc;
    }
    if ((neg = (xy.x < 0.))) {
        xy.y = rhoc - xy.y;
        s = S20;
        c = C20;
        Av = Azab;
    } else {
        xy.y += rhoc;
        s = S45;
        c = C45;
        Av = Azba;
    }
    r = hypot(xy.x, xy.y);
    rl = rp = r;
    Az = atan2(xy.x, xy.y);
    fAz = fabs(Az);
    for (i = NITER; i; --i) {
        z = 2. * atan(pow(r / F, 1 / n));
        al = acos((pow(tan(.5 * z), n) + pow(tan(.5 * (R104 - z)), n)) / T);
        if (fAz < al)
            r = rp * cos(al + (neg ? Az : -Az));
        if (fabs(rl - r) < EPS)
            break;
        rl = r;
    }
    if (!i) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    Az = Av - Az / n;
    lp.phi = asin(s * cos(z) + c * sin(z) * cos(Az));
    lp.lam = atan2(sin(Az), c / tan(z) - s * cos(Az));
    if (neg)
        lp.lam -= R110;
    else
        lp.lam = lamB - lp.lam;
    return (lp);
}

PJ *PJ_PROJECTION(bipc) {
    struct pj_bipc_data *Q = static_cast<struct pj_bipc_data *>(
        calloc(1, sizeof(struct pj_bipc_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->noskew = pj_param(P->ctx, P->params, "bns").i;
    P->inv = bipc_s_inverse;
    P->fwd = bipc_s_forward;
    P->es = 0.;
    return P;
}

#undef EPS
#undef EPS10
#undef ONEEPS
#undef NITER
#undef lamB
#undef n
#undef F
#undef Azab
#undef Azba
#undef T
#undef rhoc
#undef cAzc
#undef sAzc
#undef C45
#undef S45
#undef C20
#undef S20
#undef R110
#undef R104
