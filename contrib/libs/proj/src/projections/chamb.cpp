

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

typedef struct {
    double r, Az;
} VECT;
namespace { // anonymous namespace
struct pj_chamb {
    struct { /* control point data */
        double phi, lam;
        double cosphi, sinphi;
        VECT v;
        PJ_XY p;
    } c[3];
    PJ_XY p;
    double beta_0, beta_1, beta_2;
};
} // anonymous namespace

PROJ_HEAD(chamb, "Chamberlin Trimetric")
"\n\tMisc Sph, no inv"
    "\n\tlat_1= lon_1= lat_2= lon_2= lat_3= lon_3=";

#include <stdio.h>
#define THIRD 0.333333333333333333
#define TOL 1e-9

/* distance and azimuth from point 1 to point 2 */
static VECT vect(PJ_CONTEXT *ctx, double dphi, double c1, double s1, double c2,
                 double s2, double dlam) {
    VECT v;
    double cdl, dp, dl;

    cdl = cos(dlam);
    if (fabs(dphi) > 1. || fabs(dlam) > 1.)
        v.r = aacos(ctx, s1 * s2 + c1 * c2 * cdl);
    else { /* more accurate for smaller distances */
        dp = sin(.5 * dphi);
        dl = sin(.5 * dlam);
        v.r = 2. * aasin(ctx, sqrt(dp * dp + c1 * c2 * dl * dl));
    }
    if (fabs(v.r) > TOL)
        v.Az = atan2(c2 * sin(dlam), c1 * s2 - s1 * c2 * cdl);
    else
        v.r = v.Az = 0.;
    return v;
}

/* law of cosines */
static double lc(PJ_CONTEXT *ctx, double b, double c, double a) {
    return aacos(ctx, .5 * (b * b + c * c - a * a) / (b * c));
}

static PJ_XY chamb_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy;
    struct pj_chamb *Q = static_cast<struct pj_chamb *>(P->opaque);
    double sinphi, cosphi, a;
    VECT v[3];
    int i, j;

    sinphi = sin(lp.phi);
    cosphi = cos(lp.phi);
    for (i = 0; i < 3; ++i) { /* dist/azimiths from control */
        v[i] = vect(P->ctx, lp.phi - Q->c[i].phi, Q->c[i].cosphi,
                    Q->c[i].sinphi, cosphi, sinphi, lp.lam - Q->c[i].lam);
        if (v[i].r == 0.0)
            break;
        v[i].Az = adjlon(v[i].Az - Q->c[i].v.Az);
    }
    if (i < 3) /* current point at control point */
        xy = Q->c[i].p;
    else { /* point mean of intercepts */
        xy = Q->p;
        for (i = 0; i < 3; ++i) {
            j = i == 2 ? 0 : i + 1;
            a = lc(P->ctx, Q->c[i].v.r, v[i].r, v[j].r);
            if (v[i].Az < 0.)
                a = -a;
            if (!i) { /* coord comp unique to each arc */
                xy.x += v[i].r * cos(a);
                xy.y -= v[i].r * sin(a);
            } else if (i == 1) {
                a = Q->beta_1 - a;
                xy.x -= v[i].r * cos(a);
                xy.y -= v[i].r * sin(a);
            } else {
                a = Q->beta_2 - a;
                xy.x += v[i].r * cos(a);
                xy.y += v[i].r * sin(a);
            }
        }
        xy.x *= THIRD; /* mean of arc intercepts */
        xy.y *= THIRD;
    }
    return xy;
}

PJ *PJ_PROJECTION(chamb) {
    int i, j;
    char line[10];
    struct pj_chamb *Q =
        static_cast<struct pj_chamb *>(calloc(1, sizeof(struct pj_chamb)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    for (i = 0; i < 3; ++i) { /* get control point locations */
        (void)snprintf(line, sizeof(line), "rlat_%d", i + 1);
        Q->c[i].phi = pj_param(P->ctx, P->params, line).f;
        (void)snprintf(line, sizeof(line), "rlon_%d", i + 1);
        Q->c[i].lam = pj_param(P->ctx, P->params, line).f;
        Q->c[i].lam = adjlon(Q->c[i].lam - P->lam0);
        Q->c[i].cosphi = cos(Q->c[i].phi);
        Q->c[i].sinphi = sin(Q->c[i].phi);
    }
    for (i = 0; i < 3; ++i) { /* inter ctl pt. distances and azimuths */
        j = i == 2 ? 0 : i + 1;
        Q->c[i].v = vect(P->ctx, Q->c[j].phi - Q->c[i].phi, Q->c[i].cosphi,
                         Q->c[i].sinphi, Q->c[j].cosphi, Q->c[j].sinphi,
                         Q->c[j].lam - Q->c[i].lam);
        if (Q->c[i].v.r == 0.0) {
            proj_log_error(
                P,
                _("Invalid value for control points: they should be distinct"));
            return pj_default_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
        }
        /* co-linearity problem ignored for now */
    }
    Q->beta_0 = lc(P->ctx, Q->c[0].v.r, Q->c[2].v.r, Q->c[1].v.r);
    Q->beta_1 = lc(P->ctx, Q->c[0].v.r, Q->c[1].v.r, Q->c[2].v.r);
    Q->beta_2 = M_PI - Q->beta_0;
    Q->c[0].p.y = Q->c[2].v.r * sin(Q->beta_0);
    Q->c[1].p.y = Q->c[0].p.y;
    Q->p.y = 2. * Q->c[0].p.y;
    Q->c[2].p.y = 0.;
    Q->c[1].p.x = 0.5 * Q->c[0].v.r;
    Q->c[0].p.x = -Q->c[1].p.x;
    Q->c[2].p.x = Q->c[0].p.x + Q->c[2].v.r * cos(Q->beta_0);
    Q->p.x = Q->c[2].p.x;

    P->es = 0.;
    P->fwd = chamb_s_forward;

    return P;
}

#undef THIRD
#undef TOL
