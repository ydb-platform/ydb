
#include "proj.h"
#include "proj_internal.h"
#include <cmath>

PROJ_HEAD(robin, "Robinson") "\n\tPCyl, Sph";

#define V(C, z) (C.c0 + z * (C.c1 + z * (C.c2 + z * C.c3)))
#define DV(C, z) (C.c1 + 2 * z * C.c2 + z * z * 3. * C.c3)

/*
note: following terms based upon 5 deg. intervals in degrees.

Some background on these coefficients is available at:

http://article.gmane.org/gmane.comp.gis.proj-4.devel/6039
http://trac.osgeo.org/proj/ticket/113
*/

namespace { // anonymous namespace
struct COEFS {
    float c0, c1, c2, c3;
};
} // anonymous namespace

static const struct COEFS X[] = {
    {1.0f, 2.2199e-17f, -7.15515e-05f, 3.1103e-06f},
    {0.9986f, -0.000482243f, -2.4897e-05f, -1.3309e-06f},
    {0.9954f, -0.00083103f, -4.48605e-05f, -9.86701e-07f},
    {0.99f, -0.00135364f, -5.9661e-05f, 3.6777e-06f},
    {0.9822f, -0.00167442f, -4.49547e-06f, -5.72411e-06f},
    {0.973f, -0.00214868f, -9.03571e-05f, 1.8736e-08f},
    {0.96f, -0.00305085f, -9.00761e-05f, 1.64917e-06f},
    {0.9427f, -0.00382792f, -6.53386e-05f, -2.6154e-06f},
    {0.9216f, -0.00467746f, -0.00010457f, 4.81243e-06f},
    {0.8962f, -0.00536223f, -3.23831e-05f, -5.43432e-06f},
    {0.8679f, -0.00609363f, -0.000113898f, 3.32484e-06f},
    {0.835f, -0.00698325f, -6.40253e-05f, 9.34959e-07f},
    {0.7986f, -0.00755338f, -5.00009e-05f, 9.35324e-07f},
    {0.7597f, -0.00798324f, -3.5971e-05f, -2.27626e-06f},
    {0.7186f, -0.00851367f, -7.01149e-05f, -8.6303e-06f},
    {0.6732f, -0.00986209f, -0.000199569f, 1.91974e-05f},
    {0.6213f, -0.010418f, 8.83923e-05f, 6.24051e-06f},
    {0.5722f, -0.00906601f, 0.000182f, 6.24051e-06f},
    {0.5322f, -0.00677797f, 0.000275608f, 6.24051e-06f}};

static const struct COEFS Y[] = {
    {-5.20417e-18f, 0.0124f, 1.21431e-18f, -8.45284e-11f},
    {0.062f, 0.0124f, -1.26793e-09f, 4.22642e-10f},
    {0.124f, 0.0124f, 5.07171e-09f, -1.60604e-09f},
    {0.186f, 0.0123999f, -1.90189e-08f, 6.00152e-09f},
    {0.248f, 0.0124002f, 7.10039e-08f, -2.24e-08f},
    {0.31f, 0.0123992f, -2.64997e-07f, 8.35986e-08f},
    {0.372f, 0.0124029f, 9.88983e-07f, -3.11994e-07f},
    {0.434f, 0.0123893f, -3.69093e-06f, -4.35621e-07f},
    {0.4958f, 0.0123198f, -1.02252e-05f, -3.45523e-07f},
    {0.5571f, 0.0121916f, -1.54081e-05f, -5.82288e-07f},
    {0.6176f, 0.0119938f, -2.41424e-05f, -5.25327e-07f},
    {0.6769f, 0.011713f, -3.20223e-05f, -5.16405e-07f},
    {0.7346f, 0.0113541f, -3.97684e-05f, -6.09052e-07f},
    {0.7903f, 0.0109107f, -4.89042e-05f, -1.04739e-06f},
    {0.8435f, 0.0103431f, -6.4615e-05f, -1.40374e-09f},
    {0.8936f, 0.00969686f, -6.4636e-05f, -8.547e-06f},
    {0.9394f, 0.00840947f, -0.000192841f, -4.2106e-06f},
    {0.9761f, 0.00616527f, -0.000256f, -4.2106e-06f},
    {1.0f, 0.00328947f, -0.000319159f, -4.2106e-06f}};

#define FXC 0.8487
#define FYC 1.3523
#define C1 11.45915590261646417544
#define RC1 0.08726646259971647884
#define NODES 18
#define ONEEPS 1.000001
#define EPS 1e-10
/* Not sure at all of the appropriate number for MAX_ITER... */
#define MAX_ITER 100

static PJ_XY robin_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    long i;
    double dphi;
    (void)P;

    dphi = fabs(lp.phi);
    i = std::isnan(lp.phi) ? -1 : lround(floor(dphi * C1 + 1e-15));
    if (i < 0) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    if (i >= NODES)
        i = NODES;
    dphi = RAD_TO_DEG * (dphi - RC1 * i);
    xy.x = V(X[i], dphi) * FXC * lp.lam;
    xy.y = V(Y[i], dphi) * FYC;
    if (lp.phi < 0.)
        xy.y = -xy.y;

    return xy;
}

static PJ_LP robin_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double t;
    struct COEFS T;
    int iters;

    lp.lam = xy.x / FXC;
    lp.phi = fabs(xy.y / FYC);
    if (lp.phi >= 1.) { /* simple pathologic cases */
        if (lp.phi > ONEEPS) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        } else {
            lp.phi = xy.y < 0. ? -M_HALFPI : M_HALFPI;
            lp.lam /= X[NODES].c0;
        }
    } else { /* general problem */
        /* in Y space, reduce to table interval */
        long i = std::isnan(lp.phi) ? -1 : lround(floor(lp.phi * NODES));
        if (i < 0 || i >= NODES) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return lp;
        }
        for (;;) {
            if (Y[i].c0 > lp.phi)
                --i;
            else if (Y[i + 1].c0 <= lp.phi)
                ++i;
            else
                break;
        }
        T = Y[i];
        /* first guess, linear interp */
        t = 5. * (lp.phi - T.c0) / (Y[i + 1].c0 - T.c0);
        for (iters = MAX_ITER; iters; --iters) { /* Newton-Raphson */
            const double t1 = (V(T, t) - lp.phi) / DV(T, t);
            t -= t1;
            if (fabs(t1) < EPS)
                break;
        }
        if (iters == 0)
            proj_context_errno_set(
                P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        lp.phi = (5 * i + t) * DEG_TO_RAD;
        if (xy.y < 0.)
            lp.phi = -lp.phi;
        lp.lam /= V(X[i], t);
        if (fabs(lp.lam) > M_PI) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            lp = proj_coord_error().lp;
        }
    }
    return lp;
}

PJ *PJ_PROJECTION(robin) {
    P->es = 0.;
    P->inv = robin_s_inverse;
    P->fwd = robin_s_forward;

    return P;
}
