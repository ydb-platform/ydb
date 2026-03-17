

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(eck4, "Eckert IV") "\n\tPCyl, Sph";

// C_x = 2 / sqrt(4 * M_PI + M_PI * M_PI)
constexpr double C_x = .42223820031577120149;

// C_y = 2 * sqrt(M_PI / (4 + M_PI))
constexpr double C_y = 1.32650042817700232218;

// RC_y = 1. / C_y
constexpr double RC_y = .75386330736002178205;

// C_p = 2 + M_PI / 2
constexpr double C_p = 3.57079632679489661922;

// RC_p = 1. / C_p
constexpr double RC_p = .28004957675577868795;

static PJ_XY eck4_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    int i;
    (void)P;

    // Newton's iterative method to find theta such as
    // theta + sin(theta) * cos(theta) + 2 * sin(theta) == C_p * sin(phi)
    const double p = C_p * sin(lp.phi);
    double V = lp.phi * lp.phi;
    double theta = lp.phi * (0.895168 + V * (0.0218849 + V * 0.00826809));
    constexpr int NITER = 6;
    for (i = NITER; i; --i) {
        const double c = cos(theta);
        const double s = sin(theta);
        V = (theta + s * (c + 2.) - p) / (1. + c * (c + 2.) - s * s);
        theta -= V;
        constexpr double EPS = 1e-7;
        if (fabs(V) < EPS)
            break;
    }
    if (!i) {
        xy.x = C_x * lp.lam;
        xy.y = theta < 0. ? -C_y : C_y;
    } else {
        xy.x = C_x * lp.lam * (1. + cos(theta));
        xy.y = C_y * sin(theta);
    }
    return xy;
}

static PJ_LP eck4_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};

    const double sin_theta = xy.y * RC_y;
    const double one_minus_abs_sin_theta = 1.0 - fabs(sin_theta);
    if (one_minus_abs_sin_theta >= 0.0 && one_minus_abs_sin_theta <= 1e-12) {
        lp.lam = xy.x / C_x;
        lp.phi = sin_theta > 0 ? M_PI / 2 : -M_PI / 2;
    } else {
        const double theta = aasin(P->ctx, sin_theta);
        const double cos_theta = cos(theta);
        lp.lam = xy.x / (C_x * (1. + cos_theta));
        const double sin_phi = (theta + sin_theta * (cos_theta + 2.)) * RC_p;
        lp.phi = aasin(P->ctx, sin_phi);
    }
    if (!P->over) {
        const double fabs_lam_minus_pi = fabs(lp.lam) - M_PI;
        if (fabs_lam_minus_pi > 0.0) {
            if (fabs_lam_minus_pi > 1e-10) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return lp;
            } else {
                lp.lam = lp.lam > 0 ? M_PI : -M_PI;
            }
        }
    }
    return lp;
}

PJ *PJ_PROJECTION(eck4) {
    P->es = 0.0;
    P->inv = eck4_s_inverse;
    P->fwd = eck4_s_forward;

    return P;
}
