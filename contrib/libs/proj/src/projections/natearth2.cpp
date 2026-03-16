/*
The Natural Earth II projection was designed by Tom Patterson, US National
Park Service, in 2012, using Flex Projector. The polynomial equation was
developed by Bojan Savric and Bernhard Jenny, College of Earth, Ocean,
and Atmospheric Sciences, Oregon State University.
Port to PROJ.4 by Bojan Savric, 4 April 2016
*/

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(natearth2, "Natural Earth 2") "\n\tPCyl, Sph";

#define A0 0.84719
#define A1 -0.13063
#define A2 -0.04515
#define A3 0.05494
#define A4 -0.02326
#define A5 0.00331
#define B0 1.01183
#define B1 -0.02625
#define B2 0.01926
#define B3 -0.00396
#define C0 B0
#define C1 (9 * B1)
#define C2 (11 * B2)
#define C3 (13 * B3)
#define EPS 1e-11
#define MAX_Y (0.84719 * 0.535117535153096 * M_PI)
/* Not sure at all of the appropriate number for MAX_ITER... */
#define MAX_ITER 100

static PJ_XY natearth2_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double phi2, phi4, phi6;
    (void)P;

    phi2 = lp.phi * lp.phi;
    phi4 = phi2 * phi2;
    phi6 = phi2 * phi4;

    xy.x = lp.lam * (A0 + A1 * phi2 +
                     phi6 * phi6 * (A2 + A3 * phi2 + A4 * phi4 + A5 * phi6));
    xy.y = lp.phi * (B0 + phi4 * phi4 * (B1 + B2 * phi2 + B3 * phi4));
    return xy;
}

static PJ_LP natearth2_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double yc, y2, y4, y6, f, fder;
    int i;
    (void)P;

    /* make sure y is inside valid range */
    if (xy.y > MAX_Y) {
        xy.y = MAX_Y;
    } else if (xy.y < -MAX_Y) {
        xy.y = -MAX_Y;
    }

    /* latitude */
    yc = xy.y;
    for (i = MAX_ITER; i; --i) { /* Newton-Raphson */
        y2 = yc * yc;
        y4 = y2 * y2;
        f = (yc * (B0 + y4 * y4 * (B1 + B2 * y2 + B3 * y4))) - xy.y;
        fder = C0 + y4 * y4 * (C1 + C2 * y2 + C3 * y4);
        const double tol = f / fder;
        yc -= tol;
        if (fabs(tol) < EPS) {
            break;
        }
    }
    if (i == 0)
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    lp.phi = yc;

    /* longitude */
    y2 = yc * yc;
    y4 = y2 * y2;
    y6 = y2 * y4;

    lp.lam =
        xy.x / (A0 + A1 * y2 + y6 * y6 * (A2 + A3 * y2 + A4 * y4 + A5 * y6));

    return lp;
}

PJ *PJ_PROJECTION(natearth2) {
    P->es = 0;
    P->inv = natearth2_s_inverse;
    P->fwd = natearth2_s_forward;

    return P;
}

#undef A0
#undef A1
#undef A2
#undef A3
#undef A4
#undef A5
#undef B0
#undef B1
#undef B2
#undef B3
#undef C0
#undef C1
#undef C2
#undef C3
#undef EPS
#undef MAX_Y
#undef MAX_ITER
