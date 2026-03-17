/*
The Natural Earth projection was designed by Tom Patterson, US National Park
Service, in 2007, using Flex Projector. The shape of the original projection
was defined at every 5 degrees and piece-wise cubic spline interpolation was
used to compute the complete graticule.
The code here uses polynomial functions instead of cubic splines and
is therefore much simpler to program. The polynomial approximation was
developed by Bojan Savric, in collaboration with Tom Patterson and Bernhard
Jenny, Institute of Cartography, ETH Zurich. It slightly deviates from
Patterson's original projection by adding additional curvature to meridians
where they meet the horizontal pole line. This improvement is by intention
and designed in collaboration with Tom Patterson.
Port to PROJ.4 by Bernhard Jenny, 6 June 2011
*/

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(natearth, "Natural Earth") "\n\tPCyl, Sph";

#define A0 0.8707
#define A1 -0.131979
#define A2 -0.013791
#define A3 0.003971
#define A4 -0.001529
#define B0 1.007226
#define B1 0.015085
#define B2 -0.044475
#define B3 0.028874
#define B4 -0.005916
#define C0 B0
#define C1 (3 * B1)
#define C2 (7 * B2)
#define C3 (9 * B3)
#define C4 (11 * B4)
#define EPS 1e-11
#define MAX_Y (0.8707 * 0.52 * M_PI)
/* Not sure at all of the appropriate number for MAX_ITER... */
#define MAX_ITER 100

static PJ_XY natearth_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double phi2, phi4;
    (void)P;

    phi2 = lp.phi * lp.phi;
    phi4 = phi2 * phi2;
    xy.x = lp.lam *
           (A0 + phi2 * (A1 + phi2 * (A2 + phi4 * phi2 * (A3 + phi2 * A4))));
    xy.y = lp.phi * (B0 + phi2 * (B1 + phi4 * (B2 + B3 * phi2 + B4 * phi4)));
    return xy;
}

static PJ_LP natearth_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double yc, y2, y4, f, fder;
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
        f = (yc * (B0 + y2 * (B1 + y4 * (B2 + B3 * y2 + B4 * y4)))) - xy.y;
        fder = C0 + y2 * (C1 + y4 * (C2 + C3 * y2 + C4 * y4));
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
    lp.lam =
        xy.x / (A0 + y2 * (A1 + y2 * (A2 + y2 * y2 * y2 * (A3 + y2 * A4))));

    return lp;
}

PJ *PJ_PROJECTION(natearth) {
    P->es = 0;
    P->inv = natearth_s_inverse;
    P->fwd = natearth_s_forward;

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
