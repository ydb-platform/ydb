// Changes to handle +over are: Copyright 2011-2014 Morelli Informatik

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(vandg, "van der Grinten (I)") "\n\tMisc Sph";

#define TOL 1.e-10
#define THIRD .33333333333333333333
#define C2_27 .07407407407407407407   // 2/27
#define PI4_3 4.18879020478639098458  // 4*pi/3
#define PISQ 9.86960440108935861869   // pi^2
#define TPISQ 19.73920880217871723738 // 2*pi^2
#define HPISQ 4.93480220054467930934  // pi^2/2

static PJ_XY vandg_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    double al, al2, g, g2, p2;
    // Comments tie this formulation to Snyder (1987), p. 241.
    p2 = fabs(lp.phi / M_HALFPI); // sin(theta) from (29-6)
    if ((p2 - TOL) > 1.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    int sign = 1;
    if (P->over && fabs(lp.lam) > M_PI)
        sign = -1;
    if (p2 > 1.)
        p2 = 1.;
    if (fabs(lp.phi) <= TOL) {
        xy.x = lp.lam;
        xy.y = 0.;
    } else if (fabs(lp.lam) <= TOL || fabs(p2 - 1.) < TOL) {
        xy.x = 0.;
        xy.y = M_PI * tan(.5 * asin(p2));
        if (lp.phi < 0.)
            xy.y = -xy.y;
    } else {
        al = .5 * sign * fabs(M_PI / lp.lam - lp.lam / M_PI); // A from (29-3)
        al2 = al * al;                                        // A^2
        g = sqrt(1. - p2 * p2);                               // cos(theta)
        g = g / (p2 + g - 1.);                                // G from (29-4)
        g2 = g * g;                                           // G^2
        p2 = g * (2. / p2 - 1.);                              // P from (29-5)
        p2 = p2 * p2;                                         // P^2
        {
            // Force floating-point computations done on 80 bit on
            // i386 to go back to 64 bit. This is to avoid the test failures
            // of
            // https://github.com/OSGeo/PROJ/issues/1906#issuecomment-583168348
            // The choice of p2 is completely arbitrary.
            volatile double p2_tmp = p2;
            // cppcheck-suppress redundantAssignment
            p2 = p2_tmp;
        }
        xy.x = g - p2; // G - P^2
        g = p2 + al2;  // P^2 + A^2
        // (29-1)
        xy.x = M_PI *
               fabs(al * xy.x + sqrt(al2 * xy.x * xy.x - g * (g2 - p2))) / g;
        if (lp.lam < 0.)
            xy.x = -xy.x;
        xy.y = fabs(xy.x / M_PI);
        // y from (29-2) has been expressed in terms of x here
        xy.y = 1. - xy.y * (xy.y + 2. * al);
        if (xy.y < -TOL) {
            proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
            return xy;
        }
        if (xy.y < 0.)
            xy.y = 0.;
        else
            xy.y = sqrt(xy.y) * (lp.phi < 0. ? -M_PI : M_PI);
    }

    return xy;
}

static PJ_LP vandg_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    double t, c0, c1, c2, c3, al, r2, r, m, d, ay, x2, y2;
    // Comments tie this formulation to Snyder (1987), p. 242.
    x2 = xy.x * xy.x; // pi^2 * X^2
    if ((ay = fabs(xy.y)) < TOL) {
        lp.phi = 0.;
        t = x2 * x2 + TPISQ * (x2 + HPISQ);
        lp.lam = fabs(xy.x) <= TOL ? 0. : .5 * (x2 - PISQ + sqrt(t)) / xy.x;
        return (lp);
    }
    y2 = xy.y * xy.y;             // pi^2 * Y^2
    r = x2 + y2;                  // pi^2 * (X^2+Y^2)
    r2 = r * r;                   // pi^4 * (X^2+Y^2)^2
    c1 = -M_PI * ay * (r + PISQ); // pi^4 * c1 (29-11)
    // pi^4 * c3 (29-13)
    c3 = r2 + M_TWOPI * (ay * r + M_PI * (y2 + M_PI * (ay + M_HALFPI)));
    c2 = c1 + PISQ * (r - 3. * y2); // pi^4 * c2 (29-12)
    c0 = M_PI * ay;                 // pi^2 * Y
    c2 /= c3;                       // c2/c3
    al = c1 / c3 - THIRD * c2 * c2; // a1 (29-15)
    m = 2. * sqrt(-THIRD * al);     // m1 (29-16)
    d = C2_27 * c2 * c2 * c2 + (c0 * c0 - THIRD * c2 * c1) / c3; // d (29-14)
    const double al_mul_m = al * m;                              // a1*m1
    if (fabs(al_mul_m) < 1e-16) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return proj_coord_error().lp;
    }
    d = 3. * d / al_mul_m; // cos(3*theta1) (29-17)
    t = fabs(d);
    if ((t - TOL) <= 1.) {
        d = t > 1. ? (d > 0. ? 0. : M_PI) : acos(d); // 3*theta1 (29-17)
        if (r > PISQ) {
            // This code path is triggered for coordinates generated in the
            // forward path when |long|>180deg and +over
            d = M_TWOPI - d;
        }
        // (29-18) but change pi/3 to 4*pi/3 to flip sign of cos
        lp.phi = M_PI * (m * cos(d * THIRD + PI4_3) - THIRD * c2);
        if (xy.y < 0.)
            lp.phi = -lp.phi;
        t = r2 + TPISQ * (x2 - y2 + HPISQ);
        lp.lam = fabs(xy.x) <= TOL
                     ? 0.
                     : .5 * (r - PISQ + (t <= 0. ? 0. : sqrt(t))) / xy.x;
    } else {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }

    return lp;
}

PJ *PJ_PROJECTION(vandg) {
    P->es = 0.;
    P->inv = vandg_s_inverse;
    P->fwd = vandg_s_forward;

    return P;
}

#undef TOL
#undef THIRD
#undef C2_27
#undef PI4_3
#undef PISQ
#undef TPISQ
#undef HPISQ
