

#include <errno.h>
#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(ocea, "Oblique Cylindrical Equal Area")
"\n\tCyl, Sph"
    "lonc= alpha= or\n\tlat_1= lat_2= lon_1= lon_2=";

namespace { // anonymous namespace
struct pj_ocea {
    double rok;
    double rtk;
    double sinphi;
    double cosphi;
};
} // anonymous namespace

static PJ_XY ocea_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_ocea *Q = static_cast<struct pj_ocea *>(P->opaque);
    double t;
    xy.y = sin(lp.lam);
    t = cos(lp.lam);
    xy.x = atan((tan(lp.phi) * Q->cosphi + Q->sinphi * xy.y) / t);
    if (t < 0.)
        xy.x += M_PI;
    xy.x *= Q->rtk;
    xy.y = Q->rok * (Q->sinphi * sin(lp.phi) - Q->cosphi * cos(lp.phi) * xy.y);
    return xy;
}

static PJ_LP ocea_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_ocea *Q = static_cast<struct pj_ocea *>(P->opaque);

    xy.y /= Q->rok;
    xy.x /= Q->rtk;
    const double t = sqrt(1. - xy.y * xy.y);
    const double s = sin(xy.x);
    lp.phi = asin(xy.y * Q->sinphi + t * Q->cosphi * s);
    lp.lam = atan2(t * Q->sinphi * s - xy.y * Q->cosphi, t * cos(xy.x));
    return lp;
}

PJ *PJ_PROJECTION(ocea) {
    double phi_1, phi_2, lam_1, lam_2, lonz, alpha;

    struct pj_ocea *Q =
        static_cast<struct pj_ocea *>(calloc(1, sizeof(struct pj_ocea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    Q->rok = 1. / P->k0;
    Q->rtk = P->k0;
    double lam_p, phi_p;
    /*If the keyword "alpha" is found in the sentence then use 1point+1azimuth*/
    if (pj_param(P->ctx, P->params, "talpha").i) {
        /*Define Pole of oblique transformation from 1 point & 1 azimuth*/
        // ERO: I've added M_PI so that the alpha is the angle from point 1 to
        // point 2 from the North in a clockwise direction (to be consistent
        // with omerc behavior)
        alpha = M_PI + pj_param(P->ctx, P->params, "ralpha").f;
        lonz = pj_param(P->ctx, P->params, "rlonc").f;
        /*Equation 9-8 page 80 (http://pubs.usgs.gov/pp/1395/report.pdf)*/
        // Actually slightliy modified to use atan2(), as it is suggested by
        // Snyder for equation 9-1, but this is not mentioned here
        lam_p = atan2(-cos(alpha), -sin(P->phi0) * sin(alpha)) + lonz;
        /*Equation 9-7 page 80 (http://pubs.usgs.gov/pp/1395/report.pdf)*/
        phi_p = asin(cos(P->phi0) * sin(alpha));
        /*If the keyword "alpha" is NOT found in the sentence then use 2points*/
    } else {
        /*Define Pole of oblique transformation from 2 points*/
        phi_1 = pj_param(P->ctx, P->params, "rlat_1").f;
        phi_2 = pj_param(P->ctx, P->params, "rlat_2").f;
        lam_1 = pj_param(P->ctx, P->params, "rlon_1").f;
        lam_2 = pj_param(P->ctx, P->params, "rlon_2").f;
        /*Equation 9-1 page 80 (http://pubs.usgs.gov/pp/1395/report.pdf)*/
        lam_p = atan2(cos(phi_1) * sin(phi_2) * cos(lam_1) -
                          sin(phi_1) * cos(phi_2) * cos(lam_2),
                      sin(phi_1) * cos(phi_2) * sin(lam_2) -
                          cos(phi_1) * sin(phi_2) * sin(lam_1));

        /* take care of P->lam0 wrap-around when +lam_1=-90*/
        if (lam_1 == -M_HALFPI)
            lam_p = -lam_p;

        /*Equation 9-2 page 80 (http://pubs.usgs.gov/pp/1395/report.pdf)*/
        double cos_lamp_m_minus_lam_1 = cos(lam_p - lam_1);
        double tan_phi_1 = tan(phi_1);
        if (tan_phi_1 == 0.0) {
            // Not sure if we want to support this case, but at least this
            // avoids a division by zero, and gives the same result as the below
            // atan()
            phi_p = (cos_lamp_m_minus_lam_1 >= 0.0) ? -M_HALFPI : M_HALFPI;
        } else {
            phi_p = atan(-cos_lamp_m_minus_lam_1 / tan_phi_1);
        }
    }
    P->lam0 = lam_p + M_HALFPI;
    Q->cosphi = cos(phi_p);
    Q->sinphi = sin(phi_p);
    P->inv = ocea_s_inverse;
    P->fwd = ocea_s_forward;
    P->es = 0.;

    return P;
}
