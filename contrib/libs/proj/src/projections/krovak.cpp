/*
 * Project:  PROJ
 * Purpose:  Implementation of the krovak (Krovak) projection.
 *           Definition: http://www.ihsenergy.com/epsg/guid7.html#1.4.3
 * Author:   Thomas Flemming, tf@ttqv.com
 *
 ******************************************************************************
 * Copyright (c) 2001, Thomas Flemming, tf@ttqv.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************
 * A description of the (forward) projection is found in:
 *
 *      Bohuslav Veverka,
 *
 *      KROVAK’S PROJECTION AND ITS USE FOR THE
 *      CZECH REPUBLIC AND THE SLOVAK REPUBLIC,
 *
 *      50 years of the Research Institute of
 *      and the Slovak Republic Geodesy, Topography and Cartography
 *
 * which can be found via the Wayback Machine:
 *
 *      https://web.archive.org/web/20150216143806/https://www.vugtk.cz/odis/sborniky/sb2005/Sbornik_50_let_VUGTK/Part_1-Scientific_Contribution/16-Veverka.pdf
 *
 * Further info, including the inverse projection, is given by EPSG:
 *
 *      Guidance Note 7 part 2
 *      Coordinate Conversions and Transformations including Formulas
 *
 *      http://www.iogp.org/pubs/373-07-2.pdf
 *
 * Variable names in this file mostly follows what is used in the
 * paper by Veverka.
 *
 * According to EPSG the full Krovak projection method should have
 * the following parameters.  Within PROJ the azimuth, and pseudo
 * standard parallel are hardcoded in the algorithm and can't be
 * altered from outside. The others all have defaults to match the
 * common usage with Krovak projection.
 *
 *      lat_0 = latitude of centre of the projection
 *
 *      lon_0 = longitude of centre of the projection
 *
 *      ** = azimuth (true) of the centre line passing through the
 *           centre of the projection
 *
 *      ** = latitude of pseudo standard parallel
 *
 *      k  = scale factor on the pseudo standard parallel
 *
 *      x_0 = False Easting of the centre of the projection at the
 *            apex of the cone
 *
 *      y_0 = False Northing of the centre of the projection at
 *            the apex of the cone
 *
 *****************************************************************************/

#include <errno.h>
#include <math.h>

#include <algorithm>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(krovak, "Krovak") "\n\tPCyl, Ell";
PROJ_HEAD(mod_krovak, "Modified Krovak") "\n\tPCyl, Ell";

#define EPS 1e-15
#define UQ 1.04216856380474 /* DU(2, 59, 42, 42.69689) */
#define S0                                                                     \
    1.37008346281555 /* Latitude of pseudo standard parallel 78deg 30'00" N */
/* Not sure at all of the appropriate number for MAX_ITER... */
#define MAX_ITER 100

namespace { // anonymous namespace
struct pj_krovak_data {
    double alpha;
    double k;
    double n;
    double rho0;
    double ad;
    bool easting_northing; // true, in default mode. false when using +czech
    bool modified;
};
} // anonymous namespace

namespace pj_modified_krovak {
constexpr double X0 = 1089000.0;
constexpr double Y0 = 654000.0;
constexpr double C1 = 2.946529277E-02;
constexpr double C2 = 2.515965696E-02;
constexpr double C3 = 1.193845912E-07;
constexpr double C4 = -4.668270147E-07;
constexpr double C5 = 9.233980362E-12;
constexpr double C6 = 1.523735715E-12;
constexpr double C7 = 1.696780024E-18;
constexpr double C8 = 4.408314235E-18;
constexpr double C9 = -8.331083518E-24;
constexpr double C10 = -3.689471323E-24;

// Correction terms to be applied to regular Krovak to obtain Modified Krovak.
// Note that Xr is a Southing in metres and Yr a Westing in metres,
// and output (dX, dY) is a corrective term in (Southing, Westing) in metres
// Reference:
// https://www.cuzk.cz/Zememerictvi/Geodeticke-zaklady-na-uzemi-CR/GNSS/Nova-realizace-systemu-ETRS89-v-CR/Metodika-prevodu-ETRF2000-vs-S-JTSK-var2(101208).aspx
static void mod_krovak_compute_dx_dy(const double Xr, const double Yr,
                                     double &dX, double &dY) {
    const double Xr2 = Xr * Xr;
    const double Yr2 = Yr * Yr;
    const double Xr4 = Xr2 * Xr2;
    const double Yr4 = Yr2 * Yr2;

    dX = C1 + C3 * Xr - C4 * Yr - 2 * C6 * Xr * Yr + C5 * (Xr2 - Yr2) +
         C7 * Xr * (Xr2 - 3 * Yr2) - C8 * Yr * (3 * Xr2 - Yr2) +
         4 * C9 * Xr * Yr * (Xr2 - Yr2) + C10 * (Xr4 + Yr4 - 6 * Xr2 * Yr2);
    dY = C2 + C3 * Yr + C4 * Xr + 2 * C5 * Xr * Yr + C6 * (Xr2 - Yr2) +
         C8 * Xr * (Xr2 - 3 * Yr2) + C7 * Yr * (3 * Xr2 - Yr2) -
         4 * C10 * Xr * Yr * (Xr2 - Yr2) + C9 * (Xr4 + Yr4 - 6 * Xr2 * Yr2);
}

} // namespace pj_modified_krovak

static PJ_XY krovak_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoidal, forward */
    struct pj_krovak_data *Q = static_cast<struct pj_krovak_data *>(P->opaque);
    PJ_XY xy = {0.0, 0.0};

    const double gfi =
        pow((1. + P->e * sin(lp.phi)) / (1. - P->e * sin(lp.phi)),
            Q->alpha * P->e / 2.);

    const double u =
        2. *
        (atan(Q->k * pow(tan(lp.phi / 2. + M_PI_4), Q->alpha) / gfi) - M_PI_4);
    const double deltav = -lp.lam * Q->alpha;

    const double s =
        asin(cos(Q->ad) * sin(u) + sin(Q->ad) * cos(u) * cos(deltav));
    const double cos_s = cos(s);
    if (cos_s < 1e-12) {
        xy.x = 0;
        xy.y = 0;
        return xy;
    }
    const double d = asin(cos(u) * sin(deltav) / cos_s);

    const double eps = Q->n * d;
    const double rho = Q->rho0 * pow(tan(S0 / 2. + M_PI_4), Q->n) /
                       pow(tan(s / 2. + M_PI_4), Q->n);

    xy.x = rho * cos(eps);
    xy.y = rho * sin(eps);

    // At this point, xy.x is a southing and xy.y is a westing

    if (Q->modified) {
        using namespace pj_modified_krovak;

        const double Xp = xy.x;
        const double Yp = xy.y;

        // Reduced X and Y
        const double Xr = Xp * P->a - X0;
        const double Yr = Yp * P->a - Y0;

        double dX, dY;
        mod_krovak_compute_dx_dy(Xr, Yr, dX, dY);

        xy.x = Xp - dX / P->a;
        xy.y = Yp - dY / P->a;
    }

    // PROJ always return values in (easting, northing) (default mode)
    // or (westing, southing) (+czech mode), so swap X/Y
    std::swap(xy.x, xy.y);

    if (Q->easting_northing) {
        // The default non-Czech convention uses easting, northing, so we have
        // to reverse the sign of the coordinates. But to do so, we have to take
        // into account the false easting/northing.
        xy.x = -xy.x - 2 * P->x0 / P->a;
        xy.y = -xy.y - 2 * P->y0 / P->a;
    }

    return xy;
}

static PJ_LP krovak_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoidal, inverse */
    struct pj_krovak_data *Q = static_cast<struct pj_krovak_data *>(P->opaque);
    PJ_LP lp = {0.0, 0.0};

    if (Q->easting_northing) {
        // The default non-Czech convention uses easting, northing, so we have
        // to reverse the sign of the coordinates. But to do so, we have to take
        // into account the false easting/northing.
        xy.y = -xy.y - 2 * P->x0 / P->a;
        xy.x = -xy.x - 2 * P->y0 / P->a;
    }

    std::swap(xy.x, xy.y);

    if (Q->modified) {
        using namespace pj_modified_krovak;

        // Note: in EPSG guidance node 7-2, below Xr/Yr/dX/dY are actually
        // Xr'/Yr'/dX'/dY'
        const double Xr = xy.x * P->a - X0;
        const double Yr = xy.y * P->a - Y0;

        double dX, dY;
        mod_krovak_compute_dx_dy(Xr, Yr, dX, dY);

        xy.x = xy.x + dX / P->a;
        xy.y = xy.y + dY / P->a;
    }

    const double rho = sqrt(xy.x * xy.x + xy.y * xy.y);
    const double eps = atan2(xy.y, xy.x);

    const double d = eps / sin(S0);
    double s;
    if (rho == 0.0) {
        s = M_PI_2;
    } else {
        s = 2. * (atan(pow(Q->rho0 / rho, 1. / Q->n) * tan(S0 / 2. + M_PI_4)) -
                  M_PI_4);
    }

    const double u = asin(cos(Q->ad) * sin(s) - sin(Q->ad) * cos(s) * cos(d));
    const double deltav = asin(cos(s) * sin(d) / cos(u));

    lp.lam = P->lam0 - deltav / Q->alpha;

    /* ITERATION FOR lp.phi */
    double fi1 = u;

    int i;
    for (i = MAX_ITER; i; --i) {
        lp.phi = 2. * (atan(pow(Q->k, -1. / Q->alpha) *
                            pow(tan(u / 2. + M_PI_4), 1. / Q->alpha) *
                            pow((1. + P->e * sin(fi1)) / (1. - P->e * sin(fi1)),
                                P->e / 2.)) -
                       M_PI_4);

        if (fabs(fi1 - lp.phi) < EPS)
            break;
        fi1 = lp.phi;
    }
    if (i == 0)
        proj_context_errno_set(
            P->ctx, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);

    lp.lam -= P->lam0;

    return lp;
}

static PJ *krovak_setup(PJ *P, bool modified) {
    double u0, n0, g;
    struct pj_krovak_data *Q = static_cast<struct pj_krovak_data *>(
        calloc(1, sizeof(struct pj_krovak_data)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;

    /* we want Bessel as fixed ellipsoid */
    P->a = 6377397.155;
    P->es = 0.006674372230614;
    P->e = sqrt(P->es);

    /* if latitude of projection center is not set, use 49d30'N */
    if (!pj_param(P->ctx, P->params, "tlat_0").i)
        P->phi0 = 0.863937979737193;

    /* if center long is not set use 42d30'E of Ferro - 17d40' for Ferro */
    /* that will correspond to using longitudes relative to greenwich    */
    /* as input and output, instead of lat/long relative to Ferro */
    if (!pj_param(P->ctx, P->params, "tlon_0").i)
        P->lam0 = 0.7417649320975901 - 0.308341501185665;

    /* if scale not set default to 0.9999 */
    if (!pj_param(P->ctx, P->params, "tk").i &&
        !pj_param(P->ctx, P->params, "tk_0").i)
        P->k0 = 0.9999;

    Q->modified = modified;

    Q->easting_northing = true;
    if (pj_param(P->ctx, P->params, "tczech").i)
        Q->easting_northing = false;

    /* Set up shared parameters between forward and inverse */
    Q->alpha = sqrt(1. + (P->es * pow(cos(P->phi0), 4)) / (1. - P->es));
    u0 = asin(sin(P->phi0) / Q->alpha);
    g = pow((1. + P->e * sin(P->phi0)) / (1. - P->e * sin(P->phi0)),
            Q->alpha * P->e / 2.);
    double tan_half_phi0_plus_pi_4 = tan(P->phi0 / 2. + M_PI_4);
    if (tan_half_phi0_plus_pi_4 == 0.0) {
        proj_log_error(P, _("Invalid value for lat_0: lat_0 + PI/4 should be "
                            "different from 0"));
        return pj_default_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    Q->k = tan(u0 / 2. + M_PI_4) / pow(tan_half_phi0_plus_pi_4, Q->alpha) * g;
    n0 = sqrt(1. - P->es) / (1. - P->es * pow(sin(P->phi0), 2));
    Q->n = sin(S0);
    Q->rho0 = P->k0 * n0 / tan(S0);
    Q->ad = M_PI_2 - UQ;

    P->inv = krovak_e_inverse;
    P->fwd = krovak_e_forward;

    return P;
}

PJ *PJ_PROJECTION(krovak) { return krovak_setup(P, false); }

PJ *PJ_PROJECTION(mod_krovak) { return krovak_setup(P, true); }

#undef EPS
#undef UQ
#undef S0
#undef MAX_ITER
