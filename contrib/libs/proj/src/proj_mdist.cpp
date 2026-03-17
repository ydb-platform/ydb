/*
** libproj -- library of cartographic projections
**
** Copyright (c) 2003, 2006   Gerald I. Evenden
*/
/*
** Permission is hereby granted, free of charge, to any person obtaining
** a copy of this software and associated documentation files (the
** "Software"), to deal in the Software without restriction, including
** without limitation the rights to use, copy, modify, merge, publish,
** distribute, sublicense, and/or sell copies of the Software, and to
** permit persons to whom the Software is furnished to do so, subject to
** the following conditions:
**
** The above copyright notice and this permission notice shall be
** included in all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
** EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
** MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
** IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
** CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
** TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
** SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
/* Computes distance from equator along the meridian to latitude phi
** and inverse on unit ellipsoid.
** Precision commensurate with double precision.
*/

#include <math.h>
#include <stdlib.h>

#include "proj.h"
#include "proj_internal.h"

#define MAX_ITER 20
#define TOL 1e-14

namespace { // anonymous namespace
struct MDIST {
    int nb;
    double es;
    double E;
    double b[1];
};
} // anonymous namespace
void *proj_mdist_ini(double es) {
    double numf, numfi, twon1, denf, denfi, ens, T, twon;
    double den, El = 1., Es = 1.;
    double E[MAX_ITER] = {1.};
    struct MDIST *b;
    int i, j;

    /* generate E(e^2) and its terms E[] */
    ens = es;
    numf = twon1 = denfi = 1.;
    denf = 1.;
    twon = 4.;
    for (i = 1; i < MAX_ITER; ++i) {
        numf *= (twon1 * twon1);
        den = twon * denf * denf * twon1;
        T = numf / den;
        Es -= (E[i] = T * ens);
        ens *= es;
        twon *= 4.;
        denf *= ++denfi;
        twon1 += 2.;
        if (Es == El) /* jump out if no change */
            break;
        El = Es;
    }
    if ((b = (struct MDIST *)malloc(sizeof(struct MDIST) +
                                    (i * sizeof(double)))) == nullptr)
        return (nullptr);
    b->nb = i - 1;
    b->es = es;
    b->E = Es;
    /* generate b_n coefficients--note: collapse with prefix ratios */
    b->b[0] = Es = 1. - Es;
    numf = denf = 1.;
    numfi = 2.;
    denfi = 3.;
    for (j = 1; j < i; ++j) {
        Es -= E[j];
        numf *= numfi;
        denf *= denfi;
        b->b[j] = Es * numf / denf;
        numfi += 2.;
        denfi += 2.;
    }
    return (b);
}
double proj_mdist(double phi, double sphi, double cphi, const void *data) {
    const struct MDIST *b = (const struct MDIST *)data;
    double sc, sum, sphi2, D;
    int i;

    sc = sphi * cphi;
    sphi2 = sphi * sphi;
    D = phi * b->E - b->es * sc / sqrt(1. - b->es * sphi2);
    sum = b->b[i = b->nb];
    while (i)
        sum = b->b[--i] + sphi2 * sum;
    return (D + sc * sum);
}
double proj_inv_mdist(PJ_CONTEXT *ctx, double dist, const void *data) {
    const struct MDIST *b = (const struct MDIST *)data;
    double s, t, phi, k;
    int i;

    k = 1. / (1. - b->es);
    i = MAX_ITER;
    phi = dist;
    while (i--) {
        s = sin(phi);
        t = 1. - b->es * s * s;
        phi -= t = (proj_mdist(phi, s, cos(phi), b) - dist) * (t * sqrt(t)) * k;
        if (fabs(t) < TOL) /* that is no change */
            return phi;
    }
    /* convergence failed */
    proj_context_errno_set(ctx,
                           PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
    return phi;
}
