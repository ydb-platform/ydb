/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the Times projection.
 * Author:   Kristian Evers <kristianevers@gmail.com>
 *
 ******************************************************************************
 * Copyright (c) 2016, Kristian Evers
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************
 * Based on description of the Times Projection in
 *
 * Flattening the Earth, Snyder, J.P., 1993, p.213-214.
 *****************************************************************************/

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(times, "Times") "\n\tCyl, Sph";

static PJ_XY times_s_forward(PJ_LP lp, PJ *P) { /* Spheroidal, forward */
    double T, S, S2;
    PJ_XY xy = {0.0, 0.0};
    (void)P;

    T = tan(lp.phi / 2.0);
    S = sin(M_FORTPI * T);
    S2 = S * S;

    xy.x = lp.lam * (0.74482 - 0.34588 * S2);
    xy.y = 1.70711 * T;

    return xy;
}

static PJ_LP times_s_inverse(PJ_XY xy, PJ *P) { /* Spheroidal, inverse */
    double T, S, S2;
    PJ_LP lp = {0.0, 0.0};
    (void)P;

    T = xy.y / 1.70711;
    S = sin(M_FORTPI * T);
    S2 = S * S;

    lp.lam = xy.x / (0.74482 - 0.34588 * S2);
    lp.phi = 2 * atan(T);

    return lp;
}

PJ *PJ_PROJECTION(times) {
    P->es = 0.0;

    P->inv = times_s_inverse;
    P->fwd = times_s_forward;

    return P;
}
