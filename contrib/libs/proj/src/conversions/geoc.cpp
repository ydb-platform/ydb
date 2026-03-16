/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Conversion from geographic to geocentric latitude and back.
 * Author:   Thomas Knudsen (2017)
 *
 ******************************************************************************
 * Copyright (c) 2017, SDFE, http://www.sdfe.dk
 * Copyright (c) 2017, Thomas Knudsen
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
 *****************************************************************************/

#include <math.h>

#include "proj.h"
#include "proj_internal.h"

PROJ_HEAD(geoc, "Geocentric Latitude");

/*************************************************************************************/
PJ_COORD pj_geocentric_latitude(const PJ *P, PJ_DIRECTION direction,
                                PJ_COORD coord) {
    /**************************************************************************************
        Convert geographical latitude to geocentric (or the other way round if
        direction = PJ_INV)

        The conversion involves a call to the tangent function, which goes
    through the roof at the poles, so very close (the last centimeter) to the
    poles no conversion takes place and the input latitude is copied directly to
    the output.

        Fortunately, the geocentric latitude converges to the geographical at
    the poles, so the difference is negligible.

        For the spherical case, the geographical latitude equals the geocentric,
    and consequently, the input is copied directly to the output.
    **************************************************************************************/
    const double limit = M_HALFPI - 1e-9;
    PJ_COORD res = coord;
    if ((coord.lp.phi > limit) || (coord.lp.phi < -limit) || (P->es == 0))
        return res;
    if (direction == PJ_FWD)
        res.lp.phi = atan(P->one_es * tan(coord.lp.phi));
    else
        res.lp.phi = atan(P->rone_es * tan(coord.lp.phi));

    return res;
}

/* Geographical to geocentric */
static void forward(PJ_COORD &coo, PJ *P) {
    coo = pj_geocentric_latitude(P, PJ_FWD, coo);
}

/* Geocentric to geographical */
static void inverse(PJ_COORD &coo, PJ *P) {
    coo = pj_geocentric_latitude(P, PJ_INV, coo);
}

static PJ *PJ_CONVERSION(geoc, 1) {
    P->inv4d = inverse;
    P->fwd4d = forward;

    P->left = PJ_IO_UNITS_RADIANS;
    P->right = PJ_IO_UNITS_RADIANS;

    P->is_latlong = 1;
    return P;
}
