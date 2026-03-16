/******************************************************************************
 * Project:  PROJ
 * Purpose:  Functions related to PJ_COORD 4D geodetic spatiotemporal
 *           data type.
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2016-06-09/2016-11-06
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017 Thomas Knudsen/SDFE
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

#define FROM_PROJ_CPP

#include "proj.h"
#include "proj_internal.h"

#include "proj/internal/internal.hpp"

using namespace NS_PROJ::internal;

/* Initialize PJ_COORD struct */
PJ_COORD proj_coord(double x, double y, double z, double t) {
    PJ_COORD res;
    res.v[0] = x;
    res.v[1] = y;
    res.v[2] = z;
    res.v[3] = t;
    return res;
}

PJ_DIRECTION pj_opposite_direction(PJ_DIRECTION dir) {
    return static_cast<PJ_DIRECTION>(-dir);
}

/*****************************************************************************/
int proj_angular_input(PJ *P, enum PJ_DIRECTION dir) {
    /******************************************************************************
        Returns 1 if the operator P expects angular input coordinates when
        operating in direction dir, 0 otherwise.
        dir: {PJ_FWD, PJ_INV}
    ******************************************************************************/
    if (PJ_FWD == dir)
        return pj_left(P) == PJ_IO_UNITS_RADIANS;
    return pj_right(P) == PJ_IO_UNITS_RADIANS;
}

/*****************************************************************************/
int proj_angular_output(PJ *P, enum PJ_DIRECTION dir) {
    /******************************************************************************
        Returns 1 if the operator P provides angular output coordinates when
        operating in direction dir, 0 otherwise.
        dir: {PJ_FWD, PJ_INV}
    ******************************************************************************/
    return proj_angular_input(P, pj_opposite_direction(dir));
}

/*****************************************************************************/
int proj_degree_input(PJ *P, enum PJ_DIRECTION dir) {
    /******************************************************************************
        Returns 1 if the operator P expects degree input coordinates when
        operating in direction dir, 0 otherwise.
        dir: {PJ_FWD, PJ_INV}
    ******************************************************************************/
    if (PJ_FWD == dir)
        return pj_left(P) == PJ_IO_UNITS_DEGREES;
    return pj_right(P) == PJ_IO_UNITS_DEGREES;
}

/*****************************************************************************/
int proj_degree_output(PJ *P, enum PJ_DIRECTION dir) {
    /******************************************************************************
        Returns 1 if the operator P provides degree output coordinates when
        operating in direction dir, 0 otherwise.
        dir: {PJ_FWD, PJ_INV}
    ******************************************************************************/
    return proj_degree_input(P, pj_opposite_direction(dir));
}

double proj_torad(double angle_in_degrees) {
    return PJ_TORAD(angle_in_degrees);
}
double proj_todeg(double angle_in_radians) {
    return PJ_TODEG(angle_in_radians);
}

double proj_dmstor(const char *is, char **rs) { return dmstor(is, rs); }

char *proj_rtodms(char *s, double r, int pos, int neg) {
    // 40 is the size used for the buffer in proj.cpp
    size_t arbitrary_size = 40;
    return rtodms(s, arbitrary_size, r, pos, neg);
}

char *proj_rtodms2(char *s, size_t sizeof_s, double r, int pos, int neg) {
    return rtodms(s, sizeof_s, r, pos, neg);
}
