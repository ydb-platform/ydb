/******************************************************************************
 * Project:  PROJ
 * Purpose:  PJ_AREA related code
 *
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

/** Create an area of use */
PJ_AREA *proj_area_create(void) { return new PJ_AREA(); }

/** Assign a bounding box to an area of use. */
void proj_area_set_bbox(PJ_AREA *area, double west_lon_degree,
                        double south_lat_degree, double east_lon_degree,
                        double north_lat_degree) {
    area->bbox_set = TRUE;
    area->west_lon_degree = west_lon_degree;
    area->south_lat_degree = south_lat_degree;
    area->east_lon_degree = east_lon_degree;
    area->north_lat_degree = north_lat_degree;
}

/** Assign the name of an area of use. */
void proj_area_set_name(PJ_AREA *area, const char *name) { area->name = name; }

/** Free an area of use */
void proj_area_destroy(PJ_AREA *area) { delete area; }
