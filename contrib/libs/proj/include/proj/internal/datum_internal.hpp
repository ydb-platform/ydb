/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2023, Even Rouault <even dot rouault at spatialys dot com>
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
 ****************************************************************************/

#ifndef FROM_PROJ_CPP
#error This file should only be included from a PROJ cpp file
#endif

#ifndef DATUM_INTERNAL_HH_INCLUDED
#define DATUM_INTERNAL_HH_INCLUDED

#define UNKNOWN_ENGINEERING_DATUM "Unknown engineering datum"

constexpr const char *NON_EARTH_BODY = "Non-Earth body";

// Mars (2015) - Sphere uses R=3396190
// and Mars polar radius (as used by HIRISE JPEG2000) is 3376200m
// which is a 0.59% relative difference.
constexpr double REL_ERROR_FOR_SAME_CELESTIAL_BODY = 0.007;

constexpr const char *UNKNOWN_BASED_ON = "Unknown based on ";

#endif // DATUM_INTERNAL_HH_INCLUDED
