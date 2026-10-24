/* -*- Mode: c; c-basic-offset: 4; tab-width: 8; indent-tabs-mode: t; -*- */
/*
 * Copyright © 2010, 2012 Soren Sandmann Pedersen
 * Copyright © 2010, 2012 Red Hat, Inc.
 * Copyright © 2024 Filip Wasil, Samsung Electronics
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next
 * paragraph) shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * Author: Soren Sandmann Pedersen (sandmann@cs.au.dk)
 */

#ifndef __PIXMAN_COMBINE_FLOAT_H__
#define __PIXMAN_COMBINE_FLOAT_H__

/*
 * Porter/Duff operators
 */
typedef enum
{
    ZERO,
    ONE,
    SRC_ALPHA,
    DEST_ALPHA,
    INV_SA,
    INV_DA,
    SA_OVER_DA,
    DA_OVER_SA,
    INV_SA_OVER_DA,
    INV_DA_OVER_SA,
    ONE_MINUS_SA_OVER_DA,
    ONE_MINUS_DA_OVER_SA,
    ONE_MINUS_INV_DA_OVER_SA,
    ONE_MINUS_INV_SA_OVER_DA
} combine_factor_t;

#endif /*__PIXMAN_COMBINE_FLOAT_H__*/