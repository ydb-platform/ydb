/*
 * -----------------------------------------------------------------
 * Programmer(s): Scott D. Cohen, Alan C. Hindmarsh and
 *                Aaron Collier @ LLNL
 * -----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------
 * This is the header file for a simple C-language math library. The
 * routines listed here work with the type realtype as defined in
 * the header file sundials_types.h.
 * -----------------------------------------------------------------
 */

#ifndef _SUNDIALSMATH_H
#define _SUNDIALSMATH_H

#include <math.h>

#include <sundials/sundials_types.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/*
 * -----------------------------------------------------------------
 * Macros
 * -----------------------------------------------------------------
 * MIN(A,B) returns the minimum of A and B
 *
 * MAX(A,B) returns the maximum of A and B
 *
 * SQR(A) returns A^2
 *
 * SUNRsqrt calls the appropriate version of sqrt
 *
 * SUNRabs calls the appropriate version of abs
 *
 * SUNRexp calls the appropriate version of exp
 * -----------------------------------------------------------------
 */

#ifndef SUNMIN
#define SUNMIN(A, B) ((A) < (B) ? (A) : (B))
#endif

#ifndef SUNMAX
#define SUNMAX(A, B) ((A) > (B) ? (A) : (B))
#endif

#ifndef SUNSQR
#define SUNSQR(A) ((A)*(A))
#endif

/*
 * -----------------------------------------------------------------
 * Function : SUNRsqrt
 * -----------------------------------------------------------------
 * Usage : realtype sqrt_x;
 *         sqrt_x = SUNRsqrt(x);
 * -----------------------------------------------------------------
 * SUNRsqrt(x) returns the square root of x. If x < ZERO, then
 * SUNRsqrt returns ZERO.
 * -----------------------------------------------------------------
 */

#ifndef SUNRsqrt
#if defined(SUNDIALS_USE_GENERIC_MATH)
#define SUNRsqrt(x) ((x) <= RCONST(0.0) ? (RCONST(0.0)) : ((realtype) sqrt((double) (x))))
#elif defined(SUNDIALS_DOUBLE_PRECISION)
#define SUNRsqrt(x) ((x) <= RCONST(0.0) ? (RCONST(0.0)) : (sqrt((x))))
#elif defined(SUNDIALS_SINGLE_PRECISION)
#define SUNRsqrt(x) ((x) <= RCONST(0.0) ? (RCONST(0.0)) : (sqrtf((x))))
#elif defined(SUNDIALS_EXTENDED_PRECISION)
#define SUNRsqrt(x) ((x) <= RCONST(0.0) ? (RCONST(0.0)) : (sqrtl((x))))
#endif
#endif

/*
 * -----------------------------------------------------------------
 * Function : SUNRabs
 * -----------------------------------------------------------------
 * Usage : realtype abs_x;
 *         abs_x = SUNRabs(x);
 * -----------------------------------------------------------------
 * SUNRabs(x) returns the absolute value of x.
 * -----------------------------------------------------------------
 */

#ifndef SUNRabs
#if defined(SUNDIALS_USE_GENERIC_MATH)
#define SUNRabs(x) ((realtype) fabs((double) (x)))
#elif defined(SUNDIALS_DOUBLE_PRECISION)
#define SUNRabs(x) (fabs((x)))
#elif defined(SUNDIALS_SINGLE_PRECISION)
#define SUNRabs(x) (fabsf((x)))
#elif defined(SUNDIALS_EXTENDED_PRECISION)
#define SUNRabs(x) (fabsl((x)))
#endif
#endif

/*
 * -----------------------------------------------------------------
 * Function : SUNRexp
 * -----------------------------------------------------------------
 * Usage : realtype exp_x;
 *         exp_x = SUNRexp(x);
 * -----------------------------------------------------------------
 * SUNRexp(x) returns e^x (base-e exponential function).
 * -----------------------------------------------------------------
 */

#ifndef SUNRexp
#if defined(SUNDIALS_USE_GENERIC_MATH)
#define SUNRexp(x) ((realtype) exp((double) (x)))
#elif defined(SUNDIALS_DOUBLE_PRECISION)
#define SUNRexp(x) (exp((x)))
#elif defined(SUNDIALS_SINGLE_PRECISION)
#define SUNRexp(x) (expf((x)))
#elif defined(SUNDIALS_EXTENDED_PRECISION)
#define SUNRexp(x) (expl((x)))
#endif
#endif

/*
 * -----------------------------------------------------------------
 * Function : SUNRpowerI
 * -----------------------------------------------------------------
 * Usage : int exponent;
 *         realtype base, ans;
 *         ans = SUNRpowerI(base,exponent);
 * -----------------------------------------------------------------
 * SUNRpowerI returns the value of base^exponent, where base is of type
 * realtype and exponent is of type int.
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT realtype SUNRpowerI(realtype base, int exponent);

/*
 * -----------------------------------------------------------------
 * Function : SUNRpowerR
 * -----------------------------------------------------------------
 * Usage : realtype base, exponent, ans;
 *         ans = SUNRpowerR(base,exponent);
 * -----------------------------------------------------------------
 * SUNRpowerR returns the value of base^exponent, where both base and
 * exponent are of type realtype. If base < ZERO, then SUNRpowerR
 * returns ZERO.
 * -----------------------------------------------------------------
 */

SUNDIALS_EXPORT realtype SUNRpowerR(realtype base, realtype exponent);


#ifdef __cplusplus
}
#endif

#endif
