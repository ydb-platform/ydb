/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name(s) of the copyright holder(s) nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

#include "blis.h"

#ifdef BLIS_ENABLE_BLAS

/* srot.f -- translated by f2c (version 19991025).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/

/* Subroutine */ int PASTEF77(s,rot)(const bla_integer *n, bla_real *sx, const bla_integer *incx, bla_real *sy, const bla_integer *incy, const bla_real *c__, const bla_real *s)
{
    /* System generated locals */
    bla_integer i__1;

    /* Local variables */
    bla_integer i__;
    bla_real stemp;
    bla_integer ix, iy;


/*     applies a plane rotation. */
/*     jack dongarra, linpack, 3/11/78. */
/*     modified 12/3/93, array(1) declarations changed to array(*) */


    /* Parameter adjustments */
    --sy;
    --sx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	goto L20;
    }

/*       code for unequal increments or equal increments not equal */
/*         to 1 */

    ix = 1;
    iy = 1;
    if (*incx < 0) {
	ix = (-(*n) + 1) * *incx + 1;
    }
    if (*incy < 0) {
	iy = (-(*n) + 1) * *incy + 1;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	stemp = *c__ * sx[ix] + *s * sy[iy];
	sy[iy] = *c__ * sy[iy] - *s * sx[ix];
	sx[ix] = stemp;
	ix += *incx;
	iy += *incy;
/* L10: */
    }
    return 0;

/*       code for both increments equal to 1 */

L20:
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	stemp = *c__ * sx[i__] + *s * sy[i__];
	sy[i__] = *c__ * sy[i__] - *s * sx[i__];
	sx[i__] = stemp;
/* L30: */
    }
    return 0;
} /* srot_ */

/* drot.f -- translated by f2c (version 19991025).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/

/* Subroutine */ int PASTEF77(d,rot)(const bla_integer *n, bla_double *dx, const bla_integer *incx, bla_double *dy, const bla_integer *incy, const bla_double *c__, const bla_double *s)
{
    /* System generated locals */
    bla_integer i__1;

    /* Local variables */
    bla_integer i__;
    bla_double dtemp;
    bla_integer ix, iy;


/*     applies a plane rotation. */
/*     jack dongarra, linpack, 3/11/78. */
/*     modified 12/3/93, array(1) declarations changed to array(*) */


    /* Parameter adjustments */
    --dy;
    --dx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	goto L20;
    }

/*       code for unequal increments or equal increments not equal */
/*         to 1 */

    ix = 1;
    iy = 1;
    if (*incx < 0) {
	ix = (-(*n) + 1) * *incx + 1;
    }
    if (*incy < 0) {
	iy = (-(*n) + 1) * *incy + 1;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	dtemp = *c__ * dx[ix] + *s * dy[iy];
	dy[iy] = *c__ * dy[iy] - *s * dx[ix];
	dx[ix] = dtemp;
	ix += *incx;
	iy += *incy;
/* L10: */
    }
    return 0;

/*       code for both increments equal to 1 */

L20:
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	dtemp = *c__ * dx[i__] + *s * dy[i__];
	dy[i__] = *c__ * dy[i__] - *s * dx[i__];
	dx[i__] = dtemp;
/* L30: */
    }
    return 0;
} /* drot_ */

/* csrot.f -- translated by f2c (version 19991025).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/

/* Subroutine */ int PASTEF77(cs,rot)(const bla_integer *n, bla_scomplex *cx, const bla_integer *incx, bla_scomplex *cy, const bla_integer *incy, const bla_real *c__, const bla_real *s)
{
    /* System generated locals */
    bla_integer i__1, i__2, i__3, i__4;
    bla_scomplex q__1, q__2, q__3;

    /* Local variables */
    bla_integer i__;
    bla_scomplex ctemp;
    bla_integer ix, iy;


/*     applies a plane rotation, where the cos and sin (c and s) are bla_real */
/*     and the vectors cx and cy are complex. */
/*     jack dongarra, linpack, 3/11/78. */


    /* Parameter adjustments */
    --cy;
    --cx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	goto L20;
    }

/*       code for unequal increments or equal increments not equal */
/*         to 1 */

    ix = 1;
    iy = 1;
    if (*incx < 0) {
	ix = (-(*n) + 1) * *incx + 1;
    }
    if (*incy < 0) {
	iy = (-(*n) + 1) * *incy + 1;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = ix;
	bli_csets( (*c__ * bli_creal(cx[i__2])), (*c__ * bli_cimag(cx[i__2])), q__2 );
	i__3 = iy;
	bli_csets( (*s * bli_creal(cy[i__3])), (*s * bli_cimag(cy[i__3])), q__3 );
	bli_csets( (bli_creal(q__2) + bli_creal(q__3)), (bli_cimag(q__2) + bli_cimag(q__3)), q__1 );
	bli_csets( (bli_creal(q__1)), (bli_cimag(q__1)), ctemp );
	i__2 = iy;
	i__3 = iy;
	bli_csets( (*c__ * bli_creal(cy[i__3])), (*c__ * bli_cimag(cy[i__3])), q__2 );
	i__4 = ix;
	bli_csets( (*s * bli_creal(cx[i__4])), (*s * bli_cimag(cx[i__4])), q__3 );
	bli_csets( (bli_creal(q__2) - bli_creal(q__3)), (bli_cimag(q__2) - bli_cimag(q__3)), q__1 );
	bli_csets( (bli_creal(q__1)), (bli_cimag(q__1)), cy[i__2] );
	i__2 = ix;
	bli_csets( (bli_creal(ctemp)), (bli_cimag(ctemp)), cx[i__2] );
	ix += *incx;
	iy += *incy;
/* L10: */
    }
    return 0;

/*       code for both increments equal to 1 */

L20:
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__;
	bli_csets( (*c__ * bli_creal(cx[i__2])), (*c__ * bli_cimag(cx[i__2])), q__2 );
	i__3 = i__;
	bli_csets( (*s * bli_creal(cy[i__3])), (*s * bli_cimag(cy[i__3])), q__3 );
	bli_csets( (bli_creal(q__2) + bli_creal(q__3)), (bli_cimag(q__2) + bli_cimag(q__3)), q__1 );
	bli_csets( (bli_creal(q__1)), (bli_cimag(q__1)), ctemp );
	i__2 = i__;
	i__3 = i__;
	bli_csets( (*c__ * bli_creal(cy[i__3])), (*c__ * bli_cimag(cy[i__3])), q__2 );
	i__4 = i__;
	bli_csets( (*s * bli_creal(cx[i__4])), (*s * bli_cimag(cx[i__4])), q__3 );
	bli_csets( (bli_creal(q__2) - bli_creal(q__3)), (bli_cimag(q__2) - bli_cimag(q__3)), q__1 );
	bli_csets( (bli_creal(q__1)), (bli_cimag(q__1)), cy[i__2] );
	i__2 = i__;
	bli_csets( (bli_creal(ctemp)), (bli_cimag(ctemp)), cx[i__2] );
/* L30: */
    }
    return 0;
} /* csrot_ */

/* zdrot.f -- translated by f2c (version 19991025).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/

/* Subroutine */ int PASTEF77(zd,rot)(const bla_integer *n, bla_dcomplex *zx, const bla_integer *incx, bla_dcomplex *zy, const bla_integer *incy, const bla_double *c__, const bla_double *s)
{
    /* System generated locals */
    bla_integer i__1, i__2, i__3, i__4;
    bla_dcomplex z__1, z__2, z__3;

    /* Local variables */
    bla_integer i__;
    bla_dcomplex ztemp;
    bla_integer ix, iy;


/*     applies a plane rotation, where the cos and sin (c and s) are */
/*     double precision and the vectors zx and zy are double complex. */
/*     jack dongarra, linpack, 3/11/78. */


    /* Parameter adjustments */
    --zy;
    --zx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	goto L20;
    }

/*       code for unequal increments or equal increments not equal */
/*         to 1 */

    ix = 1;
    iy = 1;
    if (*incx < 0) {
	ix = (-(*n) + 1) * *incx + 1;
    }
    if (*incy < 0) {
	iy = (-(*n) + 1) * *incy + 1;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = ix;
	bli_zsets( (*c__ * bli_zreal(zx[i__2])), (*c__ * bli_zimag(zx[i__2])), z__2 );
	i__3 = iy;
	bli_zsets( (*s * bli_zreal(zy[i__3])), (*s * bli_zimag(zy[i__3])), z__3 );
	bli_zsets( (bli_zreal(z__2) + bli_zreal(z__3)), (bli_zimag(z__2) + bli_zimag(z__3)), z__1 );
	bli_zsets( (bli_zreal(z__1)), (bli_zimag(z__1)), ztemp );
	i__2 = iy;
	i__3 = iy;
	bli_zsets( (*c__ * bli_zreal(zy[i__3])), (*c__ * bli_zimag(zy[i__3])), z__2 );
	i__4 = ix;
	bli_zsets( (*s * bli_zreal(zx[i__4])), (*s * bli_zimag(zx[i__4])), z__3 );
	bli_zsets( (bli_zreal(z__2) - bli_zreal(z__3)), (bli_zimag(z__2) - bli_zimag(z__3)), z__1 );
	bli_zsets( (bli_zreal(z__1)), (bli_zimag(z__1)), zy[i__2] );
	i__2 = ix;
	bli_zsets( (bli_zreal(ztemp)), (bli_zimag(ztemp)), zx[i__2] );
	ix += *incx;
	iy += *incy;
/* L10: */
    }
    return 0;

/*       code for both increments equal to 1 */

L20:
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__;
	bli_zsets( (*c__ * bli_zreal(zx[i__2])), (*c__ * bli_zimag(zx[i__2])), z__2 );
	i__3 = i__;
	bli_zsets( (*s * bli_zreal(zy[i__3])), (*s * bli_zimag(zy[i__3])), z__3 );
	bli_zsets( (bli_zreal(z__2) + bli_zreal(z__3)), (bli_zimag(z__2) + bli_zimag(z__3)), z__1 );
	bli_zsets( (bli_zreal(z__1)), (bli_zimag(z__1)), ztemp );
	i__2 = i__;
	i__3 = i__;
	bli_zsets( (*c__ * bli_zreal(zy[i__3])), (*c__ * bli_zimag(zy[i__3])), z__2 );
	i__4 = i__;
	bli_zsets( (*s * bli_zreal(zx[i__4])), (*s * bli_zimag(zx[i__4])), z__3 );
	bli_zsets( (bli_zreal(z__2) - bli_zreal(z__3)), (bli_zimag(z__2) - bli_zimag(z__3)), z__1 );
	bli_zsets( (bli_zreal(z__1)), (bli_zimag(z__1)), zy[i__2] );
	i__2 = i__;
	bli_zsets( (bli_zreal(ztemp)), (bli_zimag(ztemp)), zx[i__2] );
/* L30: */
    }
    return 0;
} /* zdrot_ */

#endif

