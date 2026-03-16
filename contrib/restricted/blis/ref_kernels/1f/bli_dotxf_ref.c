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


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf, ff ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjat, \
       conj_t           conjx, \
       dim_t            m, \
       dim_t            b_n, \
       ctype*  restrict alpha, \
       ctype*  restrict a, inc_t inca, inc_t lda, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict beta, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ) \
{ \
	if ( inca == 1 && incx == 1 && incy == 1 && b_n == ff ) \
	{ \
		ctype r[ ff ]; \
\
		/* If beta is zero, clear y. Otherwise, scale by beta. */ \
		if ( PASTEMAC(ch,eq0)( *beta ) ) \
		{ \
			for ( dim_t i = 0; i < ff; ++i ) PASTEMAC(ch,set0s)( y[i] ); \
		} \
		else \
		{ \
			for ( dim_t i = 0; i < ff; ++i ) PASTEMAC(ch,scals)( *beta, y[i] ); \
		} \
\
		/* If the vectors are empty or if alpha is zero, return early. */ \
		if ( bli_zero_dim1( m ) || PASTEMAC(ch,eq0)( *alpha ) ) return; \
\
		/* Initialize r vector to 0. */ \
		for ( dim_t i = 0; i < ff; ++i ) PASTEMAC(ch,set0s)( r[i] ); \
\
		/* If a must be conjugated, we do so indirectly by first toggling the
		   effective conjugation of x and then conjugating the resulting dot
		   products. */ \
		conj_t conjx_use = conjx; \
\
		if ( bli_is_conj( conjat ) ) \
			bli_toggle_conj( &conjx_use ); \
\
		if ( bli_is_noconj( conjx_use ) ) \
		{ \
			PRAGMA_SIMD \
			for ( dim_t p = 0; p < m; ++p ) \
			for ( dim_t i = 0; i < ff; ++i ) \
			{ \
				PASTEMAC(ch,axpys)( a[p + i*lda], x[p], r[i] ); \
			} \
		} \
		else \
		{ \
			PRAGMA_SIMD \
			for ( dim_t p = 0; p < m; ++p ) \
			for ( dim_t i = 0; i < ff; ++i ) \
			{ \
				PASTEMAC(ch,axpyjs)( a[p + i*lda], x[p], r[i] ); \
			} \
		} \
\
		if ( bli_is_conj( conjat ) ) \
			for ( dim_t i = 0; i < ff; ++i ) PASTEMAC(ch,conjs)( r[i] ); \
\
		for ( dim_t i = 0; i < ff; ++i ) \
		{ \
			PASTEMAC(ch,axpys)( *alpha, r[i], y[i] ); \
		} \
	} \
	else \
	{ \
		/* Query the context for the kernel function pointer. */ \
		const num_t              dt     = PASTEMAC(ch,type); \
		PASTECH(ch,dotxv_ker_ft) kfp_dv \
		= \
		bli_cntx_get_l1v_ker_dt( dt, BLIS_DOTXV_KER, cntx ); \
\
		for ( dim_t i = 0; i < b_n; ++i ) \
		{ \
			ctype* restrict a1   = a + (0  )*inca + (i  )*lda; \
			ctype* restrict x1   = x + (0  )*incx; \
			ctype* restrict psi1 = y + (i  )*incy; \
\
			kfp_dv \
			( \
			  conjat, \
			  conjx, \
			  m, \
			  alpha, \
			  a1, inca, \
			  x1, incx, \
			  beta, \
			  psi1, \
			  cntx  \
			); \
		} \
	} \
}

//INSERT_GENTFUNC_BASIC2( dotxf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )
GENTFUNC( float,    s, dotxf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 6 )
GENTFUNC( double,   d, dotxf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 6 )
GENTFUNC( scomplex, c, dotxf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 6 )
GENTFUNC( dcomplex, z, dotxf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 6 )

