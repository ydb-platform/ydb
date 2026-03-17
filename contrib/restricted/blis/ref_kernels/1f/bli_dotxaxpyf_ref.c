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
       conj_t           conja, \
       conj_t           conjw, \
       conj_t           conjx, \
       dim_t            m, \
       dim_t            b_n, \
       ctype*  restrict alpha, \
       ctype*  restrict a, inc_t inca, inc_t lda, \
       ctype*  restrict w, inc_t incw, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict beta, \
       ctype*  restrict y, inc_t incy, \
       ctype*  restrict z, inc_t incz, \
       cntx_t* restrict cntx  \
     ) \
{ \
	/* A is m x n.                   */ \
	/* y = beta * y + alpha * A^T w; */ \
	/* z =        z + alpha * A   x; */ \
\
	if ( 1 && inca == 1 && incw == 1 && incx == 1 && \
	     incy == 1 && incz == 1 && b_n == ff ) \
	{ \
		ctype r[ ff ]; \
		ctype ax[ ff ]; \
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
		/* Scale x by alpha, storing to a temporary array ax. */ \
		if ( bli_is_conj( conjx ) ) \
		{ \
			PRAGMA_SIMD \
			for ( dim_t i = 0; i < ff; ++i ) \
				PASTEMAC(ch,scal2js)( *alpha, x[i], ax[i] ); \
		} \
		else \
		{ \
			PRAGMA_SIMD \
			for ( dim_t i = 0; i < ff; ++i ) \
				PASTEMAC(ch,scal2s)( *alpha, x[i], ax[i] ); \
		} \
\
		/* If a must be conjugated, we do so indirectly by first toggling the
		   effective conjugation of w and then conjugating the resulting dot
		   products. */ \
		conj_t conjw_use = conjw; \
\
		if ( bli_is_conj( conjat ) ) \
			bli_toggle_conj( &conjw_use ); \
\
		if ( bli_is_noconj( conjw_use ) ) \
		{ \
			if ( bli_is_noconj( conja ) ) \
			{ \
				PRAGMA_SIMD \
				for ( dim_t p = 0; p < m; ++p ) \
				for ( dim_t i = 0; i < ff; ++i ) \
				{ \
					PASTEMAC(ch,axpys)( a[p + i*lda], w[p], r[i] ); \
					PASTEMAC(ch,axpys)( ax[i], a[p + i*lda], z[p] ); \
				} \
			} \
			else \
			{ \
				PRAGMA_SIMD \
				for ( dim_t p = 0; p < m; ++p ) \
				for ( dim_t i = 0; i < ff; ++i ) \
				{ \
					PASTEMAC(ch,axpys)( a[p + i*lda], w[p], r[i] ); \
					PASTEMAC(ch,axpyjs)( ax[i], a[p + i*lda], z[p] ); \
				} \
			} \
		} \
		else \
		{ \
			if ( bli_is_noconj( conja ) ) \
			{ \
				PRAGMA_SIMD \
				for ( dim_t p = 0; p < m; ++p ) \
				for ( dim_t i = 0; i < ff; ++i ) \
				{ \
					PASTEMAC(ch,axpyjs)( a[p + i*lda], w[p], r[i] ); \
					PASTEMAC(ch,axpys)( ax[i], a[p + i*lda], z[p] ); \
				} \
			} \
			else \
			{ \
				PRAGMA_SIMD \
				for ( dim_t p = 0; p < m; ++p ) \
				for ( dim_t i = 0; i < ff; ++i ) \
				{ \
					PASTEMAC(ch,axpyjs)( a[p + i*lda], w[p], r[i] ); \
					PASTEMAC(ch,axpyjs)( ax[i], a[p + i*lda], z[p] ); \
				} \
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
		PASTECH(ch,dotxf_ker_ft) kfp_df \
		= \
		bli_cntx_get_l1f_ker_dt( dt, BLIS_DOTXF_KER, cntx ); \
		PASTECH(ch,axpyf_ker_ft) kfp_af \
		= \
		bli_cntx_get_l1f_ker_dt( dt, BLIS_AXPYF_KER, cntx ); \
\
		kfp_df \
		( \
		  conjat, \
		  conjw, \
		  m, \
		  b_n, \
		  alpha, \
		  a, inca, lda, \
		  w, incw, \
		  beta, \
		  y, incy, \
		  cntx  \
		); \
\
		kfp_af \
		( \
		  conja, \
		  conjx, \
		  m, \
		  b_n, \
		  alpha, \
		  a, inca, lda, \
		  x, incx, \
		  z, incz, \
		  cntx  \
		); \
	} \
}

//INSERT_GENTFUNC_BASIC2( dotxaxpyf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )
GENTFUNC( float,    s, dotxaxpyf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 4 )
GENTFUNC( double,   d, dotxaxpyf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 4 )
GENTFUNC( scomplex, c, dotxaxpyf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 4 )
GENTFUNC( dcomplex, z, dotxaxpyf, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, 4 )
