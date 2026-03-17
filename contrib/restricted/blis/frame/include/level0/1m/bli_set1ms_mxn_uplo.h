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

#ifndef BLIS_SET1MS_MXN_UPLO_H
#define BLIS_SET1MS_MXN_UPLO_H

// set1ms_mxn_uplo

#define bli_cset1ms_mxn_uplo( schema, diagoff, uplo, m, n, a, y, rs_y, cs_y, ld_y ) \
{ \
	doff_t diagoff_abs = bli_abs( diagoff ); \
	inc_t  offdiag_inc; \
	dim_t  i, j; \
\
	/* Handle 1e and 1r separately. */ \
	if ( bli_is_1e_packed( schema ) ) \
	{ \
		/* Set the off-diagonal increment. */ \
		if         ( diagoff > 0 )    offdiag_inc = cs_y; \
		else /* if ( diagoff < 0 ) */ offdiag_inc = rs_y; \
\
		scomplex* restrict y0   = y + (diagoff_abs  )*offdiag_inc; \
		scomplex* restrict y_ri = y0; \
		scomplex* restrict y_ir = y0 + ld_y/2; \
\
		if ( bli_is_lower( uplo ) ) \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = j; i < m; ++i ) \
			{ \
				bli_ccopy1es( *(a), \
				              *(y_ri + i*rs_y + j*cs_y), \
				              *(y_ir + i*rs_y + j*cs_y) ); \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = 0; i < j + 1; ++i ) \
			{ \
				bli_ccopy1es( *(a), \
				              *(y_ri + i*rs_y + j*cs_y), \
				              *(y_ir + i*rs_y + j*cs_y) ); \
			} \
		} \
	} \
	else /* if ( bli_is_1r_packed( schema ) ) */ \
	{ \
		inc_t rs_y2 = rs_y; \
		inc_t cs_y2 = cs_y; \
\
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */ \
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; } \
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; } \
\
		/* Set the off-diagonal increment. */ \
		if         ( diagoff > 0 )    offdiag_inc = cs_y2; \
		else /* if ( diagoff < 0 ) */ offdiag_inc = rs_y2; \
\
		float*    restrict y0  = ( float* )y + (diagoff_abs  )*offdiag_inc; \
		float*    restrict y_r = y0; \
		float*    restrict y_i = y0 + ld_y; \
\
		if ( bli_is_lower( uplo ) ) \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = j; i < m; ++i ) \
			{ \
				bli_ccopy1rs( *(a), \
				              *(y_r + i*rs_y2 + j*cs_y2), \
				              *(y_i + i*rs_y2 + j*cs_y2) ); \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = 0; i < j + 1; ++i ) \
			{ \
				bli_ccopy1rs( *(a), \
				              *(y_r + i*rs_y2 + j*cs_y2), \
				              *(y_i + i*rs_y2 + j*cs_y2) ); \
			} \
		} \
	} \
}

#define bli_zset1ms_mxn_uplo( schema, diagoff, uplo, m, n, a, y, rs_y, cs_y, ld_y ) \
{ \
	doff_t diagoff_abs = bli_abs( diagoff ); \
	inc_t  offdiag_inc; \
	dim_t  i, j; \
\
	/* Handle 1e and 1r separately. */ \
	if ( bli_is_1e_packed( schema ) ) \
	{ \
		/* Set the off-diagonal increment. */ \
		if         ( diagoff > 0 )    offdiag_inc = cs_y; \
		else /* if ( diagoff < 0 ) */ offdiag_inc = rs_y; \
\
		dcomplex* restrict y0   = y + (diagoff_abs  )*offdiag_inc; \
		dcomplex* restrict y_ri = y0; \
		dcomplex* restrict y_ir = y0 + ld_y/2; \
\
		if ( bli_is_lower( uplo ) ) \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = j; i < m; ++i ) \
			{ \
				bli_zcopy1es( *(a), \
				              *(y_ri + i*rs_y + j*cs_y), \
				              *(y_ir + i*rs_y + j*cs_y) ); \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = 0; i < j + 1; ++i ) \
			{ \
				bli_zcopy1es( *(a), \
				              *(y_ri + i*rs_y + j*cs_y), \
				              *(y_ir + i*rs_y + j*cs_y) ); \
			} \
		} \
	} \
	else /* if ( bli_is_1r_packed( schema ) ) */ \
	{ \
		inc_t rs_y2 = rs_y; \
		inc_t cs_y2 = cs_y; \
\
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */ \
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; } \
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; } \
\
		/* Set the off-diagonal increment. */ \
		if         ( diagoff > 0 )    offdiag_inc = cs_y2; \
		else /* if ( diagoff < 0 ) */ offdiag_inc = rs_y2; \
\
		double*   restrict y0  = ( double* )y + (diagoff_abs  )*offdiag_inc; \
		double*   restrict y_r = y0; \
		double*   restrict y_i = y0 + ld_y; \
\
		if ( bli_is_lower( uplo ) ) \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = j; i < m; ++i ) \
			{ \
				bli_zcopy1rs( *(a), \
				              *(y_r + i*rs_y2 + j*cs_y2), \
				              *(y_i + i*rs_y2 + j*cs_y2) ); \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			for ( j = 0; j < n; ++j ) \
			for ( i = 0; i < j + 1; ++i ) \
			{ \
				bli_zcopy1rs( *(a), \
				              *(y_r + i*rs_y2 + j*cs_y2), \
				              *(y_i + i*rs_y2 + j*cs_y2) ); \
			} \
		} \
	} \
}

#endif
