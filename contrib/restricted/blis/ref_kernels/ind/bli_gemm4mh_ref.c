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

#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, \
       ctype*     restrict b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	const num_t       dt_r      = PASTEMAC(chr,type); \
\
	PASTECH(chr,gemm_ukr_ft) \
	                  rgemm_ukr = bli_cntx_get_l3_nat_ukr_dt( dt_r, BLIS_GEMM_UKR, cntx ); \
\
	const dim_t       mr        = bli_cntx_get_blksz_def_dt( dt_r, BLIS_MR, cntx ); \
	const dim_t       nr        = bli_cntx_get_blksz_def_dt( dt_r, BLIS_NR, cntx ); \
\
	const dim_t       m         = mr; \
	const dim_t       n         = nr; \
\
	ctype_r           ct[ BLIS_STACK_BUF_MAX_SIZE \
	                      / sizeof( ctype_r ) ] \
	                      __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	inc_t             rs_ct; \
	inc_t             cs_ct; \
\
	ctype_r* restrict a_cast    = ( ctype_r* )a; \
\
	ctype_r* restrict b_cast    = ( ctype_r* )b; \
\
	ctype_r* restrict zero_r    = PASTEMAC(chr,0); \
\
	ctype_r* restrict alpha_r   = &PASTEMAC(ch,real)( *alpha ); \
	ctype_r* restrict alpha_i   = &PASTEMAC(ch,imag)( *alpha ); \
\
	const ctype_r     beta_r    = PASTEMAC(ch,real)( *beta ); \
	const ctype_r     beta_i    = PASTEMAC(ch,imag)( *beta ); \
\
	const pack_t      schema_a  = bli_auxinfo_schema_a( data ); \
	const pack_t      schema_b  = bli_auxinfo_schema_b( data ); \
\
	dim_t             n_iter; \
	dim_t             n_elem; \
\
	inc_t             incc, ldc; \
	inc_t             incct, ldct; \
\
	dim_t             i, j; \
\
\
	/* SAFETY CHECK: The higher level implementation should never
	   allow an alpha with non-zero imaginary component to be passed
	   in, because it can't be applied properly using the 4mh method.
	   If alpha is not real, then something is very wrong. */ \
	if ( !PASTEMAC(chr,eq0)( *alpha_i ) ) \
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
\
\
	/* An optimization: Set local strides and loop bounds based on the
	   strides of c, so that (a) the micro-kernel accesses ct the same
	   way it would if it were updating c directly, and (b) c is updated
	   contiguously. For c with general stride, we access ct the same way
	   we would as if it were column-stored. */ \
	if ( bli_is_row_stored( rs_c, cs_c ) ) \
	{ \
		rs_ct = n; n_iter = m; incc = cs_c; \
		cs_ct = 1; n_elem = n; ldc  = rs_c; \
	} \
	else /* column-stored or general stride */ \
	{ \
		rs_ct = 1; n_iter = n; incc = rs_c; \
		cs_ct = m; n_elem = m; ldc  = cs_c; \
	} \
	incct = 1; \
	ldct  = n_elem; \
\
\
	/* The following gemm micro-kernel call implement one "phase" of the
	   4m method:

	     c    = beta * c;
	     c_r += a_r * b_r - a_i * b_i;
	     c_i += a_r * b_i + a_i * b_r;

	   NOTE: Scaling by alpha_r is not shown above, but is implemented
	   below. */ \
\
\
	/* ct = alpha_r * a * b; */ \
	rgemm_ukr \
	( \
	  k, \
	  alpha_r, \
	  a_cast, \
	  b_cast, \
	  zero_r, \
	  ct, rs_ct, cs_ct, \
	  data, \
	  cntx  \
	); \
\
\
	/* How we accumulate the intermediate matrix product stored in ct
	   depends on (a) the schemas of A and B, and (b) the value of
	   beta. */ \
	if ( bli_is_ro_packed( schema_a ) && \
	     bli_is_ro_packed( schema_b ) ) \
	{ \
		if ( !PASTEMAC(chr,eq0)( beta_i ) ) \
		{ \
			/* c   = beta * c; 
			   c_r = c_r + ct; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
\
				PASTEMAC(ch,scals)( *beta, *gamma11 ); \
				PASTEMAC(chr,adds)(  gamma11t, *gamma11_r ); \
			} \
		} \
		else if ( PASTEMAC(chr,eq1)( beta_r ) ) \
		{ \
			/* c_r = c_r + ct;
			   c_i = c_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
\
				PASTEMAC(chr,adds)( gamma11t, *gamma11_r ); \
			} \
		} \
		else if ( !PASTEMAC(chr,eq0)( beta_r ) ) \
		{ \
			/* c_r = beta_r * c_r + ct;
			   c_i = beta_r * c_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,xpbys)(  gamma11t, beta_r, *gamma11_r ); \
				PASTEMAC(chr,scals)(            beta_r, *gamma11_i ); \
			} \
		} \
		else /* if PASTEMAC(chr,eq0)( beta_r ) */ \
		{ \
			/* c_r = ct;
			   c_i = 0; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,copys)( gamma11t, *gamma11_r ); \
				PASTEMAC(chr,set0s)(           *gamma11_i ); \
			} \
		} \
	} \
	else if ( ( bli_is_ro_packed( schema_a ) && \
	            bli_is_io_packed( schema_b ) ) || \
	          ( bli_is_io_packed( schema_a ) && \
	            bli_is_ro_packed( schema_b ) ) \
	        ) \
	{ \
		if ( PASTEMAC(chr,eq1)( beta_r ) ) \
		{ \
			/* c_r = c_r + 0;
			   c_i = c_i + ct; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_i = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,adds)( gamma11t, *gamma11_i ); \
			} \
		} \
		else /* if PASTEMAC(chr,eq0)( beta_r ) */ \
		{ \
			/* c_r = 0;
			   c_i = ct; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,set0s)(           *gamma11_r ); \
				PASTEMAC(chr,copys)( gamma11t, *gamma11_i ); \
			} \
		} \
	} \
	else /* if ( bli_is_io_packed( schema_a ) && \
	             bli_is_io_packed( schema_b ) ) */ \
	{ \
		if ( PASTEMAC(chr,eq1)( beta_r ) ) \
		{ \
			/* c_r = c_r - ct;
			   c_i = c_i + 0; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
\
				PASTEMAC(chr,subs)( gamma11t, *gamma11_r ); \
			} \
		} \
		else /* if PASTEMAC(chr,eq0)( beta_r ) */ \
		{ \
			/* c_r = -ct;
			   c_i = 0; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t  = *(ct + i*incct + j*ldct); \
				ctype*   restrict gamma11   =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,copys)( -gamma11t, *gamma11_r ); \
				PASTEMAC(chr,set0s)(            *gamma11_i ); \
			} \
		} \
	} \
}

INSERT_GENTFUNCCO_BASIC2( gemm4mh, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )
