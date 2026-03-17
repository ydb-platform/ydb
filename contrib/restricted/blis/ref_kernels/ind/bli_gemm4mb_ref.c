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
	ctype_r           ct_r[ BLIS_STACK_BUF_MAX_SIZE \
	                        / sizeof( ctype_r ) ] \
	                        __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	ctype_r           ct_i[ BLIS_STACK_BUF_MAX_SIZE \
	                        / sizeof( ctype_r ) ] \
	                        __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	inc_t             rs_ct; \
	inc_t             cs_ct; \
\
	const inc_t       is_a      = bli_auxinfo_is_a( data ); \
	const inc_t       is_b      = bli_auxinfo_is_b( data ); \
\
	ctype_r* restrict a_r       = ( ctype_r* )a; \
	ctype_r* restrict a_i       = ( ctype_r* )a + is_a; \
\
	ctype_r* restrict b_r       = ( ctype_r* )b; \
	ctype_r* restrict b_i       = ( ctype_r* )b + is_b; \
\
	ctype_r* restrict zero_r    = PASTEMAC(chr,0); \
\
	ctype_r* restrict alpha_r   = &PASTEMAC(ch,real)( *alpha ); \
	ctype_r* restrict alpha_i   = &PASTEMAC(ch,imag)( *alpha ); \
\
	const ctype_r     beta_r    = PASTEMAC(ch,real)( *beta ); \
	const ctype_r     beta_i    = PASTEMAC(ch,imag)( *beta ); \
\
	ctype_r           m_alpha_r = -PASTEMAC(ch,real)( *alpha ); \
\
	const pack_t      schema_b  = bli_auxinfo_schema_b( data ); \
\
	void*             a_next    = bli_auxinfo_next_a( data ); \
	void*             b_next    = bli_auxinfo_next_b( data ); \
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
	   in, because it can't be applied properly using the 4mb method.
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
\
	if ( bli_is_ro_packed( schema_b ) ) \
	{ \
		/* The following gemm micro-kernel calls implement the first half of
		   the 4mb method (which uses b_r):

		     c    = beta * c;
		     c_r += a_r * b_r;
		     c_i += a_i * b_r;

		   NOTE: Scaling by alpha_r is not shown above, but is implemented
		   below. */ \
\
		bli_auxinfo_set_next_ab( a_i, b_r, data ); \
\
		rgemm_ukr \
		( \
		  k, \
		  alpha_r, \
		  a_r, \
		  b_r, \
		  zero_r, \
		  ct_r, rs_ct, cs_ct, \
		  data, \
		  cntx  \
		); \
\
		bli_auxinfo_set_next_ab( a_next, b_next, data ); \
\
		rgemm_ukr \
		( \
		  k, \
		  alpha_r, \
		  a_i, \
		  b_r, \
		  zero_r, \
		  ct_i, rs_ct, cs_ct, \
		  data, \
		  cntx  \
		); \
	} \
	else /* if ( bli_is_io_packed( schema_b ) ) */ \
	{ \
		/* The following gemm micro-kernel calls implement the second half of
		   the 4mb method (which uses b_i):

		     c_r += -a_i * b_i;
		     c_i +=  a_r * b_i;

		   NOTE: Scaling by alpha_r is not shown above, but is implemented
		   below. */ \
\
		bli_auxinfo_set_next_ab( a_i, b_i, data ); \
\
		rgemm_ukr \
		( \
		  k, \
		  alpha_r, \
		  a_r, \
		  b_i, \
		  zero_r, \
		  ct_i, rs_ct, cs_ct, \
		  data, \
		  cntx  \
		); \
\
		bli_auxinfo_set_next_ab( a_next, b_next, data ); \
\
		rgemm_ukr \
		( \
		  k, \
		  &m_alpha_r, \
		  a_i, \
		  b_i, \
		  zero_r, \
		  ct_r, rs_ct, cs_ct, \
		  data, \
		  cntx  \
		); \
	} \
\
\
\
	/* How we accumulate the intermediate matrix product stored in ct_r
	   and ct_i depends on (a) the schema of B, and (b) the value of
	   beta. */ \
	if ( bli_is_ro_packed( schema_b ) ) \
	{ \
		if ( !PASTEMAC(chr,eq0)( beta_i ) ) \
		{ \
			/* c = beta * c + ct; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c    + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(ch,xpbyris)( gamma11t_r, \
				                      gamma11t_i, \
				                      beta_r, \
				                      beta_i, \
				                      *gamma11_r, \
				                      *gamma11_i ); \
			} \
		} \
		else if ( PASTEMAC(chr,eq1)( beta_r ) ) \
		{ \
			/* c_r = c_r + ct_r; */ \
			/* c_i = c_i + ct_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,adds)( gamma11t_r, *gamma11_r ); \
				PASTEMAC(chr,adds)( gamma11t_i, *gamma11_i ); \
			} \
		} \
		else if ( !PASTEMAC(chr,eq0)( beta_r ) ) \
		{ \
			/* c_r = beta_r * c_r + ct_r; */ \
			/* c_i = beta_r * c_i + ct_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,xpbys)( gamma11t_r, beta_r, *gamma11_r ); \
				PASTEMAC(chr,xpbys)( gamma11t_i, beta_r, *gamma11_i ); \
			} \
		} \
		else /* if PASTEMAC(chr,eq0)( beta_r ) */ \
		{ \
			/* c_r = ct_r; */ \
			/* c_i = ct_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,copys)( gamma11t_r, *gamma11_r ); \
				PASTEMAC(chr,copys)( gamma11t_i, *gamma11_i ); \
			} \
		} \
	} \
	else /* if ( bli_is_io_packed( schema_b ) ) */ \
	{ \
		/* NOTE: If this branch executes, it means we are in the second
		   half of the 4mb computation in which we multiply the b_i
		   sub-panel by the entire block of A. Here, we know that beta
		   will either be equal to one (for interior cases within gemm
		   macro-kernel), or zero (for edge cases). */ \
\
		if ( PASTEMAC(chr,eq1)( beta_r ) ) \
		{ \
			/* c_r = c_r + ct_r; */ \
			/* c_i = c_i + ct_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,adds)( gamma11t_r, *gamma11_r ); \
				PASTEMAC(chr,adds)( gamma11t_i, *gamma11_i ); \
			} \
		} \
		else /* if PASTEMAC(chr,eq0)( beta_r ) */ \
		{ \
			/* c_r = ct_r; */ \
			/* c_i = ct_i; */ \
			for ( j = 0; j < n_iter; ++j ) \
			for ( i = 0; i < n_elem; ++i ) \
			{ \
				const ctype_r     gamma11t_r = *(ct_r + i*incct + j*ldct); \
				const ctype_r     gamma11t_i = *(ct_i + i*incct + j*ldct); \
				ctype*   restrict gamma11    =   c  + i*incc  + j*ldc  ; \
				ctype_r* restrict gamma11_r  = &PASTEMAC(ch,real)( *gamma11 ); \
				ctype_r* restrict gamma11_i  = &PASTEMAC(ch,imag)( *gamma11 ); \
\
				PASTEMAC(chr,copys)( gamma11t_r, *gamma11_r ); \
				PASTEMAC(chr,copys)( gamma11t_i, *gamma11_i ); \
			} \
		} \
	} \
\
/*PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: b1_r", k, n, b_r, n, 1, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: b1_i", k, n, b_i, n, 1, "%4.1f", "" );*/ \
/*PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: a1_r", m, k, a_r, 1, m, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: a1_i", m, k, a_i, 1, m, "%4.1f", "" );*/ \
/*PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: ct_r", 8, 6, ct_r, rs_ct, cs_ct, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemm4mb_ukr: ct_i", 8, 6, ct_i, rs_ct, cs_ct, "%4.1f", "" );*/ \
}

INSERT_GENTFUNCCO_BASIC2( gemm4mb, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

