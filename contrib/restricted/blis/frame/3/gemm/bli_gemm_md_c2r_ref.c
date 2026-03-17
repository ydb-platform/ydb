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

#ifdef BLIS_ENABLE_GEMM_MD

#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, suf ) \
\
void PASTEMAC2(ch,opname,suf) \
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
	const num_t       dt        = PASTEMAC(ch,type); \
	const num_t       dt_r      = PASTEMAC(chr,type); \
\
	PASTECH(chr,gemm_ukr_ft) \
	                  rgemm_ukr = bli_cntx_get_l3_nat_ukr_dt( dt_r, BLIS_GEMM_UKR, cntx ); \
	const bool        col_pref  = bli_cntx_l3_nat_ukr_prefers_cols_dt( dt_r, BLIS_GEMM_UKR, cntx ); \
	const bool        row_pref  = !col_pref; \
\
	const dim_t       mr        = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
	const dim_t       nr        = bli_cntx_get_blksz_def_dt( dt, BLIS_NR, cntx ); \
\
	ctype             ct[ BLIS_STACK_BUF_MAX_SIZE \
	                      / sizeof( ctype_r ) ] \
	                      __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	inc_t             rs_ct; \
	inc_t             cs_ct; \
\
	ctype_r* restrict a_r       = ( ctype_r* )a; \
\
	ctype_r* restrict b_r       = ( ctype_r* )b; \
\
	ctype_r* restrict zero_r    = PASTEMAC(chr,0); \
\
	ctype_r* restrict alpha_r   = &PASTEMAC(ch,real)( *alpha ); \
/*
	ctype_r* restrict alpha_i   = &PASTEMAC(ch,imag)( *alpha ); \
*/ \
\
	ctype_r* restrict beta_r    = &PASTEMAC(ch,real)( *beta ); \
	ctype_r* restrict beta_i    = &PASTEMAC(ch,imag)( *beta ); \
\
	ctype_r*          c_use; \
	inc_t             rs_c_use; \
	inc_t             cs_c_use; \
\
	bool              using_ct; \
\
	/* This virtual microkernel is used by ccr and crc mixed-domain cases
	   when any of the following conditions are met:
	   - beta is complex (ie: has a non-zero imaginary component)
	   - C is general-stored
	   - the computation precision differs from the storage of C
	   If, however, none of the above conditions are met, then the real
	   domain macrokernel can be (and will be) called instead of calling
	   the complex macrokernel (and this virtual microkernel). */ \
\
/*
PASTEMAC(chr,fprintm)( stdout, "gemm_ukr: a", mr, k, \
                       a_r, 1, mr, "%5.2f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemm_ukr: b", k, nr, \
                       b_r, nr, 1, "%5.2f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemm_ukr: c before", mr, nr, \
                       c_use, rs_c_use, cs_c_use, "%5.2f", "" ); \
*/ \
\
	/* SAFETY CHECK: The higher level implementation should never
	   allow an alpha with non-zero imaginary component to be passed
	   in, because it can't be applied properly using the 1m method.
	   If alpha is not real, then something is very wrong. */ \
/*
	if ( !PASTEMAC(chr,eq0)( *alpha_i ) ) \
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
*/ \
\
	/* If beta has a non-zero imaginary component OR if c is stored with
	   general stride, then we compute the alpha*a*b product into temporary
	   storage and then accumulate that result into c afterwards. Note that
	   the other two cases concerning disagreement between the storage of C
	   and the output preference of the micro-kernel, should ONLY occur in
	   the context of trsm, whereby this virtual micro-kernel is called
	   directly from the trsm macro-kernel to update the micro-tile b11
	   that exists within the packed row-panel of B. Indeed that is the
	   reason those cases MUST be explicitly handled. */ \
	if      ( !PASTEMAC(chr,eq0)( *beta_i ) )               using_ct = TRUE; \
	else if ( bli_is_col_stored( rs_c, cs_c ) && row_pref ) using_ct = TRUE; \
	else if ( bli_is_row_stored( rs_c, cs_c ) && col_pref ) using_ct = TRUE; \
	else if ( bli_is_gen_stored( rs_c, cs_c ) )             using_ct = TRUE; \
	else                                                    using_ct = FALSE; \
\
\
	if ( using_ct ) \
	{ \
		/* In the atypical cases, we compute the result into temporary
		   workspace ct and then accumulate it back to c at the end. */ \
\
		/* Set the strides of ct based on the preference of the underlying
		   native real domain gemm micro-kernel. Note that we set the ct
		   strides in units of complex elements. */ \
		if ( col_pref ) { rs_ct = 1;  cs_ct = mr; } \
		else            { rs_ct = nr; cs_ct = 1; } \
\
		c_use    = ( ctype_r* )ct; \
		rs_c_use = rs_ct; \
		cs_c_use = cs_ct; \
\
		/* Convert the strides from being in units of complex elements to
		   be in units of real elements. Note that we don't need to check for
		   general storage here because that case corresponds to the scenario
		   where we are using the ct buffer and its rs_ct/cs_ct strides. */ \
		if ( bli_is_col_stored( rs_c_use, cs_c_use ) ) cs_c_use *= 2; \
		else                                           rs_c_use *= 2; \
\
\
		/* c = beta * c + alpha_r * a * b; */ \
		rgemm_ukr \
		( \
		  k, \
		  alpha_r, \
		  a_r, \
		  b_r, \
		  zero_r, \
		  c_use, rs_c_use, cs_c_use, \
		  data, \
		  cntx  \
		); \
\
		dim_t i, j; \
\
		/* Accumulate the final result in ct back to c. */ \
		if ( PASTEMAC(ch,eq1)( *beta ) ) \
		{ \
			for ( j = 0; j < nr; ++j ) \
			for ( i = 0; i < mr; ++i ) \
			{ \
				PASTEMAC(ch,adds)( *(ct + i*rs_ct + j*cs_ct), \
				                   *(c  + i*rs_c  + j*cs_c ) ); \
			} \
		} \
		else if ( PASTEMAC(ch,eq0)( *beta ) ) \
		{ \
			for ( j = 0; j < nr; ++j ) \
			for ( i = 0; i < mr; ++i ) \
			{ \
				PASTEMAC(ch,copys)( *(ct + i*rs_ct + j*cs_ct), \
				                    *(c  + i*rs_c  + j*cs_c ) ); \
			} \
		} \
		else \
		{ \
			for ( j = 0; j < nr; ++j ) \
			for ( i = 0; i < mr; ++i ) \
			{ \
				PASTEMAC(ch,xpbys)( *(ct + i*rs_ct + j*cs_ct), \
				                    *beta, \
				                    *(c  + i*rs_c  + j*cs_c ) ); \
			} \
		} \
	} \
	else \
	{ \
		/* In the typical cases, we use the real part of beta and
		   accumulate directly into the output matrix c. */ \
\
		c_use    = ( ctype_r* )c; \
		rs_c_use = rs_c; \
		cs_c_use = cs_c; \
\
		/* Convert the strides from being in units of complex elements to
		   be in units of real elements. Note that we don't need to check for
		   general storage here because that case corresponds to the scenario
		   where we are using the ct buffer and its rs_ct/cs_ct strides. */ \
		if ( bli_is_col_stored( rs_c_use, cs_c_use ) ) cs_c_use *= 2; \
		else                                           rs_c_use *= 2; \
\
		/* c = beta * c + alpha_r * a * b; */ \
		rgemm_ukr \
		( \
		  k, \
		  alpha_r, \
		  a_r, \
		  b_r, \
		  beta_r, \
		  c_use, rs_c_use, cs_c_use, \
		  data, \
		  cntx  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC( gemm_md_c2r, BLIS_REF_SUFFIX )

#endif
