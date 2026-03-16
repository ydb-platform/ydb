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
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, arch, suf, trsmkerid ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a1x, \
       ctype*     restrict a11, \
       ctype*     restrict bx1, \
       ctype*     restrict b11, \
       ctype*     restrict c11, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	const num_t       dt          = PASTEMAC(ch,type); \
	const num_t       dt_r        = PASTEMAC(chr,type); \
\
	PASTECH(chr,gemm_ukr_ft) \
	                  rgemm_ukr   = bli_cntx_get_l3_nat_ukr_dt( dt_r, BLIS_GEMM_UKR, cntx ); \
\
	PASTECH(ch,trsm_ukr_ft) \
	                ctrsm_vir_ukr = bli_cntx_get_l3_vir_ukr_dt( dt, trsmkerid, cntx ); \
\
	const bool        col_pref    = bli_cntx_l3_nat_ukr_prefers_cols_dt( dt_r, BLIS_GEMM_UKR, cntx ); \
\
	const dim_t       mr          = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
	const dim_t       nr          = bli_cntx_get_blksz_def_dt( dt, BLIS_NR, cntx ); \
\
	const dim_t       mr_r        = bli_cntx_get_blksz_def_dt( dt_r, BLIS_MR, cntx ); \
	const dim_t       nr_r        = bli_cntx_get_blksz_def_dt( dt_r, BLIS_NR, cntx ); \
\
	ctype             bt[ BLIS_STACK_BUF_MAX_SIZE \
	                      / sizeof( ctype ) ] \
	                      __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	inc_t             rs_bt; \
	inc_t             cs_bt; \
\
	inc_t             rs_bt_r; \
	inc_t             cs_bt_r; \
\
	const dim_t       packnr      = bli_cntx_get_blksz_max_dt( dt, BLIS_NR, cntx ); \
\
	const pack_t      schema_b    = bli_cntx_schema_b_panel( cntx ); \
\
	const dim_t       k2          = 2 * k; \
\
	ctype_r* restrict a1x_r       = ( ctype_r* )a1x; \
\
	ctype_r* restrict bx1_r       = ( ctype_r* )bx1; \
\
	const inc_t       rs_b        = packnr; \
	const inc_t       cs_b        = 1; \
\
	ctype_r* restrict zero_r      = PASTEMAC(chr,0); \
	ctype_r* restrict minus_one_r = PASTEMAC(chr,m1); \
\
	const ctype_r     alpha_r     = PASTEMAC(ch,real)( *alpha ); \
	const ctype_r     alpha_i     = PASTEMAC(ch,imag)( *alpha ); \
\
	ctype_r*          b_use; \
	inc_t             rs_b_use; \
	inc_t             cs_b_use; \
\
\
	/* Handle alphas with non-zero imaginary components. */ \
	/* NOTE: This branch should never execute because alphas with
	   non-zero imaginary components should be applied during
	   packing, and so the only alphas we should see here are
	   those exclusively in the real domain, either because the
	   value originally had no imaginary compoent (e.g. 4.0) or
	   because a 1.0 was sent in as a placeholder since the alpha
	   was applied during packing. */ \
	if ( 0 ) \
	if ( !PASTEMAC(chr,eq0)( alpha_i ) ) \
	{ \
		bli_abort(); \
\
/*
		ctype_r* restrict one_r = PASTEMAC(chr,1); \
\
		const inc_t ld_b = rs_b; \
\
		PASTEMAC(ch,scal1ms_mxn)( schema_b, \
		                          mr, \
		                          nr, \
		                          alpha, \
		                          b11, rs_b, cs_b, ld_b ); \
\
		alpha_r = *one_r; \
*/ \
	} \
\
\
	{ \
		/* Set the strides for the temporary bt matrix based on the native
		   real domain micro-kernel storage preferences. */ \
		if ( col_pref ) { rs_bt   = 1;    cs_bt   = mr;     \
		                  rs_bt_r = 1;    cs_bt_r = mr_r; } \
		else            { rs_bt   = nr;   cs_bt   = 1;      \
		                  rs_bt_r = nr_r; cs_bt_r = 1;    } \
\
		b_use    = ( ctype_r* )bt; \
		rs_b_use = rs_bt_r; \
		cs_b_use = cs_bt_r; \
	} \
\
\
	/* Since b11 is stored in the 1e or 1r schema, we cannot update it
	   directly, and instead must compute the matrix product in a local
	   temporary microtile and then accumulate it into b11 according to
	   its schema. */ \
\
\
	/* lower: bt = -1.0 * a10 * b01;
	   upper: bt = -1.0 * a12 * b21; */ \
	rgemm_ukr \
	( \
	  k2, \
	  minus_one_r, \
	  a1x_r, \
	  bx1_r, \
	  zero_r, \
	  b_use, rs_b_use, cs_b_use, \
	  data, \
	  cntx  \
	); \
\
\
	if ( bli_is_1e_packed( schema_b ) ) \
	{ \
		const inc_t     ld_b = rs_b; \
\
		ctype* restrict b11_ri = ( ctype* )b11; \
		ctype* restrict b11_ir = ( ctype* )b11 + ld_b/2; \
\
		dim_t i, j; \
\
		/* b11 = alpha * b11 + bt; */ \
		for ( j = 0; j < nr; ++j ) \
		for ( i = 0; i < mr; ++i ) \
		{ \
			ctype*   restrict beta11t   = bt     + i*rs_bt + j*cs_bt; \
			ctype_r* restrict beta11t_r = &PASTEMAC(ch,real)( *beta11t ); \
			ctype_r* restrict beta11t_i = &PASTEMAC(ch,imag)( *beta11t ); \
			ctype*   restrict beta11_ri = b11_ri + i*rs_b  + j*cs_b; \
			ctype_r* restrict beta11_r  = &PASTEMAC(ch,real)( *beta11_ri ); \
			ctype_r* restrict beta11_i  = &PASTEMAC(ch,imag)( *beta11_ri ); \
			ctype*   restrict beta11_ir = b11_ir + i*rs_b  + j*cs_b; \
\
			PASTEMAC3(ch,chr,ch,xpbyris) \
			( \
			  *beta11t_r, \
			  *beta11t_i, \
			  alpha_r, \
			  alpha_i, /* alpha_i not referenced */ \
			  *beta11_r, \
			  *beta11_i  \
			); \
\
			PASTEMAC(ch,sets)( -*beta11_i, \
			                    *beta11_r, *beta11_ir ); \
		} \
	} \
	else /* if ( bli_is_1r_packed( schema_b ) ) */ \
	{ \
		const inc_t       ld_b  =     rs_b; \
		const inc_t       rs_b2 = 2 * rs_b; \
		const inc_t       cs_b2 =     cs_b; \
\
		ctype_r* restrict b11_r = ( ctype_r* )b11; \
		ctype_r* restrict b11_i = ( ctype_r* )b11 + ld_b; \
\
		dim_t i, j; \
\
		/* b11 = alpha * b11 + bt; */ \
		for ( j = 0; j < nr; ++j ) \
		for ( i = 0; i < mr; ++i ) \
		{ \
			ctype*   restrict beta11t   = bt    + i*rs_bt + j*cs_bt; \
			ctype_r* restrict beta11t_r = &PASTEMAC(ch,real)( *beta11t ); \
			ctype_r* restrict beta11t_i = &PASTEMAC(ch,imag)( *beta11t ); \
			ctype_r* restrict beta11_r  = b11_r + i*rs_b2 + j*cs_b2; \
			ctype_r* restrict beta11_i  = b11_i + i*rs_b2 + j*cs_b2; \
\
			PASTEMAC3(ch,chr,ch,xpbyris) \
			( \
			  *beta11t_r, \
			  *beta11t_i, \
			  alpha_r, \
			  alpha_i, /* alpha_i not referenced */ \
			  *beta11_r, \
			  *beta11_i  \
			); \
		} \
	} \
\
\
	/* b11 = inv(a11) * b11;
	   c11 = b11; */ \
	ctrsm_vir_ukr \
	( \
	  a11, \
	  b11, \
	  c11, rs_c, cs_c, \
	  data, \
	  cntx  \
	); \
}

INSERT_GENTFUNCCO_BASIC3( gemmtrsm1m_l, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, BLIS_TRSM_L_UKR )
INSERT_GENTFUNCCO_BASIC3( gemmtrsm1m_u, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, BLIS_TRSM_U_UKR )
