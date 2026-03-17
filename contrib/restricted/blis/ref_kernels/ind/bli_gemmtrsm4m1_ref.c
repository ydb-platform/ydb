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
	const dim_t       mr          = bli_cntx_get_blksz_def_dt( dt_r, BLIS_MR, cntx ); \
	const dim_t       nr          = bli_cntx_get_blksz_def_dt( dt_r, BLIS_NR, cntx ); \
\
	const dim_t       packnr      = bli_cntx_get_blksz_max_dt( dt_r, BLIS_NR, cntx ); \
\
	const dim_t       m           = mr; \
	const dim_t       n           = nr; \
\
	const inc_t       is_a        = bli_auxinfo_is_a( data ); \
	const inc_t       is_b        = bli_auxinfo_is_b( data ); \
\
	ctype_r* restrict a1x_r       = ( ctype_r* )a1x; \
	ctype_r* restrict a1x_i       = ( ctype_r* )a1x + is_a; \
\
	ctype_r* restrict bx1_r       = ( ctype_r* )bx1; \
	ctype_r* restrict bx1_i       = ( ctype_r* )bx1 + is_b; \
\
	ctype_r* restrict b11_r       = ( ctype_r* )b11; \
	ctype_r* restrict b11_i       = ( ctype_r* )b11 + is_b; \
\
	const inc_t       rs_b        = packnr; \
	const inc_t       cs_b        = 1; \
\
	ctype_r* restrict one_r       = PASTEMAC(chr,1); \
	ctype_r* restrict minus_one_r = PASTEMAC(chr,m1); \
\
	/* A hack to avoid a 'restrict' warning triggered by passing in the
	   same address (one_r) for both alpha and beta when calling the last
	   of the four matrix products. We now use one_r for alpha and this
	   new local variable, onel, for beta. (See issue #328.) */ \
	ctype_r           onel; \
	ctype_r* restrict onel_r      = &onel; \
	PASTEMAC(chr,set1s)( onel ); \
\
	ctype_r           alpha_r     = PASTEMAC(ch,real)( *alpha ); \
	ctype_r           alpha_i     = PASTEMAC(ch,imag)( *alpha ); \
\
	void*             a_next      = bli_auxinfo_next_a( data ); \
	void*             b_next      = bli_auxinfo_next_b( data ); \
\
	dim_t             i, j; \
\
/*
printf( "gemmtrsm4m1_l_ukr: is_a = %lu  is_b = %lu\n", is_a, is_b ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: a1x11p_r", m, k+m, \
                       a1x_r, 1, PASTEMAC(chr,packmr), "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: a1x11p_i", m, k+m, \
                       a1x_i, 1, PASTEMAC(chr,packmr), "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_r", k+m, n, \
                       bx1_r, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_i", k+m, n, \
                       bx1_i, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
*/ \
\
	/* Copy the contents of c to a temporary buffer ct. */ \
	if ( !PASTEMAC(chr,eq0)( alpha_i ) ) \
	{ \
		/* We can handle a non-zero imaginary component on alpha, but to do
		   so we have to manually scale b and then use alpha == 1 for the
		   micro-kernel calls. */ \
		for ( i = 0; i < m; ++i ) \
		for ( j = 0; j < n; ++j ) \
		PASTEMAC(ch,scalris)( alpha_r, \
		                      alpha_i, \
		                      *(b11_r + i*rs_b + j*cs_b), \
		                      *(b11_i + i*rs_b + j*cs_b) ); \
\
		/* Use alpha.r == 1.0. */ \
		alpha_r = *one_r; \
	} \
\
\
	/* lower: b11.r = alpha.r * b11.r - ( a10.r * b01.r - a10.i * b01.i );
	          b11.i = alpha.r * b11.i - ( a10.r * b01.i + a10.i * b01.r );

	   upper: b11.r = alpha.r * b11.r - ( a12.r * b21.r - a12.i * b21.i );
	          b11.i = alpha.r * b11.i - ( a12.r * b21.i + a12.i * b21.r ); */ \
\
	bli_auxinfo_set_next_ab( a1x_r, bx1_i, data ); \
\
	/* lower: b11.r = alpha.r * b11.r - a10.r * b01.r;
	   upper: b11.r = alpha.r * b11.r - a12.r * b21.r; */ \
	rgemm_ukr \
	( \
	  k, \
	  minus_one_r, \
	  a1x_r, \
	  bx1_r, \
	  &alpha_r, \
	  b11_r, rs_b, cs_b, \
	  data, \
	  cntx  \
	); \
\
	bli_auxinfo_set_next_ab( a1x_i, bx1_r, data ); \
\
	/* lower: b11.i = alpha.r * b11.i - a10.r * b01.i;
	   upper: b11.i = alpha.r * b11.i - a12.r * b21.i; */ \
	rgemm_ukr \
	( \
	  k, \
	  minus_one_r, \
	  a1x_r, \
	  bx1_i, \
	  &alpha_r, \
	  b11_i, rs_b, cs_b, \
	  data, \
	  cntx  \
	); \
\
	bli_auxinfo_set_next_ab( a1x_i, bx1_i, data ); \
\
	/* lower: b11.i =     1.0 * b11.i - a10.i * b01.r;
	   upper: b11.i =     1.0 * b11.i - a12.i * b21.r; */ \
	rgemm_ukr \
	( \
	  k, \
	  minus_one_r, \
	  a1x_i, \
	  bx1_r, \
	  one_r, \
	  b11_i, rs_b, cs_b, \
	  data, \
	  cntx  \
	); \
\
	bli_auxinfo_set_next_ab( a_next, b_next, data ); \
\
	/* lower: b11.r =     1.0 * b11.r + a10.i * b01.i;
	   upper: b11.r =     1.0 * b11.r + a12.i * b21.i; */ \
	rgemm_ukr \
	( \
	  k, \
	  one_r, \
	  a1x_i, \
	  bx1_i, \
	  onel_r, \
	  b11_r, rs_b, cs_b, \
	  data, \
	  cntx  \
	); \
/*
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_r post-gemm", k+m, n, \
                       bx1_r, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_i post-gemm", k+m, n, \
                       bx1_i, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
*/ \
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
\
/*
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_r after", k+m, n, \
                       bx1_r, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
PASTEMAC(chr,fprintm)( stdout, "gemmtrsm4m1_l_ukr: bx111p_i after", k+m, n, \
                       bx1_i, PASTEMAC(chr,packnr), 1, "%4.1f", "" ); \
*/ \
}

INSERT_GENTFUNCCO_BASIC3( gemmtrsm4m1_l, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, BLIS_TRSM_L_UKR )
INSERT_GENTFUNCCO_BASIC3( gemmtrsm4m1_u, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX, BLIS_TRSM_U_UKR )
