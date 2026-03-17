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

// An implementation that indexes through B with the assumption that all
// elements were broadcast (duplicated) by a factor of NP/NR.

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
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
	const num_t     dt     = PASTEMAC(ch,type); \
\
	const dim_t     mr     = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
	const dim_t     nr     = bli_cntx_get_blksz_def_dt( dt, BLIS_NR, cntx ); \
\
	const inc_t     packmr = bli_cntx_get_blksz_max_dt( dt, BLIS_MR, cntx ); \
	const inc_t     packnr = bli_cntx_get_blksz_max_dt( dt, BLIS_NR, cntx ); \
\
	const dim_t     m      = mr; \
	const dim_t     n      = nr; \
\
	const inc_t     cs_a   = packmr; \
\
	const inc_t     rs_b   = packnr; \
\
	/* Assume that the degree of duplication is equal to packnr / nr. */ \
	const inc_t     cs_b   = packnr / nr; \
\
	ctype           ab[ BLIS_STACK_BUF_MAX_SIZE \
	                    / sizeof( ctype ) ] \
	                    __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	const inc_t     rs_ab  = 1; \
	const inc_t     cs_ab  = mr; \
\
	dim_t           l, j, i; \
\
	ctype           ai; \
	ctype           bj; \
\
\
	/* Initialize the accumulator elements in ab to zero. */ \
	for ( i = 0; i < m * n; ++i ) \
	{ \
		PASTEMAC(ch,set0s)( *(ab + i) ); \
	} \
\
	/* Perform a series of k rank-1 updates into ab. */ \
	for ( l = 0; l < k; ++l ) \
	{ \
		ctype* restrict abij = ab; \
\
		/* In an optimized implementation, these two loops over MR and NR
		   are typically fully unrolled. */ \
		for ( j = 0; j < n; ++j ) \
		{ \
			bj = *(b + j*cs_b); \
\
			for ( i = 0; i < m; ++i ) \
			{ \
				ai = *(a + i); \
\
				PASTEMAC(ch,dots)( ai, bj, *abij ); \
\
				abij += rs_ab; \
			} \
		} \
\
		a += cs_a; \
		b += rs_b; \
	} \
\
	/* Scale the result in ab by alpha. */ \
	for ( i = 0; i < m * n; ++i ) \
	{ \
		PASTEMAC(ch,scals)( *alpha, *(ab + i) ); \
	} \
\
	/* If beta is zero, overwrite c with the scaled result in ab. Otherwise,
	   scale by beta and then add the scaled redult in ab. */ \
	if ( PASTEMAC(ch,eq0)( *beta ) ) \
	{ \
		PASTEMAC(ch,copys_mxn)( m, \
		                        n, \
		                        ab, rs_ab, cs_ab, \
		                        c,  rs_c,  cs_c ); \
	} \
	else \
	{ \
		PASTEMAC(ch,xpbys_mxn)( m, \
		                        n, \
		                        ab, rs_ab, cs_ab, \
		                        beta, \
		                        c,  rs_c,  cs_c ); \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmbb, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

