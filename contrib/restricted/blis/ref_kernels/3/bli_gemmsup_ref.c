/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2019, Advanced Micro Devices, Inc.

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

//
// -- Row storage case ---------------------------------------------------------
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t              conja, \
       conj_t              conjb, \
       dim_t               m, \
       dim_t               n, \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, inc_t rs_a, inc_t cs_a, \
       ctype*     restrict b, inc_t rs_b, inc_t cs_b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	/* NOTE: This microkernel can actually handle arbitrarily large
       values of m, n, and k. */ \
\
	if ( bli_is_noconj( conja ) && bli_is_noconj( conjb ) ) \
	{ \
		/* Traverse c by rows. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			ctype* restrict ci = &c[ i*rs_c ]; \
			ctype* restrict ai = &a[ i*rs_a ]; \
\
			for ( dim_t j = 0; j < n; ++j ) \
			{ \
				ctype* restrict cij = &ci[ j*cs_c ]; \
				ctype* restrict bj  = &b [ j*cs_b ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dots)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_noconj( conja ) && bli_is_conj( conjb ) ) \
	{ \
		/* Traverse c by rows. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			ctype* restrict ci = &c[ i*rs_c ]; \
			ctype* restrict ai = &a[ i*rs_a ]; \
\
			for ( dim_t j = 0; j < n; ++j ) \
			{ \
				ctype* restrict cij = &ci[ j*cs_c ]; \
				ctype* restrict bj  = &b [ j*cs_b ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,axpyjs)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_conj( conja ) && bli_is_noconj( conjb ) ) \
	{ \
		/* Traverse c by rows. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			ctype* restrict ci = &c[ i*rs_c ]; \
			ctype* restrict ai = &a[ i*rs_a ]; \
\
			for ( dim_t j = 0; j < n; ++j ) \
			{ \
				ctype* restrict cij = &ci[ j*cs_c ]; \
				ctype* restrict bj  = &b [ j*cs_b ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dotjs)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_conj( conja ) && bli_is_conj( conjb ) ) */ \
	{ \
		/* Traverse c by rows. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			ctype* restrict ci = &c[ i*rs_c ]; \
			ctype* restrict ai = &a[ i*rs_a ]; \
\
			for ( dim_t j = 0; j < n; ++j ) \
			{ \
				ctype* restrict cij = &ci[ j*cs_c ]; \
				ctype* restrict bj  = &b [ j*cs_b ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dots)( *aij, *bij, ab ); \
				} \
\
				/* Conjugate the result to simulate conj(a^T) * conj(b). */ \
				PASTEMAC(ch,conjs)( ab ); \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmsup_r, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

//
// -- Column storage case ------------------------------------------------------
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t              conja, \
       conj_t              conjb, \
       dim_t               m, \
       dim_t               n, \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, inc_t rs_a, inc_t cs_a, \
       ctype*     restrict b, inc_t rs_b, inc_t cs_b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	/* NOTE: This microkernel can actually handle arbitrarily large
       values of m, n, and k. */ \
\
	if ( bli_is_noconj( conja ) && bli_is_noconj( conjb ) ) \
	{ \
		/* Traverse c by columns. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict cj = &c[ j*cs_c ]; \
			ctype* restrict bj = &b[ j*cs_b ]; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict cij = &cj[ i*rs_c ]; \
				ctype* restrict ai  = &a [ i*rs_a ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dots)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_noconj( conja ) && bli_is_conj( conjb ) ) \
	{ \
		/* Traverse c by columns. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict cj = &c[ j*cs_c ]; \
			ctype* restrict bj = &b[ j*cs_b ]; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict cij = &cj[ i*rs_c ]; \
				ctype* restrict ai  = &a [ i*rs_a ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,axpyjs)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_conj( conja ) && bli_is_noconj( conjb ) ) \
	{ \
		/* Traverse c by columns. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict cj = &c[ j*cs_c ]; \
			ctype* restrict bj = &b[ j*cs_b ]; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict cij = &cj[ i*rs_c ]; \
				ctype* restrict ai  = &a [ i*rs_a ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dotjs)( *aij, *bij, ab ); \
				} \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_conj( conja ) && bli_is_conj( conjb ) ) */ \
	{ \
		/* Traverse c by columns. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict cj = &c[ j*cs_c ]; \
			ctype* restrict bj = &b[ j*cs_b ]; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict cij = &cj[ i*rs_c ]; \
				ctype* restrict ai  = &a [ i*rs_a ]; \
				ctype           ab; \
\
				PASTEMAC(ch,set0s)( ab ); \
\
				/* Perform a dot product to update the (i,j) element of c. */ \
				for ( dim_t l = 0; l < k; ++l ) \
				{ \
					ctype* restrict aij = &ai[ l*cs_a ]; \
					ctype* restrict bij = &bj[ l*rs_b ]; \
\
					PASTEMAC(ch,dots)( *aij, *bij, ab ); \
				} \
\
				/* Conjugate the result to simulate conj(a^T) * conj(b). */ \
				PASTEMAC(ch,conjs)( ab ); \
\
				/* If beta is one, add ab into c. If beta is zero, overwrite c
				   with the result in ab. Otherwise, scale by beta and accumulate
				   ab to c. */ \
				if ( PASTEMAC(ch,eq1)( *beta ) ) \
				{ \
					PASTEMAC(ch,axpys)( *alpha, ab, *cij ); \
				} \
				else if ( PASTEMAC(ch,eq0)( *beta ) ) \
				{ \
					PASTEMAC(ch,scal2s)( *alpha, ab, *cij ); \
				} \
				else \
				{ \
					PASTEMAC(ch,axpbys)( *alpha, ab, *beta, *cij ); \
				} \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmsup_c, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

//
// -- General storage case -----------------------------------------------------
//

INSERT_GENTFUNC_BASIC2( gemmsup_g, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )








#if 0

//
// -- Row storage case ---------------------------------------------------------
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t              conja, \
       conj_t              conjb, \
       dim_t               m, \
       dim_t               n, \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, inc_t rs_a, inc_t cs_a, \
       ctype*     restrict b, inc_t rs_b, inc_t cs_b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	const dim_t     mn     = m * n; \
\
	ctype           ab[ BLIS_STACK_BUF_MAX_SIZE \
	                    / sizeof( ctype ) ] \
	                    __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	const inc_t     rs_ab  = n; \
	const inc_t     cs_ab  = 1; \
\
\
	/* Assumptions: m <= mr, n <= nr so that the temporary array ab is
	   sufficiently large enough to hold the m x n microtile.

	   The ability to handle m < mr and n < nr is being provided so that
	   optimized ukernels can call one of these reference implementations
	   for their edge cases, if they choose. When they do so, they will
	   need to call the function directly, by its configuration-mangled
	   name, since it will have been overwritten in the context when
	   the optimized ukernel functions are registered. */ \
\
\
	/* Initialize the accumulator elements in ab to zero. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,set0s)( ab[i] ); \
	} \
\
	/* Perform a series of k rank-1 updates into ab. */ \
	for ( dim_t l = 0; l < k; ++l ) \
	{ \
		/* Traverse ab by rows; assume cs_ab = 1. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			for ( dim_t j = 0; j < n; ++j ) \
			{ \
				PASTEMAC(ch,dots) \
				( \
				  a[ i*rs_a ], \
				  b[ j*cs_b ], \
				  ab[ i*rs_ab + j*cs_ab ]  \
				); \
			} \
		} \
\
		a += cs_a; \
		b += rs_b; \
	} \
\
	/* Scale the result in ab by alpha. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,scals)( *alpha, ab[i] ); \
	} \
\
\
	/* If beta is one, add ab into c. If beta is zero, overwrite c with the
	   result in ab. Otherwise, scale by beta and accumulate ab to c. */ \
	if ( PASTEMAC(ch,eq1)( *beta ) ) \
	{ \
		/* Traverse ab and c by rows; assume cs_a = cs_a = 1. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			PASTEMAC(ch,adds) \
			( \
			  ab[ i*rs_ab + j*1 ], \
			  c[  i*rs_c  + j*1 ]  \
			) \
		} \
	} \
	else if ( PASTEMAC(ch,eq0)( *beta ) ) \
	{ \
\
		/* Traverse ab and c by rows; assume cs_a = cs_a = 1. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			PASTEMAC(ch,copys) \
			( \
			  ab[ i*rs_ab + j*1 ], \
			  c[  i*rs_c  + j*1 ]  \
			) \
		} \
	} \
	else /* beta != 0 && beta != 1 */ \
	{ \
		/* Traverse ab and c by rows; assume cs_a = cs_a = 1. */ \
		for ( dim_t i = 0; i < m; ++i ) \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			PASTEMAC(ch,xpbys) \
			( \
			  ab[ i*rs_ab + j*1 ], \
			  *beta, \
			  c[  i*rs_c  + j*1 ]  \
			) \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmsup_r, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

//
// -- Column storage case ------------------------------------------------------
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t              conja, \
       conj_t              conjb, \
       dim_t               m, \
       dim_t               n, \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, inc_t rs_a, inc_t cs_a, \
       ctype*     restrict b, inc_t rs_b, inc_t cs_b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	const dim_t     mn     = m * n; \
\
	ctype           ab[ BLIS_STACK_BUF_MAX_SIZE \
	                    / sizeof( ctype ) ] \
	                    __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	const inc_t     rs_ab  = 1; \
	const inc_t     cs_ab  = m; \
\
\
	/* Assumptions: m <= mr, n <= nr so that the temporary array ab is
	   sufficiently large enough to hold the m x n microtile.

	   The ability to handle m < mr and n < nr is being provided so that
	   optimized ukernels can call one of these reference implementations
	   for their edge cases, if they choose. When they do so, they will
	   need to call the function directly, by its configuration-mangled
	   name, since it will have been overwritten in the context when
	   the optimized ukernel functions are registered. */ \
\
\
	/* Initialize the accumulator elements in ab to zero. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,set0s)( ab[i] ); \
	} \
\
	/* Perform a series of k rank-1 updates into ab. */ \
	for ( dim_t l = 0; l < k; ++l ) \
	{ \
		/* Traverse ab by columns; assume rs_ab = 1. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				PASTEMAC(ch,dots) \
				( \
				  a[ i*rs_a ], \
				  b[ j*cs_b ], \
				  ab[ i*rs_ab + j*cs_ab ]  \
				); \
			} \
		} \
\
		a += cs_a; \
		b += rs_b; \
	} \
\
	/* Scale the result in ab by alpha. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,scals)( *alpha, ab[i] ); \
	} \
\
\
	/* If beta is one, add ab into c. If beta is zero, overwrite c with the
	   result in ab. Otherwise, scale by beta and accumulate ab to c. */ \
	if ( PASTEMAC(ch,eq1)( *beta ) ) \
	{ \
		/* Traverse ab and c by columns; assume rs_a = rs_a = 1. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,adds) \
			( \
			  ab[ i*1 + j*cs_ab ], \
			  c[  i*1 + j*cs_c  ]  \
			) \
		} \
	} \
	else if ( PASTEMAC(ch,eq0)( *beta ) ) \
	{ \
		/* Traverse ab and c by columns; assume rs_a = rs_a = 1. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,copys) \
			( \
			  ab[ i*1 + j*cs_ab ], \
			  c[  i*1 + j*cs_c  ]  \
			) \
		} \
	} \
	else /* beta != 0 && beta != 1 */ \
	{ \
		/* Traverse ab and c by columns; assume rs_a = rs_a = 1. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,xpbys) \
			( \
			  ab[ i*1 + j*cs_ab ], \
			  *beta, \
			  c[  i*1 + j*cs_c  ]  \
			) \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmsup_c, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

//
// -- General storage case -----------------------------------------------------
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t              conja, \
       conj_t              conjb, \
       dim_t               m, \
       dim_t               n, \
       dim_t               k, \
       ctype*     restrict alpha, \
       ctype*     restrict a, inc_t rs_a, inc_t cs_a, \
       ctype*     restrict b, inc_t rs_b, inc_t cs_b, \
       ctype*     restrict beta, \
       ctype*     restrict c, inc_t rs_c, inc_t cs_c, \
       auxinfo_t* restrict data, \
       cntx_t*    restrict cntx  \
     ) \
{ \
	const dim_t     mn     = m * n; \
\
	ctype           ab[ BLIS_STACK_BUF_MAX_SIZE \
	                    / sizeof( ctype ) ] \
	                    __attribute__((aligned(BLIS_STACK_BUF_ALIGN_SIZE))); \
	const inc_t     rs_ab  = 1; \
	const inc_t     cs_ab  = m; \
\
\
	/* Assumptions: m <= mr, n <= nr so that the temporary array ab is
	   sufficiently large enough to hold the m x n microtile.

	   The ability to handle m < mr and n < nr is being provided so that
	   optimized ukernels can call one of these reference implementations
	   for their edge cases, if they choose. When they do so, they will
	   need to call the function directly, by its configuration-mangled
	   name, since it will have been overwritten in the context when
	   the optimized ukernel functions are registered. */ \
\
\
	/* Initialize the accumulator elements in ab to zero. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,set0s)( ab[i] ); \
	} \
\
	/* Perform a series of k rank-1 updates into ab. */ \
	for ( dim_t l = 0; l < k; ++l ) \
	{ \
		/* General storage: doesn't matter how we traverse ab. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				PASTEMAC(ch,dots) \
				( \
				  a[ i*rs_a ], \
				  b[ j*cs_b ], \
				  ab[ i*rs_ab + j*cs_ab ]  \
				); \
			} \
		} \
\
		a += cs_a; \
		b += rs_b; \
	} \
\
	/* Scale the result in ab by alpha. */ \
	for ( dim_t i = 0; i < mn; ++i ) \
	{ \
		PASTEMAC(ch,scals)( *alpha, ab[i] ); \
	} \
\
\
	/* If beta is one, add ab into c. If beta is zero, overwrite c with the
	   result in ab. Otherwise, scale by beta and accumulate ab to c. */ \
	if ( PASTEMAC(ch,eq1)( *beta ) ) \
	{ \
		/* General storage: doesn't matter how we traverse ab and c. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,adds) \
			( \
			  ab[ i*rs_ab + j*cs_ab ], \
			  c[  i*rs_c  + j*cs_c  ]  \
			) \
		} \
	} \
	else if ( PASTEMAC(ch,eq0)( *beta ) ) \
	{ \
		/* General storage: doesn't matter how we traverse ab and c. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,copys) \
			( \
			  ab[ i*rs_ab + j*cs_ab ], \
			  c[  i*rs_c  + j*cs_c  ]  \
			) \
		} \
	} \
	else /* beta != 0 && beta != 1 */ \
	{ \
		/* General storage: doesn't matter how we traverse ab and c. */ \
		for ( dim_t j = 0; j < n; ++j ) \
		for ( dim_t i = 0; i < m; ++i ) \
		{ \
			PASTEMAC(ch,xpbys) \
			( \
			  ab[ i*rs_ab + j*cs_ab ], \
			  *beta, \
			  c[  i*rs_c  + j*cs_c  ]  \
			) \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( gemmsup_g, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

#endif
