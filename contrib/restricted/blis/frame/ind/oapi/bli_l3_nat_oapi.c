/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018 - 2020, Advanced Micro Devices, Inc.

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

// NOTE: The function definitions in this file can be consolidated with the
// definitions for the other induced methods. The only advantage of keeping
// them separate is that it allows us to avoid the very small loop overhead
// of executing one iteration of a for loop, plus the overhead of calling a
// function that does nothing (ie: the _cntx_init_stage() function).

// -- gemm/her2k/syr2k/gemmt ---------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth ) \
\
void PASTEMAC(opname,imeth) \
     ( \
       obj_t*  alpha, \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  beta, \
       obj_t*  c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     ) \
{ \
	bli_init_once(); \
\
	/* Obtain a valid (native) context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Invoke the operation's front end. */ \
	PASTEMAC(opname,_front) \
	( \
	  alpha, a, b, beta, c, cntx, rntm, NULL \
	); \
}

// If a sandbox was enabled, do not define bli_gemmnat() since it will be
// defined in the sandbox environment.
#ifndef BLIS_ENABLE_SANDBOX
GENFRONT( gemm, gemm, nat )
#endif
GENFRONT( gemmt, gemm, nat )
GENFRONT( her2k, gemm, nat )
GENFRONT( syr2k, gemm, nat )


// -- hemm/symm/trmm3 ----------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth ) \
\
void PASTEMAC(opname,imeth) \
     ( \
       side_t  side, \
       obj_t*  alpha, \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  beta, \
       obj_t*  c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     ) \
{ \
	bli_init_once(); \
\
	/* Obtain a valid (native) context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Invoke the operation's front end. */ \
	PASTEMAC(opname,_front) \
	( \
	  side, alpha, a, b, beta, c, cntx, rntm, NULL \
	); \
}

GENFRONT( hemm, gemm, nat )
GENFRONT( symm, gemm, nat )
GENFRONT( trmm3, gemm, nat )


// -- herk/syrk ----------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth ) \
\
void PASTEMAC(opname,imeth) \
     ( \
       obj_t*  alpha, \
       obj_t*  a, \
       obj_t*  beta, \
       obj_t*  c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     ) \
{ \
	bli_init_once(); \
\
	/* Obtain a valid (native) context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Invoke the operation's front end. */ \
	PASTEMAC(opname,_front) \
	( \
	  alpha, a, beta, c, cntx, rntm, NULL \
	); \
}

GENFRONT( herk, gemm, nat )
GENFRONT( syrk, gemm, nat )


// -- trmm ---------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth ) \
\
void PASTEMAC(opname,imeth) \
     ( \
       side_t  side, \
       obj_t*  alpha, \
       obj_t*  a, \
       obj_t*  b, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     ) \
{ \
	bli_init_once(); \
\
	/* Obtain a valid (native) context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Invoke the operation's front end. */ \
	PASTEMAC(opname,_front) \
	( \
	  side, alpha, a, b, cntx, rntm, NULL \
	); \
}

GENFRONT( trmm, gemm, nat )


// -- trsm ---------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth ) \
\
void PASTEMAC(opname,imeth) \
     ( \
       side_t  side, \
       obj_t*  alpha, \
       obj_t*  a, \
       obj_t*  b, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     ) \
{ \
	bli_init_once(); \
\
	/* Obtain a valid (native) context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Invoke the operation's front end. */ \
	PASTEMAC(opname,_front) \
	( \
	  side, alpha, a, b, cntx, rntm, NULL \
	); \
}

GENFRONT( trsm, trsm, nat )

