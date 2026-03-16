/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018 - 2019, Advanced Micro Devices, Inc.

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

// -- gemm/her2k/syr2k ---------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth, nstage ) \
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
	ind_t   ind      = PASTEMAC0(imeth); \
	num_t   dt       = bli_obj_dt( c ); \
	obj_t*  beta_use = beta; \
\
	dim_t   i; \
\
	/* If the objects are in the real domain, execute the native
	   implementation. */ \
	if ( bli_obj_is_real( c ) ) \
	{ \
		PASTEMAC(opname,nat)( alpha, a, b, beta, c, cntx, rntm ); \
		return; \
	} \
\
	/* A temporary hack to easily specify the 1m algorithm (block-panel or
	   panel-block). */ \
/*
	if ( PASTEMAC(opname,imeth) == bli_gemm1m ) \
	{ \
		bli_gemm1mbp( alpha, a, b, beta, c ); \
		return; \
	} \
	else if ( PASTEMAC(opname,imeth) == bli_gemm3m1 ) \
	{ \
		bli_gemm1mpb( alpha, a, b, beta, c ); \
		return; \
	} \
*/ \
\
	/* Query a context for the current induced method. This context is
	   managed and cached by the gks and should not be freed by the caller.
	   Note that the datatype argument is needed because it will be passed
	   in when bli_gks_query_ind_cntx() eventually calls the induced method's
	   _cntx_init() function. */ \
	cntx = bli_gks_query_ind_cntx( ind, dt ); \
\
	/* 3mh and 4mh change the context for each stage, and so in order to
	   remain thread-safe, we must make a local copy of the context for
	   those induced methods. */ \
	cntx_t cntx_l; \
	if ( ind == BLIS_3MH || ind == BLIS_4MH ) { cntx_l = *cntx; cntx = &cntx_l; } \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Some induced methods execute in multiple "stages". */ \
	for ( i = 0; i < nstage; ++i ) \
	{ \
		/* Prepare the context for the ith stage of computation. */ \
		bli_cntx_ind_stage( ind, i, cntx ); \
\
		/* For multi-stage methods, use BLIS_ONE as beta after the first
		   stage. */ \
		if ( i > 0 ) beta_use = &BLIS_ONE; \
\
		/* Invoke the operation's front end and request the default control
		   tree. */ \
		PASTEMAC(opname,_front)( alpha, a, b, beta_use, c, cntx, rntm, NULL ); \
	} \
}

// gemm
GENFRONT( gemm, gemm, 3mh, 3 )
GENFRONT( gemm, gemm, 3m1, 1 )
GENFRONT( gemm, gemm, 4mh, 4 )
GENFRONT( gemm, gemm, 4mb, 1 )
GENFRONT( gemm, gemm, 4m1, 1 )
GENFRONT( gemm, gemm, 1m,  1 )

// her2k
GENFRONT( her2k, gemm, 3mh, 3 )
GENFRONT( her2k, gemm, 3m1, 1 )
GENFRONT( her2k, gemm, 4mh, 4 )
//GENFRONT( her2k, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( her2k, gemm, 4m1, 1 )
GENFRONT( her2k, gemm, 1m,  1 )

// syr2k
GENFRONT( syr2k, gemm, 3mh, 3 )
GENFRONT( syr2k, gemm, 3m1, 1 )
GENFRONT( syr2k, gemm, 4mh, 4 )
//GENFRONT( syr2k, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( syr2k, gemm, 4m1, 1 )
GENFRONT( syr2k, gemm, 1m,  1 )


// -- hemm/symm/trmm3 ----------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth, nstage ) \
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
	ind_t   ind      = PASTEMAC0(imeth); \
	num_t   dt       = bli_obj_dt( c ); \
	obj_t*  beta_use = beta; \
\
	dim_t   i; \
\
	/* If the objects are in the real domain, execute the native
	   implementation. */ \
	if ( bli_obj_is_real( c ) ) \
	{ \
		PASTEMAC(opname,nat)( side, alpha, a, b, beta, c, cntx, rntm ); \
		return; \
	} \
\
	/* Query a context for the current induced method. This context is
	   managed and cached by the gks and should not be freed by the caller.
	   Note that the datatype argument is needed because it will be passed
	   in when bli_gks_query_ind_cntx() eventually calls the induced method's
	   _cntx_init() function. */ \
	cntx = bli_gks_query_ind_cntx( ind, dt ); \
\
	/* 3mh and 4mh change the context for each stage, and so in order to
	   remain thread-safe, we must make a local copy of the context for
	   those induced methods. */ \
	cntx_t cntx_l; \
	if ( ind == BLIS_3MH || ind == BLIS_4MH ) { cntx_l = *cntx; cntx = &cntx_l; } \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Some induced methods execute in multiple "stages". */ \
	for ( i = 0; i < nstage; ++i ) \
	{ \
		/* Prepare the context for the ith stage of computation. */ \
		bli_cntx_ind_stage( ind, i, cntx ); \
\
		/* For multi-stage methods, use BLIS_ONE as beta after the first
		   stage. */ \
		if ( i > 0 ) beta_use = &BLIS_ONE; \
\
		/* Invoke the operation's front end and request the default control
		   tree. */ \
		PASTEMAC(opname,_front)( side, alpha, a, b, beta_use, c, cntx, rntm, NULL ); \
	} \
}

// hemm
GENFRONT( hemm, gemm, 3mh, 3 )
GENFRONT( hemm, gemm, 3m1, 1 )
GENFRONT( hemm, gemm, 4mh, 4 )
//GENFRONT( hemm, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( hemm, gemm, 4m1, 1 )
GENFRONT( hemm, gemm, 1m,  1 )

// symm
GENFRONT( symm, gemm, 3mh, 3 )
GENFRONT( symm, gemm, 3m1, 1 )
GENFRONT( symm, gemm, 4mh, 4 )
//GENFRONT( symm, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( symm, gemm, 4m1, 1 )
GENFRONT( symm, gemm, 1m,  1 )

// trmm3
GENFRONT( trmm3, gemm, 3mh, 3 )
GENFRONT( trmm3, gemm, 3m1, 1 )
GENFRONT( trmm3, gemm, 4mh, 4 )
//GENFRONT( trmm3, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( trmm3, gemm, 4m1, 1 )
GENFRONT( trmm3, gemm, 1m,  1 )


// -- herk/syrk ----------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth, nstage ) \
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
	ind_t   ind      = PASTEMAC0(imeth); \
	num_t   dt       = bli_obj_dt( c ); \
	obj_t*  beta_use = beta; \
\
	dim_t   i; \
\
	/* If the objects are in the real domain, execute the native
	   implementation. */ \
	if ( bli_obj_is_real( c ) ) \
	{ \
		PASTEMAC(opname,nat)( alpha, a, beta, c, cntx, rntm ); \
		return; \
	} \
\
	/* Query a context for the current induced method. This context is
	   managed and cached by the gks and should not be freed by the caller.
	   Note that the datatype argument is needed because it will be passed
	   in when bli_gks_query_ind_cntx() eventually calls the induced method's
	   _cntx_init() function. */ \
	cntx = bli_gks_query_ind_cntx( ind, dt ); \
\
	/* 3mh and 4mh change the context for each stage, and so in order to
	   remain thread-safe, we must make a local copy of the context for
	   those induced methods. */ \
	cntx_t cntx_l; \
	if ( ind == BLIS_3MH || ind == BLIS_4MH ) { cntx_l = *cntx; cntx = &cntx_l; } \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Some induced methods execute in multiple "stages". */ \
	for ( i = 0; i < nstage; ++i ) \
	{ \
		/* Prepare the context for the ith stage of computation. */ \
		bli_cntx_ind_stage( ind, i, cntx ); \
\
		/* For multi-stage methods, use BLIS_ONE as beta after the first
		   stage. */ \
		if ( i > 0 ) beta_use = &BLIS_ONE; \
\
		/* Invoke the operation's front end and request the default control
		   tree. */ \
		PASTEMAC(opname,_front)( alpha, a, beta_use, c, cntx, rntm, NULL ); \
	} \
}

// herk
GENFRONT( herk, gemm, 3mh, 3 )
GENFRONT( herk, gemm, 3m1, 1 )
GENFRONT( herk, gemm, 4mh, 4 )
//GENFRONT( herk, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( herk, gemm, 4m1, 1 )
GENFRONT( herk, gemm, 1m,  1 )

// syrk
GENFRONT( syrk, gemm, 3mh, 3 )
GENFRONT( syrk, gemm, 3m1, 1 )
GENFRONT( syrk, gemm, 4mh, 4 )
//GENFRONT( syrk, gemm, 4mb, 1 ) // Not implemented.
GENFRONT( syrk, gemm, 4m1, 1 )
GENFRONT( syrk, gemm, 1m,  1 )


// -- trmm ---------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth, nstage ) \
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
	ind_t   ind      = PASTEMAC0(imeth); \
	num_t   dt       = bli_obj_dt( b ); \
\
	dim_t   i; \
\
	/* If the objects are in the real domain, execute the native
	   implementation. */ \
	if ( bli_obj_is_real( b ) ) \
	{ \
		PASTEMAC(opname,nat)( side, alpha, a, b, cntx, rntm ); \
		return; \
	} \
\
	/* Query a context for the current induced method. This context is
	   managed and cached by the gks and should not be freed by the caller.
	   Note that the datatype argument is needed because it will be passed
	   in when bli_gks_query_ind_cntx() eventually calls the induced method's
	   _cntx_init() function. */ \
	cntx = bli_gks_query_ind_cntx( ind, dt ); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	/* Some induced methods execute in multiple "stages". */ \
	for ( i = 0; i < nstage; ++i ) \
	{ \
		/* Prepare the context for the ith stage of computation. */ \
		bli_cntx_ind_stage( ind, i, cntx ); \
\
		/* Invoke the operation's front end and request the default control
		   tree. */ \
		PASTEMAC(opname,_front)( side, alpha, a, b, cntx, rntm, NULL ); \
	} \
}

// trmm
//GENFRONT( trmm, gemm, 3mh, 3 ) // Unimplementable.
GENFRONT( trmm, gemm, 3m1, 1 )
//GENFRONT( trmm, gemm, 4mh, 4 ) // Unimplementable.
//GENFRONT( trmm, gemm, 4mb, 1 ) // Unimplementable.
GENFRONT( trmm, gemm, 4m1, 1 )
GENFRONT( trmm, gemm, 1m,  1 )


// -- trsm ---------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, cname, imeth, nstage ) \
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
	ind_t   ind      = PASTEMAC0(imeth); \
	num_t   dt       = bli_obj_dt( b ); \
\
	/* If the objects are in the real domain, execute the native
	   implementation. */ \
	if ( bli_obj_is_real( b ) ) \
	{ \
		PASTEMAC(opname,nat)( side, alpha, a, b, cntx, rntm ); \
		return; \
	} \
\
	/* Query a context for the current induced method. This context is
	   managed and cached by the gks and should not be freed by the caller.
	   Note that the datatype argument is needed because it will be passed
	   in when bli_gks_query_ind_cntx() eventually calls the induced method's
	   _cntx_init() function. */ \
	cntx = bli_gks_query_ind_cntx( ind, dt ); \
\
	/* Initialize a local runtime with global settings if necessary. Note
	   that in the case that a runtime is passed in, we make a local copy. */ \
	rntm_t rntm_l; \
	if ( rntm == NULL ) { bli_rntm_init_from_global( &rntm_l ); rntm = &rntm_l; } \
	else                { rntm_l = *rntm;                       rntm = &rntm_l; } \
\
	{ \
		/* NOTE: trsm cannot be implemented via any induced method that
		   needs to execute in stages (e.g. 3mh, 4mh). */ \
\
		/* Invoke the operation's front end and request the default control
		   tree. */ \
		PASTEMAC(opname,_front)( side, alpha, a, b, cntx, rntm, NULL ); \
	} \
}

// trsm
//GENFRONT( trmm, trsm, 3mh, 3 ) // Unimplementable.
GENFRONT( trsm, trsm, 3m1, 1 )
//GENFRONT( trmm, trsm, 4mh, 4 ) // Unimplementable.
//GENFRONT( trmm, trsm, 4mb, 1 ) // Unimplementable.
GENFRONT( trsm, trsm, 4m1, 1 )
GENFRONT( trsm, trsm, 1m,  1 )

