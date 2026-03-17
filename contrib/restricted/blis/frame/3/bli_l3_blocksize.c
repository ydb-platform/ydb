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


dim_t bli_l3_determine_kc
      (
        dir_t   direct,
        dim_t   i,
        dim_t   dim,
        obj_t*  a,
        obj_t*  b,
        bszid_t bszid,
        cntx_t* cntx,
        cntl_t* cntl
      )
{
	opid_t family = bli_cntl_family( cntl );

	if      ( family == BLIS_GEMM )
		return bli_gemm_determine_kc( direct, i, dim, a, b, bszid, cntx );
	else if ( family == BLIS_HERK )
		return bli_herk_determine_kc( direct, i, dim, a, b, bszid, cntx );
	else if ( family == BLIS_TRMM )
		return bli_trmm_determine_kc( direct, i, dim, a, b, bszid, cntx );
	else if ( family == BLIS_TRSM )
		return bli_trsm_determine_kc( direct, i, dim, a, b, bszid, cntx );

	// This should never execute.
	return bli_gemm_determine_kc( direct, i, dim, a, b, bszid, cntx );
}

// -----------------------------------------------------------------------------

//
// NOTE: We call a gemm/hemm/symm, trmm, or trsm-specific blocksize
// function to determine the kc blocksize so that we can implement the
// "nudging" of kc to be a multiple of mr or nr, as needed.
//

#undef  GENFRONT
#define GENFRONT( opname, l3op ) \
\
dim_t PASTEMAC0(opname) \
      ( \
        dir_t   direct, \
        dim_t   i, \
        dim_t   dim, \
        obj_t*  a, \
        obj_t*  b, \
        bszid_t bszid, \
        cntx_t* cntx  \
      ) \
{ \
	if ( direct == BLIS_FWD ) \
		return PASTEMAC(l3op,_determine_kc_f)( i, dim, a, b, bszid, cntx ); \
	else \
		return PASTEMAC(l3op,_determine_kc_b)( i, dim, a, b, bszid, cntx ); \
}

GENFRONT( gemm_determine_kc, gemm )
GENFRONT( herk_determine_kc, herk )
GENFRONT( trmm_determine_kc, trmm )
GENFRONT( trsm_determine_kc, trsm )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, chdir ) \
\
dim_t PASTEMAC0(opname) \
      ( \
        dim_t   i, \
        dim_t   dim, \
        obj_t*  a, \
        obj_t*  b, \
        bszid_t bszid, \
        cntx_t* cntx  \
      ) \
{ \
	num_t    dt; \
	blksz_t* bsize; \
	dim_t    mnr; \
	dim_t    b_alg, b_max; \
	dim_t    b_use; \
 \
	/* bli_*_determine_kc_f():

	   We assume that this function is being called from an algorithm that
	   is moving "forward" (ie: top to bottom, left to right, top-left
	   to bottom-right). */ \
\
	/* bli_*_determine_kc_b():

	   We assume that this function is being called from an algorithm that
	   is moving "backward" (ie: bottom to top, right to left, bottom-right
	   to top-left). */ \
\
	/* Extract the execution datatype and use it to query the corresponding
	   blocksize and blocksize maximum values from the blksz_t object. */ \
	dt    = bli_obj_exec_dt( a ); \
	bsize = bli_cntx_get_blksz( bszid, cntx ); \
	b_alg = bli_blksz_get_def( dt, bsize ); \
	b_max = bli_blksz_get_max( dt, bsize ); \
\
	/* Nudge the default and maximum kc blocksizes up to the nearest
	   multiple of MR if A is Hermitian or symmetric, or NR if B is
	   Hermitian or symmetric. If neither case applies, then we leave
	   the blocksizes unchanged. */ \
	if      ( bli_obj_root_is_herm_or_symm( a ) ) \
	{ \
		mnr   = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
		b_alg = bli_align_dim_to_mult( b_alg, mnr ); \
		b_max = bli_align_dim_to_mult( b_max, mnr ); \
	} \
	else if ( bli_obj_root_is_herm_or_symm( b ) ) \
	{ \
		mnr   = bli_cntx_get_blksz_def_dt( dt, BLIS_NR, cntx ); \
		b_alg = bli_align_dim_to_mult( b_alg, mnr ); \
		b_max = bli_align_dim_to_mult( b_max, mnr ); \
	} \
\
	/* Call the bli_determine_blocksize_[fb]_sub() helper routine defined
	   in bli_blksz.c */ \
	b_use = PASTEMAC2(determine_blocksize_,chdir,_sub)( i, dim, b_alg, b_max ); \
\
	return b_use; \
}

GENFRONT( gemm_determine_kc_f, f )
GENFRONT( gemm_determine_kc_b, b )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, chdir ) \
\
dim_t PASTEMAC0(opname) \
      ( \
        dim_t   i, \
        dim_t   dim, \
        obj_t*  a, \
        obj_t*  b, \
        bszid_t bszid, \
        cntx_t* cntx  \
      ) \
{ \
	num_t    dt; \
	blksz_t* bsize; \
	dim_t    b_alg, b_max; \
	dim_t    b_use; \
 \
	/* bli_*_determine_kc_f():

	   We assume that this function is being called from an algorithm that
	   is moving "forward" (ie: top to bottom, left to right, top-left
	   to bottom-right). */ \
\
	/* bli_*_determine_kc_b():

	   We assume that this function is being called from an algorithm that
	   is moving "backward" (ie: bottom to top, right to left, bottom-right
	   to top-left). */ \
\
	/* Extract the execution datatype and use it to query the corresponding
	   blocksize and blocksize maximum values from the blksz_t object. */ \
	dt    = bli_obj_exec_dt( a ); \
	bsize = bli_cntx_get_blksz( bszid, cntx ); \
	b_alg = bli_blksz_get_def( dt, bsize ); \
	b_max = bli_blksz_get_max( dt, bsize ); \
\
	/* Notice that for herk, we do not need to perform any special handling
	   for the default and maximum kc blocksizes vis-a-vis MR or NR. */ \
\
	/* Call the bli_determine_blocksize_[fb]_sub() helper routine defined
	   in bli_blksz.c */ \
	b_use = PASTEMAC2(determine_blocksize_,chdir,_sub)( i, dim, b_alg, b_max ); \
\
	return b_use; \
}

GENFRONT( herk_determine_kc_f, f )
GENFRONT( herk_determine_kc_b, b )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, chdir ) \
\
dim_t PASTEMAC0(opname) \
      ( \
        dim_t   i, \
        dim_t   dim, \
        obj_t*  a, \
        obj_t*  b, \
        bszid_t bszid, \
        cntx_t* cntx  \
      ) \
{ \
	num_t    dt; \
	blksz_t* bsize; \
	dim_t    mnr; \
	dim_t    b_alg, b_max; \
	dim_t    b_use; \
 \
	/* bli_*_determine_kc_f():

	   We assume that this function is being called from an algorithm that
	   is moving "forward" (ie: top to bottom, left to right, top-left
	   to bottom-right). */ \
\
	/* bli_*_determine_kc_b():

	   We assume that this function is being called from an algorithm that
	   is moving "backward" (ie: bottom to top, right to left, bottom-right
	   to top-left). */ \
\
	/* Extract the execution datatype and use it to query the corresponding
	   blocksize and blocksize maximum values from the blksz_t object. */ \
	dt    = bli_obj_exec_dt( a ); \
	bsize = bli_cntx_get_blksz( bszid, cntx ); \
	b_alg = bli_blksz_get_def( dt, bsize ); \
	b_max = bli_blksz_get_max( dt, bsize ); \
\
	/* Nudge the default and maximum kc blocksizes up to the nearest
	   multiple of MR if the triangular matrix is on the left, or NR
	   if the triangular matrix is one the right. */ \
	if ( bli_obj_root_is_triangular( a ) ) \
		mnr = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
	else \
		mnr = bli_cntx_get_blksz_def_dt( dt, BLIS_NR, cntx ); \
\
	b_alg = bli_align_dim_to_mult( b_alg, mnr ); \
	b_max = bli_align_dim_to_mult( b_max, mnr ); \
\
	/* Call the bli_determine_blocksize_[fb]_sub() helper routine defined
	   in bli_blksz.c */ \
	b_use = PASTEMAC2(determine_blocksize_,chdir,_sub)( i, dim, b_alg, b_max ); \
\
	return b_use; \
}

GENFRONT( trmm_determine_kc_f, f )
GENFRONT( trmm_determine_kc_b, b )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname, chdir ) \
\
dim_t PASTEMAC0(opname) \
      ( \
        dim_t   i, \
        dim_t   dim, \
        obj_t*  a, \
        obj_t*  b, \
        bszid_t bszid, \
        cntx_t* cntx  \
      ) \
{ \
	num_t    dt; \
	blksz_t* bsize; \
	dim_t    mnr; \
	dim_t    b_alg, b_max; \
	dim_t    b_use; \
 \
	/* bli_*_determine_kc_f():

	   We assume that this function is being called from an algorithm that
	   is moving "forward" (ie: top to bottom, left to right, top-left
	   to bottom-right). */ \
\
	/* bli_*_determine_kc_b():

	   We assume that this function is being called from an algorithm that
	   is moving "backward" (ie: bottom to top, right to left, bottom-right
	   to top-left). */ \
\
	/* Extract the execution datatype and use it to query the corresponding
	   blocksize and blocksize maximum values from the blksz_t object. */ \
	dt    = bli_obj_exec_dt( a ); \
	bsize = bli_cntx_get_blksz( bszid, cntx ); \
	b_alg = bli_blksz_get_def( dt, bsize ); \
	b_max = bli_blksz_get_max( dt, bsize ); \
\
	/* Nudge the default and maximum kc blocksizes up to the nearest
	   multiple of MR. We always use MR (rather than sometimes using NR)
	   because even when the triangle is on the right, packing of that
	   matrix uses MR, since only left-side trsm micro-kernels are
	   supported. */ \
	mnr   = bli_cntx_get_blksz_def_dt( dt, BLIS_MR, cntx ); \
	b_alg = bli_align_dim_to_mult( b_alg, mnr ); \
	b_max = bli_align_dim_to_mult( b_max, mnr ); \
\
	/* Call the bli_determine_blocksize_[fb]_sub() helper routine defined
	   in bli_blksz.c */ \
	b_use = PASTEMAC2(determine_blocksize_,chdir,_sub)( i, dim, b_alg, b_max ); \
\
	return b_use; \
}

GENFRONT( trsm_determine_kc_f, f )
GENFRONT( trsm_determine_kc_b, b )

