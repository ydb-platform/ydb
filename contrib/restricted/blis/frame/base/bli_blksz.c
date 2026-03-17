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

blksz_t* bli_blksz_create_ed
     (
       dim_t b_s, dim_t be_s,
       dim_t b_d, dim_t be_d,
       dim_t b_c, dim_t be_c,
       dim_t b_z, dim_t be_z
     )
{
	blksz_t* b = bli_malloc_intl( sizeof( blksz_t ) );

	bli_blksz_init_ed
	(
	  b,
	  b_s, be_s,
	  b_d, be_d,
	  b_c, be_c,
	  b_z, be_z
	);

	return b;
}

blksz_t* bli_blksz_create
     (
       dim_t b_s,  dim_t b_d,  dim_t b_c,  dim_t b_z,
       dim_t be_s, dim_t be_d, dim_t be_c, dim_t be_z
     )
{
	blksz_t* b = bli_malloc_intl( sizeof( blksz_t ) );

	bli_blksz_init
	(
	  b,
	  b_s,  b_d,  b_c,  b_z,
	  be_s, be_d, be_c, be_z
	);

	return b;
}

void bli_blksz_init_ed
     (
       blksz_t* b,
       dim_t b_s, dim_t be_s,
       dim_t b_d, dim_t be_d,
       dim_t b_c, dim_t be_c,
       dim_t b_z, dim_t be_z
     )
{
	b->v[BLIS_FLOAT]    = b_s;
	b->v[BLIS_DOUBLE]   = b_d;
	b->v[BLIS_SCOMPLEX] = b_c;
	b->v[BLIS_DCOMPLEX] = b_z;

	b->e[BLIS_FLOAT]    = be_s;
	b->e[BLIS_DOUBLE]   = be_d;
	b->e[BLIS_SCOMPLEX] = be_c;
	b->e[BLIS_DCOMPLEX] = be_z;
}

void bli_blksz_init
     (
       blksz_t* b,
       dim_t b_s,  dim_t b_d,  dim_t b_c,  dim_t b_z,
       dim_t be_s, dim_t be_d, dim_t be_c, dim_t be_z
     )
{
	b->v[BLIS_FLOAT]    = b_s;
	b->v[BLIS_DOUBLE]   = b_d;
	b->v[BLIS_SCOMPLEX] = b_c;
	b->v[BLIS_DCOMPLEX] = b_z;

	b->e[BLIS_FLOAT]    = be_s;
	b->e[BLIS_DOUBLE]   = be_d;
	b->e[BLIS_SCOMPLEX] = be_c;
	b->e[BLIS_DCOMPLEX] = be_z;
}

void bli_blksz_init_easy
     (
       blksz_t* b,
       dim_t b_s,  dim_t b_d,  dim_t b_c,  dim_t b_z
     )
{
	b->v[BLIS_FLOAT]    = b->e[BLIS_FLOAT]    = b_s;
	b->v[BLIS_DOUBLE]   = b->e[BLIS_DOUBLE]   = b_d;
	b->v[BLIS_SCOMPLEX] = b->e[BLIS_SCOMPLEX] = b_c;
	b->v[BLIS_DCOMPLEX] = b->e[BLIS_DCOMPLEX] = b_z;
}

void bli_blksz_free
     (
       blksz_t* b
     )
{
	bli_free_intl( b );
}

// -----------------------------------------------------------------------------

#if 0
void bli_blksz_reduce_dt_to
     (
       num_t dt_bm, blksz_t* bmult,
       num_t dt_bs, blksz_t* blksz
     )
{
	dim_t blksz_def = bli_blksz_get_def( dt_bs, blksz );
	dim_t blksz_max = bli_blksz_get_max( dt_bs, blksz );

	dim_t bmult_val = bli_blksz_get_def( dt_bm, bmult );

	// If the blocksize multiple is zero, we do nothing.
	if ( bmult_val == 0 ) return;

	// Round the default and maximum blocksize values down to their
	// respective nearest multiples of bmult_val. (Notice that we
	// ignore the "max" entry in the bmult object since that would
	// correspond to the packing dimension, which plays no role
	// as a blocksize multiple.)
	blksz_def = ( blksz_def / bmult_val ) * bmult_val;
	blksz_max = ( blksz_max / bmult_val ) * bmult_val;

	// Make sure the new blocksize values are at least the blocksize
	// multiple.
	if ( blksz_def == 0 ) blksz_def = bmult_val;
	if ( blksz_max == 0 ) blksz_max = bmult_val;

	// Store the new blocksizes back to the object.
	bli_blksz_set_def( blksz_def, dt_bs, blksz );
	bli_blksz_set_max( blksz_max, dt_bs, blksz );
}
#endif

// -----------------------------------------------------------------------------

void bli_blksz_reduce_def_to
     (
       num_t dt_bm, blksz_t* bmult,
       num_t dt_bs, blksz_t* blksz
     )
{
	dim_t blksz_def = bli_blksz_get_def( dt_bs, blksz );

	dim_t bmult_val = bli_blksz_get_def( dt_bm, bmult );

	// If the blocksize multiple is zero, we do nothing.
	if ( bmult_val == 0 ) return;

	// Round the default and maximum blocksize values down to their
	// respective nearest multiples of bmult_val. (Notice that we
	// ignore the "max" entry in the bmult object since that would
	// correspond to the packing dimension, which plays no role
	// as a blocksize multiple.)
	blksz_def = ( blksz_def / bmult_val ) * bmult_val;

	// Make sure the new blocksize values are at least the blocksize
	// multiple.
	if ( blksz_def == 0 ) blksz_def = bmult_val;

	// Store the new blocksizes back to the object.
	bli_blksz_set_def( blksz_def, dt_bs, blksz );
}

// -----------------------------------------------------------------------------

void bli_blksz_reduce_max_to
     (
       num_t dt_bm, blksz_t* bmult,
       num_t dt_bs, blksz_t* blksz
     )
{
	dim_t blksz_max = bli_blksz_get_max( dt_bs, blksz );

	dim_t bmult_val = bli_blksz_get_def( dt_bm, bmult );

	// If the blocksize multiple is zero, we do nothing.
	if ( bmult_val == 0 ) return;

	// Round the blocksize values down to its nearest multiple of
	// of bmult_val. (Notice that we ignore the "max" entry in the
	// bmult object since that would correspond to the packing
	// dimension, which plays no role as a blocksize multiple.)
	blksz_max = ( blksz_max / bmult_val ) * bmult_val;

	// Make sure the new blocksize value is at least the blocksize
	// multiple.
	if ( blksz_max == 0 ) blksz_max = bmult_val;

	// Store the new blocksize back to the object.
	bli_blksz_set_max( blksz_max, dt_bs, blksz );
}

// -----------------------------------------------------------------------------

dim_t bli_determine_blocksize
     (
       dir_t   direct,
       dim_t   i,
       dim_t   dim,
       obj_t*  obj,
       bszid_t bszid,
       cntx_t* cntx
     )
{
	if ( direct == BLIS_FWD )
		return bli_determine_blocksize_f( i, dim, obj, bszid, cntx );
	else
		return bli_determine_blocksize_b( i, dim, obj, bszid, cntx );
}

dim_t bli_determine_blocksize_f
     (
       dim_t   i,
       dim_t   dim,
       obj_t*  obj,
       bszid_t bszid,
       cntx_t* cntx
     )
{
	num_t    dt;
	blksz_t* bsize;
	dim_t    b_alg, b_max;
	dim_t    b_use;

	// Extract the execution datatype and use it to query the corresponding
	// blocksize and blocksize maximum values from the blksz_t object.
	dt    = bli_obj_exec_dt( obj );
	bsize = bli_cntx_get_blksz( bszid, cntx );
	b_alg = bli_blksz_get_def( dt, bsize );
	b_max = bli_blksz_get_max( dt, bsize );

	b_use = bli_determine_blocksize_f_sub( i, dim, b_alg, b_max );

	return b_use;
}

dim_t bli_determine_blocksize_b
     (
       dim_t   i,
       dim_t   dim,
       obj_t*  obj,
       bszid_t bszid,
       cntx_t* cntx
     )
{
	num_t    dt;
	blksz_t* bsize;
	dim_t    b_alg, b_max;
	dim_t    b_use;

	// Extract the execution datatype and use it to query the corresponding
	// blocksize and blocksize maximum values from the blksz_t object.
	dt    = bli_obj_exec_dt( obj );
	bsize = bli_cntx_get_blksz( bszid, cntx );
	b_alg = bli_blksz_get_def( dt, bsize );
	b_max = bli_blksz_get_max( dt, bsize );

	b_use = bli_determine_blocksize_b_sub( i, dim, b_alg, b_max );

	return b_use;
}

dim_t bli_determine_blocksize_f_sub
     (
       dim_t  i,
       dim_t  dim,
       dim_t  b_alg,
       dim_t  b_max
     )
{
	dim_t b_now;
	dim_t dim_left_now;

	// We assume that this function is being called from an algorithm that
	// is moving "forward" (ie: top to bottom, left to right, top-left
	// to bottom-right).

	// Compute how much of the matrix dimension is left, including the
	// chunk that will correspond to the blocksize we are computing now.
	dim_left_now = dim - i;

	// If the dimension currently remaining is less than the maximum
	// blocksize, use it instead of the default blocksize b_alg.
	// Otherwise, use b_alg.
	if ( dim_left_now <= b_max )
	{
		b_now = dim_left_now;
	}
	else
	{
		b_now = b_alg;
	}

	return b_now;
}

dim_t bli_determine_blocksize_b_sub
     (
       dim_t  i,
       dim_t  dim,
       dim_t  b_alg,
       dim_t  b_max
     )
{
	dim_t b_now;
	dim_t dim_left_now;
	dim_t dim_at_edge;

	// We assume that this function is being called from an algorithm that
	// is moving "backward" (ie: bottom to top, right to left, bottom-right
	// to top-left).

	// Compute how much of the matrix dimension is left, including the
	// chunk that will correspond to the blocksize we are computing now.
	dim_left_now = dim - i;

	// Sanity check: if dim_left_now is zero, then we can return zero
	// without going any further.
	if ( dim_left_now == 0 )
		return 0;

	dim_at_edge = dim_left_now % b_alg;

	// If dim_left_now is a multiple of b_alg, we can safely return b_alg
	// without going any further.
	if ( dim_at_edge == 0 )
		return b_alg;

	// If the dimension currently remaining is less than the maximum
	// blocksize, use it as the chosen blocksize. If this is not the case,
	// then we know dim_left_now is greater than the maximum blocksize.
	// To determine how much of it we should use for the current blocksize,
	// we inspect dim_at_edge; if it is smaller than (or equal to) b_max -
	// b_alg, then we use b_alg + dim_at_edge. Otherwise, dim_at_edge is
	// greater than b_max - b_alg, in which case we use dim_at_edge.
	if ( dim_left_now <= b_max )
	{
		b_now = dim_left_now;
	}
	else // if ( dim_left_now > b_max )
	{
		if ( dim_at_edge <= b_max - b_alg )
		{
			b_now = b_alg + dim_at_edge;
		}
		else // if ( dim_at_edge > b_max - b_alg )
		{
			b_now = dim_at_edge;
		}
	}

	return b_now;
}

