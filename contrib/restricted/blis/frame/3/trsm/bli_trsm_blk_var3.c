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

void bli_trsm_blk_var3
     (
       obj_t*  a,
       obj_t*  b,
       obj_t*  c,
       cntx_t* cntx,
       rntm_t* rntm,
       cntl_t* cntl,
       thrinfo_t* thread
     )
{
	obj_t a1, b1;
	dim_t b_alg;

	// Determine the direction in which to partition (forwards or backwards).
	dir_t direct = bli_l3_direct( a, b, c, cntl );

	// Prune any zero region that exists along the partitioning dimension.
	bli_l3_prune_unref_mparts_k( a, b, c, cntl );

	// Query dimension in partitioning direction.
	dim_t k_trans = bli_obj_width_after_trans( a );

	// Partition along the k dimension.
	for ( dim_t i = 0; i < k_trans; i += b_alg )
	{
		// Determine the current algorithmic blocksize.
		b_alg = bli_trsm_determine_kc( direct, i, k_trans, a, b,
		                               bli_cntl_bszid( cntl ), cntx );

		// Acquire partitions for A1 and B1.
		bli_acquire_mpart_ndim( direct, BLIS_SUBPART1,
		                        i, b_alg, a, &a1 );
		bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
		                        i, b_alg, b, &b1 );

		// Perform trsm subproblem.
		bli_trsm_int
		(
		  &BLIS_ONE,
		  &a1,
		  &b1,
		  &BLIS_ONE,
		  c,
		  cntx,
		  rntm,
		  bli_cntl_sub_node( cntl ),
		  bli_thrinfo_sub_node( thread )
		);

		//bli_thread_ibarrier( thread );
		bli_thread_barrier( bli_thrinfo_sub_node( thread ) );

		// This variant executes multiple rank-k updates. Therefore, if the
		// internal alpha scalars on A/B and C are non-zero, we must ensure
		// that they are only used in the first iteration.
		if ( i == 0 )
		{
			bli_obj_scalar_reset( a ); bli_obj_scalar_reset( b );
			bli_obj_scalar_reset( c );
		}
	}
}

