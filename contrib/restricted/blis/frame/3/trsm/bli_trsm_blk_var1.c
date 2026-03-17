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

//#define PRINT

void bli_trsm_blk_var1
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
	dim_t my_start, my_end;
	dim_t b_alg;

	// Determine the direction in which to partition (forwards or backwards).
	dir_t direct = bli_l3_direct( a, b, c, cntl );

	// Prune any zero region that exists along the partitioning dimension.
	bli_l3_prune_unref_mparts_m( a, b, c, cntl );

	// Isolate the diagonal block A11 and its corresponding row panel C1.
	const dim_t kc = bli_obj_width( a );
	obj_t a11, c1;
	bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
	                        0, kc, a, &a11 );
	bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
	                        0, kc, c, &c1 );

	// All threads iterate over the entire diagonal block A11.
	my_start = 0; my_end = kc;

#ifdef PRINT
	printf( "bli_trsm_blk_var1(): a11 is %d x %d at offsets (%3d, %3d)\n",
	        (int)bli_obj_length( &a11 ), (int)bli_obj_width( &a11 ),
	        (int)bli_obj_row_off( &a11 ), (int)bli_obj_col_off( &a11 ) );
	printf( "bli_trsm_blk_var1(): entering trsm subproblem loop.\n" );
#endif

	// Partition along the m dimension for the trsm subproblem.
	for ( dim_t i = my_start; i < my_end; i += b_alg )
	{
		obj_t a11_1, c1_1;

		b_alg = bli_determine_blocksize( direct, i, my_end, &a11,
		                                 bli_cntl_bszid( cntl ), cntx );

		// Acquire partitions for A1 and C1.
		bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
		                        i, b_alg, &a11, &a11_1 );
		bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
		                        i, b_alg, &c1, &c1_1 );

#ifdef PRINT
		printf( "bli_trsm_blk_var1():   a11_1 is %d x %d at offsets (%3d, %3d)\n",
		        (int)bli_obj_length( &a11_1 ), (int)bli_obj_width( &a11_1 ),
		        (int)bli_obj_row_off( &a11_1 ), (int)bli_obj_col_off( &a11_1 ) );
#endif

		// Perform trsm subproblem.
		bli_trsm_int
		(
		  &BLIS_ONE,
		  &a11_1,
		  b,
		  &BLIS_ONE,
		  &c1_1,
		  cntx,
		  rntm,
		  bli_cntl_sub_prenode( cntl ),
		  bli_thrinfo_sub_prenode( thread )
		);
	}

#ifdef PRINT
	printf( "bli_trsm_blk_var1(): finishing trsm subproblem loop.\n" );
#endif

	// We must execute a barrier here because the upcoming rank-k update
	// requires the packed matrix B to be fully updated by the trsm
	// subproblem.
	bli_thread_barrier( thread );

	// Isolate the remaining part of the column panel matrix A, which we do by
	// acquiring the subpartition ahead of A11 (that is, A21 or A01, depending
	// on whether we are moving forwards or backwards, respectively).
	obj_t ax1, cx1;
	bli_acquire_mpart_mdim( direct, BLIS_SUBPART1A,
	                        0, kc, a, &ax1 );
	bli_acquire_mpart_mdim( direct, BLIS_SUBPART1A,
	                        0, kc, c, &cx1 );

#ifdef PRINT
	printf( "bli_trsm_blk_var1(): ax1 is %d x %d at offsets (%3d, %3d)\n",
	        (int)bli_obj_length( &ax1 ), (int)bli_obj_width( &ax1 ),
	        (int)bli_obj_row_off( &ax1 ), (int)bli_obj_col_off( &ax1 ) );
#endif

	// Determine the current thread's subpartition range for the gemm
	// subproblem over Ax1.
	bli_thread_range_mdim
	(
	  direct, thread, &ax1, b, &cx1, cntl, cntx,
      &my_start, &my_end
	);

#ifdef PRINT
	printf( "bli_trsm_blk_var1(): entering gemm subproblem loop (%d->%d).\n", (int)my_start, (int)my_end );
#endif

	// Partition along the m dimension for the gemm subproblem.
	for ( dim_t i = my_start; i < my_end; i += b_alg )
	{
		obj_t a11, c1;

		// Determine the current algorithmic blocksize.
		b_alg = bli_determine_blocksize( direct, i, my_end, &ax1,
		                                 bli_cntl_bszid( cntl ), cntx );

		// Acquire partitions for A1 and C1.
		bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
		                        i, b_alg, &ax1, &a11 );
		bli_acquire_mpart_mdim( direct, BLIS_SUBPART1,
		                        i, b_alg, &cx1, &c1 );

#ifdef PRINT
		printf( "bli_trsm_blk_var1():   a11 is %d x %d at offsets (%3d, %3d)\n",
		        (int)bli_obj_length( &a11 ), (int)bli_obj_width( &a11 ),
		        (int)bli_obj_row_off( &a11 ), (int)bli_obj_col_off( &a11 ) );
#endif

		// Perform gemm subproblem. (Note that we use the same backend
		// function as before, since we're calling the same macrokernel.)
		bli_trsm_int
		(
		  &BLIS_ONE,
		  &a11,
		  b,
		  &BLIS_ONE,
		  &c1,
		  cntx,
		  rntm,
		  bli_cntl_sub_node( cntl ),
		  bli_thrinfo_sub_node( thread )
		);
	}
#ifdef PRINT
	printf( "bli_trsm_blk_var1(): finishing gemm subproblem loop.\n" );
#endif
}

