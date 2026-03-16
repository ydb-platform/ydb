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

void bli_thrinfo_sup_grow
     (
       rntm_t*    rntm,
       bszid_t*   bszid_par,
       thrinfo_t* thread
     )
{
	if ( thread == &BLIS_GEMM_SINGLE_THREADED ||
	     thread == &BLIS_PACKM_SINGLE_THREADED ) return;

	// NOTE: If bli_thrinfo_sup_rgrow() is being called, the sub_node field will
	// always be non-NULL, and so there's no need to check it.
	//if ( bli_cntl_sub_node( cntl ) != NULL )
	{
		// We only need to take action if the thrinfo_t sub-node is NULL; if it
		// is non-NULL, then it has already been created and we'll use it as-is.
		if ( bli_thrinfo_sub_node( thread ) == NULL )
		{
			// Create a new node (or, if needed, multiple nodes) along the
			// main sub-node branch of the tree and return the pointer to the
			// (highest) child.
			thrinfo_t* thread_child = bli_thrinfo_sup_rgrow
			(
			  rntm,
			  bszid_par,
			  &bszid_par[1],
			  thread
			);

			// Attach the child thrinfo_t node for the primary branch to its
			// parent structure.
			bli_thrinfo_set_sub_node( thread_child, thread );
		}
	}
}

// -----------------------------------------------------------------------------

thrinfo_t* bli_thrinfo_sup_rgrow
     (
       rntm_t*    rntm,
       bszid_t*   bszid_par,
       bszid_t*   bszid_cur,
       thrinfo_t* thread_par
     )
{
	thrinfo_t* thread_cur;

	// We must handle two cases: those where the next node in the
	// control tree is a partitioning node, and those where it is
	// a non-partitioning (ie: packing) node.
	if ( *bszid_cur != BLIS_NO_PART )
	{
		// Create the child thrinfo_t node corresponding to cntl_cur,
		// with cntl_par being the parent.
		thread_cur = bli_thrinfo_sup_create_for_cntl
		(
		  rntm,
		  bszid_par,
		  bszid_cur,
		  thread_par
		);
	}
	else // if ( *bszid_cur == BLIS_NO_PART )
	{
		// Recursively grow the thread structure and return the top-most
		// thrinfo_t node of that segment.
		thrinfo_t* thread_seg = bli_thrinfo_sup_rgrow
		(
		  rntm,
		  bszid_par,
		  &bszid_cur[1],
		  thread_par
		);

		// Create a thrinfo_t node corresponding to cntl_cur. Since the
		// corresponding cntl node, cntl_cur, is a non-partitioning node
		// (bszid = BLIS_NO_PART), this means it's a packing node. Packing
		// thrinfo_t nodes are formed differently than those corresponding to
		// partitioning nodes; specifically, their work_id's are set equal to
		// the their comm_id's. Also, notice that the free_comm field is set
		// to FALSE since cntl_cur is a non-partitioning node. The reason:
		// the communicator used here will be freed when thread_seg, or one
		// of its descendents, is freed.
		thread_cur = bli_thrinfo_create
		(
		  rntm,                                            // rntm
		  bli_thrinfo_ocomm( thread_seg ),                 // ocomm
		  bli_thread_ocomm_id( thread_seg ),               // ocomm_id
		  bli_rntm_calc_num_threads_in( bszid_cur, rntm ), // n_way
		  bli_thread_ocomm_id( thread_seg ),               // work_id
		  FALSE,                                           // free_comm
		  BLIS_NO_PART,                                    // bszid
		  thread_seg                                       // sub_node
		);
	}

	return thread_cur;
}

#define BLIS_NUM_STATIC_COMMS 80

thrinfo_t* bli_thrinfo_sup_create_for_cntl
     (
       rntm_t*    rntm,
       bszid_t*   bszid_par,
       bszid_t*   bszid_chl,
       thrinfo_t* thread_par
     )
{
#if 1
	// If we are running with a single thread, all of the code can be reduced
	// and simplified to this.
	if ( bli_rntm_calc_num_threads( rntm ) == 1 )
	{
		thrinfo_t* thread_chl = bli_thrinfo_create
		(
		  rntm,                        // rntm
		  &BLIS_SINGLE_COMM,           // ocomm
		  0,                           // ocomm_id
		  1,                           // n_way
		  0,                           // work_id
		  FALSE,                       // free_comm
		  BLIS_NO_PART,                // bszid
		  NULL                         // sub_node
		);
		return thread_chl;
	}
#endif

	thrcomm_t*  static_comms[ BLIS_NUM_STATIC_COMMS ];
	thrcomm_t** new_comms = NULL;

	const dim_t parent_nt_in   = bli_thread_num_threads( thread_par );
	const dim_t parent_n_way   = bli_thread_n_way( thread_par );
	const dim_t parent_comm_id = bli_thread_ocomm_id( thread_par );
	const dim_t parent_work_id = bli_thread_work_id( thread_par );

	// Sanity check: make sure the number of threads in the parent's
	// communicator is divisible by the number of new sub-groups.
	if ( parent_nt_in % parent_n_way != 0 )
	{
		printf( "Assertion failed: parent_nt_in <mod> parent_n_way != 0\n" );
		bli_abort();
	}

	// Compute:
	// - the number of threads inside the new child comm,
	// - the current thread's id within the new communicator,
	// - the current thread's work id, given the ways of parallelism
	//   to be obtained within the next loop.
	const dim_t child_nt_in   = bli_rntm_calc_num_threads_in( bszid_chl, rntm );
	const dim_t child_n_way   = bli_rntm_ways_for( *bszid_chl, rntm );
	const dim_t child_comm_id = parent_comm_id % child_nt_in;
	const dim_t child_work_id = child_comm_id / ( child_nt_in / child_n_way );

//printf( "thread %d: child_n_way = %d child_nt_in = %d parent_n_way = %d (bszid = %d->%d)\n", (int)child_comm_id, (int)child_nt_in, (int)child_n_way, (int)parent_n_way, (int)bli_cntl_bszid( cntl_par ), (int)bszid_chl );

	// The parent's chief thread creates a temporary array of thrcomm_t
	// pointers.
	if ( bli_thread_am_ochief( thread_par ) )
	{
		if ( parent_n_way > BLIS_NUM_STATIC_COMMS )
			new_comms = bli_malloc_intl( parent_n_way * sizeof( thrcomm_t* ) );
		else
			new_comms = static_comms;
	}

	// Broadcast the temporary array to all threads in the parent's
	// communicator.
	new_comms = bli_thread_broadcast( thread_par, new_comms );

	// Chiefs in the child communicator allocate the communicator
	// object and store it in the array element corresponding to the
	// parent's work id.
	if ( child_comm_id == 0 )
		new_comms[ parent_work_id ] = bli_thrcomm_create( rntm, child_nt_in );

	bli_thread_barrier( thread_par );

	// All threads create a new thrinfo_t node using the communicator
	// that was created by their chief, as identified by parent_work_id.
	thrinfo_t* thread_chl = bli_thrinfo_create
	(
	  rntm,                        // rntm
	  new_comms[ parent_work_id ], // ocomm
	  child_comm_id,               // ocomm_id
	  child_n_way,                 // n_way
	  child_work_id,               // work_id
	  TRUE,                        // free_comm
	  *bszid_chl,                  // bszid
	  NULL                         // sub_node
	);

	bli_thread_barrier( thread_par );

	// The parent's chief thread frees the temporary array of thrcomm_t
	// pointers.
	if ( bli_thread_am_ochief( thread_par ) )
	{
		if ( parent_n_way > BLIS_NUM_STATIC_COMMS )
			bli_free_intl( new_comms );
	}

	return thread_chl;
}

