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

thrinfo_t* bli_thrinfo_create
     (
       rntm_t*    rntm,
       thrcomm_t* ocomm,
       dim_t      ocomm_id,
       dim_t      n_way,
       dim_t      work_id, 
       bool       free_comm,
       bszid_t    bszid,
       thrinfo_t* sub_node
     )
{
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_thrinfo_create(): " );
	#endif

    thrinfo_t* thread = bli_sba_acquire( rntm, sizeof( thrinfo_t ) );

    bli_thrinfo_init
	(
	  thread,
	  ocomm, ocomm_id,
	  n_way, work_id, 
	  free_comm,
	  bszid,
	  sub_node
	);

    return thread;
}

void bli_thrinfo_init
     (
       thrinfo_t* thread,
       thrcomm_t* ocomm,
       dim_t      ocomm_id,
       dim_t      n_way,
       dim_t      work_id, 
       bool       free_comm,
       bszid_t    bszid,
       thrinfo_t* sub_node
     )
{
	bli_thrinfo_set_ocomm( ocomm, thread );
	bli_thrinfo_set_ocomm_id( ocomm_id, thread );
	bli_thrinfo_set_n_way( n_way, thread );
	bli_thrinfo_set_work_id( work_id, thread );
	bli_thrinfo_set_free_comm( free_comm, thread );
	bli_thrinfo_set_bszid( bszid, thread );

	bli_thrinfo_set_sub_node( sub_node, thread );
	bli_thrinfo_set_sub_prenode( NULL, thread );
}

void bli_thrinfo_init_single
     (
       thrinfo_t* thread
     )
{
	bli_thrinfo_init
	(
	  thread,
	  &BLIS_SINGLE_COMM, 0,
	  1,
	  0,
	  FALSE,
	  BLIS_NO_PART,
	  thread
	);
}

void bli_thrinfo_free
     (
       rntm_t*    rntm,
       thrinfo_t* thread
     )
{
	if ( thread == NULL ||
	     thread == &BLIS_PACKM_SINGLE_THREADED ||
	     thread == &BLIS_GEMM_SINGLE_THREADED
	   ) return;

	thrinfo_t* thrinfo_sub_prenode = bli_thrinfo_sub_prenode( thread );
	thrinfo_t* thrinfo_sub_node    = bli_thrinfo_sub_node( thread );

	// Recursively free all children of the current thrinfo_t.
	if ( thrinfo_sub_prenode != NULL )
	{
		bli_thrinfo_free( rntm, thrinfo_sub_prenode );
	}

	// Recursively free all children of the current thrinfo_t.
	if ( thrinfo_sub_node != NULL )
	{
		bli_thrinfo_free( rntm, thrinfo_sub_node );
	}

	// Free the communicators, but only if the current thrinfo_t struct
	// is marked as needing them to be freed. The most common example of
	// thrinfo_t nodes NOT marked as needing their comms freed are those
	// associated with packm thrinfo_t nodes.
	if ( bli_thrinfo_needs_free_comm( thread ) )
	{
		// The ochief always frees his communicator.
		if ( bli_thread_am_ochief( thread ) )
			bli_thrcomm_free( rntm, bli_thrinfo_ocomm( thread ) );
	}

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_thrinfo_free(): " );
	#endif

	// Free the thrinfo_t struct.
	bli_sba_release( rntm, thread );
}

// -----------------------------------------------------------------------------

void bli_thrinfo_grow
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	// First, consider the prenode branch of the thrinfo_t tree, which should be
	// expanded only if there exists a prenode branch in the cntl_t tree.

	if ( bli_cntl_sub_prenode( cntl ) != NULL )
	{
		// We only need to take action if the thrinfo_t sub-node is NULL; if it
		// is non-NULL, then it has already been created and we'll use it as-is.
		if ( bli_thrinfo_sub_prenode( thread ) == NULL )
		{
			// Assertion / sanity check.
			if ( bli_cntl_bszid( cntl ) != BLIS_MC )
			{
				printf( "Assertion failed: Expanding prenode for non-IC loop?\n" );
				bli_abort();
			}

			// Now we must create the packa, jr, and ir nodes that make up
			// the prenode branch of current cntl_t node.

			// Create a new node (or, if needed, multiple nodes) along the
			// prenode branch of the tree and return the pointer to the
			// (highest) child.
			thrinfo_t* thread_prenode = bli_thrinfo_rgrow_prenode
			(
			  rntm,
			  cntl,
			  bli_cntl_sub_prenode( cntl ),
			  thread
			);

			// Attach the child thrinfo_t node for the secondary branch to its
			// parent structure.
			bli_thrinfo_set_sub_prenode( thread_prenode, thread );
		}
	}

	// Now, grow the primary branch of the thrinfo_t tree.

	// NOTE: If bli_thrinfo_rgrow() is being called, the sub_node field will
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
			thrinfo_t* thread_child = bli_thrinfo_rgrow
			(
			  rntm,
			  cntl,
			  bli_cntl_sub_node( cntl ),
			  thread
			);

			// Attach the child thrinfo_t node for the primary branch to its
			// parent structure.
			bli_thrinfo_set_sub_node( thread_child, thread );
		}
	}
}

// -----------------------------------------------------------------------------

thrinfo_t* bli_thrinfo_rgrow
     (
       rntm_t*    rntm,
       cntl_t*    cntl_par,
       cntl_t*    cntl_cur,
       thrinfo_t* thread_par
     )
{
	thrinfo_t* thread_cur;

	// We must handle two cases: those where the next node in the
	// control tree is a partitioning node, and those where it is
	// a non-partitioning (ie: packing) node.
	if ( bli_cntl_bszid( cntl_cur ) != BLIS_NO_PART )
	{
		// Create the child thrinfo_t node corresponding to cntl_cur,
		// with cntl_par being the parent.
		thread_cur = bli_thrinfo_create_for_cntl
		(
		  rntm,
		  cntl_par,
		  cntl_cur,
		  thread_par
		);
	}
	else // if ( bli_cntl_bszid( cntl_cur ) == BLIS_NO_PART )
	{
		// Recursively grow the thread structure and return the top-most
		// thrinfo_t node of that segment.
		thrinfo_t* thread_seg = bli_thrinfo_rgrow
		(
		  rntm,
		  cntl_par,
		  bli_cntl_sub_node( cntl_cur ),
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
		  rntm,                                           // rntm
		  bli_thrinfo_ocomm( thread_seg ),                // ocomm
		  bli_thread_ocomm_id( thread_seg ),              // ocomm_id
		  bli_cntl_calc_num_threads_in( rntm, cntl_cur ), // n_way
		  bli_thread_ocomm_id( thread_seg ),              // work_id
		  FALSE,                                          // free_comm
		  BLIS_NO_PART,                                   // bszid
		  thread_seg                                      // sub_node
		);
	}

	return thread_cur;
}

#define BLIS_NUM_STATIC_COMMS 80

thrinfo_t* bli_thrinfo_create_for_cntl
     (
       rntm_t*    rntm,
       cntl_t*    cntl_par,
       cntl_t*    cntl_chl,
       thrinfo_t* thread_par
     )
{
	thrcomm_t*  static_comms[ BLIS_NUM_STATIC_COMMS ];
	thrcomm_t** new_comms = NULL;

	const bszid_t bszid_chl = bli_cntl_bszid( cntl_chl );

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
	const dim_t child_nt_in   = bli_cntl_calc_num_threads_in( rntm, cntl_chl );
	const dim_t child_n_way   = bli_rntm_ways_for( bszid_chl, rntm );
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
	  bszid_chl,                   // bszid
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

// -----------------------------------------------------------------------------

thrinfo_t* bli_thrinfo_rgrow_prenode
     (
       rntm_t*    rntm,
       cntl_t*    cntl_par,
       cntl_t*    cntl_cur,
       thrinfo_t* thread_par
     )
{
	thrinfo_t* thread_cur;

	// We must handle two cases: those where the next node in the
	// control tree is a partitioning node, and those where it is
	// a non-partitioning (ie: packing) node.
	if ( bli_cntl_bszid( cntl_cur ) != BLIS_NO_PART )
	{
		// Create the child thrinfo_t node corresponding to cntl_cur,
		// with cntl_par being the parent.
		thread_cur = bli_thrinfo_create_for_cntl_prenode
		(
		  rntm,
		  cntl_par,
		  cntl_cur,
		  thread_par
		);
	}
	else // if ( bli_cntl_bszid( cntl_cur ) == BLIS_NO_PART )
	{
		// Recursively grow the thread structure and return the top-most
		// thrinfo_t node of that segment.
		thrinfo_t* thread_seg = bli_thrinfo_rgrow_prenode
		(
		  rntm,
		  cntl_par,
		  bli_cntl_sub_node( cntl_cur ),
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
		  rntm,                                           // rntm
		  bli_thrinfo_ocomm( thread_seg ),                // ocomm
		  bli_thread_ocomm_id( thread_seg ),              // ocomm_id
		  bli_cntl_calc_num_threads_in( rntm, cntl_par ), // n_way
		  bli_thread_ocomm_id( thread_seg ),              // work_id
		  FALSE,                                          // free_comm
		  BLIS_NO_PART,                                   // bszid
		  thread_seg                                      // sub_node
		);
	}

	return thread_cur;
}

thrinfo_t* bli_thrinfo_create_for_cntl_prenode
     (
       rntm_t*    rntm,
       cntl_t*    cntl_par,
       cntl_t*    cntl_chl,
       thrinfo_t* thread_par
     )
{
	// NOTE: This function only has to work for the ic -> (pa -> jr)
	// thrinfo_t tree branch extension. After that, the function
	// bli_thrinfo_create_for_cntl() will be called for the last jr->ir
	// branch extension.

	const bszid_t bszid_chl = bli_cntl_bszid( cntl_chl );

	const dim_t parent_nt_in   = bli_thread_num_threads( thread_par );
	const dim_t parent_n_way   = bli_thread_n_way( thread_par );
	const dim_t parent_comm_id = bli_thread_ocomm_id( thread_par );
	//const dim_t parent_work_id = bli_thread_work_id( thread_par );

	// Sanity check: make sure the number of threads in the parent's
	// communicator is divisible by the number of new sub-groups.
	if ( parent_nt_in % parent_n_way != 0 )
	{
		printf( "Assertion failed: parent_nt_in (%d) <mod> parent_n_way (%d) != 0\n",
		        ( int )parent_nt_in, ( int )parent_n_way );
		bli_abort();
	}

	//dim_t child_nt_in   = bli_cntl_calc_num_threads_in( rntm, cntl_chl );
	//dim_t child_n_way   = bli_rntm_ways_for( bszid_chl, rntm );
	const dim_t child_nt_in   = parent_nt_in;
	const dim_t child_n_way   = parent_nt_in;
	const dim_t child_comm_id = parent_comm_id % child_nt_in;
	const dim_t child_work_id = child_comm_id / ( child_nt_in / child_n_way );

	bli_thread_barrier( thread_par );

	// NOTE: Recall that parent_comm_id == child_comm_id, so checking for the
	// parent's chief-ness is equivalent to checking for chief-ness in the new
	// about-to-be-created communicator group.
	thrcomm_t* new_comm = NULL;
	if ( bli_thread_am_ochief( thread_par ) )
		new_comm = bli_thrcomm_create( rntm, child_nt_in );

	// Broadcast the new thrcomm_t address to the other threads in the
	// parent's group.
	new_comm = bli_thread_broadcast( thread_par, new_comm );

	// All threads create a new thrinfo_t node using the communicator
	// that was created by their chief, as identified by parent_work_id.
	thrinfo_t* thread_chl = bli_thrinfo_create
	(
	  rntm,          // rntm
	  new_comm,      // ocomm
	  child_comm_id, // ocomm_id
	  child_n_way,   // n_way
	  child_work_id, // work_id
	  TRUE,          // free_comm
	  bszid_chl,     // bszid
	  NULL           // sub_node
	);

	bli_thread_barrier( thread_par );

	return thread_chl;
}

// -----------------------------------------------------------------------------

#if 0
void bli_thrinfo_grow_tree
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	cntl_t*    cntl_jc     = cntl;
	thrinfo_t* thrinfo_jc  = thread;

	bli_thrinfo_grow( rntm, cntl_jc, thrinfo_jc );

	// inside jc loop:
	cntl_t*    cntl_pc     = bli_cntl_sub_node( cntl_jc );
	thrinfo_t* thrinfo_pc  = bli_thrinfo_sub_node( thrinfo_jc );

	bli_thrinfo_grow( rntm, cntl_pc, thrinfo_pc );

	// inside pc loop:
	cntl_t*    cntl_pb     = bli_cntl_sub_node( cntl_pc );
	thrinfo_t* thrinfo_pb  = bli_thrinfo_sub_node( thrinfo_pc );

	bli_thrinfo_grow( rntm, cntl_pb, thrinfo_pb );

	// after pb packing:
	cntl_t*    cntl_ic     = bli_cntl_sub_node( cntl_pb );
	thrinfo_t* thrinfo_ic  = bli_thrinfo_sub_node( thrinfo_pb );

	bli_thrinfo_grow( rntm, cntl_ic, thrinfo_ic );

	// -- main branch --

	// inside ic loop:
	cntl_t*    cntl_pa     = bli_cntl_sub_node( cntl_ic );
	thrinfo_t* thrinfo_pa  = bli_thrinfo_sub_node( thrinfo_ic );

	bli_thrinfo_grow( rntm, cntl_pa, thrinfo_pa );

	// after pa packing:
	cntl_t*    cntl_jr     = bli_cntl_sub_node( cntl_pa );
	thrinfo_t* thrinfo_jr  = bli_thrinfo_sub_node( thrinfo_pa );

	bli_thrinfo_grow( rntm, cntl_jr, thrinfo_jr );

	// inside jr loop:
	//cntl_t*    cntl_ir     = bli_cntl_sub_node( cntl_jr );
	//thrinfo_t* thrinfo_ir  = bli_thrinfo_sub_node( thrinfo_jr );

	// -- trsm branch --

	// inside ic loop:
	cntl_t*    cntl_pa0    = bli_cntl_sub_prenode( cntl_ic );
	thrinfo_t* thrinfo_pa0 = bli_thrinfo_sub_prenode( thrinfo_ic );

	bli_thrinfo_grow( rntm, cntl_pa0, thrinfo_pa0 );

	// after pa packing:
	cntl_t*    cntl_jr0    = bli_cntl_sub_node( cntl_pa0 );
	thrinfo_t* thrinfo_jr0 = bli_thrinfo_sub_node( thrinfo_pa0 );

	bli_thrinfo_grow( rntm, cntl_jr0, thrinfo_jr0 );

	// inside jr loop:
	//cntl_t*    cntl_ir0   = bli_cntl_sub_node( cntl_jr0 );
	//thrinfo_t* thrinfo_ir0= bli_thrinfo_sub_node( thrinfo_jr0 );
}

void bli_thrinfo_grow_tree_ic
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	cntl_t*    cntl_ic     = cntl;
	thrinfo_t* thrinfo_ic  = thread;

	bli_thrinfo_grow( rntm, cntl_ic, thrinfo_ic );

	// -- main branch --

	// inside ic loop:
	cntl_t*    cntl_pa     = bli_cntl_sub_node( cntl_ic );
	thrinfo_t* thrinfo_pa  = bli_thrinfo_sub_node( thrinfo_ic );

	bli_thrinfo_grow( rntm, cntl_pa, thrinfo_pa );

	// after pa packing:
	cntl_t*    cntl_jr     = bli_cntl_sub_node( cntl_pa );
	thrinfo_t* thrinfo_jr  = bli_thrinfo_sub_node( thrinfo_pa );

	bli_thrinfo_grow( rntm, cntl_jr, thrinfo_jr );

	// inside jr loop:
	//cntl_t*    cntl_ir     = bli_cntl_sub_node( cntl_jr );
	//thrinfo_t* thrinfo_ir  = bli_thrinfo_sub_node( thrinfo_jr );

	// -- trsm branch --

	// inside ic loop:
	cntl_t*    cntl_pa0    = bli_cntl_sub_prenode( cntl_ic );
	thrinfo_t* thrinfo_pa0 = bli_thrinfo_sub_prenode( thrinfo_ic );

	bli_thrinfo_grow( rntm, cntl_pa0, thrinfo_pa0 );

	// after pa packing:
	cntl_t*    cntl_jr0    = bli_cntl_sub_node( cntl_pa0 );
	thrinfo_t* thrinfo_jr0 = bli_thrinfo_sub_node( thrinfo_pa0 );

	bli_thrinfo_grow( rntm, cntl_jr0, thrinfo_jr0 );

	// inside jr loop:
	//cntl_t*    cntl_ir0   = bli_cntl_sub_node( cntl_jr0 );
	//thrinfo_t* thrinfo_ir0= bli_thrinfo_sub_node( thrinfo_jr0 );
}
#endif
