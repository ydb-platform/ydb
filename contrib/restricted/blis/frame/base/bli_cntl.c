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

cntl_t* bli_cntl_create_node
     (
       rntm_t* rntm,
       opid_t  family,
       bszid_t bszid,
       void_fp var_func,
       void*   params,
       cntl_t* sub_node
     )
{
	cntl_t* cntl;
	mem_t*  pack_mem;

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntl_create_node(): " );
	#endif

	// Allocate the cntl_t struct.
	cntl = bli_sba_acquire( rntm, sizeof( cntl_t ) );

	bli_cntl_set_family( family, cntl );
	bli_cntl_set_bszid( bszid, cntl );
	bli_cntl_set_var_func( var_func, cntl );
	bli_cntl_set_params( params, cntl );
	bli_cntl_set_sub_prenode( NULL, cntl );
	bli_cntl_set_sub_node( sub_node, cntl );

	// Query the address of the node's packed mem_t entry so we can initialize
	// key fields (to NULL or 0).
	// NOTE: This initialization is important, since it allows threads to
	// discern whether blocks have been acquired from the memory allocator.
	pack_mem = bli_cntl_pack_mem( cntl );
	bli_mem_clear( pack_mem );

	return cntl;
}

void bli_cntl_free_node
     (
       rntm_t* rntm,
       cntl_t* cntl
     )
{
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntl_free_node(): " );
	#endif

	bli_sba_release( rntm, cntl );
}

void bli_cntl_clear_node
     (
       cntl_t* cntl
     )
{
	mem_t* pack_mem;

	// Clear various fields in the control tree. Clearing these fields
	// actually is not needed, but we do it for debugging/completeness.
	bli_cntl_set_var_func( NULL, cntl );
	bli_cntl_set_params( NULL, cntl );
	bli_cntl_set_sub_prenode( NULL, cntl );
	bli_cntl_set_sub_node( NULL, cntl );

	// Clearing these fields is potentially more important if the control
	// tree is cached somewhere and reused.
	pack_mem = bli_cntl_pack_mem( cntl );
	bli_mem_clear( pack_mem );
}

// -----------------------------------------------------------------------------

void bli_cntl_free
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	if ( thread != NULL ) bli_cntl_free_w_thrinfo( rntm, cntl, thread );
	else                  bli_cntl_free_wo_thrinfo( rntm, cntl );
}

void bli_cntl_free_w_thrinfo
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	// Base case: simply return when asked to free NULL nodes.
	if ( cntl == NULL ) return;

	cntl_t* cntl_sub_prenode = bli_cntl_sub_prenode( cntl );
	cntl_t* cntl_sub_node    = bli_cntl_sub_node( cntl );
	void*   cntl_params      = bli_cntl_params( cntl );
	mem_t*  cntl_pack_mem    = bli_cntl_pack_mem( cntl );

	// Don't immediately dereference the prenode and subnode of the thrinfo_t
	// node. In some cases, the thrinfo_t tree is not built out all the way,
	// perhaps because there are more ways of parallelization than micropanels
	// of data in this dimension, or because the problem is small enough that
	// there is no gemm subproblem in bli_trsm_blk_var1(). Thus, we start with
	// NULL values for these variables and only dereference the fields of the
	// thrinfo_t struct if the thrinfo_t exists (ie: is non-NULL). We will also
	// have to check the thrinfo_t pointer for NULLness before using it below,
	// when checking if we need to free the pack_mem field of the cntl_t node
	// (see below).
	thrinfo_t* thread_sub_prenode = NULL;
	thrinfo_t* thread_sub_node    = NULL;

	if ( thread != NULL )
	{
		thread_sub_prenode = bli_thrinfo_sub_prenode( thread );
		thread_sub_node    = bli_thrinfo_sub_node( thread );
	}

	// Only recurse into prenode branch if it exists.
	if ( cntl_sub_prenode != NULL )
	{
		// Recursively free all memory associated with the sub-prenode and its
		// children.
		bli_cntl_free_w_thrinfo( rntm, cntl_sub_prenode, thread_sub_prenode );
	}

	// Only recurse into the child node if it exists.
	if ( cntl_sub_node != NULL )
	{
		// Recursively free all memory associated with the sub-node and its
		// children.
		bli_cntl_free_w_thrinfo( rntm, cntl_sub_node, thread_sub_node );
	}

	// Free the current node's params field, if it is non-NULL.
	if ( cntl_params != NULL )
	{
		#ifdef BLIS_ENABLE_MEM_TRACING
		printf( "bli_cntl_free_w_thrinfo(): " );
		#endif

		bli_sba_release( rntm, cntl_params );
	}

	// Release the current node's pack mem_t entry back to the memory
	// broker from which it originated, but only if the mem_t entry is
	// allocated, and only if the current thread is chief for its group.
	// Also note that we don't proceed with either of the above tests if
	// the thrinfo_t pointer is NULL. (See above for background on when
	// this can happen.)
	if ( thread != NULL )
	if ( bli_thread_am_ochief( thread ) )
	if ( bli_mem_is_alloc( cntl_pack_mem ) )
	{
		#ifdef BLIS_ENABLE_MEM_TRACING
		printf( "bli_cntl_free_w_thrinfo(): releasing mem pool block.\n" );
		#endif

		bli_membrk_release( rntm, cntl_pack_mem );
	}

	// Free the current node.
	bli_cntl_free_node( rntm, cntl );
}

void bli_cntl_free_wo_thrinfo
     (
       rntm_t* rntm,
       cntl_t* cntl
     )
{
	// Base case: simply return when asked to free NULL nodes.
	if ( cntl == NULL ) return;

	cntl_t* cntl_sub_prenode = bli_cntl_sub_prenode( cntl );
	cntl_t* cntl_sub_node    = bli_cntl_sub_node( cntl );
	void*   cntl_params      = bli_cntl_params( cntl );
	mem_t*  cntl_pack_mem    = bli_cntl_pack_mem( cntl );

	{
		// Recursively free all memory associated with the sub-prenode and its
		// children.
		bli_cntl_free_wo_thrinfo( rntm, cntl_sub_prenode );
	}

	{
		// Recursively free all memory associated with the sub-node and its
		// children.
		bli_cntl_free_wo_thrinfo( rntm, cntl_sub_node );
	}

	// Free the current node's params field, if it is non-NULL.
	if ( cntl_params != NULL )
	{
		bli_sba_release( rntm, cntl_params );
	}

	// Release the current node's pack mem_t entry back to the memory
	// broker from which it originated, but only if the mem_t entry is
	// allocated.
	if ( bli_mem_is_alloc( cntl_pack_mem ) )
	{
		bli_membrk_release( rntm, cntl_pack_mem );
	}

	// Free the current node.
	bli_cntl_free_node( rntm, cntl );
}

// -----------------------------------------------------------------------------

cntl_t* bli_cntl_copy
     (
       rntm_t* rntm,
       cntl_t* cntl
     )
{
	// Make a copy of the current node. Notice that the source node
	// should NOT have any allocated/cached mem_t entries, and that
	// bli_cntl_create_node() creates a node with a cleared mem_t
	// field.
	cntl_t* cntl_copy = bli_cntl_create_node
	(
      rntm,
	  bli_cntl_family( cntl ),
	  bli_cntl_bszid( cntl ),
	  bli_cntl_var_func( cntl ),
	  NULL, NULL
	);

	// Check the params field of the existing control tree; if it's non-NULL,
	// copy it.
	if ( bli_cntl_params( cntl ) != NULL )
	{
		// Detect the size of the params struct by reading the first field
		// as a uint64_t, and then allocate this many bytes for a new params
		// struct.
		uint64_t params_size = bli_cntl_params_size( cntl );
		void*    params_orig = bli_cntl_params( cntl );
		void*    params_copy = bli_sba_acquire( rntm, ( size_t )params_size );

		// Copy the original params struct to the new memory region.
		memcpy( params_copy, params_orig, params_size );

		// Save the address of the new params struct into the new control
		// tree node.
		bli_cntl_set_params( params_copy, cntl_copy );
	}

	// If the sub-prenode exists, copy it recursively.
	if ( bli_cntl_sub_prenode( cntl ) != NULL )
	{
		cntl_t* sub_prenode_copy = bli_cntl_copy
		(
		  rntm,
		  bli_cntl_sub_prenode( cntl )
		);

		// Save the address of the new sub-node (sub-tree) to the existing
		// node.
		bli_cntl_set_sub_prenode( sub_prenode_copy, cntl_copy );
	}

	// If the sub-node exists, copy it recursively.
	if ( bli_cntl_sub_node( cntl ) != NULL )
	{
		cntl_t* sub_node_copy = bli_cntl_copy
		(
		  rntm,
		  bli_cntl_sub_node( cntl )
		);

		// Save the address of the new sub-node (sub-tree) to the existing
		// node.
		bli_cntl_set_sub_node( sub_node_copy, cntl_copy );
	}

	// Return the address of the newly created node.
	return cntl_copy;
}

void bli_cntl_mark_family
     (
       opid_t  family,
       cntl_t* cntl
     )
{
	// This function sets the family field of all cntl tree nodes that are
	// children of cntl. It's used by bli_l3_cntl_create_if() after making
	// a copy of a user-given cntl tree, if the user provided one, to mark
	// the operation family, which is used to determine appropriate behavior
	// by various functions when executing the blocked variants.

	// Set the family of the root node.
	bli_cntl_set_family( family, cntl );

	// Recursively set the family field of the sub-tree rooted at the sub-node,
	// if it exists.
	if ( bli_cntl_sub_prenode( cntl ) != NULL )
	{
		bli_cntl_mark_family( family, bli_cntl_sub_prenode( cntl ) );
	}

	// Recursively set the family field of the sub-tree rooted at the prenode,
	// if it exists.
	if ( bli_cntl_sub_node( cntl ) != NULL )
	{
		bli_cntl_mark_family( family, bli_cntl_sub_node( cntl ) );
	}
}

// -----------------------------------------------------------------------------

dim_t bli_cntl_calc_num_threads_in
     (
       rntm_t* rntm,
       cntl_t* cntl
     )
{
	dim_t n_threads_in = 1;

	for ( ; cntl != NULL; cntl = bli_cntl_sub_node( cntl ) )
	{
		bszid_t bszid = bli_cntl_bszid( cntl );
		dim_t   cur_way;

		// We assume bszid is in {NC,KC,MC,NR,MR,KR} if it is not
		// BLIS_NO_PART.
		if ( bszid != BLIS_NO_PART )
			cur_way = bli_rntm_ways_for( bszid, rntm );
		else
			cur_way = 1;

		n_threads_in *= cur_way;
	}

	return n_threads_in;
}

