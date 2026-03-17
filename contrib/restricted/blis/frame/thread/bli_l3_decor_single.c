/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018, Advanced Micro Devices, Inc.

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

#ifndef BLIS_ENABLE_MULTITHREADING

void bli_l3_thread_decorator
     (
       l3int_t    func,
       opid_t     family,
       obj_t*     alpha,
       obj_t*     a,
       obj_t*     b,
       obj_t*     beta,
       obj_t*     c,
       cntx_t*    cntx,
       rntm_t*    rntm,
       cntl_t*    cntl
     )
{
	// This is part of a hack to support mixed domain in bli_gemm_front().
	// Sometimes we need to specify a non-standard schema for A and B, and
	// we decided to transmit them via the schema field in the obj_t's
	// rather than pass them in as function parameters. Once the values
	// have been read, we immediately reset them back to their expected
	// values for unpacked objects.
	pack_t schema_a = bli_obj_pack_schema( a );
	pack_t schema_b = bli_obj_pack_schema( b );
	bli_obj_set_pack_schema( BLIS_NOT_PACKED, a );
	bli_obj_set_pack_schema( BLIS_NOT_PACKED, b );

	// For sequential execution, we use only one thread.
	const dim_t n_threads = 1;

	// NOTE: The sba was initialized in bli_init().

	// Check out an array_t from the small block allocator. This is done
	// with an internal lock to ensure only one application thread accesses
	// the sba at a time. bli_sba_checkout_array() will also automatically
	// resize the array_t, if necessary.
	array_t* restrict array = bli_sba_checkout_array( n_threads );

	// Access the pool_t* for thread 0 and embed it into the rntm. We do
	// this up-front only so that we can create the global comm below.
	bli_sba_rntm_set_pool( 0, array, rntm );

	// Set the packing block allocator field of the rntm.
	bli_membrk_rntm_set_membrk( rntm );

	// Allcoate a global communicator for the root thrinfo_t structures.
	thrcomm_t* restrict gl_comm = bli_thrcomm_create( rntm, n_threads );


	{
		// NOTE: We don't need to create another copy of the rntm_t since
		// it was already copied in one of the high-level oapi functions.
		rntm_t* restrict rntm_p = rntm;

		cntl_t*    cntl_use;
		thrinfo_t* thread;

		const dim_t tid = 0;

		// Use the thread id to access the appropriate pool_t* within the
		// array_t, and use it to set the sba_pool field within the rntm_t.
		// If the pool_t* element within the array_t is NULL, it will first
		// be allocated/initialized.
		// NOTE: This is commented out because, in the single-threaded case,
		// this is redundant since it's already been done above.
		//bli_sba_rntm_set_pool( tid, array, rntm_p );

		// NOTE: Unlike with the _openmp.c and _pthreads.c variants, we don't
		// need to alias objects for A, B, and C since they were already aliased
		// in bli_*_front(). However, we may add aliasing here in the future so
		// that, with all three (_single.c, _openmp.c, _pthreads.c) implementations
		// consistently providing local aliases, we can then eliminate aliasing
		// elsewhere.

		// Create a default control tree for the operation, if needed.
		bli_l3_cntl_create_if( family, schema_a, schema_b,
		                       a, b, c, rntm_p, cntl, &cntl_use );

		// Create the root node of the thread's thrinfo_t structure.
		bli_l3_thrinfo_create_root( tid, gl_comm, rntm_p, cntl_use, &thread );

		func
		(
		  alpha,
		  a,
		  b,
		  beta,
		  c,
		  cntx,
		  rntm_p,
		  cntl_use,
		  thread
		);

		// Free the thread's local control tree.
		bli_l3_cntl_free( rntm_p, cntl_use, thread );

		// Free the current thread's thrinfo_t structure.
		bli_l3_thrinfo_free( rntm_p, thread );
	}

	// We shouldn't free the global communicator since it was already freed
	// by the global communicator's chief thread in bli_l3_thrinfo_free()
	// (called above).

	// Check the array_t back into the small block allocator. Similar to the
	// check-out, this is done using a lock embedded within the sba to ensure
	// mutual exclusion.
	bli_sba_checkin_array( array );
}

#endif

