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

#ifdef BLIS_ENABLE_OPENMP

// Define a dummy function bli_l3_thread_entry(), which is needed in the
// pthreads version, so that when building Windows DLLs (with OpenMP enabled
// or no multithreading) we don't risk having an unresolved symbol.
void* bli_l3_thread_entry( void* data_void ) { return NULL; }

//#define PRINT_THRINFO

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

	// Query the total number of threads from the rntm_t object.
	const dim_t n_threads = bli_rntm_num_threads( rntm );

	#ifdef PRINT_THRINFO
	thrinfo_t** threads = bli_malloc_intl( n_threads * sizeof( thrinfo_t* ) );
	#endif

	// NOTE: The sba was initialized in bli_init().

	// Check out an array_t from the small block allocator. This is done
	// with an internal lock to ensure only one application thread accesses
	// the sba at a time. bli_sba_checkout_array() will also automatically
	// resize the array_t, if necessary.
	array_t* restrict array = bli_sba_checkout_array( n_threads );

	// Access the pool_t* for thread 0 and embed it into the rntm. We do
	// this up-front only so that we have the rntm_t.sba_pool field
	// initialized and ready for the global communicator creation below.
	bli_sba_rntm_set_pool( 0, array, rntm );

	// Set the packing block allocator field of the rntm. This will be
	// inherited by all of the child threads when they make local copies of
	// the rntm below.
	bli_membrk_rntm_set_membrk( rntm );

	// Allocate a global communicator for the root thrinfo_t structures.
	thrcomm_t* restrict gl_comm = bli_thrcomm_create( rntm, n_threads );


	_Pragma( "omp parallel num_threads(n_threads)" )
	{
		// Create a thread-local copy of the master thread's rntm_t. This is
		// necessary since we want each thread to be able to track its own
		// small block pool_t as it executes down the function stack.
		rntm_t           rntm_l = *rntm;
		rntm_t* restrict rntm_p = &rntm_l;

		// Query the thread's id from OpenMP.
		const dim_t tid = omp_get_thread_num();

		// Check for a somewhat obscure OpenMP thread-mistmatch issue.
		bli_l3_thread_decorator_thread_check( n_threads, tid, gl_comm, rntm_p );

		// Use the thread id to access the appropriate pool_t* within the
		// array_t, and use it to set the sba_pool field within the rntm_t.
		// If the pool_t* element within the array_t is NULL, it will first
		// be allocated/initialized.
		bli_sba_rntm_set_pool( tid, array, rntm_p );


		obj_t      a_t, b_t, c_t;
		cntl_t*    cntl_use;
		thrinfo_t* thread;

		// Alias thread-local copies of A, B, and C. These will be the objects
		// we pass down the algorithmic function stack. Making thread-local
		// aliases is highly recommended in case a thread needs to change any
		// of the properties of an object without affecting other threads'
		// objects.
		bli_obj_alias_to( a, &a_t );
		bli_obj_alias_to( b, &b_t );
		bli_obj_alias_to( c, &c_t );

		// Create a default control tree for the operation, if needed.
		bli_l3_cntl_create_if( family, schema_a, schema_b,
		                       &a_t, &b_t, &c_t, rntm_p, cntl, &cntl_use );

		// Create the root node of the current thread's thrinfo_t structure.
		bli_l3_thrinfo_create_root( tid, gl_comm, rntm_p, cntl_use, &thread );

#if 1
		func
		(
		  alpha,
		  &a_t,
		  &b_t,
		  beta,
		  &c_t,
		  cntx,
		  rntm_p,
		  cntl_use,
		  thread
		);
#else
		bli_thrinfo_grow_tree
		(
		  rntm_p,
		  cntl_use,
		  thread
		);
#endif

		// Free the thread's local control tree.
		bli_l3_cntl_free( rntm_p, cntl_use, thread );

		#ifdef PRINT_THRINFO
		threads[tid] = thread;
		#else
		// Free the current thread's thrinfo_t structure.
		bli_l3_thrinfo_free( rntm_p, thread );
		#endif
	}

	// We shouldn't free the global communicator since it was already freed
	// by the global communicator's chief thread in bli_l3_thrinfo_free()
	// (called above).

	#ifdef PRINT_THRINFO
	if ( family != BLIS_TRSM ) bli_l3_thrinfo_print_gemm_paths( threads );
	else                       bli_l3_thrinfo_print_trsm_paths( threads );
	exit(1);
	#endif

	// Check the array_t back into the small block allocator. Similar to the
	// check-out, this is done using a lock embedded within the sba to ensure
	// mutual exclusion.
	bli_sba_checkin_array( array );
}

// -----------------------------------------------------------------------------

void bli_l3_thread_decorator_thread_check
     (
       dim_t      n_threads,
       dim_t      tid,
       thrcomm_t* gl_comm,
       rntm_t*    rntm
     )
{
	dim_t n_threads_real = omp_get_num_threads();

	// Check if the number of OpenMP threads created within this parallel
	// region is different from the number of threads that were requested
	// of BLIS. This inequality may trigger when, for example, the
	// following conditions are satisfied:
	// - an application is executing an OpenMP parallel region in which
	//   BLIS is invoked,
	// - BLIS is configured for multithreading via OpenMP,
	// - OMP_NUM_THREADS = t > 1,
	// - the number of threads requested of BLIS (regardless of method)
	//   is p <= t,
	// - OpenMP nesting is disabled.
	// In this situation, the application spawns t threads. Each application
	// thread calls gemm (for example). Each gemm will attempt to spawn p
	// threads via OpenMP. However, since nesting is disabled, the OpenMP
	// implementation finds that t >= p threads are already spawned, and
	// thus it doesn't spawn *any* additional threads for each gemm.
	if ( n_threads_real != n_threads )
	{
		// If the number of threads active in the current region is not
		// equal to the number requested of BLIS, we then only continue
		// if the number of threads in the current region is 1. If, for
		// example, BLIS requested 4 threads but only got 3, then we
		// abort().
		//if ( tid == 0 )
		//{
			if ( n_threads_real != 1 )
			{
				bli_print_msg( "A different number of threads was "
				               "created than was requested.",
				               __FILE__, __LINE__ );
				bli_abort();
			}

			//n_threads = 1; // not needed since it has no effect?
			bli_thrcomm_init( 1, gl_comm );
			bli_rntm_set_num_threads_only( 1, rntm );
			bli_rntm_set_ways_only( 1, 1, 1, 1, 1, rntm );
		//}

		// Synchronize all threads and continue.
		_Pragma( "omp barrier" )
	}
}

#endif

