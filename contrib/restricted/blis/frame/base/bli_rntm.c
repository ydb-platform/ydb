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

// The global rntm_t structure, which holds the global thread settings
// along with a few other key parameters.
rntm_t global_rntm;

// A mutex to allow synchronous access to global_rntm.
bli_pthread_mutex_t global_rntm_mutex = BLIS_PTHREAD_MUTEX_INITIALIZER;

// ----------------------------------------------------------------------------

void bli_rntm_init_from_global( rntm_t* rntm )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	// Acquire the mutex protecting global_rntm.
	bli_pthread_mutex_lock( &global_rntm_mutex );

	*rntm = global_rntm;

	// Release the mutex protecting global_rntm.
	bli_pthread_mutex_unlock( &global_rntm_mutex );
}

// -----------------------------------------------------------------------------

void bli_rntm_set_ways_for_op
     (
       opid_t  l3_op,
       side_t  side,
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     )
{
	// Set the number of ways for each loop, if needed, depending on what
	// kind of information is already stored in the rntm_t object.
	bli_rntm_set_ways_from_rntm( m, n, k, rntm );

#if 0
printf( "bli_rntm_set_ways_for_op()\n" );
bli_rntm_print( rntm );
#endif

	// Now modify the number of ways, if necessary, based on the operation.
	if ( l3_op == BLIS_TRMM ||
	     l3_op == BLIS_TRSM )
	{
		dim_t jc = bli_rntm_jc_ways( rntm );
		dim_t pc = bli_rntm_pc_ways( rntm );
		dim_t ic = bli_rntm_ic_ways( rntm );
		dim_t jr = bli_rntm_jr_ways( rntm );
		dim_t ir = bli_rntm_ir_ways( rntm );

		// Notice that, if we do need to update the ways, we don't need to
		// update the num_threads field since we only reshuffle where the
		// parallelism is extracted, not the total amount of parallelism.

		if ( l3_op == BLIS_TRMM )
		{
			// We reconfigure the parallelism extracted from trmm_r due to a
			// dependency in the jc loop. (NOTE: This dependency does not exist
			// for trmm3.)
			if ( bli_is_left( side ) )
			{
				bli_rntm_set_ways_only
				(
				  jc,
				  pc,
				  ic,
				  jr,
				  ir,
				  rntm
				);
			}
			else // if ( bli_is_right( side ) )
			{
				bli_rntm_set_ways_only
				(
				  1,
				  pc,
				  ic,
				  jr * jc,
				  ir,
				  rntm
				);
			}
		}
		else if ( l3_op == BLIS_TRSM )
		{
//printf( "bli_rntm_set_ways_for_op(): jc%d ic%d jr%d\n", (int)jc, (int)ic, (int)jr );
			if ( bli_is_left( side ) )
			{
				bli_rntm_set_ways_only
				(
				  jc,
				  1,
				  ic * pc,
				  jr * ir,
				  1,
				  rntm
				);
			}
			else // if ( bli_is_right( side ) )
			{
				bli_rntm_set_ways_only
				(
				  1,
				  1,
				  ic * pc * jc * ir * jr,
				  1,
				  1,
				  rntm
				);
			}
		}
	}
}

void bli_rntm_set_ways_from_rntm
     (
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     )
{
	dim_t nt = bli_rntm_num_threads( rntm );

	dim_t jc = bli_rntm_jc_ways( rntm );
	dim_t pc = bli_rntm_pc_ways( rntm );
	dim_t ic = bli_rntm_ic_ways( rntm );
	dim_t jr = bli_rntm_jr_ways( rntm );
	dim_t ir = bli_rntm_ir_ways( rntm );

	bool  auto_factor = FALSE;

#ifdef BLIS_ENABLE_MULTITHREADING

	bool  nt_set   = FALSE;
	bool  ways_set = FALSE;

	// If the rntm was fed in as a copy of the global runtime via
	// bli_rntm_init_from_global(), we know that either:
	// - the num_threads field is -1 and all of the ways are -1;
	// - the num_threads field is -1 and all of the ways are set;
	// - the num_threads field is set and all of the ways are -1.
	// However, we can't be sure that a user-provided rntm_t isn't
	// initialized uncleanly. So here we have to enforce some rules
	// to get the rntm_t into a predictable state.

	// First, we establish whether or not the number of threads is set.
	if ( nt > 0 ) nt_set = TRUE;

	// Take this opportunity to set the auto_factor field.
	if ( nt_set ) auto_factor = TRUE;

	// Next, we establish whether or not any of the ways of parallelism
	// for each loop were set. If any of the ways are set (positive), we
	// then we assume the user wanted to use those positive values and
	// default the non-positive values to 1.
	if ( jc > 0 || pc > 0 || ic > 0 || jr > 0 || ir > 0 )
	{
		ways_set = TRUE;

		if ( jc < 1 ) jc = 1;
		if ( pc < 1 ) pc = 1;
		if ( ic < 1 ) ic = 1;
		if ( jr < 1 ) jr = 1;
		if ( ir < 1 ) ir = 1;
	}

	// Now we use the values of nt_set and ways_set to determine how to
	// interpret the original values we found in the rntm_t object.

	if ( ways_set == TRUE )
	{
		// If the ways were set, then we use the values that were given
		// and interpreted above (we set any non-positive value to 1).
		// The only thing left to do is calculate the correct number of
		// threads.

		nt = jc * pc * ic * jr * ir;
	}
	else if ( ways_set == FALSE && nt_set == TRUE )
	{
		// If the ways were not set but the number of thread was set, then
		// we attempt to automatically generate a thread factorization that
		// will work given the problem size.

#ifdef BLIS_DISABLE_AUTO_PRIME_NUM_THREADS
		// If use of prime numbers is disallowed for automatic thread
		// factorizations, we first check if the number of threads requested
		// is prime. If it is prime, and it exceeds a minimum threshold, then
		// we reduce the number of threads by one so that the number is not
		// prime. This will allow for automatic thread factorizations to span
		// two dimensions (loops), which tends to be more efficient.
		if ( bli_is_prime( nt ) && BLIS_NT_MAX_PRIME < nt ) nt -= 1;
#endif

		pc = 1;

		//printf( "m n = %d %d  BLIS_THREAD_RATIO_M _N = %d %d\n", (int)m, (int)n, (int)BLIS_THREAD_RATIO_M, (int)BLIS_THREAD_RATIO_N );

		bli_thread_partition_2x2( nt, m*BLIS_THREAD_RATIO_M,
		                              n*BLIS_THREAD_RATIO_N, &ic, &jc );

		//printf( "jc ic = %d %d\n", (int)jc, (int)ic );

		for ( ir = BLIS_THREAD_MAX_IR ; ir > 1 ; ir-- )
		{
			if ( ic % ir == 0 ) { ic /= ir; break; }
		}

		for ( jr = BLIS_THREAD_MAX_JR ; jr > 1 ; jr-- )
		{
			if ( jc % jr == 0 ) { jc /= jr; break; }
		}
	}
	else // if ( ways_set == FALSE && nt_set == FALSE )
	{
		// If neither the ways nor the number of threads were set, then
		// the rntm was not meaningfully changed since initialization,
		// and thus we'll default to single-threaded execution.

		nt = 1;
		jc = pc = ic = jr = ir = 1;
	}

#else

	// When multithreading is disabled, always set the rntm_t ways
	// values to 1.
	nt = 1;
	jc = pc = ic = jr = ir = 1;

#endif

	// Save the results back in the runtime object.
	bli_rntm_set_auto_factor_only( auto_factor, rntm );
	bli_rntm_set_num_threads_only( nt, rntm );
	bli_rntm_set_ways_only( jc, pc, ic, jr, ir, rntm );
}

void bli_rntm_set_ways_from_rntm_sup
     (
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     )
{
	dim_t nt = bli_rntm_num_threads( rntm );

	dim_t jc = bli_rntm_jc_ways( rntm );
	dim_t pc = bli_rntm_pc_ways( rntm );
	dim_t ic = bli_rntm_ic_ways( rntm );
	dim_t jr = bli_rntm_jr_ways( rntm );
	dim_t ir = bli_rntm_ir_ways( rntm );

	bool  auto_factor = FALSE;

#ifdef BLIS_ENABLE_MULTITHREADING

	bool  nt_set   = FALSE;
	bool  ways_set = FALSE;

	// If the rntm was fed in as a copy of the global runtime via
	// bli_rntm_init_from_global(), we know that either:
	// - the num_threads field is -1 and all of the ways are -1;
	// - the num_threads field is -1 and all of the ways are set;
	// - the num_threads field is set and all of the ways are -1.
	// However, we can't be sure that a user-provided rntm_t isn't
	// initialized uncleanly. So here we have to enforce some rules
	// to get the rntm_t into a predictable state.

	// First, we establish whether or not the number of threads is set.
	if ( nt > 0 ) nt_set = TRUE;

	// Take this opportunity to set the auto_factor field.
	if ( nt_set ) auto_factor = TRUE;

	// Next, we establish whether or not any of the ways of parallelism
	// for each loop were set. If any of the ways are set (positive), we
	// then we assume the user wanted to use those positive values and
	// default the non-positive values to 1.
	if ( jc > 0 || pc > 0 || ic > 0 || jr > 0 || ir > 0 )
	{
		ways_set = TRUE;

		if ( jc < 1 ) jc = 1;
		if ( pc < 1 ) pc = 1;
		if ( ic < 1 ) ic = 1;
		if ( jr < 1 ) jr = 1;
		if ( ir < 1 ) ir = 1;
	}

	// Now we use the values of nt_set and ways_set to determine how to
	// interpret the original values we found in the rntm_t object.

	if ( ways_set == TRUE )
	{
		// If the ways were set, then we use the values that were given
		// and interpreted above (we set any non-positive value to 1).
		// The only thing left to do is calculate the correct number of
		// threads.

		nt = jc * pc * ic * jr * ir;
	}
	else if ( ways_set == FALSE && nt_set == TRUE )
	{
		// If the ways were not set but the number of thread was set, then
		// we attempt to automatically generate a thread factorization that
		// will work given the problem size.

#ifdef BLIS_DISABLE_AUTO_PRIME_NUM_THREADS
		// If use of prime numbers is disallowed for automatic thread
		// factorizations, we first check if the number of threads requested
		// is prime. If it is prime, and it exceeds a minimum threshold, then
		// we reduce the number of threads by one so that the number is not
		// prime. This will allow for automatic thread factorizations to span
		// two dimensions (loops), which tends to be more efficient.
		if ( bli_is_prime( nt ) && BLIS_NT_MAX_PRIME < nt ) nt -= 1;
#endif

		pc = 1;

		//bli_thread_partition_2x2( nt, m*BLIS_THREAD_SUP_RATIO_M,
		//                              n*BLIS_THREAD_SUP_RATIO_N, &ic, &jc );
		bli_thread_partition_2x2( nt, m,
		                              n, &ic, &jc );

//printf( "bli_rntm_set_ways_from_rntm_sup(): jc = %d  ic = %d\n", (int)jc, (int)ic );
#if 0
		for ( ir = BLIS_THREAD_SUP_MAX_IR ; ir > 1 ; ir-- )
		{
			if ( ic % ir == 0 ) { ic /= ir; break; }
		}

		for ( jr = BLIS_THREAD_SUP_MAX_JR ; jr > 1 ; jr-- )
		{
			if ( jc % jr == 0 ) { jc /= jr; break; }
		}
#else
		ir = 1;
		jr = 1;

#endif
	}
	else // if ( ways_set == FALSE && nt_set == FALSE )
	{
		// If neither the ways nor the number of threads were set, then
		// the rntm was not meaningfully changed since initialization,
		// and thus we'll default to single-threaded execution.

		nt = 1;
		jc = pc = ic = jr = ir = 1;
	}

#else

	// When multithreading is disabled, always set the rntm_t ways
	// values to 1.
	nt = 1;
	jc = pc = ic = jr = ir = 1;

#endif

	// Save the results back in the runtime object.
	bli_rntm_set_auto_factor_only( auto_factor, rntm );
	bli_rntm_set_num_threads_only( nt, rntm );
	bli_rntm_set_ways_only( jc, pc, ic, jr, ir, rntm );
}

void bli_rntm_print
     (
       rntm_t* rntm
     )
{
	dim_t af = bli_rntm_auto_factor( rntm );

	dim_t nt = bli_rntm_num_threads( rntm );

	dim_t jc = bli_rntm_jc_ways( rntm );
	dim_t pc = bli_rntm_pc_ways( rntm );
	dim_t ic = bli_rntm_ic_ways( rntm );
	dim_t jr = bli_rntm_jr_ways( rntm );
	dim_t ir = bli_rntm_ir_ways( rntm );

	printf( "rntm contents    nt  jc  pc  ic  jr  ir\n" );
	printf( "autofac? %1d | %4d%4d%4d%4d%4d%4d\n", (int)af,
	                                               (int)nt, (int)jc, (int)pc,
	                                               (int)ic, (int)jr, (int)ir );
}

// -----------------------------------------------------------------------------

dim_t bli_rntm_calc_num_threads_in
     (
       bszid_t* restrict bszid_cur,
       rntm_t*  restrict rntm
     )
{
	/*                                     // bp algorithm:
	   bszid_t bszids[7] = { BLIS_NC,      // level 0: 5th loop
	                         BLIS_KC,      // level 1: 4th loop
	                         BLIS_NO_PART, // level 2: pack B
	                         BLIS_MC,      // level 3: 3rd loop
	                         BLIS_NO_PART, // level 4: pack A
	                         BLIS_NR,      // level 5: 2nd loop
	                         BLIS_MR,      // level 6: 1st loop
	                         BLIS_KR       // level 7: ukr loop

	                         ...           // pb algorithm:
	                         BLIS_NR,      // level 5: 2nd loop
	                         BLIS_MR,      // level 6: 1st loop
	                         BLIS_KR       // level 7: ukr loop
	                       }; */
	dim_t n_threads_in = 1;

	// Starting with the current element of the bszids array (pointed
	// to by bszid_cur), multiply all of the corresponding ways of
	// parallelism.
	for ( ; *bszid_cur != BLIS_KR; bszid_cur++ )
	{
		const bszid_t bszid = *bszid_cur;

		//if ( bszid == BLIS_KR ) break;

		// We assume bszid is in {NC,KC,MC,NR,MR,KR} if it is not
		// BLIS_NO_PART.
		if ( bszid != BLIS_NO_PART )
		{
			const dim_t cur_way = bli_rntm_ways_for( bszid, rntm );

			n_threads_in *= cur_way;
		}
	}

	return n_threads_in;
}

#if 0
	for ( ; *bszid_cur != BLIS_KR; bszid_cur++ )
	{
		const bszid_t bszid = *bszid_cur;
		dim_t         cur_way = 1;

		// We assume bszid is in {NC,KC,MC,NR,MR,KR} if it is not
		// BLIS_NO_PART.
		if ( bszid != BLIS_NO_PART )
			cur_way = bli_rntm_ways_for( bszid, rntm );
		else
			cur_way = 1;

		n_threads_in *= cur_way;
	}
#endif

