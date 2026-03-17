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

void* bli_thrcomm_bcast
     (
       dim_t      id,
       void*      to_send,
       thrcomm_t* comm
     )
{   
	if ( comm == NULL || comm->n_threads == 1 ) return to_send;

	if ( id == 0 ) comm->sent_object = to_send;

	bli_thrcomm_barrier( id, comm );
	void* object = comm->sent_object;
	bli_thrcomm_barrier( id, comm );

	return object;
}

// Use __sync_* builtins (assumed available) if __atomic_* ones are not present.
#ifndef __ATOMIC_RELAXED

#define __ATOMIC_RELAXED
#define __ATOMIC_ACQUIRE
#define __ATOMIC_RELEASE
#define __ATOMIC_ACQ_REL

#define __atomic_load_n(ptr, constraint) \
    __sync_fetch_and_add(ptr, 0)
#define __atomic_add_fetch(ptr, value, constraint) \
    __sync_add_and_fetch(ptr, value)
#define __atomic_fetch_add(ptr, value, constraint) \
    __sync_fetch_and_add(ptr, value)
#define __atomic_fetch_xor(ptr, value, constraint) \
    __sync_fetch_and_xor(ptr, value)

#endif

void bli_thrcomm_barrier_atomic( dim_t t_id, thrcomm_t* comm )
{
	// Return early if the comm is NULL or if there is only one
	// thread participating.
	if ( comm == NULL || comm->n_threads == 1 ) return;

	// Read the "sense" variable. This variable is akin to a unique ID for
	// the current barrier. The first n-1 threads will spin on this variable
	// until it changes. The sense variable gets incremented by the last
	// thread to enter the barrier, just before it exits. But it turns out
	// that you don't need many unique IDs before you can wrap around. In 
	// fact, if everything else is working, a binary variable is sufficient,
	// which is what we do here (i.e., 0 is incremented to 1, which is then
	// decremented back to 0, and so forth).
	gint_t orig_sense = __atomic_load_n( &comm->barrier_sense, __ATOMIC_RELAXED );

	// Register ourselves (the current thread) as having arrived by
	// incrementing the barrier_threads_arrived variable. We must perform
	// this increment (and a subsequent read) atomically.
	dim_t my_threads_arrived =
	__atomic_add_fetch( &comm->barrier_threads_arrived, 1, __ATOMIC_ACQ_REL );

	// If the current thread was the last thread to have arrived, then
	// it will take actions that effectively ends and resets the barrier.
	if ( my_threads_arrived == comm->n_threads )
	{
		// Reset the variable tracking the number of threads that have arrived
		// to zero (which returns the barrier to the "empty" state. Then
		// atomically toggle the barrier sense variable. This will signal to
		// the other threads (which are spinning in the branch elow) that it
		// is now safe to exit the barrier.
		comm->barrier_threads_arrived = 0;
		__atomic_fetch_xor( &comm->barrier_sense, 1, __ATOMIC_RELEASE );
	}
	else
	{
		// If the current thread is NOT the last thread to have arrived, then
		// it spins on the sense variable until that sense variable changes at
		// which time these threads will exit the barrier.
		while ( __atomic_load_n( &comm->barrier_sense, __ATOMIC_ACQUIRE ) == orig_sense )
			; // Empty loop body.
	}
}

