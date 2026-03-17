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

#ifdef BLIS_ENABLE_OPENMP

thrcomm_t* bli_thrcomm_create( rntm_t* rntm, dim_t n_threads )
{
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_thrcomm_create(): " );
	#endif

	thrcomm_t* comm = bli_sba_acquire( rntm, sizeof(thrcomm_t) );

	bli_thrcomm_init( n_threads, comm );

	return comm;
}

void bli_thrcomm_free( rntm_t* rntm, thrcomm_t* comm )
{
	if ( comm == NULL ) return;

	bli_thrcomm_cleanup( comm );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_thrcomm_free(): " );
	#endif

	bli_sba_release( rntm, comm );
}

#ifndef BLIS_TREE_BARRIER

void bli_thrcomm_init( dim_t n_threads, thrcomm_t* comm )
{
	if ( comm == NULL ) return;
	comm->sent_object = NULL;
	comm->n_threads = n_threads;
	comm->barrier_sense = 0;
	comm->barrier_threads_arrived = 0;
}


void bli_thrcomm_cleanup( thrcomm_t* comm )
{
	if ( comm == NULL ) return;
}

//'Normal' barrier for openmp
//barrier routine taken from art of multicore programming
void bli_thrcomm_barrier( dim_t t_id, thrcomm_t* comm )
{
#if 0
	if ( comm == NULL || comm->n_threads == 1 )
		return;
	gint_t my_sense = comm->barrier_sense;
	dim_t my_threads_arrived;

	_Pragma( "omp atomic capture" )
		my_threads_arrived = ++(comm->barrier_threads_arrived);

	if ( my_threads_arrived == comm->n_threads )
	{
		comm->barrier_threads_arrived = 0;
		comm->barrier_sense = !comm->barrier_sense;
	}
	else
	{
		volatile gint_t* listener = &comm->barrier_sense;
		while ( *listener == my_sense ) {}
	}
#endif
	bli_thrcomm_barrier_atomic( t_id, comm );
}

#else

void bli_thrcomm_init( dim_t n_threads, thrcomm_t* comm )
{
	if ( comm == NULL ) return;
	comm->sent_object = NULL;
	comm->n_threads = n_threads;
	comm->barriers = bli_malloc_intl( sizeof( barrier_t* ) * n_threads );
	bli_thrcomm_tree_barrier_create( n_threads, BLIS_TREE_BARRIER_ARITY, comm->barriers, 0 );
}

//Tree barrier used for Intel Xeon Phi
barrier_t* bli_thrcomm_tree_barrier_create( int num_threads, int arity, barrier_t** leaves, int leaf_index )
{
	barrier_t* me = bli_malloc_intl( sizeof(barrier_t) );

	me->dad = NULL;
	me->signal = 0;

	// Base Case
	if ( num_threads <= arity )
	{
		//Now must be registered as a leaf
		for ( int i = 0; i < num_threads; i++ )
		{
			leaves[ leaf_index + i ] = me;
		}
		me->count = num_threads;
		me->arity = num_threads;
	}
	else
	{
		// Otherwise this node has children
		int threads_per_kid = num_threads / arity;
		int defecit = num_threads - threads_per_kid * arity;

		for ( int i = 0; i < arity; i++ )
		{
			int threads_this_kid = threads_per_kid;
			if ( i < defecit ) threads_this_kid++;

			barrier_t* kid = bli_thrcomm_tree_barrier_create( threads_this_kid, arity, leaves, leaf_index );
			kid->dad = me;

			leaf_index += threads_this_kid;
		}  
		me->count = arity;
		me->arity = arity;
	}  

	return me;
}

void bli_thrcomm_cleanup( thrcomm_t* comm )
{
	if ( comm == NULL ) return;
	for ( dim_t i = 0; i < comm->n_threads; i++ )
	{
	   bli_thrcomm_tree_barrier_free( comm->barriers[i] );
	}
	bli_free_intl( comm->barriers );
}

void bli_thrcomm_tree_barrier_free( barrier_t* barrier )
{
	if ( barrier == NULL )
		return;
	barrier->count--;
	if ( barrier->count == 0 )
	{
		bli_thrcomm_tree_barrier_free( barrier->dad );
		bli_free_intl( barrier );
	}
	return;
}

void bli_thrcomm_barrier( dim_t t_id, thrcomm_t* comm )
{
	bli_thrcomm_tree_barrier( comm->barriers[t_id] );
}

void bli_thrcomm_tree_barrier( barrier_t* barack )
{
	int my_signal = barack->signal;
	int my_count;

	_Pragma( "omp atomic capture" )
		my_count = barack->count--;

	if ( my_count == 1 )
	{
		if ( barack->dad != NULL )
		{
			bli_thrcomm_tree_barrier( barack->dad );
		}
		barack->count = barack->arity;
		barack->signal = !barack->signal;
	}
	else
	{
		volatile int* listener = &barack->signal;
		while ( *listener == my_signal ) {}
	}
}

#endif

#endif

