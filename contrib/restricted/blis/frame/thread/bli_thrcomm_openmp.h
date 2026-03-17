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

#ifndef BLIS_THRCOMM_OPENMP_H
#define BLIS_THRCOMM_OPENMP_H

// Define thrcomm_t for situations when OpenMP multithreading is enabled.
#ifdef BLIS_ENABLE_OPENMP

#include <omp.h>

// Define thrcomm_t for tree barriers and non-tree barriers.
#ifdef BLIS_TREE_BARRIER
struct barrier_s
{   
	int               arity;
	int               count;
	struct barrier_s* dad;
	volatile int      signal;
};  
typedef struct barrier_s barrier_t;

struct thrcomm_s
{   
	void*       sent_object;
	dim_t       n_threads;
	barrier_t** barriers;
}; 
#else
struct thrcomm_s
{
	void*  sent_object;
	dim_t  n_threads;

	// NOTE: barrier_sense was originally a gint_t-based bool_t, but upon
	// redefining bool_t as bool we discovered that some gcc __atomic built-ins
	// don't allow the use of bool for the variables being operated upon.
	// (Specifically, this was observed of __atomic_fetch_xor(), but it likely
	// applies to all other related built-ins.) Thus, we get around this by
	// redefining barrier_sense as a gint_t.
	//volatile gint_t  barrier_sense;
	gint_t barrier_sense;
	dim_t  barrier_threads_arrived;
};
#endif

typedef struct thrcomm_s thrcomm_t;

// Prototypes specific to tree barriers.
#ifdef BLIS_TREE_BARRIER
barrier_t* bli_thrcomm_tree_barrier_create( int num_threads, int arity, barrier_t** leaves, int leaf_index );
void        bli_thrcomm_tree_barrier_free( barrier_t* barrier );
void        bli_thrcomm_tree_barrier( barrier_t* barack );
#endif

#endif

#endif

