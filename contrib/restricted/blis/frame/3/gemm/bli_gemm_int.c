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

void bli_gemm_int
     (
       obj_t*  alpha,
       obj_t*  a,
       obj_t*  b,
       obj_t*  beta,
       obj_t*  c,
       cntx_t* cntx,
       rntm_t* rntm,
       cntl_t* cntl,
       thrinfo_t* thread
     )
{
	obj_t        a_local;
	obj_t        b_local;
	obj_t        c_local;
	gemm_var_oft f;

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
		bli_gemm_basic_check( alpha, a, b, beta, c, cntx );

	// If C has a zero dimension, return early.
	if ( bli_obj_has_zero_dim( c ) )
	{
		return;
	}

	// If A or B has a zero dimension, scale C by beta and return early.
	if ( bli_obj_has_zero_dim( a ) ||
	     bli_obj_has_zero_dim( b ) )
	{
		if ( bli_thread_am_ochief( thread ) )
			bli_scalm( beta, c );
		bli_thread_barrier( thread );
		return;
	}

	// If A or B is marked as being filled with zeros, scale C by beta and
	// return early.
	if ( bli_obj_is_zeros( a ) ||
	     bli_obj_is_zeros( b ) )
	{
		// This should never execute.
		bli_abort();

		if ( bli_thread_am_ochief( thread ) )
			bli_scalm( beta, c );
		bli_thread_barrier( thread );
		return;
	}

	// Alias A, B, and C in case we need to update attached scalars.
	bli_obj_alias_to( a, &a_local );
	bli_obj_alias_to( b, &b_local );
	bli_obj_alias_to( c, &c_local );

	// If alpha is non-unit, typecast and apply it to the scalar attached
	// to B.
	if ( !bli_obj_equals( alpha, &BLIS_ONE ) )
	{
		bli_obj_scalar_apply_scalar( alpha, &b_local );
	}

	// If beta is non-unit, typecast and apply it to the scalar attached
	// to C.
	if ( !bli_obj_equals( beta, &BLIS_ONE ) )
	{
		bli_obj_scalar_apply_scalar( beta, &c_local );
	}

	// Create the next node in the thrinfo_t structure.
	bli_thrinfo_grow( rntm, cntl, thread );

	// Extract the function pointer from the current control tree node.
	f = bli_cntl_var_func( cntl );

	// Somewhat hackish support for 4m1b method implementation.
	{
		ind_t im = bli_cntx_method( cntx );

		if ( im != BLIS_NAT )
		{
			if ( im == BLIS_4M1B )
			if ( f == bli_gemm_ker_var2 ) f = bli_gemm4mb_ker_var2;
		}
	}

	// Invoke the variant.
	f
	(
	  &a_local,
	  &b_local,
	  &c_local,
	  cntx,
	  rntm,
	  cntl,
	  thread
	);
}

