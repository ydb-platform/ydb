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

void bli_packm_int
     (
       obj_t*  a,
       obj_t*  p,
       cntx_t* cntx,
       cntl_t* cntl,
       thrinfo_t* thread
     )
{
	bli_init_once();

	packm_var_oft f;

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
		bli_packm_int_check( a, p, cntx );

	// Sanity check; A should never have a zero dimension. If we must support
	// it, then we should fold it into the next alias-and-early-exit block.
	//if ( bli_obj_has_zero_dim( a ) ) bli_abort();

	// Let us now check to see if the object has already been packed. First
	// we check if it has been packed to an unspecified (row or column)
	// format, in which case we can return, since by now aliasing has already
	// taken place in packm_init().
	// NOTE: The reason we don't need to even look at the control tree in
	// this case is as follows: an object's pack status is only set to
	// BLIS_PACKED_UNSPEC for situations when the actual format used is
	// not important, as long as its packed into contiguous rows or
	// contiguous columns. A good example of this is packing for matrix
	// operands in the level-2 operations.
	if ( bli_obj_pack_schema( a ) == BLIS_PACKED_UNSPEC )
	{
		return;
	}

	// At this point, we can be assured that cntl is not NULL. Now we check
	// if the object has already been packed to the desired schema (as en-
	// coded in the control tree). If so, we can return, as above.
	// NOTE: In most cases, an object's pack status will be BLIS_NOT_PACKED
	// and thus packing will be called for (but in some cases packing has
	// already taken place, or does not need to take place, and so that will
	// be indicated by the pack status). Also, not all combinations of
	// current pack status and desired pack schema are valid.
	if ( bli_obj_pack_schema( a ) == bli_cntl_packm_params_pack_schema( cntl ) )
	{
		return;
	}

	// If the object is marked as being filled with zeros, then we can skip
	// the packm operation entirely.
	if ( bli_obj_is_zeros( a ) )
	{
		return;
	}

	// Extract the function pointer from the current control tree node.
	f = bli_cntl_packm_params_var_func( cntl );

	// Invoke the variant with kappa_use.
	f
	(
	  a,
	  p,
	  cntx,
	  cntl,
	  thread
	);
}

