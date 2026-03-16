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

dir_t bli_l3_direct
     (
       obj_t*  a,
       obj_t*  b,
       obj_t*  c,
       cntl_t* cntl
     )
{
	// Query the operation family.
	opid_t family = bli_cntl_family( cntl );

	if      ( family == BLIS_GEMM ) return bli_gemm_direct( a, b, c );
	else if ( family == BLIS_HERK ) return bli_herk_direct( a, b, c );
	else if ( family == BLIS_TRMM ) return bli_trmm_direct( a, b, c );
	else if ( family == BLIS_TRSM ) return bli_trsm_direct( a, b, c );

	// This should never execute.
	return BLIS_FWD;
}

// -----------------------------------------------------------------------------

dir_t bli_gemm_direct
     (
       obj_t* a,
       obj_t* b,
       obj_t* c
     )
{
	// For gemm, movement may be forwards (or backwards).

	return BLIS_FWD;
}

dir_t bli_herk_direct
     (
       obj_t* a,
       obj_t* b,
       obj_t* c
     )
{
	// For herk, movement may be forwards (or backwards).

	return BLIS_FWD;
}

dir_t bli_trmm_direct
     (
       obj_t* a,
       obj_t* b,
       obj_t* c
     )
{
	dir_t direct;

	// For trmm, movement for the parameter cases is as follows:
	// - left,lower:  backwards
	// - left,upper:  forwards
	// - right,lower: forwards
	// - right,upper: backwards

	if ( bli_obj_root_is_triangular( a ) )
	{
		if ( bli_obj_root_is_lower( a ) ) direct = BLIS_BWD;
		else                              direct = BLIS_FWD;
	}
	else // if ( bli_obj_root_is_triangular( b ) )
	{
		if ( bli_obj_root_is_lower( b ) ) direct = BLIS_FWD;
		else                              direct = BLIS_BWD;
	}

	return direct;
}

dir_t bli_trsm_direct
     (
       obj_t* a,
       obj_t* b,
       obj_t* c
     )
{
	dir_t direct;

	// For trsm, movement for the parameter cases is as follows:
	// - left,lower:  forwards
	// - left,upper:  backwards
	// - right,lower: backwards
	// - right,upper: forwards

	if ( bli_obj_root_is_triangular( a ) )
	{
		if ( bli_obj_root_is_lower( a ) ) direct = BLIS_FWD;
		else                              direct = BLIS_BWD;
	}
	else // if ( bli_obj_root_is_triangular( b ) )
	{
		if ( bli_obj_root_is_lower( b ) ) direct = BLIS_BWD;
		else                              direct = BLIS_FWD;
	}

	return direct;
}

