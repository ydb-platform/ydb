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

void bli_prune_unref_mparts( obj_t* p, mdim_t mdim_p,
                             obj_t* s, mdim_t mdim_s )
{
	// If the primary object is general, it has no structure, and
	// therefore, no unreferenced parts.
	if ( bli_obj_is_general( p ) ) return;

	// If the primary object is BLIS_ZEROS, set the dimensions so that the
	// matrix is empty. This is not strictly needed but rather a minor
	// optimization, as it would prevent threads that would otherwise get
	// subproblems on BLIS_ZEROS operands from calling the macro-kernel,
	// because bli_thread_range*() would return empty ranges, which would
	// cause the variant's for loop from executing any iterations.
	// NOTE: this should only ever execute if the primary object is
	// triangular because that is the only structure type with subpartitions
	// that can be marked as BLIS_ZEROS.
	if ( bli_obj_is_triangular( p ) &&
	     bli_obj_is_zeros( p ) ) { bli_obj_set_dim( mdim_p, 0, p );
	                               bli_obj_set_dim( mdim_s, 0, s );
	                               return; }

	// If the primary object is hermitian, symmetric, or triangular, we
	// assume that the unstored region will be unreferenced (otherwise,
	// the caller should not be invoking this function on that object).
	//if ( bli_obj_is_herm_or_symm( p ) ||
	//     bli_obj_is_triangular( p ) )
	{
		doff_t diagoff_p = bli_obj_diag_offset( p );
		dim_t  m         = bli_obj_length( p );
		dim_t  n         = bli_obj_width( p );
		uplo_t uplo      = bli_obj_uplo( p );
		dim_t  off_inc   = 0;
		dim_t  q;

		// Support implicit transposition on p and s.
		if ( bli_obj_has_trans( p ) )
		{
			bli_reflect_about_diag( &diagoff_p, &uplo, &m, &n );
			bli_toggle_dim( &mdim_p );
		}
		if ( bli_obj_has_trans( s ) )
		{
			bli_toggle_dim( &mdim_s );
		}

		// Prune away any zero region of the matrix depending on the
		// dimension of the primary object being partitioned and the
		// triangle in which it is stored.
		if ( bli_obj_is_lower( p ) )
		{
			if ( bli_is_m_dim( mdim_p ) )
			{ bli_prune_unstored_region_top_l( &diagoff_p, &m, &n, &off_inc ); }
			else // if ( bli_is_n_dim( mdim_p ) )
			{ bli_prune_unstored_region_right_l( &diagoff_p, &m, &n, &off_inc ); }
		}
		else if ( bli_obj_is_upper( p ) )
		{
			if ( bli_is_m_dim( mdim_p ) )
			{ bli_prune_unstored_region_bottom_u( &diagoff_p, &m, &n, &off_inc ); }
			else // if ( bli_is_n_dim( mdim_p ) )
			{ bli_prune_unstored_region_left_u( &diagoff_p, &m, &n, &off_inc ); }
		}
		else if ( bli_obj_is_dense( p ) )
		{
			// Hermitian, symmetric, and triangular matrices are almost
			// never dense, but if one were found to be dense, it would
			// have no unreferenced regions to prune.
			return;
		}
		else // if ( bli_obj_is_zeros( p ) )
		{
			// Sanity check. Hermitian/symmetric matrices should never have
			// zero subpartitions.
			bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
		}

		// Select the (potentially modified) dimension along which we are
		// partitioning.
		if         ( bli_is_m_dim( mdim_p ) )    q = m;
		else /* if ( bli_is_n_dim( mdim_p ) ) */ q = n;

		// Update the affected objects in case anything changed. Notice that
		// it is okay to update the dimension and diagonal offset fields of
		// packed primary objects, as long as we do so in tandem with the
		// secondary object to maintain conformality. This just means that
		// the "ignore-able" zero region is skipped over here, rather than
		// within the macro-kernel.
		bli_obj_set_diag_offset( diagoff_p, p );
		bli_obj_set_dim( mdim_p, q, p );
		bli_obj_set_dim( mdim_s, q, s );

		// Only update the affected offset fields if the object in question
		// is NOT a packed object. Otherwise, bli_obj_buffer_at_off() will
		// compute the wrong address within the macro-kernel object wrapper.
		if ( !bli_obj_is_packed( p ) ) { bli_obj_inc_off( mdim_p, off_inc, p ); }
		if ( !bli_obj_is_packed( s ) ) { bli_obj_inc_off( mdim_s, off_inc, s ); }
	}
}

