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


// -- Matrix partitioning ------------------------------------------------------


void bli_packm_acquire_mpart_t2b( subpart_t requested_part,
                                  dim_t     i,
                                  dim_t     b,
                                  obj_t*    obj,
                                  obj_t*    sub_obj )
{
	dim_t m, n;

	// For now, we only support acquiring the middle subpartition.
	if ( requested_part != BLIS_SUBPART1 )
	{
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
	}

	// Partitioning top-to-bottom through packed column panels (which are
	// row-stored) is not yet supported.
	if ( bli_obj_is_col_packed( obj ) )
	{
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
	}

	// Query the dimensions of the parent object.
	m = bli_obj_length( obj );
	n = bli_obj_width( obj );

	// Foolproofing: do not let b exceed what's left of the m dimension at
	// row offset i.
	if ( b > m - i ) b = m - i;

	// Begin by copying the info, elem size, buffer, row stride, and column
	// stride fields of the parent object. Note that this omits copying view
	// information because the new partition will have its own dimensions
	// and offsets.
	bli_obj_init_subpart_from( obj, sub_obj );

	// Modify offsets and dimensions of requested partition.
	bli_obj_set_dims( b, n, sub_obj );

	// Tweak the padded length of the subpartition to trick the underlying
	// implementation into only zero-padding for the narrow submatrix of
	// interest. Usually, the value we want is b (for non-edge cases), but
	// at the edges, we want the remainder of the mem_t region in the m
	// dimension. Edge cases are defined as occurring when i + b is exactly
	// equal to the inherited sub-object's length (which happens since the
	// determine_blocksize function would have returned a smaller value of
	// b for the edge iteration). In these cases, we arrive at the new
	// packed length by simply subtracting off i.
	{
		dim_t  m_pack_max = bli_obj_padded_length( sub_obj );
		dim_t  m_pack_cur;

		if ( i + b == m ) m_pack_cur = m_pack_max - i;
		else              m_pack_cur = b;

		bli_obj_set_padded_length( m_pack_cur, sub_obj );
	}

	// Translate the desired offsets to a panel offset and adjust the
	// buffer pointer of the subpartition object.
	{
		char* buf_p        = bli_obj_buffer( sub_obj );
		siz_t elem_size    = bli_obj_elem_size( sub_obj );
		dim_t off_to_panel = bli_packm_offset_to_panel_for( i, sub_obj );

		buf_p = buf_p + elem_size * off_to_panel;

		bli_obj_set_buffer( buf_p, sub_obj );
	}
}



void bli_packm_acquire_mpart_l2r( subpart_t requested_part,
                                  dim_t     j,
                                  dim_t     b,
                                  obj_t*    obj,
                                  obj_t*    sub_obj )
{
	dim_t m, n;

	// Check parameters.
	//if ( bli_error_checking_is_enabled() )
	//	bli_packm_acquire_mpart_l2r_check( requested_part, j, b, obj, sub_obj );

	// For now, we only support acquiring the middle subpartition.
	if ( requested_part != BLIS_SUBPART1 )
	{
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
	}

	// Partitioning left-to-right through packed row panels (which are
	// column-stored) is not yet supported.
	if ( bli_obj_is_row_packed( obj ) )
	{
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
	}

	// Query the dimensions of the parent object.
	m = bli_obj_length( obj );
	n = bli_obj_width( obj );

	// Foolproofing: do not let b exceed what's left of the n dimension at
	// column offset j.
	if ( b > n - j ) b = n - j;

	// Begin by copying the info, elem size, buffer, row stride, and column
	// stride fields of the parent object. Note that this omits copying view
	// information because the new partition will have its own dimensions
	// and offsets.
	bli_obj_init_subpart_from( obj, sub_obj );

	// Modify offsets and dimensions of requested partition.
	bli_obj_set_dims( m, b, sub_obj );

	// Tweak the padded width of the subpartition to trick the underlying
	// implementation into only zero-padding for the narrow submatrix of
	// interest. Usually, the value we want is b (for non-edge cases), but
	// at the edges, we want the remainder of the mem_t region in the n
	// dimension. Edge cases are defined as occurring when j + b is exactly
	// equal to the inherited sub-object's width (which happens since the
	// determine_blocksize function would have returned a smaller value of
	// b for the edge iteration). In these cases, we arrive at the new
	// packed width by simply subtracting off j.
	{
		dim_t  n_pack_max = bli_obj_padded_width( sub_obj );
		dim_t  n_pack_cur;

		if ( j + b == n ) n_pack_cur = n_pack_max - j;
		else              n_pack_cur = b;

		bli_obj_set_padded_width( n_pack_cur, sub_obj );
	}

	// Translate the desired offsets to a panel offset and adjust the
	// buffer pointer of the subpartition object.
	{
		char* buf_p        = bli_obj_buffer( sub_obj );
		siz_t elem_size    = bli_obj_elem_size( sub_obj );
		dim_t off_to_panel = bli_packm_offset_to_panel_for( j, sub_obj );

		buf_p = buf_p + elem_size * off_to_panel;

		bli_obj_set_buffer( buf_p, sub_obj );
	}
}



void bli_packm_acquire_mpart_tl2br( subpart_t requested_part,
                                    dim_t     ij,
                                    dim_t     b,
                                    obj_t*    obj,
                                    obj_t*    sub_obj )
{
	bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
}



dim_t bli_packm_offset_to_panel_for( dim_t offmn, obj_t* p )
{
	dim_t panel_off;

	if      ( bli_obj_pack_schema( p ) == BLIS_PACKED_ROWS )
	{
		// For the "packed rows" schema, a single row is effectively one
		// row panel, and so we use the row offset as the panel offset.
		// Then we multiply this offset by the effective panel stride
		// (ie: the row stride) to arrive at the desired offset.
		panel_off = offmn * bli_obj_row_stride( p );
	}
	else if ( bli_obj_pack_schema( p ) == BLIS_PACKED_COLUMNS )
	{
		// For the "packed columns" schema, a single column is effectively one
		// column panel, and so we use the column offset as the panel offset.
		// Then we multiply this offset by the effective panel stride
		// (ie: the column stride) to arrive at the desired offset.
		panel_off = offmn * bli_obj_col_stride( p );
	}
	else if ( bli_obj_pack_schema( p ) == BLIS_PACKED_ROW_PANELS )
	{
		// For the "packed row panels" schema, the column stride is equal to
		// the panel dimension (length). So we can divide it into offmn
		// (interpreted as a row offset) to arrive at a panel offset. Then
		// we multiply this offset by the panel stride to arrive at the total
		// offset to the panel (in units of elements).
		panel_off = offmn / bli_obj_col_stride( p );
		panel_off = panel_off * bli_obj_panel_stride( p );

		// Sanity check.
		if ( offmn % bli_obj_col_stride( p ) > 0 ) bli_abort();
	}
	else if ( bli_obj_pack_schema( p ) == BLIS_PACKED_COL_PANELS )
	{
		// For the "packed column panels" schema, the row stride is equal to
		// the panel dimension (width). So we can divide it into offmn
		// (interpreted as a column offset) to arrive at a panel offset. Then
		// we multiply this offset by the panel stride to arrive at the total
		// offset to the panel (in units of elements).
		panel_off = offmn / bli_obj_row_stride( p );
		panel_off = panel_off * bli_obj_panel_stride( p );

		// Sanity check.
		if ( offmn % bli_obj_row_stride( p ) > 0 ) bli_abort();
	}
	else
	{
		panel_off = 0;
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED );
	}

	return panel_off;
}
