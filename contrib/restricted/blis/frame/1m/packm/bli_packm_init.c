/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2016, Hewlett Packard Enterprise Development LP

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

siz_t bli_packm_init
     (
       obj_t*  a,
       obj_t*  p,
       cntx_t* cntx,
       cntl_t* cntl
     )
{
	bli_init_once();

	// The purpose of packm_init() is to initialize an object P so that
	// a source object A can be packed into P via one of the packm
	// implementations. This initialization precedes the acquisition of a
	// suitable block of memory from the memory allocator (if such a block
	// of memory has not already been allocated previously).

	bszid_t   bmult_id_m;
	bszid_t   bmult_id_n;
	bool      does_invert_diag;
	bool      rev_iter_if_upper;
	bool      rev_iter_if_lower;
	pack_t    schema;
	//packbuf_t pack_buf_type;
	siz_t     size_needed;

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
		bli_packm_init_check( a, p, cntx );

	// Extract various fields from the control tree.
	bmult_id_m        = bli_cntl_packm_params_bmid_m( cntl );
	bmult_id_n        = bli_cntl_packm_params_bmid_n( cntl );
	does_invert_diag  = bli_cntl_packm_params_does_invert_diag( cntl );
	rev_iter_if_upper = bli_cntl_packm_params_rev_iter_if_upper( cntl );
	rev_iter_if_lower = bli_cntl_packm_params_rev_iter_if_lower( cntl );
	schema            = bli_cntl_packm_params_pack_schema( cntl );
	//pack_buf_type     = bli_cntl_packm_params_pack_buf_type( cntl );

#if 0
	// Let us now check to see if the object has already been packed. First
	// we check if it has been packed to an unspecified (row or column)
	// format, in which case we can alias the object and return.
	// NOTE: The reason we don't need to even look at the control tree in
	// this case is as follows: an object's pack status is only set to
	// BLIS_PACKED_UNSPEC for situations when the actual format used is
	// not important, as long as its packed into contiguous rows or
	// contiguous columns. A good example of this is packing for matrix
	// operands in the level-2 operations.
	if ( bli_obj_pack_schema( a ) == BLIS_PACKED_UNSPEC )
	{
		bli_obj_alias_to( a, p );
		return 0;
	}

	// Now we check if the object has already been packed to the desired
	// schema (as encoded in the control tree). If so, we can alias and
	// return 0.
	// NOTE: In most cases, an object's pack status will be BLIS_NOT_PACKED
	// and thus packing will be called for (but in some cases packing has
	// already taken place, or does not need to take place, and so that will
	// be indicated by the pack status). Also, not all combinations of
	// current pack status and desired pack schema are valid.
	if ( bli_obj_pack_schema( a ) == pack_schema )
	{
		bli_obj_alias_to( a, p );
		return 0;
	}
#endif

	// If the object is marked as being filled with zeros, then we can skip
	// the packm operation entirely and alias.
	if ( bli_obj_is_zeros( a ) )
	{
		bli_obj_alias_to( a, p );
		return 0;
	}

#if 0
	pack_t schema;

	if ( bli_cntx_method( cntx ) != BLIS_NAT )
	{
		// We now ignore the pack_schema field in the control tree and
		// extract the schema from the context, depending on whether we are
		// preparing to pack a block of A or panel of B. For A and B, we must
		// obtain the schema from the context since the induced methods reuse
		// the same control trees used by native execution, and those induced
		// methods specify the schema used by the current execution phase
		// within the context (whereas the control tree does not change).

		if ( pack_buf_type == BLIS_BUFFER_FOR_A_BLOCK )
		{
			schema = bli_cntx_schema_a_block( cntx );
		}
		else if ( pack_buf_type == BLIS_BUFFER_FOR_B_PANEL )
		{
			schema = bli_cntx_schema_b_panel( cntx );
		}
		else // if ( pack_buf_type == BLIS_BUFFER_FOR_C_PANEL )
		{
			schema = bli_cntl_packm_params_pack_schema( cntl );
		}
	}
	else // ( bli_cntx_method( cntx ) == BLIS_NAT )
	{
		// For native execution, we obtain the schema from the control tree
		// node. (Notice that it doesn't matter if the pack_buf_type is for
		// A or B.)
		schema = bli_cntl_packm_params_pack_schema( cntl );
	}
	// This is no longer needed now that we branch between native and
	// non-native cases above.
#if 0
	if ( pack_buf_type == BLIS_BUFFER_FOR_C_PANEL )
	{
		// If we get a request to pack C for some reason, it is likely
		// not part of an induced method, and so it would be safe (and
		// necessary) to read the pack schema from the control tree.
		schema = bli_cntl_packm_params_pack_schema( cntl );
	}
#endif
#endif

	// Prepare a few other variables based on properties of the control
	// tree.

	invdiag_t invert_diag;
	packord_t pack_ord_if_up;
	packord_t pack_ord_if_lo;

	if ( does_invert_diag ) invert_diag = BLIS_INVERT_DIAG;
	else                    invert_diag = BLIS_NO_INVERT_DIAG;

	if ( rev_iter_if_upper ) pack_ord_if_up = BLIS_PACK_REV_IF_UPPER;
	else                     pack_ord_if_up = BLIS_PACK_FWD_IF_UPPER;

	if ( rev_iter_if_lower ) pack_ord_if_lo = BLIS_PACK_REV_IF_LOWER;
	else                     pack_ord_if_lo = BLIS_PACK_FWD_IF_LOWER;

	// Initialize object p for the final packed matrix.
	size_needed
	=
	bli_packm_init_pack
	(
	  invert_diag,
	  schema,
	  pack_ord_if_up,
	  pack_ord_if_lo,
	  bmult_id_m,
	  bmult_id_n,
	  a,
	  p,
	  cntx
	);

	// Return the size needed for memory allocation of the packed buffer.
	return size_needed;
}


siz_t bli_packm_init_pack
     (
       invdiag_t invert_diag,
       pack_t    schema,
       packord_t pack_ord_if_up,
       packord_t pack_ord_if_lo,
       bszid_t   bmult_id_m,
       bszid_t   bmult_id_n,
       obj_t*    a,
       obj_t*    p,
       cntx_t*   cntx
     )
{
	bli_init_once();

	num_t     dt_tar       = bli_obj_target_dt( a );
	num_t     dt_scalar    = bli_obj_scalar_dt( a );
	trans_t   transa       = bli_obj_onlytrans_status( a );
	dim_t     m_a          = bli_obj_length( a );
	dim_t     n_a          = bli_obj_width( a );
	dim_t     bmult_m_def  = bli_cntx_get_blksz_def_dt( dt_tar, bmult_id_m, cntx );
	dim_t     bmult_m_pack = bli_cntx_get_blksz_max_dt( dt_tar, bmult_id_m, cntx );
	dim_t     bmult_n_def  = bli_cntx_get_blksz_def_dt( dt_tar, bmult_id_n, cntx );
	dim_t     bmult_n_pack = bli_cntx_get_blksz_max_dt( dt_tar, bmult_id_n, cntx );

	dim_t     m_p, n_p;
	dim_t     m_p_pad, n_p_pad;
	siz_t     size_p;
	siz_t     elem_size_p;
	inc_t     rs_p, cs_p;
	inc_t     is_p;


	// We begin by copying the fields of A.
	bli_obj_alias_to( a, p );

	// Typecast the internal scalar value to the target datatype.
	// Note that if the typecasting is needed, this must happen BEFORE we
	// change the datatype of P to reflect the target_dt.
	if ( dt_scalar != dt_tar )
	{
		bli_obj_scalar_cast_to( dt_tar, p );
	}

	// Update the storage datatype of P to be the target datatype of A.
	bli_obj_set_dt( dt_tar, p );

	// Update the dimension fields to explicitly reflect a transposition,
	// if needed.
	// Then, clear the conjugation and transposition fields from the object
	// since matrix packing in BLIS is deemed to take care of all conjugation
	// and transposition necessary.
	// Then, we adjust the properties of P when A needs a transposition.
	// We negate the diagonal offset, and if A is upper- or lower-stored,
	// we either toggle the uplo of P.
	// Finally, if we mark P as dense since we assume that all matrices,
	// regardless of structure, will be densified.
	bli_obj_set_dims_with_trans( transa, m_a, n_a, p );
	bli_obj_set_conjtrans( BLIS_NO_TRANSPOSE, p );
	if ( bli_does_trans( transa ) )
	{
		bli_obj_negate_diag_offset( p );
		if ( bli_obj_is_upper_or_lower( a ) )
			bli_obj_toggle_uplo( p );
	}

	// If we are packing micropanels, mark P as dense. Otherwise, we are
	// probably being called in the context of a level-2 operation, in
	// which case we do not want to overwrite the uplo field of P (inherited
	// from A) with BLIS_DENSE because that information may be needed by
	// the level-2 operation's unblocked variant to decide whether to
	// execute a "lower" or "upper" branch of code.
	if ( bli_is_panel_packed( schema ) )
	{
		bli_obj_set_uplo( BLIS_DENSE, p );
	}

	// Reset the view offsets to (0,0).
	bli_obj_set_offs( 0, 0, p );

	// Set the invert diagonal field.
	bli_obj_set_invert_diag( invert_diag, p );

	// Set the pack status of P to the pack schema prescribed in the control
	// tree node.
	bli_obj_set_pack_schema( schema, p );

	// Set the packing order bits.
	bli_obj_set_pack_order_if_upper( pack_ord_if_up, p );
	bli_obj_set_pack_order_if_lower( pack_ord_if_lo, p );

	// Compute the dimensions padded by the dimension multiples. These
	// dimensions will be the dimensions of the packed matrices, including
	// zero-padding, and will be used by the macro- and micro-kernels.
	// We compute them by starting with the effective dimensions of A (now
	// in P) and aligning them to the dimension multiples (typically equal
	// to register blocksizes). This does waste a little bit of space for
	// level-2 operations, but that's okay with us.
	m_p     = bli_obj_length( p );
	n_p     = bli_obj_width( p );
	m_p_pad = bli_align_dim_to_mult( m_p, bmult_m_def );
	n_p_pad = bli_align_dim_to_mult( n_p, bmult_n_def );

	// Save the padded dimensions into the packed object. It is important
	// to save these dimensions since they represent the actual dimensions
	// of the zero-padded matrix.
	bli_obj_set_padded_dims( m_p_pad, n_p_pad, p );

	// Now we prepare to compute strides, align them, and compute the
	// total number of bytes needed for the packed buffer. The caller
	// will then use that value to acquire an appropriate block of memory
	// from the memory allocator.

	// Extract the element size for the packed object.
	elem_size_p = bli_obj_elem_size( p );

	// Set the row and column strides of p based on the pack schema.
	if      ( bli_is_row_packed( schema ) &&
	          !bli_is_panel_packed( schema ) )
	{
		// For regular row storage, the padded width of our matrix
		// should be used for the row stride, with the column stride set
		// to one. By using the WIDTH of the mem_t region, we allow for
		// zero-padding (if necessary/desired) along the right edge of
		// the matrix.
		rs_p = n_p_pad;
		cs_p = 1;

		// Align the leading dimension according to the heap stride
		// alignment size so that the second, third, etc rows begin at
		// aligned addresses.
		rs_p = bli_align_dim_to_size( rs_p, elem_size_p,
		                              BLIS_HEAP_STRIDE_ALIGN_SIZE );

		// Store the strides in P.
		bli_obj_set_strides( rs_p, cs_p, p );

		// Compute the size of the packed buffer.
		size_p = m_p_pad * rs_p * elem_size_p;
	}
	else if ( bli_is_col_packed( schema ) &&
	          !bli_is_panel_packed( schema ) )
	{
		// For regular column storage, the padded length of our matrix
		// should be used for the column stride, with the row stride set
		// to one. By using the LENGTH of the mem_t region, we allow for
		// zero-padding (if necessary/desired) along the bottom edge of
		// the matrix.
		cs_p = m_p_pad;
		rs_p = 1;

		// Align the leading dimension according to the heap stride
		// alignment size so that the second, third, etc columns begin at
		// aligned addresses.
		cs_p = bli_align_dim_to_size( cs_p, elem_size_p,
		                              BLIS_HEAP_STRIDE_ALIGN_SIZE );

		// Store the strides in P.
		bli_obj_set_strides( rs_p, cs_p, p );

		// Compute the size of the packed buffer.
		size_p = cs_p * n_p_pad * elem_size_p;
	}
	else if ( bli_is_row_packed( schema ) &&
	          bli_is_panel_packed( schema ) )
	{
		dim_t m_panel;
		dim_t ps_p, ps_p_orig;

		// The panel dimension (for each datatype) should be equal to the
		// default (logical) blocksize multiple in the m dimension.
		m_panel = bmult_m_def;

		// The "column stride" of a row-micropanel packed object is interpreted
		// as the column stride WITHIN a micropanel. Thus, this is equal to the
		// packing (storage) blocksize multiple, which may be equal to the
		// default (logical) blocksize multiple).
		cs_p = bmult_m_pack;

		// The "row stride" of a row-micropanel packed object is interpreted
		// as the row stride WITHIN a micropanel. Thus, it is unit.
		rs_p = 1;

		// The "panel stride" of a micropanel packed object is interpreted as
		// the distance between the (0,0) element of panel k and the (0,0)
		// element of panel k+1. We use the padded width computed above to
		// allow for zero-padding (if necessary/desired) along the far end
		// of each micropanel (ie: the right edge of the matrix). Zero-padding
		// can also occur along the long edge of the last micropanel if the m
		// dimension of the matrix is not a whole multiple of MR.
		ps_p = cs_p * n_p_pad;

		// As a general rule, we don't want micropanel strides to be odd. This
		// is primarily motivated by our desire to support interleaved 3m
		// micropanels, in which case we have to scale the panel stride
		// by 3/2. That division by 2 means the numerator (prior to being
		// scaled by 3) must be even.
		if ( bli_is_odd( ps_p ) ) ps_p += 1;

		// Preserve this early panel stride value for use later, if needed.
		ps_p_orig = ps_p;

		// Here, we adjust the panel stride, if necessary. Remember: ps_p is
		// always interpreted as being in units of the datatype of the object
		// which is not necessarily how the micropanels will be stored. For
		// interleaved 3m, we will increase ps_p by 50%, and for ro/io/rpi,
		// we halve ps_p. Why? Because the macro-kernel indexes in units of
		// the complex datatype. So these changes "trick" it into indexing
		// the correct amount.
		if ( bli_is_3mi_packed( schema ) )
		{
			ps_p = ( ps_p * 3 ) / 2;
		}
		else if ( bli_is_3ms_packed( schema ) ||
		          bli_is_ro_packed( schema )  ||
		          bli_is_io_packed( schema )  ||
		          bli_is_rpi_packed( schema ) )
		{
			// The division by 2 below assumes that ps_p is an even number.
			// However, it is possible that, at this point, ps_p is an odd.
			// If it is indeed odd, we nudge it higher.
			if ( bli_is_odd( ps_p ) ) ps_p += 1;

			// Despite the fact that the packed micropanels will contain
			// real elements, the panel stride that we store in the obj_t
			// (which is passed into the macro-kernel) needs to be in units
			// of complex elements, since the macro-kernel will index through
			// micropanels via complex pointer arithmetic for trmm/trsm.
			// Since the indexing "increment" will be twice as large as each
			// actual stored element, we divide the panel_stride by 2.
			ps_p = ps_p / 2;
		}

		// Set the imaginary stride (in units of fundamental elements) for
		// 3m and 4m (separated or interleaved). We use ps_p_orig since
		// that variable tracks the number of real part elements contained
		// within each micropanel of the source matrix. Therefore, this
		// is the number of real elements that must be traversed before
		// reaching the imaginary part (3mi/4mi) of the packed micropanel,
		// or the real part of the next micropanel (3ms).
		if      ( bli_is_3mi_packed( schema ) ) is_p = ps_p_orig;
		else if ( bli_is_4mi_packed( schema ) ) is_p = ps_p_orig;
		else if ( bli_is_3ms_packed( schema ) ) is_p = ps_p_orig * ( m_p_pad / m_panel );
		else                                    is_p = 1;

		// Store the strides and panel dimension in P.
		bli_obj_set_strides( rs_p, cs_p, p );
		bli_obj_set_imag_stride( is_p, p );
		bli_obj_set_panel_dim( m_panel, p );
		bli_obj_set_panel_stride( ps_p, p );
		bli_obj_set_panel_length( m_panel, p );
		bli_obj_set_panel_width( n_p, p );

		// Compute the size of the packed buffer.
		size_p = ps_p * ( m_p_pad / m_panel ) * elem_size_p;
	}
	else if ( bli_is_col_packed( schema ) &&
	          bli_is_panel_packed( schema ) )
	{
		dim_t n_panel;
		dim_t ps_p, ps_p_orig;

		// The panel dimension (for each datatype) should be equal to the
		// default (logical) blocksize multiple in the n dimension.
		n_panel = bmult_n_def;

		// The "row stride" of a column-micropanel packed object is interpreted
		// as the row stride WITHIN a micropanel. Thus, this is equal to the
		// packing (storage) blocksize multiple (which may be equal to the
		// default (logical) blocksize multiple.
		rs_p = bmult_n_pack;

		// The "column stride" of a column-micropanel packed object is
		// interpreted as the column stride WITHIN a micropanel. Thus, it is
		// unit.
		cs_p = 1;

		// The "panel stride" of a micropanel packed object is interpreted as
		// the distance between the (0,0) element of panel k and the (0,0)
		// element of panel k+1. We use the padded length computed above to
		// allow for zero-padding (if necessary/desired) along the far end
		// of each micropanel (ie: the bottom edge of the matrix). Zero-padding
		// can also occur along the long edge of the last micropanel if the n
		// dimension of the matrix is not a whole multiple of NR.
		ps_p = m_p_pad * rs_p;

		// As a general rule, we don't want micropanel strides to be odd. This
		// is primarily motivated by our desire to support interleaved 3m
		// micropanels, in which case we have to scale the panel stride
		// by 3/2. That division by 2 means the numerator (prior to being
		// scaled by 3) must be even.
		if ( bli_is_odd( ps_p ) ) ps_p += 1;

		// Preserve this early panel stride value for use later, if needed.
		ps_p_orig = ps_p;

		// Here, we adjust the panel stride, if necessary. Remember: ps_p is
		// always interpreted as being in units of the datatype of the object
		// which is not necessarily how the micropanels will be stored. For
		// interleaved 3m, we will increase ps_p by 50%, and for ro/io/rpi,
		// we halve ps_p. Why? Because the macro-kernel indexes in units of
		// the complex datatype. So these changes "trick" it into indexing
		// the correct amount.
		if ( bli_is_3mi_packed( schema ) )
		{
			ps_p = ( ps_p * 3 ) / 2;
		}
		else if ( bli_is_3ms_packed( schema ) ||
		          bli_is_ro_packed( schema ) ||
		          bli_is_io_packed( schema ) ||
		          bli_is_rpi_packed( schema ) )
		{
			// The division by 2 below assumes that ps_p is an even number.
			// However, it is possible that, at this point, ps_p is an odd.
			// If it is indeed odd, we nudge it higher.
			if ( bli_is_odd( ps_p ) ) ps_p += 1;

			// Despite the fact that the packed micropanels will contain
			// real elements, the panel stride that we store in the obj_t
			// (which is passed into the macro-kernel) needs to be in units
			// of complex elements, since the macro-kernel will index through
			// micropanels via complex pointer arithmetic for trmm/trsm.
			// Since the indexing "increment" will be twice as large as each
			// actual stored element, we divide the panel_stride by 2.
			ps_p = ps_p / 2;
		}

		// Set the imaginary stride (in units of fundamental elements) for
		// 3m and 4m (separated or interleaved). We use ps_p_orig since
		// that variable tracks the number of real part elements contained
		// within each micropanel of the source matrix. Therefore, this
		// is the number of real elements that must be traversed before
		// reaching the imaginary part (3mi/4mi) of the packed micropanel,
		// or the real part of the next micropanel (3ms).
		if      ( bli_is_3mi_packed( schema ) ) is_p = ps_p_orig;
		else if ( bli_is_4mi_packed( schema ) ) is_p = ps_p_orig;
		else if ( bli_is_3ms_packed( schema ) ) is_p = ps_p_orig * ( n_p_pad / n_panel );
		else                                    is_p = 1;

		// Store the strides and panel dimension in P.
		bli_obj_set_strides( rs_p, cs_p, p );
		bli_obj_set_imag_stride( is_p, p );
		bli_obj_set_panel_dim( n_panel, p );
		bli_obj_set_panel_stride( ps_p, p );
		bli_obj_set_panel_length( m_p, p );
		bli_obj_set_panel_width( n_panel, p );

		// Compute the size of the packed buffer.
		size_p = ps_p * ( n_p_pad / n_panel ) * elem_size_p;
	}
	else
	{
		// NOTE: When implementing block storage, we only need to implement
		// the following two cases:
		// - row-stored blocks in row-major order
		// - column-stored blocks in column-major order
		// The other two combinations coincide with that of packed row-panel
		// and packed column- panel storage.

		size_p = 0;
	}

	return size_p;
}

