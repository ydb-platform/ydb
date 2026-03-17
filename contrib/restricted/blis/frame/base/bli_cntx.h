/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2016, Hewlett Packard Enterprise Development LP
   Copyright (C) 2019, Advanced Micro Devices, Inc.

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

#ifndef BLIS_CNTX_H
#define BLIS_CNTX_H


// Context object type (defined in bli_type_defs.h)

/*
typedef struct cntx_s
{
	blksz_t*  blkszs;
	bszid_t*  bmults;

	func_t*   l3_vir_ukrs;
	func_t*   l3_nat_ukrs;
	mbool_t*  l3_nat_ukrs_prefs;

	blksz_t*  l3_sup_thresh;
	void**    l3_sup_handlers;
	blksz_t*  l3_sup_blkszs;
	func_t*   l3_sup_kers;
	mbool_t*  l3_sup_kers_prefs;

	func_t*   l1f_kers;
	func_t*   l1v_kers;

	func_t*   packm_kers;
	func_t*   unpackm_kers;

	ind_t     method;
	pack_t    schema_a;
	pack_t    schema_b;
	pack_t    schema_c;

} cntx_t;
*/

// -----------------------------------------------------------------------------

//
// -- cntx_t query (fields only) -----------------------------------------------
//

BLIS_INLINE blksz_t* bli_cntx_blkszs_buf( cntx_t* cntx )
{
	return cntx->blkszs;
}
BLIS_INLINE bszid_t* bli_cntx_bmults_buf( cntx_t* cntx )
{
	return cntx->bmults;
}
BLIS_INLINE func_t* bli_cntx_l3_vir_ukrs_buf( cntx_t* cntx )
{
	return cntx->l3_vir_ukrs;
}
BLIS_INLINE func_t* bli_cntx_l3_nat_ukrs_buf( cntx_t* cntx )
{
	return cntx->l3_nat_ukrs;
}
BLIS_INLINE mbool_t* bli_cntx_l3_nat_ukrs_prefs_buf( cntx_t* cntx )
{
	return cntx->l3_nat_ukrs_prefs;
}
BLIS_INLINE blksz_t* bli_cntx_l3_sup_thresh_buf( cntx_t* cntx )
{
	return cntx->l3_sup_thresh;
}
BLIS_INLINE void** bli_cntx_l3_sup_handlers_buf( cntx_t* cntx )
{
	return cntx->l3_sup_handlers;
}
BLIS_INLINE blksz_t* bli_cntx_l3_sup_blkszs_buf( cntx_t* cntx )
{
	return cntx->l3_sup_blkszs;
}
BLIS_INLINE func_t* bli_cntx_l3_sup_kers_buf( cntx_t* cntx )
{
	return cntx->l3_sup_kers;
}
BLIS_INLINE mbool_t* bli_cntx_l3_sup_kers_prefs_buf( cntx_t* cntx )
{
	return cntx->l3_sup_kers_prefs;
}
BLIS_INLINE func_t* bli_cntx_l1f_kers_buf( cntx_t* cntx )
{
	return cntx->l1f_kers;
}
BLIS_INLINE func_t* bli_cntx_l1v_kers_buf( cntx_t* cntx )
{
	return cntx->l1v_kers;
}
BLIS_INLINE func_t* bli_cntx_packm_kers_buf( cntx_t* cntx )
{
	return cntx->packm_kers;
}
BLIS_INLINE func_t* bli_cntx_unpackm_kers_buf( cntx_t* cntx )
{
	return cntx->unpackm_kers;
}
BLIS_INLINE ind_t bli_cntx_method( cntx_t* cntx )
{
	return cntx->method;
}
BLIS_INLINE pack_t bli_cntx_schema_a_block( cntx_t* cntx )
{
	return cntx->schema_a_block;
}
BLIS_INLINE pack_t bli_cntx_schema_b_panel( cntx_t* cntx )
{
	return cntx->schema_b_panel;
}
BLIS_INLINE pack_t bli_cntx_schema_c_panel( cntx_t* cntx )
{
	return cntx->schema_c_panel;
}

// -----------------------------------------------------------------------------

//
// -- cntx_t modification (fields only) ----------------------------------------
//

BLIS_INLINE void bli_cntx_set_method( ind_t method, cntx_t* cntx )
{
	cntx->method = method;
}
BLIS_INLINE void bli_cntx_set_schema_a_block( pack_t schema, cntx_t* cntx )
{
	cntx->schema_a_block = schema;
}
BLIS_INLINE void bli_cntx_set_schema_b_panel( pack_t schema, cntx_t* cntx )
{
	cntx->schema_b_panel = schema;
}
BLIS_INLINE void bli_cntx_set_schema_c_panel( pack_t schema, cntx_t* cntx )
{
	cntx->schema_c_panel = schema;
}
BLIS_INLINE void bli_cntx_set_schema_ab_blockpanel( pack_t sa, pack_t sb, cntx_t* cntx )
{
	bli_cntx_set_schema_a_block( sa, cntx );
	bli_cntx_set_schema_b_panel( sb, cntx );
}

// -----------------------------------------------------------------------------

//
// -- cntx_t query (complex) ---------------------------------------------------
//

BLIS_INLINE blksz_t* bli_cntx_get_blksz( bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blkszs = bli_cntx_blkszs_buf( cntx );
	blksz_t* blksz  = &blkszs[ bs_id ];

	// Return the address of the blksz_t identified by bs_id.
	return blksz;
}

BLIS_INLINE dim_t bli_cntx_get_blksz_def_dt( num_t dt, bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blksz  = bli_cntx_get_blksz( bs_id, cntx );
	dim_t    bs_dt  = bli_blksz_get_def( dt, blksz );

	// Return the main (default) blocksize value for the datatype given.
	return bs_dt;
}

BLIS_INLINE dim_t bli_cntx_get_blksz_max_dt( num_t dt, bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blksz  = bli_cntx_get_blksz( bs_id, cntx );
	dim_t    bs_dt  = bli_blksz_get_max( dt, blksz );

	// Return the auxiliary (maximum) blocksize value for the datatype given.
	return bs_dt;
}

BLIS_INLINE bszid_t bli_cntx_get_bmult_id( bszid_t bs_id, cntx_t* cntx )
{
	bszid_t* restrict bmults = bli_cntx_bmults_buf( cntx );
	bszid_t           bm_id  = bmults[ bs_id ];

	return bm_id;
}

BLIS_INLINE blksz_t* bli_cntx_get_bmult( bszid_t bs_id, cntx_t* cntx )
{
	bszid_t           bm_id  = bli_cntx_get_bmult_id( bs_id, cntx );
	blksz_t* restrict bmult  = bli_cntx_get_blksz( bm_id, cntx );

	return bmult;
}

BLIS_INLINE dim_t bli_cntx_get_bmult_dt( num_t dt, bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* bmult  = bli_cntx_get_bmult( bs_id, cntx );
	dim_t    bm_dt  = bli_blksz_get_def( dt, bmult );

	return bm_dt;
}

// -----------------------------------------------------------------------------

BLIS_INLINE func_t* bli_cntx_get_l3_vir_ukrs( l3ukr_t ukr_id, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l3_vir_ukrs_buf( cntx );
	func_t* func  = &funcs[ ukr_id ];

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_l3_vir_ukr_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	func_t* func = bli_cntx_get_l3_vir_ukrs( ukr_id, cntx );

	return bli_func_get_dt( dt, func );
}

BLIS_INLINE func_t* bli_cntx_get_l3_nat_ukrs( l3ukr_t ukr_id, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l3_nat_ukrs_buf( cntx );
	func_t* func  = &funcs[ ukr_id ];

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_l3_nat_ukr_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	func_t* func = bli_cntx_get_l3_nat_ukrs( ukr_id, cntx );

	return bli_func_get_dt( dt, func );
}

// -----------------------------------------------------------------------------

BLIS_INLINE mbool_t* bli_cntx_get_l3_nat_ukr_prefs( l3ukr_t ukr_id, cntx_t* cntx )
{
	mbool_t* mbools = bli_cntx_l3_nat_ukrs_prefs_buf( cntx );
	mbool_t* mbool  = &mbools[ ukr_id ];

	return mbool;
}

BLIS_INLINE bool bli_cntx_get_l3_nat_ukr_prefs_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	mbool_t* mbool = bli_cntx_get_l3_nat_ukr_prefs( ukr_id, cntx );

	return ( bool )bli_mbool_get_dt( dt, mbool );
}

// -----------------------------------------------------------------------------

BLIS_INLINE blksz_t* bli_cntx_get_l3_sup_thresh( threshid_t thresh_id, cntx_t* cntx )
{
	blksz_t* threshs = bli_cntx_l3_sup_thresh_buf( cntx );
	blksz_t* thresh  = &threshs[ thresh_id ];

	// Return the address of the blksz_t identified by thresh_id.
	return thresh;
}

BLIS_INLINE dim_t bli_cntx_get_l3_sup_thresh_dt( num_t dt, threshid_t thresh_id, cntx_t* cntx )
{
	blksz_t* threshs   = bli_cntx_get_l3_sup_thresh( thresh_id, cntx );
	dim_t    thresh_dt = bli_blksz_get_def( dt, threshs );

	// Return the main (default) threshold value for the datatype given.
	return thresh_dt;
}

BLIS_INLINE bool bli_cntx_l3_sup_thresh_is_met( num_t dt, dim_t m, dim_t n, dim_t k, cntx_t* cntx )
{
	if ( m < bli_cntx_get_l3_sup_thresh_dt( dt, BLIS_MT, cntx ) ) return TRUE;
	if ( n < bli_cntx_get_l3_sup_thresh_dt( dt, BLIS_NT, cntx ) ) return TRUE;
	if ( k < bli_cntx_get_l3_sup_thresh_dt( dt, BLIS_KT, cntx ) ) return TRUE;

	return FALSE;
}

// -----------------------------------------------------------------------------

BLIS_INLINE void* bli_cntx_get_l3_sup_handler( opid_t op, cntx_t* cntx )
{
	void** funcs = bli_cntx_l3_sup_handlers_buf( cntx );
	void*  func  = funcs[ op ];

	return func;
}

// -----------------------------------------------------------------------------

BLIS_INLINE blksz_t* bli_cntx_get_l3_sup_blksz( bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blkszs = bli_cntx_l3_sup_blkszs_buf( cntx );
	blksz_t* blksz  = &blkszs[ bs_id ];

	// Return the address of the blksz_t identified by bs_id.
	return blksz;
}

BLIS_INLINE dim_t bli_cntx_get_l3_sup_blksz_def_dt( num_t dt, bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blksz  = bli_cntx_get_l3_sup_blksz( bs_id, cntx );
	dim_t    bs_dt  = bli_blksz_get_def( dt, blksz );

	// Return the main (default) blocksize value for the datatype given.
	return bs_dt;
}

BLIS_INLINE dim_t bli_cntx_get_l3_sup_blksz_max_dt( num_t dt, bszid_t bs_id, cntx_t* cntx )
{
	blksz_t* blksz  = bli_cntx_get_l3_sup_blksz( bs_id, cntx );
	dim_t    bs_dt  = bli_blksz_get_max( dt, blksz );

	// Return the auxiliary (maximum) blocksize value for the datatype given.
	return bs_dt;
}

// -----------------------------------------------------------------------------

BLIS_INLINE func_t* bli_cntx_get_l3_sup_kers( stor3_t stor_id, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l3_sup_kers_buf( cntx );
	func_t* func  = &funcs[ stor_id ];

	return func;
}

BLIS_INLINE void* bli_cntx_get_l3_sup_ker_dt( num_t dt, stor3_t stor_id, cntx_t* cntx )
{
	func_t* func = bli_cntx_get_l3_sup_kers( stor_id, cntx );

	return bli_func_get_dt( dt, func );
}

// -----------------------------------------------------------------------------

BLIS_INLINE mbool_t* bli_cntx_get_l3_sup_ker_prefs( stor3_t stor_id, cntx_t* cntx )
{
	mbool_t* mbools = bli_cntx_l3_sup_kers_prefs_buf( cntx );
	mbool_t* mbool  = &mbools[ stor_id ];

	return mbool;
}

BLIS_INLINE bool bli_cntx_get_l3_sup_ker_prefs_dt( num_t dt, stor3_t stor_id, cntx_t* cntx )
{
	mbool_t* mbool = bli_cntx_get_l3_sup_ker_prefs( stor_id, cntx );

	return ( bool )bli_mbool_get_dt( dt, mbool );
}

// -----------------------------------------------------------------------------

BLIS_INLINE func_t* bli_cntx_get_l1f_kers( l1fkr_t ker_id, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l1f_kers_buf( cntx );
	func_t* func  = &funcs[ ker_id ];

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_l1f_ker_dt( num_t dt, l1fkr_t ker_id, cntx_t* cntx )
{
	func_t* func = bli_cntx_get_l1f_kers( ker_id, cntx );

	return bli_func_get_dt( dt, func );
}

// -----------------------------------------------------------------------------

BLIS_INLINE func_t* bli_cntx_get_l1v_kers( l1vkr_t ker_id, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l1v_kers_buf( cntx );
	func_t* func  = &funcs[ ker_id ];

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_l1v_ker_dt( num_t dt, l1vkr_t ker_id, cntx_t* cntx )
{
	func_t* func = bli_cntx_get_l1v_kers( ker_id, cntx );

	return bli_func_get_dt( dt, func );
}

// -----------------------------------------------------------------------------

BLIS_INLINE func_t* bli_cntx_get_packm_kers( l1mkr_t ker_id, cntx_t* cntx )
{
	func_t* func = NULL;

	// Only index to the requested packm func_t if the packm kernel being
	// requested is one that is explicitly supported.
	if ( 0 <= ( gint_t )ker_id &&
	          ( gint_t )ker_id < BLIS_NUM_PACKM_KERS )
	{
		func_t* funcs = bli_cntx_packm_kers_buf( cntx );

		func = &funcs[ ker_id ];
	}

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_packm_ker_dt( num_t dt, l1mkr_t ker_id, cntx_t* cntx )
{
	void_fp fp = NULL;

	// Only query the context for the packm func_t (and then extract the
	// datatype-specific function pointer) if the packm kernel being
	// requested is one that is explicitly supported.
	if ( 0 <= ( gint_t )ker_id &&
	          ( gint_t )ker_id < BLIS_NUM_PACKM_KERS )
	{
		func_t* func = bli_cntx_get_packm_kers( ker_id, cntx );

		fp = bli_func_get_dt( dt, func );
	}

	return fp;
}

BLIS_INLINE func_t* bli_cntx_get_unpackm_kers( l1mkr_t ker_id, cntx_t* cntx )
{
	func_t* func = NULL;

	// Only index to the requested unpackm func_t if the unpackm kernel being
	// requested is one that is explicitly supported.
	if ( 0 <= ( gint_t )ker_id &&
	          ( gint_t )ker_id < BLIS_NUM_UNPACKM_KERS )
	{
		func_t* funcs = bli_cntx_unpackm_kers_buf( cntx );

		func = &funcs[ ker_id ];
	}

	return func;
}

BLIS_INLINE void_fp bli_cntx_get_unpackm_ker_dt( num_t dt, l1mkr_t ker_id, cntx_t* cntx )
{
	void_fp fp = NULL;

	// Only query the context for the unpackm func_t (and then extract the
	// datatype-specific function pointer) if the unpackm kernel being
	// requested is one that is explicitly supported.
	if ( 0 <= ( gint_t )ker_id &&
	          ( gint_t )ker_id < BLIS_NUM_UNPACKM_KERS )
	{
		func_t* func = bli_cntx_get_unpackm_kers( ker_id, cntx );

		fp = bli_func_get_dt( dt, func );
	}

	return fp;
}

// -----------------------------------------------------------------------------

BLIS_INLINE bool bli_cntx_l3_nat_ukr_prefers_rows_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	const bool prefs = bli_cntx_get_l3_nat_ukr_prefs_dt( dt, ukr_id, cntx );

	// A ukernel preference of TRUE means the ukernel prefers row storage.
	return ( bool )
	       ( prefs == TRUE );
}

BLIS_INLINE bool bli_cntx_l3_nat_ukr_prefers_cols_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	const bool prefs = bli_cntx_get_l3_nat_ukr_prefs_dt( dt, ukr_id, cntx );

	// A ukernel preference of FALSE means the ukernel prefers column storage.
	return ( bool )
	       ( prefs == FALSE );
}

BLIS_INLINE bool bli_cntx_l3_nat_ukr_prefers_storage_of( obj_t* obj, l3ukr_t ukr_id, cntx_t* cntx )
{
	// Note that we use the computation datatype, which may differ from the
	// storage datatype of C (when performing a mixed datatype operation).
	const num_t dt    = bli_obj_comp_dt( obj );
	const bool  ukr_prefers_rows
	                  = bli_cntx_l3_nat_ukr_prefers_rows_dt( dt, ukr_id, cntx );
	const bool  ukr_prefers_cols
	                  = bli_cntx_l3_nat_ukr_prefers_cols_dt( dt, ukr_id, cntx );
	bool        r_val = FALSE;

	if      ( bli_obj_is_row_stored( obj ) && ukr_prefers_rows ) r_val = TRUE;
	else if ( bli_obj_is_col_stored( obj ) && ukr_prefers_cols ) r_val = TRUE;

	return r_val;
}

BLIS_INLINE bool bli_cntx_l3_nat_ukr_dislikes_storage_of( obj_t* obj, l3ukr_t ukr_id, cntx_t* cntx )
{
	return ( bool )
	       !bli_cntx_l3_nat_ukr_prefers_storage_of( obj, ukr_id, cntx );
}

// -----------------------------------------------------------------------------

BLIS_INLINE bool bli_cntx_l3_vir_ukr_prefers_rows_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	// For induced methods, return the ukernel storage preferences of the
	// corresponding real micro-kernel.
	// NOTE: This projection to real domain becomes unnecessary if you
	// set the exec_dt for 1m to the real projection of the storage
	// datatype.
	if ( bli_cntx_method( cntx ) != BLIS_NAT )
	    dt = bli_dt_proj_to_real( dt );

	return bli_cntx_l3_nat_ukr_prefers_rows_dt( dt, ukr_id, cntx );
}

BLIS_INLINE bool bli_cntx_l3_vir_ukr_prefers_cols_dt( num_t dt, l3ukr_t ukr_id, cntx_t* cntx )
{
	// For induced methods, return the ukernel storage preferences of the
	// corresponding real micro-kernel.
	// NOTE: This projection to real domain becomes unnecessary if you
	// set the exec_dt for 1m to the real projection of the storage
	// datatype.
	if ( bli_cntx_method( cntx ) != BLIS_NAT )
	    dt = bli_dt_proj_to_real( dt );

	return bli_cntx_l3_nat_ukr_prefers_cols_dt( dt, ukr_id, cntx );
}

BLIS_INLINE bool bli_cntx_l3_vir_ukr_prefers_storage_of( obj_t* obj, l3ukr_t ukr_id, cntx_t* cntx )
{
	// Note that we use the computation datatype, which may differ from the
	// storage datatype of C (when performing a mixed datatype operation).
	const num_t dt    = bli_obj_comp_dt( obj );
	const bool  ukr_prefers_rows
	                  = bli_cntx_l3_vir_ukr_prefers_rows_dt( dt, ukr_id, cntx );
	const bool  ukr_prefers_cols
	                  = bli_cntx_l3_vir_ukr_prefers_cols_dt( dt, ukr_id, cntx );
	bool        r_val = FALSE;

	if      ( bli_obj_is_row_stored( obj ) && ukr_prefers_rows ) r_val = TRUE;
	else if ( bli_obj_is_col_stored( obj ) && ukr_prefers_cols ) r_val = TRUE;

	return r_val;
}

BLIS_INLINE bool bli_cntx_l3_vir_ukr_dislikes_storage_of( obj_t* obj, l3ukr_t ukr_id, cntx_t* cntx )
{
	return ( bool )
	       !bli_cntx_l3_vir_ukr_prefers_storage_of( obj, ukr_id, cntx );
}

// -----------------------------------------------------------------------------

BLIS_INLINE bool bli_cntx_l3_sup_ker_prefers_rows_dt( num_t dt, stor3_t stor_id, cntx_t* cntx )
{
	const bool prefs = bli_cntx_get_l3_sup_ker_prefs_dt( dt, stor_id, cntx );

	// A ukernel preference of TRUE means the ukernel prefers row storage.
	return ( bool )
	       ( prefs == TRUE );
}

BLIS_INLINE bool bli_cntx_l3_sup_ker_prefers_cols_dt( num_t dt, stor3_t stor_id, cntx_t* cntx )
{
	const bool prefs = bli_cntx_get_l3_sup_ker_prefs_dt( dt, stor_id, cntx );

	// A ukernel preference of FALSE means the ukernel prefers column storage.
	return ( bool )
	       ( prefs == FALSE );
}

#if 0
// NOTE: These static functions aren't needed yet.

BLIS_INLINE bool bli_cntx_l3_sup_ker_prefers_storage_of( obj_t* obj, stor3_t stor_id, cntx_t* cntx )
{
	const num_t dt    = bli_obj_dt( obj );
	const bool  ukr_prefers_rows
	                  = bli_cntx_l3_sup_ker_prefers_rows_dt( dt, stor_id, cntx );
	const bool  ukr_prefers_cols
	                  = bli_cntx_l3_sup_ker_prefers_cols_dt( dt, stor_id, cntx );
	bool        r_val = FALSE;

	if      ( bli_obj_is_row_stored( obj ) && ukr_prefers_rows ) r_val = TRUE;
	else if ( bli_obj_is_col_stored( obj ) && ukr_prefers_cols ) r_val = TRUE;

	return r_val;
}

BLIS_INLINE bool bli_cntx_l3_sup_ker_dislikes_storage_of( obj_t* obj, stor3_t stor_id, cntx_t* cntx )
{
	return ( bool )
	       !bli_cntx_l3_sup_ker_prefers_storage_of( obj, stor_id, cntx );
}
#endif

// -----------------------------------------------------------------------------

//
// -- cntx_t modification (complex) --------------------------------------------
//

// NOTE: The framework does not use any of the following functions. We provide
// them in order to facilitate creating/modifying custom contexts.

BLIS_INLINE void bli_cntx_set_blksz( bszid_t bs_id, blksz_t* blksz, bszid_t mult_id, cntx_t* cntx )
{
	blksz_t* blkszs = bli_cntx_blkszs_buf( cntx );
	bszid_t* bmults = bli_cntx_bmults_buf( cntx );

	blkszs[ bs_id ] = *blksz;
	bmults[ bs_id ] = mult_id;
}

BLIS_INLINE void bli_cntx_set_blksz_def_dt( num_t dt, bszid_t bs_id, dim_t bs, cntx_t* cntx )
{
	blksz_t* blkszs = bli_cntx_blkszs_buf( cntx );
	blksz_t* blksz  = &blkszs[ bs_id ];

	bli_blksz_set_def( bs, dt, blksz );
}

BLIS_INLINE void bli_cntx_set_blksz_max_dt( num_t dt, bszid_t bs_id, dim_t bs, cntx_t* cntx )
{
	blksz_t* blkszs = bli_cntx_blkszs_buf( cntx );
	blksz_t* blksz  = &blkszs[ bs_id ];

	bli_blksz_set_max( bs, dt, blksz );
}

BLIS_INLINE void bli_cntx_set_l3_vir_ukr( l3ukr_t ukr_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l3_vir_ukrs_buf( cntx );

	funcs[ ukr_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_l3_nat_ukr( l3ukr_t ukr_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l3_nat_ukrs_buf( cntx );

	funcs[ ukr_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_l3_nat_ukr_prefs( l3ukr_t ukr_id, mbool_t* prefs, cntx_t* cntx )
{
	mbool_t* mbools = bli_cntx_l3_nat_ukrs_prefs_buf( cntx );

	mbools[ ukr_id ] = *prefs;
}

BLIS_INLINE void bli_cntx_set_l1f_ker( l1fkr_t ker_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l1f_kers_buf( cntx );

	funcs[ ker_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_l1v_ker( l1vkr_t ker_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_l1v_kers_buf( cntx );

	funcs[ ker_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_packm_ker( l1mkr_t ker_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_get_packm_kers( ker_id, cntx );

	funcs[ ker_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_packm_ker_dt( void_fp fp, num_t dt, l1mkr_t ker_id, cntx_t* cntx )
{
	func_t* func = ( func_t* )bli_cntx_get_packm_kers( ker_id, cntx );

	bli_func_set_dt( fp, dt, func );
}

BLIS_INLINE void bli_cntx_set_unpackm_ker( l1mkr_t ker_id, func_t* func, cntx_t* cntx )
{
	func_t* funcs = bli_cntx_get_unpackm_kers( ker_id, cntx );

	funcs[ ker_id ] = *func;
}

BLIS_INLINE void bli_cntx_set_unpackm_ker_dt( void_fp fp, num_t dt, l1mkr_t ker_id, cntx_t* cntx )
{
	func_t* func = ( func_t* )bli_cntx_get_unpackm_kers( ker_id, cntx );

	bli_func_set_dt( fp, dt, func );
}

// -----------------------------------------------------------------------------

// Function prototypes

BLIS_EXPORT_BLIS void bli_cntx_clear( cntx_t* cntx );

BLIS_EXPORT_BLIS void bli_cntx_set_blkszs( ind_t method, dim_t n_bs, ... );

BLIS_EXPORT_BLIS void bli_cntx_set_ind_blkszs( ind_t method, dim_t n_bs, ... );

BLIS_EXPORT_BLIS void bli_cntx_set_l3_nat_ukrs( dim_t n_ukrs, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_l3_vir_ukrs( dim_t n_ukrs, ... );

BLIS_EXPORT_BLIS void bli_cntx_set_l3_sup_thresh( dim_t n_thresh, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_l3_sup_handlers( dim_t n_ops, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_l3_sup_blkszs( dim_t n_bs, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_l3_sup_kers( dim_t n_ukrs, ... );

BLIS_EXPORT_BLIS void bli_cntx_set_l1f_kers( dim_t n_kers, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_l1v_kers( dim_t n_kers, ... );
BLIS_EXPORT_BLIS void bli_cntx_set_packm_kers( dim_t n_kers, ... );

BLIS_EXPORT_BLIS void bli_cntx_print( cntx_t* cntx );


#endif

