/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018, Advanced Micro Devices, Inc.

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

//
// Define BLAS-like interfaces to the variants.
//

#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname, varname ) \
\
void PASTEMAC(ch,varname) \
     ( \
       trans_t          transc, \
       pack_t           schema, \
       dim_t            m, \
       dim_t            n, \
       dim_t            m_max, \
       dim_t            n_max, \
       ctype*  restrict kappa, \
       ctype*  restrict c, inc_t rs_c, inc_t cs_c, \
       ctype*  restrict p, inc_t rs_p, inc_t cs_p, \
                           dim_t pd_p, inc_t ps_p, \
       cntx_t* restrict cntx, \
       thrinfo_t* restrict thread  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict c_cast     = c; \
	ctype* restrict p_cast     = p; \
\
	dim_t           iter_dim; \
	dim_t           n_iter; \
	dim_t           it, ic; \
	dim_t           ic0; \
	doff_t          ic_inc; \
	dim_t           panel_len_full; \
	dim_t           panel_len_i; \
	dim_t           panel_len_max; \
	dim_t           panel_len_max_i; \
	dim_t           panel_dim_i; \
	dim_t           panel_dim_max; \
	inc_t           vs_c; \
	inc_t           ldc; \
	inc_t           ldp, p_inc; \
	conj_t          conjc; \
\
\
	/* Extract the conjugation bit from the transposition argument. */ \
	conjc = bli_extract_conj( transc ); \
\
	/* If c needs a transposition, induce it so that we can more simply
	   express the remaining parameters and code. */ \
	if ( bli_does_trans( transc ) ) \
	{ \
		bli_swap_incs( &rs_c, &cs_c ); \
		bli_toggle_trans( &transc ); \
	} \
\
	/* Create flags to incidate row or column storage. Note that the
	   schema bit that encodes row or column is describing the form of
	   micro-panel, not the storage in the micro-panel. Hence the
	   mismatch in "row" and "column" semantics. */ \
	bool row_stored = bli_is_col_packed( schema ); \
	/*bool col_stored = bli_is_row_packed( schema );*/ \
\
	/* If the row storage flag indicates row storage, then we are packing
	   to column panels; otherwise, if the strides indicate column storage,
	   we are packing to row panels. */ \
	if ( row_stored ) \
	{ \
		/* Prepare to pack to row-stored column panels. */ \
		iter_dim       = n; \
		panel_len_full = m; \
		panel_len_max  = m_max; \
		panel_dim_max  = pd_p; \
		vs_c           = cs_c; \
		ldc            = rs_c; \
		ldp            = rs_p; \
	} \
	else /* if ( col_stored ) */ \
	{ \
		/* Prepare to pack to column-stored row panels. */ \
		iter_dim       = m; \
		panel_len_full = n; \
		panel_len_max  = n_max; \
		panel_dim_max  = pd_p; \
		vs_c           = rs_c; \
		ldc            = cs_c; \
		ldp            = cs_p; \
	} \
\
	/* Compute the total number of iterations we'll need. */ \
	n_iter = iter_dim / panel_dim_max + ( iter_dim % panel_dim_max ? 1 : 0 ); \
\
	/* Set the initial values and increments for indices related to C and P
	   based on whether reverse iteration was requested. */ \
	{ \
		ic0    = 0; \
		ic_inc = panel_dim_max; \
	} \
\
	ctype* restrict p_begin = p_cast; \
\
	/* Query the number of threads and thread ids from the current thread's
	   packm thrinfo_t node. */ \
	const dim_t nt  = bli_thread_n_way( thread ); \
	const dim_t tid = bli_thread_work_id( thread ); \
\
	/* Suppress warnings in case tid isn't used (ie: as in slab partitioning). */ \
	( void )nt; \
	( void )tid; \
\
	dim_t it_start, it_end, it_inc; \
\
	/* Determine the thread range and increment using the current thread's
	   packm thrinfo_t node. NOTE: The definition of bli_thread_range_jrir()
	   will depend on whether slab or round-robin partitioning was requested
	   at configure-time. */ \
	bli_thread_range_jrir( thread, n_iter, 1, FALSE, &it_start, &it_end, &it_inc ); \
\
	/* Iterate over every logical micropanel in the source matrix. */ \
	for ( ic  = ic0,    it  = 0; it < n_iter; \
	      ic += ic_inc, it += 1 ) \
	{ \
		panel_dim_i = bli_min( panel_dim_max, iter_dim - ic ); \
\
		ctype* restrict c_begin = c_cast   + (ic  )*vs_c; \
\
		ctype* restrict c_use = c_begin; \
		ctype* restrict p_use = p_begin; \
\
		{ \
			panel_len_i     = panel_len_full; \
			panel_len_max_i = panel_len_max; \
\
			/* The definition of bli_packm_my_iter() will depend on whether slab
			   or round-robin partitioning was requested at configure-time. */ \
			if ( bli_packm_my_iter( it, it_start, it_end, tid, nt ) ) \
			{ \
				PASTEMAC(ch,packm_cxk) \
				( \
				  conjc, \
				  schema, \
				  panel_dim_i, \
				  panel_dim_max, \
				  panel_len_i, \
				  panel_len_max_i, \
				  kappa_cast, \
				  c_use, vs_c, ldc, \
				  p_use,       ldp, \
				  cntx  \
				); \
			} \
\
			/* NOTE: This value is equivalent to ps_p. */ \
			p_inc = ps_p; \
		} \
\
		p_begin += p_inc; \
\
/*
if ( row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_sup_var1: b packed", panel_len_max, panel_dim_max, \
                      p_use,         rs_p, cs_p, "%5.2f", "" ); \
if ( !row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_sup_var1: a packed", panel_dim_max, panel_len_max, \
                      p_use,         rs_p, cs_p, "%5.2f", "" ); \
*/ \
	} \
\
}

INSERT_GENTFUNCR_BASIC( packm, packm_sup_var1 )



/*
if ( row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_var2: b", m, n, \
                      c_cast,        rs_c, cs_c, "%4.1f", "" ); \
if ( col_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_var2: a", m, n, \
                      c_cast,        rs_c, cs_c, "%4.1f", "" ); \
*/
/*
if ( row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: b packed", *m_panel_max, *n_panel_max, \
                               p_use, rs_p, cs_p, "%5.2f", "" ); \
else \
PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: a packed", *m_panel_max, *n_panel_max, \
                               p_use, rs_p, cs_p, "%5.2f", "" ); \
*/ \
\
/*
if ( col_stored ) { \
	if ( bli_thread_work_id( thread ) == 0 ) \
	{ \
	printf( "packm_blk_var1: thread %lu  (a = %p, ap = %p)\n", bli_thread_work_id( thread ), c_use, p_use ); \
	fflush( stdout ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: a", *m_panel_use, *n_panel_use, \
	                      ( ctype* )c_use,         rs_c, cs_c, "%4.1f", "" ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: ap", *m_panel_max, *n_panel_max, \
	                      ( ctype* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
	fflush( stdout ); \
	} \
bli_thread_barrier( thread ); \
	if ( bli_thread_work_id( thread ) == 1 ) \
	{ \
	printf( "packm_blk_var1: thread %lu  (a = %p, ap = %p)\n", bli_thread_work_id( thread ), c_use, p_use ); \
	fflush( stdout ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: a", *m_panel_use, *n_panel_use, \
	                      ( ctype* )c_use,         rs_c, cs_c, "%4.1f", "" ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: ap", *m_panel_max, *n_panel_max, \
	                      ( ctype* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
	fflush( stdout ); \
	} \
bli_thread_barrier( thread ); \
} \
else { \
	if ( bli_thread_work_id( thread ) == 0 ) \
	{ \
	printf( "packm_blk_var1: thread %lu  (b = %p, bp = %p)\n", bli_thread_work_id( thread ), c_use, p_use ); \
	fflush( stdout ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: b", *m_panel_use, *n_panel_use, \
	                      ( ctype* )c_use,         rs_c, cs_c, "%4.1f", "" ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: bp", *m_panel_max, *n_panel_max, \
	                      ( ctype* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
	fflush( stdout ); \
	} \
bli_thread_barrier( thread ); \
	if ( bli_thread_work_id( thread ) == 1 ) \
	{ \
	printf( "packm_blk_var1: thread %lu  (b = %p, bp = %p)\n", bli_thread_work_id( thread ), c_use, p_use ); \
	fflush( stdout ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: b", *m_panel_use, *n_panel_use, \
	                      ( ctype* )c_use,         rs_c, cs_c, "%4.1f", "" ); \
	PASTEMAC(ch,fprintm)( stdout, "packm_blk_var1: bp", *m_panel_max, *n_panel_max, \
	                      ( ctype* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
	fflush( stdout ); \
	} \
bli_thread_barrier( thread ); \
} \
*/
/*
		if ( bli_is_4mi_packed( schema ) ) { \
		printf( "packm_var2: is_p_use = %lu\n", is_p_use ); \
		if ( col_stored ) { \
		if ( 0 ) \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: a_r", *m_panel_use, *n_panel_use, \
		                       ( ctype_r* )c_use,         2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: ap_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use,            rs_p, cs_p, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: ap_i", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use + is_p_use, rs_p, cs_p, "%4.1f", "" ); \
		} \
		if ( row_stored ) { \
		if ( 0 ) \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: b_r", *m_panel_use, *n_panel_use, \
		                       ( ctype_r* )c_use,         2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: bp_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use,            rs_p, cs_p, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: bp_i", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use + is_p_use, rs_p, cs_p, "%4.1f", "" ); \
		} \
		} \
*/
/*
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: bp_rpi", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
*/
/*
		if ( row_stored ) { \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: b_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )c_use,        2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: b_i", *m_panel_max, *n_panel_max, \
		                       (( ctype_r* )c_use)+rs_c, 2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: bp_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
		inc_t is_b = rs_p * *m_panel_max; \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: bp_i", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use + is_b, rs_p, cs_p, "%4.1f", "" ); \
		} \
*/
/*
		if ( col_stored ) { \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: a_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )c_use,        2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: a_i", *m_panel_max, *n_panel_max, \
		                       (( ctype_r* )c_use)+rs_c, 2*rs_c, 2*cs_c, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: ap_r", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use,         rs_p, cs_p, "%4.1f", "" ); \
		PASTEMAC(chr,fprintm)( stdout, "packm_var2: ap_i", *m_panel_max, *n_panel_max, \
		                       ( ctype_r* )p_use + p_inc, rs_p, cs_p, "%4.1f", "" ); \
		} \
*/

#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname, varname ) \
\
void PASTEMAC(ch,varname) \
     ( \
       trans_t          transc, \
       pack_t           schema, \
       dim_t            m, \
       dim_t            n, \
       ctype*  restrict kappa, \
       ctype*  restrict c, inc_t rs_c, inc_t cs_c, \
       ctype*  restrict p, inc_t rs_p, inc_t cs_p, \
       cntx_t* restrict cntx, \
       thrinfo_t* restrict thread  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict c_cast     = c; \
	ctype* restrict p_cast     = p; \
\
	dim_t           iter_dim; \
	dim_t           n_iter; \
	dim_t           it; \
	dim_t           vector_len; \
	inc_t           incc, ldc; \
	inc_t           incp, ldp; \
	conj_t          conjc; \
\
\
	/* Extract the conjugation bit from the transposition argument. */ \
	conjc = bli_extract_conj( transc ); \
\
	/* If c needs a transposition, induce it so that we can more simply
	   express the remaining parameters and code. */ \
	if ( bli_does_trans( transc ) ) \
	{ \
		bli_swap_incs( &rs_c, &cs_c ); \
		bli_toggle_trans( &transc ); \
	} \
\
	/* Create flags to incidate row or column storage. Note that the
	   schema bit that encodes row or column is describing the form of
	   micro-panel, not the storage in the micro-panel. Hence the
	   mismatch in "row" and "column" semantics. */ \
	bool col_stored = bli_is_col_packed( schema ); \
	/*bool row_stored = bli_is_row_packed( schema );*/ \
\
	if ( col_stored ) \
	{ \
		/* Prepare to pack to a column-stored matrix. */ \
		iter_dim       = n; \
		vector_len     = m; \
		incc           = rs_c; \
		ldc            = cs_c; \
		incp           = 1; \
		ldp            = cs_p; \
	} \
	else /* if ( row_stored ) */ \
	{ \
		/* Prepare to pack to a row-stored matrix. */ \
		iter_dim       = m; \
		vector_len     = n; \
		incc           = cs_c; \
		ldc            = rs_c; \
		incp           = 1; \
		ldp            = rs_p; \
	} \
\
	/* Compute the total number of iterations we'll need. */ \
	n_iter = iter_dim; \
\
\
	ctype* restrict p_begin = p_cast; \
\
	/* Query the number of threads and thread ids from the current thread's
	   packm thrinfo_t node. */ \
	const dim_t nt  = bli_thread_n_way( thread ); \
	const dim_t tid = bli_thread_work_id( thread ); \
\
	/* Suppress warnings in case tid isn't used (ie: as in slab partitioning). */ \
	( void )nt; \
	( void )tid; \
\
	dim_t it_start, it_end, it_inc; \
\
	/* Determine the thread range and increment using the current thread's
	   packm thrinfo_t node. NOTE: The definition of bli_thread_range_jrir()
	   will depend on whether slab or round-robin partitioning was requested
	   at configure-time. */ \
	bli_thread_range_jrir( thread, n_iter, 1, FALSE, &it_start, &it_end, &it_inc ); \
\
	/* Iterate over every logical micropanel in the source matrix. */ \
	for ( it = 0; it < n_iter; it += 1 ) \
	{ \
		ctype* restrict c_begin = c_cast + (it  )*ldc; \
\
		ctype* restrict c_use = c_begin; \
		ctype* restrict p_use = p_begin; \
\
		{ \
			/* The definition of bli_packm_my_iter() will depend on whether slab
			   or round-robin partitioning was requested at configure-time. */ \
			if ( bli_packm_my_iter( it, it_start, it_end, tid, nt ) ) \
			{ \
				PASTEMAC2(ch,scal2v,BLIS_TAPI_EX_SUF) \
				( \
				  conjc, \
				  vector_len, \
				  kappa_cast, \
				  c_use, incc, \
				  p_use, incp, \
				  cntx, \
				  NULL  \
				); \
			} \
\
		} \
\
		p_begin += ldp; \
\
/*
if ( row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_sup_var1: b packed", panel_len_max, panel_dim_max, \
                      p_use,         rs_p, cs_p, "%5.2f", "" ); \
if ( !row_stored ) \
PASTEMAC(ch,fprintm)( stdout, "packm_sup_var1: a packed", panel_dim_max, panel_len_max, \
                      p_use,         rs_p, cs_p, "%5.2f", "" ); \
*/ \
	} \
}

INSERT_GENTFUNCR_BASIC( packm, packm_sup_var2 )

