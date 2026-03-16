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

#define FUNCPTR_T packm_fp

typedef void (*FUNCPTR_T)
     (
       struc_t strucc,
       doff_t  diagoffc,
       diag_t  diagc,
       uplo_t  uploc,
       trans_t transc,
       pack_t  schema,
       bool    invdiag,
       bool    revifup,
       bool    reviflo,
       dim_t   m,
       dim_t   n,
       dim_t   m_max,
       dim_t   n_max,
       void*   kappa,
       void*   c, inc_t rs_c, inc_t cs_c,
       void*   p, inc_t rs_p, inc_t cs_p,
                  inc_t is_p,
                  dim_t pd_p, inc_t ps_p,
       void_fp packm_ker,
       cntx_t* cntx,
       thrinfo_t* thread
     );

static FUNCPTR_T GENARRAY(ftypes,packm_blk_var1);


static func_t packm_struc_cxk_kers[BLIS_NUM_PACK_SCHEMA_TYPES] =
{
    /* float (0)  scomplex (1)  double (2)  dcomplex (3) */
// 0000 row/col panels
    { { bli_spackm_struc_cxk,      bli_cpackm_struc_cxk,
        bli_dpackm_struc_cxk,      bli_zpackm_struc_cxk,      } },
// 0001 row/col panels: 4m interleaved
    { { NULL,                      bli_cpackm_struc_cxk_4mi,
        NULL,                      bli_zpackm_struc_cxk_4mi,  } },
// 0010 row/col panels: 3m interleaved
    { { NULL,                      bli_cpackm_struc_cxk_3mis,
        NULL,                      bli_zpackm_struc_cxk_3mis, } },
// 0011 row/col panels: 4m separated (NOT IMPLEMENTED)
    { { NULL,                      NULL,
        NULL,                      NULL,                      } },
// 0100 row/col panels: 3m separated
    { { NULL,                      bli_cpackm_struc_cxk_3mis,
        NULL,                      bli_zpackm_struc_cxk_3mis, } },
// 0101 row/col panels: real only
    { { NULL,                      bli_cpackm_struc_cxk_rih,
        NULL,                      bli_zpackm_struc_cxk_rih,  } },
// 0110 row/col panels: imaginary only
    { { NULL,                      bli_cpackm_struc_cxk_rih,
        NULL,                      bli_zpackm_struc_cxk_rih,  } },
// 0111 row/col panels: real+imaginary only
    { { NULL,                      bli_cpackm_struc_cxk_rih,
        NULL,                      bli_zpackm_struc_cxk_rih,  } },
// 1000 row/col panels: 1m-expanded (1e)
    { { NULL,                      bli_cpackm_struc_cxk_1er,
        NULL,                      bli_zpackm_struc_cxk_1er,  } },
// 1001 row/col panels: 1m-reordered (1r)
    { { NULL,                      bli_cpackm_struc_cxk_1er,
        NULL,                      bli_zpackm_struc_cxk_1er,  } },
};


void bli_packm_blk_var1
     (
       obj_t*   c,
       obj_t*   p,
       cntx_t*  cntx,
       cntl_t*  cntl,
       thrinfo_t* t
     )
{
#ifdef BLIS_ENABLE_GEMM_MD
	// Call a different packm implementation when the storage and target
	// datatypes differ.
	if ( bli_obj_dt( c ) != bli_obj_target_dt( c ) )
	{
		bli_packm_blk_var1_md( c, p, cntx, cntl, t );
		return;
	}
#endif

	num_t     dt_p       = bli_obj_dt( p );

	struc_t   strucc     = bli_obj_struc( c );
	doff_t    diagoffc   = bli_obj_diag_offset( c );
	diag_t    diagc      = bli_obj_diag( c );
	uplo_t    uploc      = bli_obj_uplo( c );
	trans_t   transc     = bli_obj_conjtrans_status( c );
	pack_t    schema     = bli_obj_pack_schema( p );
	bool      invdiag    = bli_obj_has_inverted_diag( p );
	bool      revifup    = bli_obj_is_pack_rev_if_upper( p );
	bool      reviflo    = bli_obj_is_pack_rev_if_lower( p );

	dim_t     m_p        = bli_obj_length( p );
	dim_t     n_p        = bli_obj_width( p );
	dim_t     m_max_p    = bli_obj_padded_length( p );
	dim_t     n_max_p    = bli_obj_padded_width( p );

	void*     buf_c      = bli_obj_buffer_at_off( c );
	inc_t     rs_c       = bli_obj_row_stride( c );
	inc_t     cs_c       = bli_obj_col_stride( c );

	void*     buf_p      = bli_obj_buffer_at_off( p );
	inc_t     rs_p       = bli_obj_row_stride( p );
	inc_t     cs_p       = bli_obj_col_stride( p );
	inc_t     is_p       = bli_obj_imag_stride( p );
	dim_t     pd_p       = bli_obj_panel_dim( p );
	inc_t     ps_p       = bli_obj_panel_stride( p );

	obj_t     kappa;
	void*     buf_kappa;

	func_t*   packm_kers;
	void_fp   packm_ker;

	FUNCPTR_T f;


	// Treatment of kappa (ie: packing during scaling) depends on
	// whether we are executing an induced method.
	if ( bli_is_nat_packed( schema ) )
	{
		// This branch is for native execution, where we assume that
		// the micro-kernel will always apply the alpha scalar of the
		// higher-level operation. Thus, we use BLIS_ONE for kappa so
		// that the underlying packm implementation does not perform
		// any scaling during packing.
		buf_kappa = bli_obj_buffer_for_const( dt_p, &BLIS_ONE );
	}
	else // if ( bli_is_ind_packed( schema ) )
	{
		obj_t* kappa_p;

		// The value for kappa we use will depend on whether the scalar
		// attached to A has a nonzero imaginary component. If it does,
		// then we will apply the scalar during packing to facilitate
		// implementing induced complex domain algorithms in terms of
		// real domain micro-kernels. (In the aforementioned situation,
		// applying a real scalar is easy, but applying a complex one is
		// harder, so we avoid the need altogether with the code below.)
		if ( bli_obj_scalar_has_nonzero_imag( p ) )
		{
			//printf( "applying non-zero imag kappa\n" );

			// Detach the scalar.
			bli_obj_scalar_detach( p, &kappa );

			// Reset the attached scalar (to 1.0).
			bli_obj_scalar_reset( p );

			kappa_p = &kappa;
		}
		else
		{
			// If the internal scalar of A has only a real component, then
			// we will apply it later (in the micro-kernel), and so we will
			// use BLIS_ONE to indicate no scaling during packing.
			kappa_p = &BLIS_ONE;
		}

		// Acquire the buffer to the kappa chosen above.
		buf_kappa = bli_obj_buffer_for_1x1( dt_p, kappa_p );
	}


#if 0
	if      ( bli_is_4mi_packed( schema ) ) packm_kers = packm_struc_cxk_4mi_kers;
	else if ( bli_is_3mi_packed( schema ) ||
	          bli_is_3ms_packed( schema ) ) packm_kers = packm_struc_cxk_3mis_kers;
	else if ( bli_is_ro_packed( schema ) ||
	          bli_is_io_packed( schema ) ||
	         bli_is_rpi_packed( schema ) )  packm_kers = packm_struc_cxk_rih_kers;
	else                                    packm_kers = packm_struc_cxk_kers;
#else
	// The original idea here was to read the packm_ukr from the context
	// if it is non-NULL. The problem is, it requires that we be able to
	// assume that the packm_ukr field is initialized to NULL, which it
	// currently is not.

	//func_t* cntx_packm_kers = bli_cntx_get_packm_ukr( cntx );

	//if ( bli_func_is_null_dt( dt_c, cntx_packm_kers ) )
	{
		// If the packm structure-aware kernel func_t in the context is
		// NULL (which is the default value after the context is created),
		// we use the default lookup table to determine the right func_t
		// for the current schema.
		const dim_t i = bli_pack_schema_index( schema );

		packm_kers = &packm_struc_cxk_kers[ i ];
	}
#if 0
	else // cntx's packm func_t overrides
	{
		// If the packm structure-aware kernel func_t in the context is
		// non-NULL (ie: assumed to be valid), we use that instead.
		//packm_kers = bli_cntx_packm_ukrs( cntx );
		packm_kers = cntx_packm_kers;
	}
#endif
#endif

	// Query the datatype-specific function pointer from the func_t object.
	packm_ker = bli_func_get_dt( dt_p, packm_kers );

	// Index into the type combination array to extract the correct
	// function pointer.
	f = ftypes[dt_p];

	// Invoke the function.
	f( strucc,
	   diagoffc,
	   diagc,
	   uploc,
	   transc,
	   schema,
	   invdiag,
	   revifup,
	   reviflo,
	   m_p,
	   n_p,
	   m_max_p,
	   n_max_p,
	   buf_kappa,
	   buf_c, rs_c, cs_c,
	   buf_p, rs_p, cs_p,
	          is_p,
	          pd_p, ps_p,
	   packm_ker,
	   cntx,
	   t );
}


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname, varname ) \
\
void PASTEMAC(ch,varname) \
     ( \
       struc_t strucc, \
       doff_t  diagoffc, \
       diag_t  diagc, \
       uplo_t  uploc, \
       trans_t transc, \
       pack_t  schema, \
       bool    invdiag, \
       bool    revifup, \
       bool    reviflo, \
       dim_t   m, \
       dim_t   n, \
       dim_t   m_max, \
       dim_t   n_max, \
       void*   kappa, \
       void*   c, inc_t rs_c, inc_t cs_c, \
       void*   p, inc_t rs_p, inc_t cs_p, \
                  inc_t is_p, \
                  dim_t pd_p, inc_t ps_p, \
       void_fp packm_ker, \
       cntx_t* cntx, \
       thrinfo_t* thread  \
     ) \
{ \
	PASTECH2(ch,opname,_ker_ft) packm_ker_cast = packm_ker; \
\
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict c_cast     = c; \
	ctype* restrict p_cast     = p; \
	ctype* restrict c_begin; \
	ctype* restrict p_begin; \
\
	dim_t           iter_dim; \
	dim_t           n_iter; \
	dim_t           it, ic, ip; \
	dim_t           ic0, ip0; \
	doff_t          ic_inc, ip_inc; \
	doff_t          diagoffc_i; \
	doff_t          diagoffc_inc; \
	dim_t           panel_len_full; \
	dim_t           panel_len_i; \
	dim_t           panel_len_max; \
	dim_t           panel_len_max_i; \
	dim_t           panel_dim_i; \
	dim_t           panel_dim_max; \
	dim_t           panel_off_i; \
	inc_t           vs_c; \
	inc_t           ldc; \
	inc_t           ldp, p_inc; \
	dim_t*          m_panel_full; \
	dim_t*          n_panel_full; \
	dim_t*          m_panel_use; \
	dim_t*          n_panel_use; \
	dim_t*          m_panel_max; \
	dim_t*          n_panel_max; \
	conj_t          conjc; \
	bool            row_stored; \
	bool            col_stored; \
	inc_t           is_p_use; \
	dim_t           ss_num; \
	dim_t           ss_den; \
\
	ctype* restrict c_use; \
	ctype* restrict p_use; \
	doff_t          diagoffp_i; \
\
\
	/* If C is zeros and part of a triangular matrix, then we don't need
	   to pack it. */ \
	if ( bli_is_zeros( uploc ) && \
	     bli_is_triangular( strucc ) ) return; \
\
	/* Extract the conjugation bit from the transposition argument. */ \
	conjc = bli_extract_conj( transc ); \
\
	/* If c needs a transposition, induce it so that we can more simply
	   express the remaining parameters and code. */ \
	if ( bli_does_trans( transc ) ) \
	{ \
		bli_swap_incs( &rs_c, &cs_c ); \
		bli_negate_diag_offset( &diagoffc ); \
		bli_toggle_uplo( &uploc ); \
		bli_toggle_trans( &transc ); \
	} \
\
	/* Create flags to incidate row or column storage. Note that the
	   schema bit that encodes row or column is describing the form of
	   micro-panel, not the storage in the micro-panel. Hence the
	   mismatch in "row" and "column" semantics. */ \
	row_stored = bli_is_col_packed( schema ); \
	col_stored = bli_is_row_packed( schema ); \
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
		ldc            = rs_c; \
		vs_c           = cs_c; \
		diagoffc_inc   = -( doff_t )panel_dim_max; \
		ldp            = rs_p; \
		m_panel_full   = &m; \
		n_panel_full   = &panel_dim_i; \
		m_panel_use    = &panel_len_i; \
		n_panel_use    = &panel_dim_i; \
		m_panel_max    = &panel_len_max_i; \
		n_panel_max    = &panel_dim_max; \
	} \
	else /* if ( col_stored ) */ \
	{ \
		/* Prepare to pack to column-stored row panels. */ \
		iter_dim       = m; \
		panel_len_full = n; \
		panel_len_max  = n_max; \
		panel_dim_max  = pd_p; \
		ldc            = cs_c; \
		vs_c           = rs_c; \
		diagoffc_inc   = ( doff_t )panel_dim_max; \
		ldp            = cs_p; \
		m_panel_full   = &panel_dim_i; \
		n_panel_full   = &n; \
		m_panel_use    = &panel_dim_i; \
		n_panel_use    = &panel_len_i; \
		m_panel_max    = &panel_dim_max; \
		n_panel_max    = &panel_len_max_i; \
	} \
\
	/* Compute the storage stride scaling. Usually this is just 1. However,
	   in the case of interleaved 3m, we need to scale by 3/2, and in the
	   cases of real-only, imag-only, or summed-only, we need to scale by
	   1/2. In both cases, we are compensating for the fact that pointer
	   arithmetic occurs in terms of complex elements rather than real
	   elements. */ \
	if      ( bli_is_3mi_packed( schema ) ) { ss_num = 3; ss_den = 2; } \
	else if ( bli_is_3ms_packed( schema ) ) { ss_num = 1; ss_den = 2; } \
	else if ( bli_is_rih_packed( schema ) ) { ss_num = 1; ss_den = 2; } \
	else                                    { ss_num = 1; ss_den = 1; } \
\
	/* Compute the total number of iterations we'll need. */ \
	n_iter = iter_dim / panel_dim_max + ( iter_dim % panel_dim_max ? 1 : 0 ); \
\
	/* Set the initial values and increments for indices related to C and P
	   based on whether reverse iteration was requested. */ \
	if ( ( revifup && bli_is_upper( uploc ) && bli_is_triangular( strucc ) ) || \
	     ( reviflo && bli_is_lower( uploc ) && bli_is_triangular( strucc ) ) ) \
	{ \
		ic0    = (n_iter - 1) * panel_dim_max; \
		ic_inc = -panel_dim_max; \
		ip0    = n_iter - 1; \
		ip_inc = -1; \
	} \
	else \
	{ \
		ic0    = 0; \
		ic_inc = panel_dim_max; \
		ip0    = 0; \
		ip_inc = 1; \
	} \
\
	p_begin = p_cast; \
\
	/* Query the number of threads and thread ids from the current thread's
	   packm thrinfo_t node. */ \
	const dim_t nt  = bli_thread_n_way( thread ); \
	const dim_t tid = bli_thread_work_id( thread ); \
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
	for ( ic  = ic0,    ip  = ip0,    it  = 0; it < n_iter; \
	      ic += ic_inc, ip += ip_inc, it += 1 ) \
	{ \
		panel_dim_i = bli_min( panel_dim_max, iter_dim - ic ); \
\
		diagoffc_i  = diagoffc + (ip  )*diagoffc_inc; \
		c_begin     = c_cast   + (ic  )*vs_c; \
\
		if ( bli_is_triangular( strucc ) &&  \
		     bli_is_unstored_subpart_n( diagoffc_i, uploc, *m_panel_full, *n_panel_full ) ) \
		{ \
			/* This case executes if the panel belongs to a triangular
			   matrix AND is completely unstored (ie: zero). If the panel
			   is unstored, we do nothing. (Notice that we don't even
			   increment p_begin.) */ \
\
			continue; \
		} \
		else if ( bli_is_triangular( strucc ) &&  \
		          bli_intersects_diag_n( diagoffc_i, *m_panel_full, *n_panel_full ) ) \
		{ \
			/* This case executes if the panel belongs to a triangular
			   matrix AND is diagonal-intersecting. Notice that we
			   cannot bury the following conditional logic into
			   packm_struc_cxk() because we need to know the value of
			   panel_len_max_i so we can properly increment p_inc. */ \
\
			/* Sanity check. Diagonals should not intersect the short end of
			   a micro-panel. If they do, then somehow the constraints on
			   cache blocksizes being a whole multiple of the register
			   blocksizes was somehow violated. */ \
			if ( ( col_stored && diagoffc_i < 0 ) || \
			     ( row_stored && diagoffc_i > 0 ) ) \
				bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
\
			if      ( ( row_stored && bli_is_upper( uploc ) ) || \
			          ( col_stored && bli_is_lower( uploc ) ) )  \
			{ \
				panel_off_i     = 0; \
				panel_len_i     = bli_abs( diagoffc_i ) + panel_dim_i; \
				panel_len_max_i = bli_min( bli_abs( diagoffc_i ) + panel_dim_max, \
				                           panel_len_max ); \
				diagoffp_i      = diagoffc_i; \
			} \
			else /* if ( ( row_stored && bli_is_lower( uploc ) ) || \
			             ( col_stored && bli_is_upper( uploc ) ) )  */ \
			{ \
				panel_off_i     = bli_abs( diagoffc_i ); \
				panel_len_i     = panel_len_full - panel_off_i; \
				panel_len_max_i = panel_len_max  - panel_off_i; \
				diagoffp_i      = 0; \
			} \
\
			c_use = c_begin + (panel_off_i  )*ldc; \
			p_use = p_begin; \
\
			/* We need to re-compute the imaginary stride as a function of
			   panel_len_max_i since triangular packed matrices have panels
			   of varying lengths. NOTE: This imaginary stride value is
			   only referenced by the packm kernels for induced methods. */ \
			is_p_use  = ldp * panel_len_max_i; \
\
			/* We nudge the imaginary stride up by one if it is odd. */ \
			is_p_use += ( bli_is_odd( is_p_use ) ? 1 : 0 ); \
\
			/* NOTE: We MUST use round-robin partitioning when packing
			   micropanels of a triangular matrix. Hermitian/symmetric
			   and general packing may use slab or round-robin, depending
			   on which was selected at configure-time. */ \
			if ( bli_packm_my_iter_rr( it, it_start, it_end, tid, nt ) ) \
			{ \
				packm_ker_cast( strucc, \
				                diagoffp_i, \
				                diagc, \
				                uploc, \
				                conjc, \
				                schema, \
				                invdiag, \
				                *m_panel_use, \
				                *n_panel_use, \
				                *m_panel_max, \
				                *n_panel_max, \
				                kappa_cast, \
				                c_use, rs_c, cs_c, \
				                p_use, rs_p, cs_p, \
			                           is_p_use, \
				                cntx ); \
			} \
\
			/* NOTE: This value is usually LESS than ps_p because triangular
			   matrices usually have several micro-panels that are shorter
			   than a "full" micro-panel. */ \
			p_inc = ( is_p_use * ss_num ) / ss_den; \
		} \
		else if ( bli_is_herm_or_symm( strucc ) ) \
		{ \
			/* This case executes if the panel belongs to a Hermitian or
			   symmetric matrix, which includes stored, unstored, and
			   diagonal-intersecting panels. */ \
\
			c_use = c_begin; \
			p_use = p_begin; \
\
			panel_len_i     = panel_len_full; \
			panel_len_max_i = panel_len_max; \
\
			is_p_use = is_p; \
\
			/* The definition of bli_packm_my_iter() will depend on whether slab
			   or round-robin partitioning was requested at configure-time. */ \
			if ( bli_packm_my_iter( it, it_start, it_end, tid, nt ) ) \
			{ \
				packm_ker_cast( strucc, \
				                diagoffc_i, \
				                diagc, \
				                uploc, \
				                conjc, \
				                schema, \
				                invdiag, \
				                *m_panel_use, \
				                *n_panel_use, \
				                *m_panel_max, \
				                *n_panel_max, \
				                kappa_cast, \
				                c_use, rs_c, cs_c, \
				                p_use, rs_p, cs_p, \
			                           is_p_use, \
				                cntx ); \
			} \
\
			p_inc = ps_p; \
		} \
		else \
		{ \
			/* This case executes if the panel is general, or, if the
			   panel is part of a triangular matrix and is neither unstored
			   (ie: zero) nor diagonal-intersecting. */ \
\
			c_use = c_begin; \
			p_use = p_begin; \
\
			panel_len_i     = panel_len_full; \
			panel_len_max_i = panel_len_max; \
\
			is_p_use = is_p; \
\
			/* The definition of bli_packm_my_iter() will depend on whether slab
			   or round-robin partitioning was requested at configure-time. */ \
			if ( bli_packm_my_iter( it, it_start, it_end, tid, nt ) ) \
			{ \
				packm_ker_cast( BLIS_GENERAL, \
				                0, \
				                diagc, \
				                BLIS_DENSE, \
				                conjc, \
				                schema, \
				                invdiag, \
				                *m_panel_use, \
				                *n_panel_use, \
				                *m_panel_max, \
				                *n_panel_max, \
				                kappa_cast, \
				                c_use, rs_c, cs_c, \
				                p_use, rs_p, cs_p, \
			                           is_p_use, \
				                cntx ); \
			} \
\
			/* NOTE: This value is equivalent to ps_p. */ \
			p_inc = ps_p; \
		} \
\
		p_begin += p_inc; \
\
	} \
}

INSERT_GENTFUNCR_BASIC( packm, packm_blk_var1 )



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
