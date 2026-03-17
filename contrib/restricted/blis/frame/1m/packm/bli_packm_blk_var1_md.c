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

#ifdef BLIS_ENABLE_GEMM_MD

#define FUNCPTR_T packm_fp

typedef void (*FUNCPTR_T)(
                           trans_t transc,
                           pack_t  schema,
                           dim_t   m,
                           dim_t   n,
                           dim_t   m_max,
                           dim_t   n_max,
                           void*   kappa,
                           void*   c, inc_t rs_c, inc_t cs_c,
                           void*   p, inc_t rs_p, inc_t cs_p,
                                      inc_t is_p,
                                      dim_t pd_p, inc_t ps_p,
                           cntx_t* cntx,
                           thrinfo_t* thread
                         );

static FUNCPTR_T GENARRAY2_ALL(ftypes,packm_blk_var1_md);


void bli_packm_blk_var1_md
     (
       obj_t*   c,
       obj_t*   p,
       cntx_t*  cntx,
       cntl_t*  cntl,
       thrinfo_t* t
     )
{
	num_t     dt_c       = bli_obj_dt( c );
	num_t     dt_p       = bli_obj_dt( p );

	trans_t   transc     = bli_obj_conjtrans_status( c );
	pack_t    schema     = bli_obj_pack_schema( p );

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


	// Index into the type combination array to extract the correct
	// function pointer.
	f = ftypes[dt_c][dt_p];

	// Invoke the function.
	f(
	   transc,
	   schema,
	   m_p,
	   n_p,
	   m_max_p,
	   n_max_p,
	   buf_kappa,
	   buf_c, rs_c, cs_c,
	   buf_p, rs_p, cs_p,
	          is_p,
	          pd_p, ps_p,
	   cntx,
	   t );
}


#undef  GENTFUNC2
#define GENTFUNC2( ctype_c, ctype_p, chc, chp, varname ) \
\
void PASTEMAC2(chc,chp,varname) \
     ( \
       trans_t transc, \
       pack_t  schema, \
       dim_t   m, \
       dim_t   n, \
       dim_t   m_max, \
       dim_t   n_max, \
       void*   kappa, \
       void*   c, inc_t rs_c, inc_t cs_c, \
       void*   p, inc_t rs_p, inc_t cs_p, \
                  inc_t is_p, \
                  dim_t pd_p, inc_t ps_p, \
       cntx_t* cntx, \
       thrinfo_t* thread  \
     ) \
{ \
	ctype_p* restrict kappa_cast = kappa; \
	ctype_c* restrict c_cast     = c; \
	ctype_p* restrict p_cast     = p; \
	ctype_c* restrict c_begin; \
	ctype_p* restrict p_begin; \
\
	dim_t             iter_dim; \
	dim_t             n_iter; \
	dim_t             it, ic, ip; \
	doff_t            ic_inc, ip_inc; \
	dim_t             panel_len_full; \
	dim_t             panel_len_i; \
	dim_t             panel_len_max; \
	dim_t             panel_len_max_i; \
	dim_t             panel_dim_i; \
	dim_t             panel_dim_max; \
	inc_t             vs_c; \
	inc_t             p_inc; \
	dim_t*            m_panel_use; \
	dim_t*            n_panel_use; \
	dim_t*            m_panel_max; \
	dim_t*            n_panel_max; \
	conj_t            conjc; \
	bool              row_stored; \
	bool              col_stored; \
\
	ctype_c* restrict c_use; \
	ctype_p* restrict p_use; \
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
	row_stored = bli_is_col_packed( schema ); \
	col_stored = bli_is_row_packed( schema ); \
\
	( void )col_stored; \
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
		vs_c           = rs_c; \
		m_panel_use    = &panel_dim_i; \
		n_panel_use    = &panel_len_i; \
		m_panel_max    = &panel_dim_max; \
		n_panel_max    = &panel_len_max_i; \
	} \
\
	/* Compute the total number of iterations we'll need. */ \
	n_iter = iter_dim / panel_dim_max + ( iter_dim % panel_dim_max ? 1 : 0 ); \
\
	{ \
		ic_inc = panel_dim_max; \
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
	/* Suppress unused variable warnings when slab partitioning is enabled,
	   since the slab-based definition of bli_packm_my_iter() does not
	   actually use tid or nt. */ \
	( void )nt; ( void )tid; \
\
	dim_t it_start, it_end, it_inc; \
\
	/* Determine the thread range and increment using the current thread's
	   packm thrinfo_t node. NOTE: The definition of bli_thread_range_jrir()
	   will depend on whether slab or round-robin partitioning was requested
	   at configure-time. */ \
	bli_thread_range_jrir( thread, n_iter, 1, FALSE, &it_start, &it_end, &it_inc ); \
\
	for ( ic  = 0,      ip  = 0,      it  = 0; it < n_iter; \
	      ic += ic_inc, ip += ip_inc, it += 1 ) \
	{ \
		panel_dim_i = bli_min( panel_dim_max, iter_dim - ic ); \
\
		c_begin     = c_cast + (ic  )*vs_c; \
\
		{ \
			c_use = c_begin; \
			p_use = p_begin; \
\
			panel_len_i     = panel_len_full; \
			panel_len_max_i = panel_len_max; \
\
			if ( bli_packm_my_iter( it, it_start, it_end, tid, nt ) ) \
			{ \
				PASTEMAC2(chc,chp,packm_struc_cxk_md) \
				( \
				  conjc, \
				  schema, \
				  *m_panel_use, \
				  *n_panel_use, \
				  *m_panel_max, \
				  *n_panel_max, \
				  kappa_cast, \
				  c_use, rs_c, cs_c, \
				  p_use, rs_p, cs_p, \
			             is_p, \
				  cntx \
				); \
			} \
\
			p_inc = ps_p; \
		} \
\
/*
if ( row_stored ) \
PASTEMAC(chp,fprintm)( stdout, "packm_blk_var1_md: b packed", *m_panel_max, *n_panel_max, \
                                p_use, rs_p, cs_p, "%5.2f", "" ); \
else \
PASTEMAC(chp,fprintm)( stdout, "packm_blk_var1_md: a packed", *m_panel_max, *n_panel_max, \
                                p_use, rs_p, cs_p, "%5.2f", "" ); \
*/ \
\
		p_begin += p_inc; \
\
	} \
}

INSERT_GENTFUNC2_BASIC0( packm_blk_var1_md )
INSERT_GENTFUNC2_MIXDP0( packm_blk_var1_md )

#endif
