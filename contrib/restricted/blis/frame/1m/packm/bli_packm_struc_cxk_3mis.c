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

#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, varname, kername ) \
\
void PASTEMAC(ch,varname) \
     ( \
       struc_t         strucc, \
       doff_t          diagoffc, \
       diag_t          diagc, \
       uplo_t          uploc, \
       conj_t          conjc, \
       pack_t          schema, \
       bool            invdiag, \
       dim_t           m_panel, \
       dim_t           n_panel, \
       dim_t           m_panel_max, \
       dim_t           n_panel_max, \
       ctype* restrict kappa, \
       ctype* restrict c, inc_t rs_c, inc_t cs_c, \
       ctype* restrict p, inc_t rs_p, inc_t cs_p, \
                          inc_t is_p, \
       cntx_t*         cntx  \
     ) \
{ \
	dim_t  panel_dim; \
	dim_t  panel_dim_max; \
	dim_t  panel_len; \
	dim_t  panel_len_max; \
	inc_t  incc, ldc; \
	inc_t  ldp; \
\
\
	/* Determine the dimensions and relative strides of the micro-panel
	   based on its pack schema. */ \
	if ( bli_is_col_packed( schema ) ) \
	{ \
		/* Prepare to pack to row-stored column panel. */ \
		panel_dim     = n_panel; \
		panel_dim_max = n_panel_max; \
		panel_len     = m_panel; \
		panel_len_max = m_panel_max; \
		incc          = cs_c; \
		ldc           = rs_c; \
		ldp           = rs_p; \
	} \
	else /* if ( bli_is_row_packed( schema ) ) */ \
	{ \
		/* Prepare to pack to column-stored row panel. */ \
		panel_dim     = m_panel; \
		panel_dim_max = m_panel_max; \
		panel_len     = n_panel; \
		panel_len_max = n_panel_max; \
		incc          = rs_c; \
		ldc           = cs_c; \
		ldp           = cs_p; \
	} \
\
\
	/* Handle micro-panel packing based on the structure of the matrix
	   being packed. */ \
	if      ( bli_is_general( strucc ) ) \
	{ \
		/* For micro-panels of general matrices, we can call the pack
		   kernel front-end directly. */ \
		PASTEMAC(ch,kername) \
		( \
		  conjc, \
		  panel_dim, \
		  panel_dim_max, \
		  panel_len, \
		  panel_len_max, \
		  kappa, \
		  c, incc, ldc, \
		  p, is_p, ldp, \
		  cntx  \
		); \
	} \
	else if ( bli_is_herm_or_symm( strucc ) ) \
	{ \
		/* Call a helper function for micro-panels of Hermitian/symmetric
		   matrices. */ \
		PASTEMAC(ch,packm_herm_cxk_3mis) \
		( \
		  strucc, \
		  diagoffc, \
		  uploc, \
		  conjc, \
		  schema, \
		  m_panel, \
		  n_panel, \
		  m_panel_max, \
		  n_panel_max, \
		  panel_dim, \
		  panel_dim_max, \
		  panel_len, \
		  panel_len_max, \
		  kappa, \
		  c, rs_c, cs_c, \
		     incc, ldc, \
		  p, rs_p, cs_p, \
		     is_p, ldp, \
		  cntx  \
		); \
	} \
	else /* ( bli_is_triangular( strucc ) ) */ \
	{ \
		/* Call a helper function for micro-panels of triangular
		   matrices. */ \
		PASTEMAC(ch,packm_tri_cxk_3mis) \
		( \
		  strucc, \
		  diagoffc, \
		  diagc, \
		  uploc, \
		  conjc, \
		  schema, \
		  invdiag, \
		  m_panel, \
		  n_panel, \
		  m_panel_max, \
		  n_panel_max, \
		  panel_dim, \
		  panel_dim_max, \
		  panel_len, \
		  panel_len_max, \
		  kappa, \
		  c, rs_c, cs_c, \
		     incc, ldc, \
		  p, rs_p, cs_p, \
		     is_p, ldp, \
		  cntx  \
		); \
	} \
\
\
	/* If m_panel < m_panel_max, or n_panel < n_panel_max, we would normally
	   fill the edge region (the bottom m_panel_max - m_panel rows or right-
	   side n_panel_max - n_panel columns) of the micropanel with zeros.
	   However, this responsibility has been moved to the packm microkernel.
	   This change allows experts to use custom kernels that pack to custom
	   packing formats when the problem size is not a nice multiple of the
	   register blocksize. */ \
/*
	if ( m_panel != m_panel_max ) \
	{ \
		ctype_r* restrict zero_r     = PASTEMAC(chr,0); \
		dim_t             i          = m_panel; \
		dim_t             m_edge     = m_panel_max - i; \
		dim_t             n_edge     = n_panel_max; \
		ctype_r*          p_edge_r   = ( ctype_r* )p +          (i  )*rs_p; \
		ctype_r*          p_edge_i   = ( ctype_r* )p +   is_p + (i  )*rs_p; \
		ctype_r*          p_edge_rpi = ( ctype_r* )p + 2*is_p + (i  )*rs_p; \
\
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_r, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_i, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_rpi, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
	} \
*/ \
\
/*
	if ( n_panel != n_panel_max ) \
	{ \
		ctype_r* restrict zero_r     = PASTEMAC(chr,0); \
		dim_t             j          = n_panel; \
		dim_t             m_edge     = m_panel_max; \
		dim_t             n_edge     = n_panel_max - j; \
		ctype_r*          p_edge_r   = ( ctype_r* )p +          (j  )*cs_p; \
		ctype_r*          p_edge_i   = ( ctype_r* )p +   is_p + (j  )*cs_p; \
		ctype_r*          p_edge_rpi = ( ctype_r* )p + 2*is_p + (j  )*cs_p; \
\
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_r, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_i, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
		PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_edge, \
		  n_edge, \
		  zero_r, \
		  p_edge_rpi, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
	} \
*/ \
\
\
	if ( bli_is_triangular( strucc ) ) \
	{ \
		/* If this panel is an edge case in both panel dimension and length,
		   then it must be a bottom-right corner case. Set the part of the
		   diagonal that extends into the zero-padded region to identity.
		   NOTE: This is actually only necessary when packing for trsm, as
		   it helps prevent NaNs and Infs from creeping into the computation.
		   However, we set the region to identity for trmm as well. Those
		   1.0's end up getting muliplied by the 0.0's in the zero-padded
		   region of the other matrix, so there is no harm in this. */ \
		if ( m_panel != m_panel_max && \
		     n_panel != n_panel_max ) \
		{ \
			ctype_r* restrict one_r  = PASTEMAC(chr,1); \
			ctype_r* restrict zero_r = PASTEMAC(chr,0); \
			dim_t             i      = m_panel; \
			dim_t             j      = n_panel; \
			dim_t             m_br   = m_panel_max - i; \
			dim_t             n_br   = n_panel_max - j; \
			ctype_r*          p_br_r = ( ctype_r* )p +        (i  )*rs_p + (j  )*cs_p; \
			ctype_r*          p_br_i = ( ctype_r* )p + is_p + (i  )*rs_p + (j  )*cs_p; \
\
			PASTEMAC2(chr,setd,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  0, \
			  m_br, \
			  n_br, \
			  one_r, \
			  p_br_r, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
			PASTEMAC2(chr,setd,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  0, \
			  m_br, \
			  n_br, \
			  zero_r, \
			  p_br_i, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
		} \
	} \
}

INSERT_GENTFUNCCO_BASIC( packm_struc_cxk_3mis, packm_cxk_3mis )




#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, varname, kername ) \
\
void PASTEMAC(ch,varname) \
     ( \
       struc_t         strucc, \
       doff_t          diagoffc, \
       uplo_t          uploc, \
       conj_t          conjc, \
       pack_t          schema, \
       dim_t           m_panel, \
       dim_t           n_panel, \
       dim_t           m_panel_max, \
       dim_t           n_panel_max, \
       dim_t           panel_dim, \
       dim_t           panel_dim_max, \
       dim_t           panel_len, \
       dim_t           panel_len_max, \
       ctype* restrict kappa, \
       ctype* restrict c, inc_t rs_c, inc_t cs_c, \
                          inc_t incc, inc_t ldc, \
       ctype* restrict p, inc_t rs_p, inc_t cs_p, \
                          inc_t is_p, inc_t ldp, \
       cntx_t*         cntx  \
     ) \
{ \
	doff_t  diagoffc_abs; \
	dim_t   i, j; \
	bool    row_stored; \
	bool    col_stored; \
\
\
	/* Create flags to incidate row or column storage. Note that the
	   schema bit that encodes row or column is describing the form of
	   micro-panel, not the storage in the micro-panel. Hence the
	   mismatch in "row" and "column" semantics. */ \
	row_stored = bli_is_col_packed( schema ); \
	col_stored = bli_is_row_packed( schema ); \
\
\
	/* Handle the case where the micro-panel does NOT intersect the
	   diagonal separately from the case where it does intersect. */ \
	if ( !bli_intersects_diag_n( diagoffc, m_panel, n_panel ) ) \
	{ \
		/* If the current panel is unstored, we need to make a few
		   adjustments so we refer to the data where it is actually
		   stored, also taking conjugation into account. (Note this
		   implicitly assumes we are operating on a dense panel
		   within a larger symmetric or Hermitian matrix, since a
		   general matrix would not contain any unstored region.) */ \
		if ( bli_is_unstored_subpart_n( diagoffc, uploc, m_panel, n_panel ) ) \
		{ \
			c = c + diagoffc * ( doff_t )cs_c + \
			       -diagoffc * ( doff_t )rs_c;  \
			bli_swap_incs( &incc, &ldc ); \
\
			if ( bli_is_hermitian( strucc ) ) \
				bli_toggle_conj( &conjc ); \
		} \
\
		/* Pack the full panel. */ \
		PASTEMAC(ch,kername) \
		( \
		  conjc, \
		  panel_dim, \
		  panel_dim_max, \
		  panel_len, \
		  panel_len_max, \
		  kappa, \
		  c, incc, ldc, \
		  p, is_p, ldp, \
		  cntx  \
		); \
	} \
	else /* if ( bli_intersects_diag_n( diagoffc, m_panel, n_panel ) ) */ \
	{ \
		ctype_r* restrict p_r         = ( ctype_r* )p; \
\
		ctype_r* restrict one_r       = PASTEMAC(chr,1); \
		ctype_r* restrict minus_one_r = PASTEMAC(chr,m1); \
\
		ctype*   restrict c10; \
		ctype_r* restrict p10; \
		dim_t             p10_dim, p10_len; \
		inc_t             incc10, ldc10; \
		doff_t            diagoffc10; \
		conj_t            conjc10; \
\
		ctype*   restrict c12; \
		ctype_r* restrict p12; \
		dim_t             p12_dim, p12_len; \
		inc_t             incc12, ldc12; \
		doff_t            diagoffc12; \
		conj_t            conjc12; \
\
		/* Sanity check. Diagonals should not intersect the short end of
		   a micro-panel. If they do, then somehow the constraints on
		   cache blocksizes being a whole multiple of the register
		   blocksizes was somehow violated. */ \
		if ( ( col_stored && diagoffc < 0 ) || \
		     ( row_stored && diagoffc > 0 ) ) \
			bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
\
		diagoffc_abs = bli_abs( diagoffc ); \
\
		if      ( ( row_stored && bli_is_upper( uploc ) ) || \
		          ( col_stored && bli_is_lower( uploc ) ) ) \
		{ \
			p10_dim    = panel_dim; \
			p10_len    = diagoffc_abs; \
			p10        = p_r; \
			c10        = c; \
			incc10     = incc; \
			ldc10      = ldc; \
			conjc10    = conjc; \
\
			p12_dim    = panel_dim; \
			p12_len    = panel_len - p10_len; \
			j          = p10_len; \
			diagoffc12 = diagoffc_abs - j; \
			p12        = p_r + (j  )*ldp; \
			c12        = c   + (j  )*ldc; \
			c12        = c12 + diagoffc12 * ( doff_t )cs_c + \
			                  -diagoffc12 * ( doff_t )rs_c;  \
			incc12     = ldc; \
			ldc12      = incc; \
			conjc12    = conjc; \
\
			if ( bli_is_hermitian( strucc ) ) \
				bli_toggle_conj( &conjc12 ); \
		} \
		else /* if ( ( row_stored && bli_is_lower( uploc ) ) || \
		             ( col_stored && bli_is_upper( uploc ) ) ) */ \
		{ \
			p10_dim    = panel_dim; \
			p10_len    = diagoffc_abs + panel_dim; \
			diagoffc10 = diagoffc; \
			p10        = p_r; \
			c10        = c; \
			c10        = c10 + diagoffc10 * ( doff_t )cs_c + \
			                  -diagoffc10 * ( doff_t )rs_c;  \
			incc10     = ldc; \
			ldc10      = incc; \
			conjc10    = conjc; \
\
			p12_dim    = panel_dim; \
			p12_len    = panel_len - p10_len; \
			j          = p10_len; \
			p12        = p_r + (j  )*ldp; \
			c12        = c   + (j  )*ldc; \
			incc12     = incc; \
			ldc12      = ldc; \
			conjc12    = conjc; \
\
			if ( bli_is_hermitian( strucc ) ) \
				bli_toggle_conj( &conjc10 ); \
		} \
\
		/* Pack to p10. For upper storage, this includes the unstored
		   triangle of c11. */ \
		/* NOTE: Since we're only packing partial panels here, we pass in
		   p1x_len as panel_len_max; otherwise, the packm kernel will zero-
		   fill the columns up to panel_len_max, which is not what we need
		   or want to happen. */ \
		PASTEMAC(ch,kername) \
		( \
		  conjc10, \
		  p10_dim, \
		  panel_dim_max, \
		  p10_len, \
		  p10_len, \
		  kappa, \
		            c10, incc10, ldc10, \
		  ( ctype* )p10, is_p,   ldp, \
		  cntx  \
		); \
\
		/* Pack to p12. For lower storage, this includes the unstored
		   triangle of c11. */ \
		/* NOTE: Since we're only packing partial panels here, we pass in
		   p1x_len as panel_len_max; otherwise, the packm kernel will zero-
		   fill the columns up to panel_len_max, which is not what we need
		   or want to happen. */ \
		PASTEMAC(ch,kername) \
		( \
		  conjc12, \
		  p12_dim, \
		  panel_dim_max, \
		  p12_len, \
		  p12_len, \
		  kappa, \
		            c12, incc12, ldc12, \
		  ( ctype* )p12, is_p,   ldp, \
		  cntx  \
		); \
\
		/* Pack the stored triangle of c11 to p11. */ \
		{ \
			dim_t    p11_m   = panel_dim; \
			dim_t    p11_n   = panel_dim; \
			inc_t    rs_c11  = 2*rs_c; \
			inc_t    cs_c11  = 2*cs_c; \
			dim_t    j2      = diagoffc_abs; \
			ctype*   c11     = ( ctype*   )c   + (j2 )*ldc; \
			ctype_r* p11     = ( ctype_r* )p_r + (j2 )*ldp; \
			ctype_r* c11_r   = ( ctype_r* )c11; \
			ctype_r* c11_i   = ( ctype_r* )c11 + 1; \
			ctype_r* p11_r   = ( ctype_r* )p11; \
			ctype_r* p11_i   = ( ctype_r* )p11 + is_p; \
			ctype_r* alpha_r = one_r; \
			ctype_r* alpha_i = ( bli_is_conj( conjc ) ? minus_one_r : one_r ); \
			ctype_r  kappa_r = PASTEMAC(ch,real)( *kappa ); \
			ctype_r  kappa_i = PASTEMAC(ch,imag)( *kappa ); \
\
			/* Copy the real part of the stored triangle of c11 to p11_r. */ \
			PASTEMAC2(chr,scal2m,BLIS_TAPI_EX_SUF) \
			( \
			  0, \
			  BLIS_NONUNIT_DIAG, \
			  uploc, \
			  BLIS_NO_TRANSPOSE, \
			  p11_m, \
			  p11_n, \
			  alpha_r, \
			  c11_r, rs_c11, cs_c11, \
			  p11_r, rs_p,   cs_p, \
			  cntx, \
			  NULL  \
			); \
\
			/* Copy the imaginary part of the stored triangle of c11 to p11_i,
			   scaling by -1 if conjugation on c was requested. */ \
			PASTEMAC2(chr,scal2m,BLIS_TAPI_EX_SUF) \
			( \
			  0, \
			  BLIS_NONUNIT_DIAG, \
			  uploc, \
			  BLIS_NO_TRANSPOSE, \
			  p11_m, \
			  p11_n, \
			  alpha_i, \
			  c11_i, rs_c11, cs_c11, \
			  p11_i, rs_p,   cs_p, \
			  cntx, \
			  NULL  \
			); \
\
			/* If source matrix c is Hermitian, we have to zero out the
			   imaginary components of the diagonal of p11 in case the
			   corresponding elements in c11 were not already zero. */ \
			if ( bli_is_hermitian( strucc ) ) \
			{ \
				for ( i = 0; i < p11_m; ++i ) \
				{ \
					ctype_r* pi11_i = p11_i + (i  )*rs_p + (i  )*cs_p; \
\
					PASTEMAC(chr,set0s)( *pi11_i ); \
				} \
			} \
\
			/* Apply kappa to the part of p11 that corresponds to the stored
			   part of c11 that was copied above. */ \
			if ( bli_is_upper( uploc ) ) \
			{ \
				PASTEMAC(ch,scalris_mxn_u) \
				( \
				  0, \
				  p11_m, \
				  p11_n, \
				  &kappa_r, \
				  &kappa_i, \
				  p11_r, \
				  p11_i, rs_p, cs_p  \
				); \
			} \
			else \
			{ \
				PASTEMAC(ch,scalris_mxn_l) \
				( \
				  0, \
				  p11_m, \
				  p11_n, \
				  &kappa_r, \
				  &kappa_i, \
				  p11_r, \
				  p11_i, rs_p, cs_p  \
				); \
			} \
\
			/* Update the p11 section of the ri panel. It simply needs
			   to contain the sum of p11_r + p11_i. */ \
			{ \
				ctype_r* p11_rpi = p11_i + is_p; \
\
				for ( j = 0; j < p11_n; ++j ) \
				for ( i = 0; i < p11_m; ++i ) \
				{ \
					ctype_r* pi11_r   = p11_r   + (i  )*rs_p + (j  )*cs_p; \
					ctype_r* pi11_i   = p11_i   + (i  )*rs_p + (j  )*cs_p; \
					ctype_r* pi11_rpi = p11_rpi + (i  )*rs_p + (j  )*cs_p; \
\
					PASTEMAC(chr,add3s) \
					( \
					  *pi11_r, \
					  *pi11_i, \
					  *pi11_rpi  \
					); \
				} \
			} \
		} \
	} \
}

INSERT_GENTFUNCCO_BASIC( packm_herm_cxk_3mis, packm_cxk_3mis )





#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, varname, kername ) \
\
void PASTEMAC(ch,varname) \
     ( \
       struc_t         strucc, \
       doff_t          diagoffp, \
       diag_t          diagc, \
       uplo_t          uploc, \
       conj_t          conjc, \
       pack_t          schema, \
       bool            invdiag, \
       dim_t           m_panel, \
       dim_t           n_panel, \
       dim_t           m_panel_max, \
       dim_t           n_panel_max, \
       dim_t           panel_dim, \
       dim_t           panel_dim_max, \
       dim_t           panel_len, \
       dim_t           panel_len_max, \
       ctype* restrict kappa, \
       ctype* restrict c, inc_t rs_c, inc_t cs_c, \
                          inc_t incc, inc_t ldc, \
       ctype* restrict p, inc_t rs_p, inc_t cs_p, \
                          inc_t is_p, inc_t ldp, \
       cntx_t*         cntx  \
     ) \
{ \
	/* Pack the panel. */ \
	PASTEMAC(ch,kername) \
	( \
	  conjc, \
	  panel_dim, \
	  panel_dim_max, \
	  panel_len, \
	  panel_len_max, \
	  kappa, \
	  c, incc, ldc, \
	  p, is_p, ldp, \
	  cntx  \
	); \
\
\
	/* Tweak the panel according to its triangular structure */ \
	{ \
		ctype_r* p_r     = ( ctype_r* )p +      0; \
		ctype_r* p_i     = ( ctype_r* )p +   is_p; \
		ctype_r* p_rpi   = ( ctype_r* )p + 2*is_p; \
\
		dim_t    j       = bli_abs( diagoffp ); \
		ctype_r* p11_r   = p_r   + (j  )*ldp; \
		ctype_r* p11_i   = p_i   + (j  )*ldp; \
		ctype_r* p11_rpi = p_rpi + (j  )*ldp; \
\
		dim_t    p11_m   = m_panel; \
		dim_t    p11_n   = n_panel; \
\
		dim_t    min_p11_m_n; \
\
		if      ( diagoffp < 0 ) p11_m -= j; \
		else if ( diagoffp > 0 ) p11_n -= j; \
\
		min_p11_m_n = bli_min( p11_m, p11_n ); \
\
\
		/* If the diagonal of c is implicitly unit, explicitly set the
		   the diagonal of the packed panel to kappa. */ \
		if ( bli_is_unit_diag( diagc ) ) \
		{ \
			ctype_r kappa_r = PASTEMAC(ch,real)( *kappa ); \
			ctype_r kappa_i = PASTEMAC(ch,imag)( *kappa ); \
			dim_t   i; \
\
			PASTEMAC2(chr,setd,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  m_panel, \
			  n_panel, \
			  &kappa_r, \
			  p_r, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
			PASTEMAC2(chr,setd,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  m_panel, \
			  n_panel, \
			  &kappa_i, \
			  p_i, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
\
			/* Update the diagonal of the p11 section of the rpi panel.
			   It simply needs to contain the sum of diagonals of p11_r
			   and p11_i. */ \
			for ( i = 0; i < min_p11_m_n; ++i ) \
			{ \
				ctype_r* pi11_r   = p11_r   + (i  )*rs_p + (i  )*cs_p; \
				ctype_r* pi11_i   = p11_i   + (i  )*rs_p + (i  )*cs_p; \
				ctype_r* pi11_rpi = p11_rpi + (i  )*rs_p + (i  )*cs_p; \
\
				PASTEMAC(chr,add3s)( *pi11_r, *pi11_i, *pi11_rpi ); \
			} \
		} \
\
		/* If requested, invert the diagonal of the packed panel. Note
		   that we do not need to update the ri panel since inverted
		   diagonals are only needed by trsm, which does not use the
		   p11 section of the ri panel. */ \
		if ( invdiag == TRUE ) \
		{ \
			dim_t i; \
\
			for ( i = 0; i < min_p11_m_n; ++i ) \
			{ \
				ctype_r* pi11_r = p11_r + (i  )*rs_p + (i  )*cs_p; \
				ctype_r* pi11_i = p11_i + (i  )*rs_p + (i  )*cs_p; \
\
				PASTEMAC(ch,invertris)( *pi11_r, *pi11_i ); \
			} \
		} \
\
		/* Set the region opposite the diagonal of p to zero. To do this,
		   we need to reference the "unstored" region on the other side of
		   the diagonal. This amounts to toggling uploc and then shifting
		   the diagonal offset to shrink the newly referenced region (by
		   one diagonal). Note that this zero-filling is not needed for
		   trsm, since the unstored region is not referenced by the trsm
		   micro-kernel; however, zero-filling is needed for trmm, which
		   uses the gemm micro-kernel.*/ \
		{ \
			ctype_r* restrict zero_r = PASTEMAC(chr,0); \
			uplo_t            uplop  = uploc; \
\
			bli_toggle_uplo( &uplop ); \
			bli_shift_diag_offset_to_shrink_uplo( uplop, &diagoffp ); \
\
			PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  BLIS_NONUNIT_DIAG, \
			  uplop, \
			  m_panel, \
			  n_panel, \
			  zero_r, \
			  p_r, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
			PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  BLIS_NONUNIT_DIAG, \
			  uplop, \
			  m_panel, \
			  n_panel, \
			  zero_r, \
			  p_i, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
			PASTEMAC2(chr,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  BLIS_NONUNIT_DIAG, \
			  uplop, \
			  m_panel, \
			  n_panel, \
			  zero_r, \
			  p_rpi, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
		} \
	} \
}

INSERT_GENTFUNCCO_BASIC( packm_tri_cxk_3mis, packm_cxk_3mis )

