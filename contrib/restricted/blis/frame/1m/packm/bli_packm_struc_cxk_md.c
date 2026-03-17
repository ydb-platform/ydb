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

#undef  GENTFUNC2
#define GENTFUNC2( ctype_c, ctype_p, chc, chp, varname ) \
\
void PASTEMAC2(chc,chp,varname) \
     ( \
       conj_t            conjc, \
       pack_t            schema, \
       dim_t             m_panel, \
       dim_t             n_panel, \
       dim_t             m_panel_max, \
       dim_t             n_panel_max, \
       ctype_p* restrict kappa, \
       ctype_c* restrict c, inc_t rs_c, inc_t cs_c, \
       ctype_p* restrict p, inc_t rs_p, inc_t cs_p, \
                            inc_t is_p, \
       cntx_t*           cntx  \
     ) \
{ \
	dim_t  panel_dim; \
	dim_t  panel_dim_max; \
	dim_t  panel_len; \
	dim_t  panel_len_max; \
	inc_t  incc, ldc; \
	inc_t        ldp; \
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
	if ( bli_is_nat_packed( schema ) ) \
	{ \
		/* Sanity check: Make sure that kappa is 1.0. Mixed-datatype alpha
		   values are never handled when packing for native execution;
		   instead, they are passed along to the micro-kernel. */ \
		if ( !PASTEMAC(chp,eq1)( *kappa ) ) \
			bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
\
		/* Treat the micro-panel as panel_dim x panel_len and column-stored
		   (unit row stride). */ \
\
		/* NOTE: We ignore kappa for now, since it should be 1.0. */ \
		PASTEMAC2(chc,chp,castm) \
		( \
		  ( trans_t )conjc, \
		  panel_dim, \
		  panel_len, \
		  c, incc, ldc, \
		  p,    1, ldp  \
		); \
\
		/* If panel_dim < panel_dim_max, then we zero those unused rows. */ \
		if ( panel_dim < panel_dim_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       i      = panel_dim; \
			const dim_t       m_edge = panel_dim_max - i; \
			const dim_t       n_edge = panel_len_max; \
			ctype_p*          p_edge = p + (i  )*1; \
\
			PASTEMAC2(chp,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  0, \
			  BLIS_NONUNIT_DIAG, \
			  BLIS_DENSE, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p_edge, 1, ldp, \
			  cntx, \
			  NULL  \
			); \
		} \
\
		/* If panel_len < panel_len_max, then we zero those unused columns. */ \
		if ( panel_len < panel_len_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       j      = panel_len; \
			const dim_t       m_edge = panel_dim_max; \
			const dim_t       n_edge = panel_len_max - j; \
			ctype_p*          p_edge = p + (j  )*ldp; \
\
			PASTEMAC2(chp,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  0, \
			  BLIS_NONUNIT_DIAG, \
			  BLIS_DENSE, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p_edge, 1, ldp, \
			  cntx, \
			  NULL  \
			); \
		} \
	} \
	else if ( bli_is_1r_packed( schema ) ) \
	{ \
		/* Treat the micro-panel as panel_dim x panel_len and column-stored
		   (unit row stride). */ \
\
		PASTEMAC2(chc,chp,packm_cxk_1r_md) \
		( \
		  conjc, \
		  panel_dim, \
		  panel_len, \
		  kappa, \
		  c, incc, ldc, \
		  p,       ldp  \
		); \
\
		/* If panel_dim < panel_dim_max, then we zero those unused rows. */ \
		if ( panel_dim < panel_dim_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       offm   = panel_dim; \
			const dim_t       offn   = 0; \
			const dim_t       m_edge = panel_dim_max - panel_dim; \
			const dim_t       n_edge = panel_len_max; \
\
			( void ) zero; \
			( void ) m_edge; ( void )offm; \
			( void ) n_edge; ( void )offn; \
\
			PASTEMAC(chp,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
\
		/* If panel_len < panel_len_max, then we zero those unused columns. */ \
		if ( panel_len < panel_len_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       offm   = 0; \
			const dim_t       offn   = panel_len; \
			const dim_t       m_edge = panel_dim_max; \
			const dim_t       n_edge = panel_len_max - panel_len; \
\
			( void ) zero; \
			( void ) m_edge; ( void )offm; \
			( void ) n_edge; ( void )offn; \
\
			PASTEMAC(chp,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
	else if ( bli_is_1e_packed( schema ) ) \
	{ \
		/* Treat the micro-panel as panel_dim x panel_len and column-stored
		   (unit row stride). */ \
\
		PASTEMAC2(chc,chp,packm_cxk_1e_md) \
		( \
		  conjc, \
		  panel_dim, \
		  panel_len, \
		  kappa, \
		  c, incc, ldc, \
		  p,       ldp  \
		); \
\
		/* If panel_dim < panel_dim_max, then we zero those unused rows. */ \
		if ( panel_dim < panel_dim_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       offm   = panel_dim; \
			const dim_t       offn   = 0; \
			const dim_t       m_edge = panel_dim_max - panel_dim; \
			const dim_t       n_edge = panel_len_max; \
\
			( void ) zero; \
			( void ) m_edge; ( void )offm; \
			( void ) n_edge; ( void )offn; \
\
			PASTEMAC(chp,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
\
		/* If panel_len < panel_len_max, then we zero those unused columns. */ \
		if ( panel_len < panel_len_max ) \
		{ \
			ctype_p* restrict zero   = PASTEMAC(chp,0); \
			const dim_t       offm   = 0; \
			const dim_t       offn   = panel_len; \
			const dim_t       m_edge = panel_dim_max; \
			const dim_t       n_edge = panel_len_max - panel_len; \
\
			( void ) zero; \
			( void ) m_edge; ( void )offm; \
			( void ) n_edge; ( void )offn; \
\
			PASTEMAC(chp,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
	else \
	{ \
		/* Mixed-datatype packing should not occur for any other schemas. */ \
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
	} \
\
\
/*
	if ( bli_is_col_packed( schema ) ) \
	PASTEMAC(ch,fprintm)( stdout, "packm_struc_cxk: bp copied", m_panel_max, n_panel_max, \
	                      p, rs_p, cs_p, "%4.1f", "" ); \
	else if ( bli_is_row_packed( schema ) ) \
	PASTEMAC(ch,fprintm)( stdout, "packm_struc_cxk: ap copied", m_panel_max, n_panel_max, \
	                      p, rs_p, cs_p, "%4.1f", "" ); \
*/ \
}

INSERT_GENTFUNC2_BASIC0( packm_struc_cxk_md )
INSERT_GENTFUNC2_MIXDP0( packm_struc_cxk_md )


// -----------------------------------------------------------------------------

#undef  GENTFUNC2
#define GENTFUNC2( ctype_a, ctype_p, cha, chp, opname ) \
\
void PASTEMAC2(cha,chp,opname) \
     ( \
       conj_t            conja, \
       dim_t             m, \
       dim_t             n, \
	   ctype_p* restrict kappa, \
       ctype_a* restrict a, inc_t inca, inc_t lda, \
       ctype_p* restrict p,             inc_t ldp  \
     ) \
{ \
	const inc_t                    inca2    = 2 * inca; \
	const inc_t                    lda2     = 2 * lda; \
	const inc_t                    ldp2     = 2 * ldp; \
\
	PASTEMAC(chp,ctyper)* restrict kappa_r  = ( PASTEMAC(chp,ctyper)* )kappa; \
	PASTEMAC(chp,ctyper)* restrict kappa_i  = ( PASTEMAC(chp,ctyper)* )kappa + 1; \
	PASTEMAC(cha,ctyper)* restrict alpha1_r = ( PASTEMAC(cha,ctyper)* )a; \
	PASTEMAC(cha,ctyper)* restrict alpha1_i = ( PASTEMAC(cha,ctyper)* )a + 1; \
	PASTEMAC(chp,ctyper)* restrict pi1_r    = ( PASTEMAC(chp,ctyper)* )p; \
	PASTEMAC(chp,ctyper)* restrict pi1_i    = ( PASTEMAC(chp,ctyper)* )p + ldp; \
\
	( void )kappa_i; \
\
	if ( PASTEMAC(chp,eq1)( *kappa ) ) \
	{ \
		if ( bli_is_conj( conja ) ) \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC2(cha,chp,copyjris) \
					( \
					  *(alpha1_r + i*inca2), \
					  *(alpha1_i + i*inca2), \
					  *(pi1_r    + i*    1), \
					  *(pi1_i    + i*    1)  \
					); \
				} \
\
				alpha1_r += lda2; \
				alpha1_i += lda2; \
				pi1_r    += ldp2; \
				pi1_i    += ldp2; \
			} \
		} \
		else \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC2(cha,chp,copyris) \
					( \
					  *(alpha1_r + i*inca2), \
					  *(alpha1_i + i*inca2), \
					  *(pi1_r    + i*    1), \
					  *(pi1_i    + i*    1)  \
					); \
				} \
\
				alpha1_r += lda2; \
				alpha1_i += lda2; \
				pi1_r    += ldp2; \
				pi1_i    += ldp2; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conja ) ) \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC3(chp,cha,chp,scal2jris) \
					( \
					  *kappa_r, \
					  *kappa_i, \
					  *(alpha1_r + i*inca2), \
					  *(alpha1_i + i*inca2), \
					  *(pi1_r    + i*    1), \
					  *(pi1_i    + i*    1)  \
					); \
				} \
\
				alpha1_r += lda2; \
				alpha1_i += lda2; \
				pi1_r    += ldp2; \
				pi1_i    += ldp2; \
			} \
		} \
		else \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC3(chp,cha,chp,scal2ris) \
					( \
					  *kappa_r, \
					  *kappa_i, \
					  *(alpha1_r + i*inca2), \
					  *(alpha1_i + i*inca2), \
					  *(pi1_r    + i*    1), \
					  *(pi1_i    + i*    1)  \
					); \
				} \
\
				alpha1_r += lda2; \
				alpha1_i += lda2; \
				pi1_r    += ldp2; \
				pi1_i    += ldp2; \
			} \
		} \
	} \
}

INSERT_GENTFUNC2_BASIC0( packm_cxk_1r_md )
INSERT_GENTFUNC2_MIXDP0( packm_cxk_1r_md )

// -----------------------------------------------------------------------------

#undef  GENTFUNC2
#define GENTFUNC2( ctype_a, ctype_p, cha, chp, opname ) \
\
void PASTEMAC2(cha,chp,opname) \
     ( \
       conj_t            conja, \
       dim_t             m, \
       dim_t             n, \
	   ctype_p* restrict kappa, \
       ctype_a* restrict a, inc_t inca, inc_t lda, \
       ctype_p* restrict p,             inc_t ldp  \
     ) \
{ \
	const inc_t       inca1     = inca; \
	const inc_t       lda1      = lda; \
	const inc_t       ldp1      = ldp; \
\
	ctype_a* restrict alpha1_ri = ( ctype_a* )a; \
	ctype_p* restrict pi1_ri    = ( ctype_p* )p; \
	ctype_p* restrict pi1_ir    = ( ctype_p* )p + ldp1/2; \
\
	( void )inca1; \
\
	if ( PASTEMAC(chp,eq1)( *kappa ) ) \
	{ \
		if ( bli_is_conj( conja ) ) \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC2(cha,chp,copyj1es) \
					( \
					  *(alpha1_ri + i*inca1), \
					  *(pi1_ri    + i*    1), \
					  *(pi1_ir    + i*    1)  \
					); \
				} \
\
				alpha1_ri += lda1; \
				pi1_ri    += ldp1; \
				pi1_ir    += ldp1; \
			} \
		} \
		else \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC2(cha,chp,copy1es) \
					( \
					  *(alpha1_ri + i*inca1), \
					  *(pi1_ri    + i*    1), \
					  *(pi1_ir    + i*    1)  \
					); \
				} \
\
				alpha1_ri += lda1; \
				pi1_ri    += ldp1; \
				pi1_ir    += ldp1; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conja ) ) \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC3(chp,cha,chp,scal2j1es) \
					( \
					  *kappa, \
					  *(alpha1_ri + i*inca1), \
					  *(pi1_ri    + i*    1), \
					  *(pi1_ir    + i*    1)  \
					); \
				} \
\
				alpha1_ri += lda1; \
				pi1_ri    += ldp1; \
				pi1_ir    += ldp1; \
			} \
		} \
		else \
		{ \
			for ( dim_t k = n; k != 0; --k ) \
			{ \
				for ( dim_t i = 0; i < m; ++i ) \
				{ \
					PASTEMAC3(chp,cha,chp,scal21es) \
					( \
					  *kappa, \
					  *(alpha1_ri + i*inca1), \
					  *(pi1_ri    + i*    1), \
					  *(pi1_ir    + i*    1)  \
					); \
				} \
\
				alpha1_ri += lda1; \
				pi1_ri    += ldp1; \
				pi1_ir    += ldp1; \
			} \
		} \
	} \
}

INSERT_GENTFUNC2_BASIC0( packm_cxk_1e_md )
INSERT_GENTFUNC2_MIXDP0( packm_cxk_1e_md )

#endif
