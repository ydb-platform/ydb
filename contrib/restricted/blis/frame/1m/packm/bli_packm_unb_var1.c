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

#define FUNCPTR_T packm_fp

typedef void (*FUNCPTR_T)(
                           struc_t strucc,
                           doff_t  diagoffc,
                           diag_t  diagc,
                           uplo_t  uploc,
                           trans_t transc,
                           dim_t   m,
                           dim_t   n,
                           dim_t   m_max,
                           dim_t   n_max,
                           void*   kappa,
                           void*   c, inc_t rs_c, inc_t cs_c,
                           void*   p, inc_t rs_p, inc_t cs_p,
                           cntx_t* cntx
                         );

static FUNCPTR_T GENARRAY(ftypes,packm_unb_var1);


void bli_packm_unb_var1
     (
       obj_t*  c,
       obj_t*  p,
       cntx_t* cntx,
       cntl_t* cntl,
       thrinfo_t* thread
     )
{
	num_t     dt_cp     = bli_obj_dt( c );

	struc_t   strucc    = bli_obj_struc( c );
	doff_t    diagoffc  = bli_obj_diag_offset( c );
	diag_t    diagc     = bli_obj_diag( c );
	uplo_t    uploc     = bli_obj_uplo( c );
	trans_t   transc    = bli_obj_conjtrans_status( c );

	dim_t     m_p       = bli_obj_length( p );
	dim_t     n_p       = bli_obj_width( p );
	dim_t     m_max_p   = bli_obj_padded_length( p );
	dim_t     n_max_p   = bli_obj_padded_width( p );

	void*     buf_c     = bli_obj_buffer_at_off( c );
	inc_t     rs_c      = bli_obj_row_stride( c );
	inc_t     cs_c      = bli_obj_col_stride( c );

	void*     buf_p     = bli_obj_buffer_at_off( p );
	inc_t     rs_p      = bli_obj_row_stride( p );
	inc_t     cs_p      = bli_obj_col_stride( p );

	void*     buf_kappa;

	FUNCPTR_T f;


	// This variant assumes that the computational kernel will always apply
	// the alpha scalar of the higher-level operation. Thus, we use BLIS_ONE
	// for kappa so that the underlying packm implementation does not scale
	// during packing.
	buf_kappa = bli_obj_buffer_for_const( dt_cp, &BLIS_ONE );

	// Index into the type combination array to extract the correct
	// function pointer.
	f = ftypes[dt_cp];

    if( bli_thread_am_ochief( thread ) ) {
        // Invoke the function.
        f
		(
		  strucc,
          diagoffc,
          diagc,
          uploc,
          transc,
          m_p,
          n_p,
          m_max_p,
          n_max_p,
          buf_kappa,
          buf_c, rs_c, cs_c,
          buf_p, rs_p, cs_p,
		  cntx
		);
    }
}


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, varname ) \
\
void PASTEMAC(ch,varname) \
     ( \
       struc_t strucc, \
       doff_t  diagoffc, \
       diag_t  diagc, \
       uplo_t  uploc, \
       trans_t transc, \
       dim_t   m, \
       dim_t   n, \
       dim_t   m_max, \
       dim_t   n_max, \
       void*   kappa, \
       void*   c, inc_t rs_c, inc_t cs_c, \
       void*   p, inc_t rs_p, inc_t cs_p, \
       cntx_t* cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict c_cast     = c; \
	ctype* restrict p_cast     = p; \
	ctype* restrict zero       = PASTEMAC(ch,0); \
\
	/* We begin by packing the region indicated by the parameters. If
	   matrix c is dense (either because the structure is general or
	   because the structure has already been "densified"), this ends
	   up being the only action we take. Note that if kappa is unit,
	   the data is simply copied (rather than scaled by one). */ \
	PASTEMAC2(ch,scal2m,BLIS_TAPI_EX_SUF) \
	( \
	  diagoffc, \
	  diagc, \
	  uploc, \
	  transc, \
	  m, \
	  n, \
	  kappa_cast, \
	  c_cast, rs_c, cs_c, \
	  p_cast, rs_p, cs_p, \
	  cntx, \
	  NULL  \
	); \
\
	/* If uploc is upper or lower, then the structure of c is necessarily
	   non-dense (ie: Hermitian, symmetric, or triangular, where part of the
	   matrix is unstored). In these cases, we want to fill in the unstored
	   part of the matrix. How this is done depends on the structure of c. */ \
	if ( bli_is_upper_or_lower( uploc ) ) \
	{ \
		/* The Hermitian and symmetric cases are almost identical, so we
		   handle them in one conditional block. */ \
		if ( bli_is_hermitian( strucc ) || bli_is_symmetric( strucc ) ) \
		{ \
			/* First we must reflect the region referenced to the opposite
			   side of the diagonal. */ \
			c_cast = c_cast + diagoffc * ( doff_t )cs_c + \
			                 -diagoffc * ( doff_t )rs_c; \
			bli_negate_diag_offset( &diagoffc ); \
			bli_toggle_trans( &transc ); \
			if      ( bli_is_upper( uploc ) ) diagoffc += 1; \
			else if ( bli_is_lower( uploc ) ) diagoffc -= 1; \
\
			/* If c is Hermitian, we need to apply a conjugation when
			   copying the region opposite the diagonal. */ \
			if ( bli_is_hermitian( strucc ) ) \
				transc = bli_trans_toggled_conj( transc ); \
\
			/* Copy the data from the region opposite the diagonal of c
			   (as specified by the original value of diagoffc). Notice
			   that we use a diag parameter of non-unit since we can
			   assume nothing about the neighboring off-diagonal. */ \
			PASTEMAC2(ch,scal2m,BLIS_TAPI_EX_SUF) \
			( \
			  diagoffc, \
			  BLIS_NONUNIT_DIAG, \
			  uploc, \
			  transc, \
			  m, \
			  n, \
			  kappa_cast, \
			  c_cast, rs_c, cs_c, \
			  p_cast, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
		} \
		else /* if ( bli_is_triangular( strucc ) ) */ \
		{ \
			doff_t diagoffp = diagoffc; \
			uplo_t uplop    = uploc; \
\
			/* For this step we need the uplo and diagonal offset of p, which
			   we can derive from the parameters given. */ \
			if ( bli_does_trans( transc ) ) \
			{ \
				bli_negate_diag_offset( &diagoffp ); \
				bli_toggle_uplo( &uplop ); \
			} \
\
			/* For triangular matrices, we wish to reference the region
			   strictly opposite the diagonal of C. This amounts to 
			   toggling uploc and then shifting the diagonal offset to
			   shrink the stored region (by one diagonal). */ \
			bli_toggle_uplo( &uplop ); \
			bli_shift_diag_offset_to_shrink_uplo( uplop, &diagoffp ); \
\
			/* Set the region opposite the diagonal of p to zero. */ \
			PASTEMAC2(ch,setm,BLIS_TAPI_EX_SUF) \
			( \
			  BLIS_NO_CONJUGATE, \
			  diagoffp, \
			  BLIS_NONUNIT_DIAG, \
			  uplop, \
			  m, \
			  n, \
			  zero, \
			  p_cast, rs_p, cs_p, \
			  cntx, \
			  NULL  \
			); \
		} \
	} \
\
	/* The packed memory region was acquired/allocated with "aligned"
	   dimensions (ie: dimensions that were possibly inflated up to a
	   multiple). When these dimension are inflated, it creates empty
	   regions along the bottom and/or right edges of the matrix. If
	   eithe region exists, we set them to zero. This simplifies the
	   register level micro kernel in that it does not need to support
	   different register blockings for the edge cases. */ \
	if ( m != m_max ) \
	{ \
		ctype* p_edge = p_cast + (m  )*rs_p; \
\
		PASTEMAC2(ch,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_max - m, \
		  n_max, \
		  zero, \
		  p_edge, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
	} \
\
	if ( n != n_max ) \
	{ \
		ctype* p_edge = p_cast + (n  )*cs_p; \
\
		PASTEMAC2(ch,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  0, \
		  BLIS_NONUNIT_DIAG, \
		  BLIS_DENSE, \
		  m_max, \
		  n_max - n, \
		  zero, \
		  p_edge, rs_p, cs_p, \
		  cntx, \
		  NULL  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC0( packm_unb_var1 )

