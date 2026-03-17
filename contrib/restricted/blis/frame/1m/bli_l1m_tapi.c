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

// Guard the function definitions so that they are only compiled when
// #included from files that define the typed API macros.
#ifdef BLIS_ENABLE_TAPI

//
// Define BLAS-like interfaces with typed operands.
//

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, auxker ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       trans_t transx, \
       dim_t   m, \
       dim_t   n, \
       ctype*  x, inc_t rs_x, inc_t cs_x, \
       ctype*  y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
\
	/* When the diagonal of an upper- or lower-stored matrix is unit,
	   we handle it with a separate post-processing step. */ \
	if ( bli_is_upper_or_lower( uplox ) && \
	     bli_is_unit_diag( diagx ) ) \
	{ \
		PASTEMAC2(ch,auxker,BLIS_TAPI_EX_SUF) \
		( \
		  diagoffx, \
		  diagx, \
		  transx, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC( addm, addd )
INSERT_GENTFUNC_BASIC( subm, subd )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       trans_t transx, \
       dim_t   m, \
       dim_t   n, \
       ctype*  x, inc_t rs_x, inc_t cs_x, \
       ctype*  y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
\
	/* When the diagonal of an upper- or lower-stored matrix is unit,
	   we handle it with a separate post-processing step. */ \
	if ( bli_is_upper_or_lower( uplox ) && \
	     bli_is_unit_diag( diagx ) ) \
	{ \
		doff_t diagoffy = diagoffx; \
		ctype* one      = PASTEMAC(ch,1); \
\
		if ( bli_does_trans( transx ) ) \
			bli_negate_diag_offset( &diagoffy ); \
\
		PASTEMAC2(ch,setd,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  diagoffy, \
		  m, \
		  n, \
		  one, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC0( copym )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       trans_t transx, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t rs_x, inc_t cs_x, \
       ctype*  y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* If alpha is zero, then the entire operation is a no-op. */ \
	if ( PASTEMAC(ch,eq0)( *alpha ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  alpha, \
	  x, rs_x, cs_x, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
\
	/* When the diagonal of an upper- or lower-stored matrix is unit,
	   we handle it with a separate post-processing step. */ \
	if ( bli_is_upper_or_lower( uplox ) && \
	     bli_is_unit_diag( diagx ) ) \
	{ \
		PASTEMAC2(ch,axpyd,BLIS_TAPI_EX_SUF) \
		( \
		  diagoffx, \
		  diagx, \
		  transx, \
		  m, \
		  n, \
		  alpha, \
		  x, rs_x, cs_x, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC0( axpym )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       trans_t transx, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t rs_x, inc_t cs_x, \
       ctype*  y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* If alpha is zero, then we set the output matrix to zero. This
	   seemingly minor optimization is important because it will clear
	   any NaNs and Infs in x that would otherwise propogate. */ \
	if ( PASTEMAC(ch,eq0)( *alpha ) ) \
	{ \
\
		PASTEMAC2(ch,setm,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  diagoffx, \
		  diagx, \
		  uplox, \
		  m, \
		  n, \
		  alpha, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
		return; \
	} \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  alpha, \
	  x, rs_x, cs_x, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
\
	/* When the diagonal of an upper- or lower-stored matrix is unit,
	   we handle it with a separate post-processing step. */ \
	if ( bli_is_upper_or_lower( uplox ) && \
	     bli_is_unit_diag( diagx ) ) \
	{ \
		doff_t diagoffy = diagoffx; \
\
		if ( bli_does_trans( transx ) ) \
			bli_negate_diag_offset( &diagoffy ); \
\
		PASTEMAC2(ch,setd,BLIS_TAPI_EX_SUF) \
		( \
		  BLIS_NO_CONJUGATE, \
		  diagoffy, \
		  m, \
		  n, \
		  alpha, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC0( scal2m )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjalpha, \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t rs_x, inc_t cs_x  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  conjalpha, \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  m, \
	  n, \
	  alpha, \
	  x, rs_x, cs_x, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNC_BASIC0( scalm )
INSERT_GENTFUNC_BASIC0( setm )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       diag_t  diagx, \
       uplo_t  uplox, \
       trans_t transx, \
       dim_t   m, \
       dim_t   n, \
       ctype*  x, inc_t rs_x, inc_t cs_x, \
       ctype*  beta, \
       ctype*  y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* If beta is zero, then the operation reduces to copym. */ \
	if ( PASTEMAC(ch,eq0)( *beta ) ) \
	{ \
		PASTEMAC2(ch,copym,_unb_var1) \
		( \
		  diagoffx, \
		  diagx, \
		  uplox, \
		  transx, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
\
		return; \
	} \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  beta, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
\
	/* When the diagonal of an upper- or lower-stored matrix is unit,
	   we handle it with a separate post-processing step. */ \
	if ( bli_is_upper_or_lower( uplox ) && \
	     bli_is_unit_diag( diagx ) ) \
	{ \
		PASTEMAC2(ch,xpbyd,BLIS_TAPI_EX_SUF) \
		( \
		  diagoffx, \
		  diagx, \
		  transx, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  beta, \
		  y, rs_y, cs_y, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNC_BASIC0( xpbym )


#undef  GENTFUNC2
#define GENTFUNC2( ctype_x, ctype_y, chx, chy, opname ) \
\
void PASTEMAC3(chx,chy,opname,EX_SUF) \
     ( \
       doff_t   diagoffx, \
       diag_t   diagx, \
       uplo_t   uplox, \
       trans_t  transx, \
       dim_t    m, \
       dim_t    n, \
       ctype_x* x, inc_t rs_x, inc_t cs_x, \
       ctype_y* beta, \
       ctype_y* y, inc_t rs_y, inc_t cs_y  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* If beta is zero, then the operation reduces to copym. */ \
	if ( PASTEMAC(chy,eq0)( *beta ) ) \
	{ \
		PASTEMAC2(chx,chy,castm) \
		( \
		  transx, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  y, rs_y, cs_y  \
		); \
\
		return; \
	} \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC3(chx,chy,opname,_unb_var1) \
	( \
	  diagoffx, \
	  diagx, \
	  uplox, \
	  transx, \
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  beta, \
	  y, rs_y, cs_y, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNC2_BASIC0( xpbym_md )
INSERT_GENTFUNC2_MIXDP0( xpbym_md )


#endif

