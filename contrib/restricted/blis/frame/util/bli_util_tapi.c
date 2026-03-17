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

#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* asum  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If the vector length is zero, set the absolute sum return value to
	   zero and return early. */ \
	if ( bli_zero_dim1( n ) ) \
	{ \
		PASTEMAC(chr,set0s)( *asum ); \
		return; \
	} \
\
	/* Obtain a valid context from the gks if necessary. */ \
	/*if ( cntx == NULL ) cntx = bli_gks_query_cntx();*/ \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  n, \
	  x, incx, \
	  asum, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNCR_BASIC0( asumv )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       uplo_t  uploa, \
       dim_t   m, \
       ctype*  a, inc_t rs_a, inc_t cs_a  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If either dimension is zero, return early. */ \
	if ( bli_zero_dim2( m, m ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  uploa, \
	  m, \
	  a, rs_a, cs_a, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNC_BASIC0( mkherm )
INSERT_GENTFUNC_BASIC0( mksymm )
INSERT_GENTFUNC_BASIC0( mktrim )


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* norm  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If the vector length is zero, set the norm to zero and return
	   early. */ \
	if ( bli_zero_dim1( n ) ) \
	{ \
		PASTEMAC(chr,set0s)( *norm ); \
		return; \
	} \
\
	/* Obtain a valid context from the gks if necessary. */ \
	if ( cntx == NULL ) cntx = bli_gks_query_cntx(); \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  n, \
	  x, incx, \
	  norm, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNCR_BASIC0( norm1v )
INSERT_GENTFUNCR_BASIC0( normfv )
INSERT_GENTFUNCR_BASIC0( normiv )


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t   diagoffx, \
       diag_t   diagx, \
       uplo_t   uplox, \
       dim_t    m, \
       dim_t    n, \
       ctype*   x, inc_t rs_x, inc_t cs_x, \
       ctype_r* norm  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If either dimension is zero, set the norm to zero and return
	   early. */ \
	if ( bli_zero_dim2( m, n ) ) \
	{ \
		PASTEMAC(chr,set0s)( *norm ); \
		return; \
	} \
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
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  norm, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNCR_BASIC0( norm1m )
INSERT_GENTFUNCR_BASIC0( normfm )
INSERT_GENTFUNCR_BASIC0( normim )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, varname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       char*  s1, \
       dim_t  n, \
       void*  x, inc_t incx, \
       char*  format, \
       char*  s2  \
     ) \
{ \
	bli_init_once(); \
\
	PASTEMAC(ch,varname) \
	( \
	  stdout, \
	  s1, \
	  n, \
	  x, incx, \
	  format, \
	  s2  \
	); \
}

INSERT_GENTFUNC_BASIC_I( printv, fprintv )


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, varname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       char*  s1, \
       dim_t  m, \
       dim_t  n, \
       void*  x, inc_t rs_x, inc_t cs_x, \
       char*  format, \
       char*  s2  \
     ) \
{ \
	bli_init_once(); \
\
	PASTEMAC(ch,varname) \
	( \
	  stdout, \
	  s1, \
	  m, \
	  n, \
	  x, rs_x, cs_x, \
	  format, \
	  s2  \
	); \
}

INSERT_GENTFUNC_BASIC_I( printm, fprintm )


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If the vector length is zero, return early. */ \
	if ( bli_zero_dim1( n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	/*if ( cntx == NULL ) cntx = bli_gks_query_cntx();*/ \
\
	ctype_r norm; \
\
	/* Set the norm to zero. */ \
	PASTEMAC(chr,set0s)( norm ); \
\
	/* Iterate at least once, but continue iterating until the norm is not zero. */ \
	while ( PASTEMAC(chr,eq0)( norm ) ) \
	{ \
		/* Invoke the helper variant, which loops over the appropriate kernel
		   to implement the current operation. */ \
		PASTEMAC2(ch,opname,_unb_var1) \
		( \
		  n, \
		  x, incx, \
		  cntx, \
		  rntm  \
		); \
\
		/* Check the 1-norm of the randomzied vector. In the unlikely event that
		   the 1-norm is zero, it means that *all* elements are zero, in which
		   case we want to re-randomize until the 1-norm is not zero. */ \
		PASTEMAC2(ch,norm1v,BLIS_TAPI_EX_SUF) \
		( \
		  n, \
		  x, incx, \
		  &norm, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNCR_BASIC0( randv )
INSERT_GENTFUNCR_BASIC0( randnv )


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       uplo_t  uplox, \
       dim_t   m, \
       dim_t   n, \
       ctype*  x, inc_t rs_x, inc_t cs_x  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If either dimension is zero, return early. */ \
	if ( bli_zero_dim2( m, n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	/*if ( cntx == NULL ) cntx = bli_gks_query_cntx();*/ \
\
	ctype_r norm; \
\
	/* Set the norm to zero. */ \
	PASTEMAC(chr,set0s)( norm ); \
\
	/* Iterate at least once, but continue iterating until the norm is not zero. */ \
	while ( PASTEMAC(chr,eq0)( norm ) ) \
	{ \
		/* Invoke the helper variant, which loops over the appropriate kernel
		   to implement the current operation. */ \
		PASTEMAC2(ch,opname,_unb_var1) \
		( \
		  diagoffx, \
		  uplox, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  cntx, \
		  rntm  \
		); \
\
		/* Check the 1-norm of the randomzied matrix. In the unlikely event that
		   the 1-norm is zero, it means that *all* elements are zero, in which
		   case we want to re-randomize until the 1-norm is not zero. */ \
		PASTEMAC2(ch,norm1m,BLIS_TAPI_EX_SUF) \
		( \
		  diagoffx, \
		  BLIS_NONUNIT_DIAG, \
		  uplox, \
		  m, \
		  n, \
		  x, rs_x, cs_x, \
		  &norm, \
		  cntx, \
		  rntm  \
		); \
	} \
}

INSERT_GENTFUNCR_BASIC0( randm )
INSERT_GENTFUNCR_BASIC0( randnm )


#undef  GENTFUNCR
#define GENTFUNCR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* scale, \
       ctype_r* sumsq  \
       BLIS_TAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_TAPI_EX_DECLS \
\
	/* If x is zero length, return with scale and sumsq unchanged. */ \
	if ( bli_zero_dim1( n ) ) return; \
\
	/* Obtain a valid context from the gks if necessary. */ \
	/*if ( cntx == NULL ) cntx = bli_gks_query_cntx();*/ \
\
	/* Invoke the helper variant, which loops over the appropriate kernel
	   to implement the current operation. */ \
	PASTEMAC2(ch,opname,_unb_var1) \
	( \
	  n, \
	  x, incx, \
	  scale, \
	  sumsq, \
	  cntx, \
	  rntm  \
	); \
}

INSERT_GENTFUNCR_BASIC0( sumsqv )


#endif

