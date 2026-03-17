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
// #included from files that define the object API macros.
#ifdef BLIS_ENABLE_OAPI

//
// Define object-based interfaces.
//

#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x, \
       obj_t*  asum  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     n         = bli_obj_vector_dim( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     incx      = bli_obj_vector_inc( x ); \
\
	void*     buf_asum  = bli_obj_buffer_at_off( asum ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x, asum ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   n, \
	   buf_x, incx, \
	   buf_asum, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( asumv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  a  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( a ); \
\
	uplo_t    uploa     = bli_obj_uplo( a ); \
	dim_t     m         = bli_obj_length( a ); \
	void*     buf_a     = bli_obj_buffer_at_off( a ); \
	inc_t     rs_a      = bli_obj_row_stride( a ); \
	inc_t     cs_a      = bli_obj_col_stride( a ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( a ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   uploa, \
	   m, \
	   buf_a, rs_a, cs_a, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( mkherm )
GENFRONT( mksymm )
GENFRONT( mktrim )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x, \
       obj_t*  norm  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     n         = bli_obj_vector_dim( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     incx      = bli_obj_vector_inc( x ); \
	void*     buf_norm  = bli_obj_buffer_at_off( norm ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x, norm ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   n, \
	   buf_x, incx, \
	   buf_norm, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( norm1v )
GENFRONT( normfv )
GENFRONT( normiv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x, \
       obj_t*  norm  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	doff_t    diagoffx  = bli_obj_diag_offset( x ); \
	diag_t    diagx     = bli_obj_diag( x ); \
	uplo_t    uplox     = bli_obj_uplo( x ); \
	dim_t     m         = bli_obj_length( x ); \
	dim_t     n         = bli_obj_width( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     rs_x      = bli_obj_row_stride( x ); \
	inc_t     cs_x      = bli_obj_col_stride( x ); \
	void*     buf_norm  = bli_obj_buffer_at_off( norm ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x, norm ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   diagoffx, \
	   diagx, \
	   uplox, \
	   m, \
	   n, \
	   buf_x, rs_x, cs_x, \
	   buf_norm, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( norm1m )
GENFRONT( normfm )
GENFRONT( normim )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       FILE*   file, \
       char*   s1, \
       obj_t*  x, \
       char*   format, \
       char*   s2  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     n         = bli_obj_vector_dim( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     incx      = bli_obj_vector_inc( x ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( file, s1, x, format, s2 ); \
\
	/* Handle constants up front. */ \
	if ( dt == BLIS_CONSTANT ) \
	{ \
		bli_check_error_code( BLIS_NOT_YET_IMPLEMENTED ); \
	} \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH(opname,_vft) f = \
	PASTEMAC(opname,_qfp)( dt ); \
\
	f \
	( \
	   file, \
	   s1, \
	   n, \
	   buf_x, incx, \
	   format, \
	   s2  \
	); \
}

GENFRONT( fprintv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       FILE*   file, \
       char*   s1, \
       obj_t*  x, \
       char*   format, \
       char*   s2  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     m         = bli_obj_length( x ); \
	dim_t     n         = bli_obj_width( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     rs_x      = bli_obj_row_stride( x ); \
	inc_t     cs_x      = bli_obj_col_stride( x ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( file, s1, x, format, s2 ); \
\
	/* Handle constants up front. */ \
	if ( dt == BLIS_CONSTANT ) \
	{ \
		float*    sp = bli_obj_buffer_for_const( BLIS_FLOAT,    x ); \
		double*   dp = bli_obj_buffer_for_const( BLIS_DOUBLE,   x ); \
		scomplex* cp = bli_obj_buffer_for_const( BLIS_SCOMPLEX, x ); \
		dcomplex* zp = bli_obj_buffer_for_const( BLIS_DCOMPLEX, x ); \
		gint_t*   ip = bli_obj_buffer_for_const( BLIS_INT,      x ); \
\
		fprintf( file, "%s\n", s1 ); \
		fprintf( file, " float:     %9.2e\n",         bli_sreal( *sp ) ); \
		fprintf( file, " double:    %9.2e\n",         bli_dreal( *dp ) ); \
		fprintf( file, " scomplex:  %9.2e + %9.2e\n", bli_creal( *cp ), \
		                                              bli_cimag( *cp ) ); \
		fprintf( file, " dcomplex:  %9.2e + %9.2e\n", bli_zreal( *zp ), \
		                                              bli_zimag( *zp ) ); \
		fprintf( file, " int:       %ld\n",           ( long )(*ip) ); \
		fprintf( file, "\n" ); \
		return; \
	} \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH(opname,_vft) f = \
	PASTEMAC(opname,_qfp)( dt ); \
\
	f \
	( \
	   file, \
	   s1, \
	   m, \
	   n, \
	   buf_x, rs_x, cs_x, \
	   format, \
	   s2  \
	); \
}

GENFRONT( fprintm )


#undef  GENFRONT
#define GENFRONT( opname, varname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       char*   s1, \
       obj_t*  x, \
       char*   format, \
       char*   s2  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	/* Suppress compiler warning about unused variables. */ \
	( void )cntx; \
\
	/* Invoke the typed function. */ \
	PASTEMAC0(varname) \
	( \
	   stdout, \
	   s1, \
	   x, \
	   format, \
	   s2  \
	); \
}

GENFRONT( printv, fprintv )
GENFRONT( printm, fprintm )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     n         = bli_obj_vector_dim( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     incx      = bli_obj_vector_inc( x ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   n, \
	   buf_x, incx, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( randv )
GENFRONT( randnv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	doff_t    diagoffx  = bli_obj_diag_offset( x ); \
	uplo_t    uplox     = bli_obj_uplo( x ); \
	dim_t     m         = bli_obj_length( x ); \
	dim_t     n         = bli_obj_width( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     rs_x      = bli_obj_row_stride( x ); \
	inc_t     cs_x      = bli_obj_col_stride( x ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   diagoffx, \
	   uplox, \
	   m, \
	   n, \
	   buf_x, rs_x, cs_x, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( randm )
GENFRONT( randnm )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,EX_SUF) \
     ( \
       obj_t*  x, \
       obj_t*  scale, \
       obj_t*  sumsq  \
       BLIS_OAPI_EX_PARAMS  \
     ) \
{ \
	bli_init_once(); \
\
	BLIS_OAPI_EX_DECLS \
\
	num_t     dt        = bli_obj_dt( x ); \
\
	dim_t     n         = bli_obj_vector_dim( x ); \
	void*     buf_x     = bli_obj_buffer_at_off( x ); \
	inc_t     incx      = bli_obj_vector_inc( x ); \
	void*     buf_scale = bli_obj_buffer_at_off( scale ); \
	void*     buf_sumsq = bli_obj_buffer_at_off( sumsq ); \
\
	if ( bli_error_checking_is_enabled() ) \
	    PASTEMAC(opname,_check)( x, scale, sumsq ); \
\
	/* Query a type-specific function pointer, except one that uses
	   void* for function arguments instead of typed pointers. */ \
	PASTECH2(opname,BLIS_TAPI_EX_SUF,_vft) f = \
	PASTEMAC2(opname,BLIS_TAPI_EX_SUF,_qfp)( dt ); \
\
	f \
	( \
	   n, \
	   buf_x, incx, \
	   buf_scale, \
	   buf_sumsq, \
	   cntx, \
	   rntm  \
	); \
}

GENFRONT( sumsqv )



#endif

