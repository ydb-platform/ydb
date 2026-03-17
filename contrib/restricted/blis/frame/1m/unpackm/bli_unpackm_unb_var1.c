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

#define FUNCPTR_T unpackm_fp

typedef void (*FUNCPTR_T)(
                           doff_t  diagoffp,
                           uplo_t  uplop,
                           trans_t transp,
                           dim_t   m,
                           dim_t   n,
                           void*   p, inc_t rs_p, inc_t cs_p,
                           void*   c, inc_t rs_c, inc_t cs_c,
                           cntx_t* cntx
                         );

static FUNCPTR_T GENARRAY(ftypes,unpackm_unb_var1);


void bli_unpackm_unb_var1
     (
       obj_t*  p,
       obj_t*  c,
       cntx_t* cntx,
       cntl_t* cntl,
       thrinfo_t* thread
     )
{
	num_t     dt_pc     = bli_obj_dt( p );

	doff_t    diagoffp  = bli_obj_diag_offset( p );
	uplo_t    uplop     = bli_obj_uplo( p );
	trans_t   transc    = bli_obj_onlytrans_status( c );

	dim_t     m_c       = bli_obj_length( c );
	dim_t     n_c       = bli_obj_width( c );

	void*     buf_p     = bli_obj_buffer_at_off( p );
	inc_t     rs_p      = bli_obj_row_stride( p );
	inc_t     cs_p      = bli_obj_col_stride( p );

	void*     buf_c     = bli_obj_buffer_at_off( c );
	inc_t     rs_c      = bli_obj_row_stride( c );
	inc_t     cs_c      = bli_obj_col_stride( c );

	FUNCPTR_T f;

	// Index into the type combination array to extract the correct
	// function pointer.
	f = ftypes[dt_pc];

	// Invoke the function.
	f( diagoffp,
	   uplop,
	   transc,
	   m_c,
	   n_c,
	   buf_p, rs_p, cs_p,
	   buf_c, rs_c, cs_c,
	   cntx
	);
}


#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, varname ) \
\
void PASTEMAC(ch,varname)( \
                           doff_t  diagoffp, \
                           uplo_t  uplop, \
                           trans_t transp, \
                           dim_t   m, \
                           dim_t   n, \
                           void*   p, inc_t rs_p, inc_t cs_p, \
                           void*   c, inc_t rs_c, inc_t cs_c, \
                           cntx_t* cntx  \
                         ) \
{ \
	ctype* p_cast = p; \
	ctype* c_cast = c; \
\
	PASTEMAC2(ch,copym,BLIS_TAPI_EX_SUF) \
	( \
	  diagoffp,\
	  BLIS_NONUNIT_DIAG, \
	  uplop, \
	  transp, \
	  m, \
	  n, \
	  p_cast, rs_p, cs_p, \
	  c_cast, rs_c, cs_c, \
	  cntx, \
	  NULL  \
	); \
}

INSERT_GENTFUNC_BASIC( unpackm, unpackm_unb_var1 )

