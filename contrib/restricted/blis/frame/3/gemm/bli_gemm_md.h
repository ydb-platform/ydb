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

#include "bli_gemm_md_c2r_ref.h"

// Define a local struct type that makes returning two values easier.
typedef struct mddm_s
{
	dom_t comp;
	dom_t exec;
} mddm_t;

void bli_gemm_md
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     );
mddm_t bli_gemm_md_ccc( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_ccr( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_crc( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_rcc( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_rrc( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_rcr( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_crr( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );
mddm_t bli_gemm_md_rrr( obj_t* a, obj_t* b, obj_t* beta, obj_t* c, cntx_t* cntx_l, cntx_t** cntx );

// -----------------------------------------------------------------------------

void bli_gemm_md_front
     (
       obj_t*  alpha,
       obj_t*  a,
       obj_t*  b,
       obj_t*  beta,
       obj_t*  c,
       cntx_t* cntx,
       rntm_t* rntm,
       cntl_t* cntl
     );

void bli_gemm_md_zgemm
     (
       obj_t*  alpha,
       obj_t*  a,
       obj_t*  b,
       obj_t*  beta,
       obj_t*  c,
       cntx_t* cntx,
       rntm_t* rntm,
       cntl_t* cntl
     );

// -----------------------------------------------------------------------------

BLIS_INLINE bool bli_gemm_md_is_crr( obj_t* a, obj_t* b, obj_t* c )
{
	bool r_val = FALSE;

	// NOTE: The last conditional subexpression is necessary if/when we
	// allow the user to specify the computation domain. (The computation
	// domain is currently ignored, but once it is honored as a user-
	// settable value, it will affect the execution domain, which is what
	// is checked below. Until then, the last expression is not actually
	// necessary since crr is already unconditionally associated with an
	// execution domain of BLIS_REAL.)
	if ( bli_obj_is_complex( c ) &&
	     bli_obj_is_real( a )    &&
	     bli_obj_is_real( b )    &&
	     bli_obj_exec_domain( c ) == BLIS_REAL )
		r_val = TRUE;

	return r_val;
}

BLIS_INLINE bool bli_gemm_md_is_ccr( obj_t* a, obj_t* b, obj_t* c )
{
	bool r_val = FALSE;

	// NOTE: The last conditional subexpression is necessary if/when we
	// allow the user to specify the computation domain. (The computation
	// domain is currently ignored, but once it is honored as a user-
	// settable value, it will affect the execution domain, which is what
	// is checked below. Until then, the last expression is not actually
	// necessary since ccr is already unconditionally associated with an
	// execution domain of BLIS_COMPLEX.)
	if ( bli_obj_is_complex( c ) &&
	     bli_obj_is_complex( a ) &&
	     bli_obj_is_real( b )    &&
	     bli_obj_exec_domain( c ) == BLIS_COMPLEX )
		r_val = TRUE;

	return r_val;
}

BLIS_INLINE bool bli_gemm_md_is_crc( obj_t* a, obj_t* b, obj_t* c )
{
	bool r_val = FALSE;

	// NOTE: The last conditional subexpression is necessary if/when we
	// allow the user to specify the computation domain. (The computation
	// domain is currently ignored, but once it is honored as a user-
	// settable value, it will affect the execution domain, which is what
	// is checked below. Until then, the last expression is not actually
	// necessary since crc is already unconditionally associated with an
	// execution domain of BLIS_COMPLEX.)
	if ( bli_obj_is_complex( c ) &&
	     bli_obj_is_real( a )    &&
	     bli_obj_is_complex( b ) &&
	     bli_obj_exec_domain( c ) == BLIS_COMPLEX )
		r_val = TRUE;

	return r_val;
}

// -----------------------------------------------------------------------------

BLIS_INLINE void bli_gemm_md_ker_var2_recast
     (
       num_t* dt_comp,
       num_t  dt_a,
       num_t  dt_b,
       num_t  dt_c,
       dim_t* m,
       dim_t* n,
       dim_t* k,
       inc_t* pd_a, inc_t* ps_a,
       inc_t* pd_b, inc_t* ps_b,
       obj_t* c,
       inc_t* rs_c, inc_t* cs_c
     )
{
	if      ( bli_is_real( dt_c )    &&
	          bli_is_complex( dt_a ) &&
	          bli_is_complex( dt_b ) )
	{
		// The rcc case is executed with a real macrokernel, so we need to
		// double the k dimension (because both A and B are packed to the 1r
		// schema), and also the panel strides of A and B since they were
		// packed as complex matrices and we now need to convert them to
		// units of real elements.
		*k *= 2;
		*ps_a *= 2;
		*ps_b *= 2;
	}
	else if ( bli_is_complex( dt_c ) &&
	          bli_is_real( dt_a )    &&
	          bli_is_complex( dt_b ) )
	{
#if 1
		obj_t beta;

		bli_obj_scalar_detach( c, &beta );

		if ( //bli_obj_imag_equals( &beta, &BLIS_ZERO ) &&
		     bli_obj_imag_is_zero( &beta ) &&
		     bli_is_row_stored( *rs_c, *cs_c ) &&
		     bli_obj_prec( c ) == bli_obj_comp_prec( c ) )
		{
			// If beta is real, and C is not general-stored, and the computation
			// precision is equal to the storage precision of C, we can use the
			// real macrokernel (and real microkernel, which is already stored
			// to the real virtual microkernel slots of the context) instead of
			// the complex macrokernel and c2r virtual microkernel.
			*dt_comp = bli_dt_proj_to_real( *dt_comp );
			*n *= 2;
			*pd_b *= 2; *ps_b *= 2;
			*rs_c *= 2;
		}
		else
#endif
		{
			// Generally speaking, the crc case is executed with a complex
			// macrokernel, so we need to halve the panel stride of A (which
			// is real) since the macrokernel will perform the pointer
			// arithmetic in units of complex elements.
			*ps_a /= 2;
		}
	}
	else if ( bli_is_complex( dt_c ) &&
	          bli_is_complex( dt_a ) &&
	          bli_is_real( dt_b ) )
	{
#if 1
		obj_t beta;

		bli_obj_scalar_detach( c, &beta );

		if ( //bli_obj_imag_equals( &beta, &BLIS_ZERO ) &&
		     bli_obj_imag_is_zero( &beta ) &&
		     bli_is_col_stored( *rs_c, *cs_c ) &&
		     bli_obj_prec( c ) == bli_obj_comp_prec( c ) )
		{
			// If beta is real, and C is not general-stored, and the computation
			// precision is equal to the storage precision of C, we can use the
			// real macrokernel (and real microkernel, which is already stored
			// to the real virtual microkernel slots of the context) instead of
			// the complex macrokernel and c2r virtual microkernel.
			*dt_comp = bli_dt_proj_to_real( *dt_comp );
			*m *= 2;
			*pd_a *= 2; *ps_a *= 2;
			*cs_c *= 2;
		}
		else
#endif
		{
			// Generally speaking, the ccr case is executed with a complex
			// macrokernel, so we need to halve the panel stride of B (which
			// is real) since the macrokernel will perform the pointer
			// arithmetic in units of complex elements.
			*ps_b /= 2;
		}
	}
#if 0
	else if ( bli_is_real( dt_c ) &&
	          bli_is_real( dt_a ) &&
	          bli_is_real( dt_b ) )
	{
		// No action needed.
//printf( "gemm_md.h: rrr: m n k are now %d %d %d\n", (int)*m, (int)*n, (int)*k );
	}
	else if ( bli_is_complex( dt_c ) &&
	          bli_is_real( dt_a ) &&
	          bli_is_real( dt_b ) )
	{
		// No action needed.
	}
	else if ( bli_is_real( dt_c ) &&
	          bli_is_complex( dt_a ) &&
	          bli_is_real( dt_b ) )
	{
		// No action needed.
	}
	else if ( bli_is_real( dt_c ) &&
	          bli_is_real( dt_a ) &&
	          bli_is_complex( dt_b ) )
	{
		// No action needed.
	}
#endif
}

// -----------------------------------------------------------------------------

//
// Prototype object-based interfaces.
//

#undef  GENPROT
#define GENPROT( opname ) \
\
void PASTEMAC0(opname) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c, \
       cntx_t* cntx, \
       rntm_t* rntm, \
       cntl_t* cntl, \
       thrinfo_t* thread  \
     );

GENPROT( gemm_ker_var2_md )

//
// Prototype BLAS-like interfaces with void pointer operands.
//

#undef  GENTPROT2
#define GENTPROT2( ctype_c, ctype_e, chc, che, varname ) \
\
void PASTEMAC2(chc,che,varname) \
     ( \
       pack_t  schema_a, \
       pack_t  schema_b, \
       dim_t   m, \
       dim_t   n, \
       dim_t   k, \
       void*   alpha, \
       void*   a, inc_t cs_a, inc_t is_a, \
                  dim_t pd_a, inc_t ps_a, \
       void*   b, inc_t rs_b, inc_t is_b, \
                  dim_t pd_b, inc_t ps_b, \
       void*   beta, \
       void*   c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm, \
       thrinfo_t* thread  \
     );

INSERT_GENTPROT2_BASIC0( gemm_ker_var2_md )
INSERT_GENTPROT2_MIXDP0( gemm_ker_var2_md )

