/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018 - 2020, Advanced Micro Devices, Inc.

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

void bli_gemm_front
     (
       obj_t*  alpha,
       obj_t*  a,
       obj_t*  b,
       obj_t*  beta,
       obj_t*  c,
       cntx_t* cntx,
       rntm_t* rntm,
       cntl_t* cntl
     )
{
	bli_init_once();

	obj_t   a_local;
	obj_t   b_local;
	obj_t   c_local;

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
		bli_gemm_check( alpha, a, b, beta, c, cntx );

	// If C has a zero dimension, return early.
	if ( bli_obj_has_zero_dim( c ) )
	{
		return;
	}

	// If alpha is zero, or if A or B has a zero dimension, scale C by beta
	// and return early.
	if ( bli_obj_equals( alpha, &BLIS_ZERO ) ||
	     bli_obj_has_zero_dim( a ) ||
	     bli_obj_has_zero_dim( b ) )
	{
		bli_scalm( beta, c );
		return;
	}

#if 0
#ifdef BLIS_ENABLE_SMALL_MATRIX
	// Only handle small problems separately for homogeneous datatypes.
	if ( bli_obj_dt( a ) == bli_obj_dt( b ) &&
	     bli_obj_dt( a ) == bli_obj_dt( c ) &&
	     bli_obj_comp_prec( c ) == bli_obj_prec( c ) )
	{
		err_t status = bli_gemm_small( alpha, a, b, beta, c, cntx, cntl );
		if ( status == BLIS_SUCCESS ) return;
	}
#endif
#endif

	// Alias A, B, and C in case we need to apply transformations.
	bli_obj_alias_to( a, &a_local );
	bli_obj_alias_to( b, &b_local );
	bli_obj_alias_to( c, &c_local );

#ifdef BLIS_ENABLE_GEMM_MD
	cntx_t cntx_local;

	// If any of the storage datatypes differ, or if the computation precision
	// differs from the storage precision of C, utilize the mixed datatype
	// code path.
	// NOTE: If we ever want to support the caller setting the computation
	// domain explicitly, we will need to check the computation dt against the
	// storage dt of C (instead of the computation precision against the
	// storage precision of C).
	if ( bli_obj_dt( &c_local ) != bli_obj_dt( &a_local ) ||
	     bli_obj_dt( &c_local ) != bli_obj_dt( &b_local ) ||
	     bli_obj_comp_prec( &c_local ) != bli_obj_prec( &c_local ) )
	{
		// Handle mixed datatype cases in bli_gemm_md(), which may modify
		// the objects or the context. (If the context is modified, cntx
		// is adjusted to point to cntx_local.)
		bli_gemm_md( &a_local, &b_local, beta, &c_local, &cntx_local, &cntx );
	}
	//else // homogeneous datatypes
#endif

	// Load the pack schemas from the context and embed them into the objects
	// for A and B. (Native contexts are initialized with the correct pack
	// schemas, as are contexts for 1m, and if necessary bli_gemm_md() would
	// have made a copy and modified the schemas, so reading them from the
	// context should be a safe bet at this point.) This is a sort of hack for
	// communicating the desired pack schemas to bli_gemm_cntl_create() (via
	// bli_l3_thread_decorator() and bli_l3_cntl_create_if()). This allows us
	// to subsequently access the schemas from the control tree, which
	// hopefully reduces some confusion, particularly in bli_packm_init().
	const pack_t schema_a = bli_cntx_schema_a_block( cntx );
	const pack_t schema_b = bli_cntx_schema_b_panel( cntx );

	bli_obj_set_pack_schema( schema_a, &a_local );
	bli_obj_set_pack_schema( schema_b, &b_local );

	// Next, we handle the possibility of needing to typecast alpha to the
	// computation datatype and/or beta to the storage datatype of C.

	// Attach alpha to B, and in the process typecast alpha to the target
	// datatype of the matrix (which in this case is equal to the computation
	// datatype).
	bli_obj_scalar_attach( BLIS_NO_CONJUGATE, alpha, &b_local );

	// Attach beta to C, and in the process typecast beta to the target
	// datatype of the matrix (which in this case is equal to the storage
	// datatype of C).
	bli_obj_scalar_attach( BLIS_NO_CONJUGATE, beta,  &c_local );

	// Change the alpha and beta pointers to BLIS_ONE since the values have
	// now been typecast and attached to the matrices above.
	alpha = &BLIS_ONE;
	beta  = &BLIS_ONE;

#ifdef BLIS_ENABLE_GEMM_MD
	// Don't perform the following optimization for ccr or crc cases, as
	// those cases are sensitive to the ukernel storage preference (ie:
	// transposing the operation would break them).
	if ( !bli_gemm_md_is_ccr( &a_local, &b_local, &c_local ) &&
	     !bli_gemm_md_is_crc( &a_local, &b_local, &c_local ) )
#endif
	// An optimization: If C is stored by rows and the micro-kernel prefers
	// contiguous columns, or if C is stored by columns and the micro-kernel
	// prefers contiguous rows, transpose the entire operation to allow the
	// micro-kernel to access elements of C in its preferred manner.
	if ( bli_cntx_l3_vir_ukr_dislikes_storage_of( &c_local, BLIS_GEMM_UKR, cntx ) )
	{
		bli_obj_swap( &a_local, &b_local );

		bli_obj_induce_trans( &a_local );
		bli_obj_induce_trans( &b_local );
		bli_obj_induce_trans( &c_local );

		// We must also swap the pack schemas, which were set by bli_gemm_md()
		// or the inlined code above.
		bli_obj_swap_pack_schemas( &a_local, &b_local );
	}

	// Parse and interpret the contents of the rntm_t object to properly
	// set the ways of parallelism for each loop, and then make any
	// additional modifications necessary for the current operation.
	bli_rntm_set_ways_for_op
	(
	  BLIS_GEMM,
	  BLIS_LEFT, // ignored for gemm/hemm/symm
	  bli_obj_length( &c_local ),
	  bli_obj_width( &c_local ),
	  bli_obj_width( &a_local ),
	  rntm
	);

	obj_t* cp    = &c_local;
	obj_t* betap = beta;

#ifdef BLIS_ENABLE_GEMM_MD
#ifdef BLIS_ENABLE_GEMM_MD_EXTRA_MEM
	// If any of the following conditions are met, create a temporary matrix
	// conformal to C into which we will accumulate the matrix product:
	// - the storage precision of C differs from the computation precision;
	// - the domains are mixed as crr;
	// - the storage format of C does not match the preferred orientation
	//   of the ccr or crc cases.
	// Then, after the computation is complete, this matrix will be copied
	// or accumulated back to C.
	const bool is_ccr_mismatch =
	             ( bli_gemm_md_is_ccr( &a_local, &b_local, &c_local ) &&
                   !bli_obj_is_col_stored( &c_local ) );
	const bool is_crc_mismatch =
	             ( bli_gemm_md_is_crc( &a_local, &b_local, &c_local ) &&
                   !bli_obj_is_row_stored( &c_local ) );

	obj_t ct;
	bool  use_ct = FALSE;

	// FGVZ: Consider adding another guard here that only creates and uses a
	// temporary matrix for accumulation if k < c * kc, where c is some small
	// constant like 2. And don't forget to use the same conditional for the
	// castm() and free() at the end.
	if (
	     bli_obj_prec( &c_local ) != bli_obj_comp_prec( &c_local ) ||
	     bli_gemm_md_is_crr( &a_local, &b_local, &c_local ) ||
	     is_ccr_mismatch ||
	     is_crc_mismatch
	   )
	{
		use_ct = TRUE;
	}

	// If we need a temporary matrix conformal to C for whatever reason,
	// we create it and prepare to use it now.
	if ( use_ct )
	{
		const dim_t m     = bli_obj_length( &c_local );
		const dim_t n     = bli_obj_width( &c_local );
		      inc_t rs    = bli_obj_row_stride( &c_local );
		      inc_t cs    = bli_obj_col_stride( &c_local );

		      num_t dt_ct = bli_obj_domain( &c_local ) |
		                    bli_obj_comp_prec( &c_local );

		// When performing the crr case, accumulate to a contiguously-stored
		// real matrix so we do not have to repeatedly update C with general
		// stride.
		if ( bli_gemm_md_is_crr( &a_local, &b_local, &c_local ) )
			dt_ct = BLIS_REAL | bli_obj_comp_prec( &c_local );

		// When performing the mismatched ccr or crc cases, now is the time
		// to specify the appropriate storage so the gemm_md_c2r_ref() virtual
		// microkernel can output directly to C (instead of using a temporary
		// microtile).
		if      ( is_ccr_mismatch ) { rs = 1; cs = m; }
		else if ( is_crc_mismatch ) { rs = n; cs = 1; }

		bli_obj_create( dt_ct, m, n, rs, cs, &ct );

		const num_t dt_exec = bli_obj_exec_dt( &c_local );
		const num_t dt_comp = bli_obj_comp_dt( &c_local );

		bli_obj_set_target_dt( dt_ct, &ct );
		bli_obj_set_exec_dt( dt_exec, &ct );
		bli_obj_set_comp_dt( dt_comp, &ct );

		// A naive approach would cast C to the comptuation datatype,
		// compute with beta, and then cast the result back to the
		// user-provided output matrix. However, we employ a different
		// approach that halves the number of memops on C (or its
		// typecast temporary) by writing the A*B product directly to
		// temporary storage, and then using xpbym to scale the
		// output matrix by beta and accumulate/cast the A*B product.
		//bli_castm( &c_local, &ct );
		betap = &BLIS_ZERO;

		cp = &ct;
	}
#endif
#endif

	// Invoke the internal back-end via the thread handler.
	bli_l3_thread_decorator
	(
	  bli_gemm_int,
	  BLIS_GEMM, // operation family id
	  alpha,
	  &a_local,
	  &b_local,
	  betap,
	  cp,
	  cntx,
	  rntm,
	  cntl
	);

#ifdef BLIS_ENABLE_GEMM_MD
#ifdef BLIS_ENABLE_GEMM_MD_EXTRA_MEM
	// If we created a temporary matrix conformal to C for whatever reason,
	// we copy/accumulate the result back to C and then release the object.
	if ( use_ct )
    {
		obj_t beta_local;

		bli_obj_scalar_detach( &c_local, &beta_local );

		//bli_castnzm( &ct, &c_local );
		bli_xpbym( &ct, &beta_local, &c_local );

		bli_obj_free( &ct );
	}
#endif
#endif
}

// -----------------------------------------------------------------------------

#if 0
	if ( bli_obj_dt( a ) != bli_obj_dt( b ) ||
	     bli_obj_dt( a ) != bli_obj_dt( c ) ||
	     bli_obj_comp_prec( c ) != bli_obj_prec( c ) )
	{
		const bool a_is_real = bli_obj_is_real( a );
		const bool a_is_comp = bli_obj_is_complex( a );
		const bool b_is_real = bli_obj_is_real( b );
		const bool b_is_comp = bli_obj_is_complex( b );
		const bool c_is_real = bli_obj_is_real( c );
		const bool c_is_comp = bli_obj_is_complex( c );

		const bool a_is_single = bli_obj_is_single_prec( a );
		const bool a_is_double = bli_obj_is_double_prec( a );
		const bool b_is_single = bli_obj_is_single_prec( b );
		const bool b_is_double = bli_obj_is_double_prec( b );
		const bool c_is_single = bli_obj_is_single_prec( c );
		const bool c_is_double = bli_obj_is_double_prec( c );

		const bool comp_single = bli_obj_comp_prec( c ) == BLIS_SINGLE_PREC;
		const bool comp_double = bli_obj_comp_prec( c ) == BLIS_DOUBLE_PREC;

		const bool mixeddomain = bli_obj_domain( c ) != bli_obj_domain( a ) ||
		                         bli_obj_domain( c ) != bli_obj_domain( b );

		( void )a_is_real; ( void )a_is_comp;
		( void )b_is_real; ( void )b_is_comp;
		( void )c_is_real; ( void )c_is_comp;
		( void )a_is_single; ( void )a_is_double;
		( void )b_is_single; ( void )b_is_double;
		( void )c_is_single; ( void )c_is_double;
		( void )comp_single; ( void )comp_double;

		if (
		     //( c_is_comp && a_is_comp && b_is_real ) ||
		     //( c_is_comp && a_is_real && b_is_comp ) ||
		     //( c_is_real && a_is_comp && b_is_comp ) ||
		     //( c_is_comp && a_is_real && b_is_real ) ||
		     //( c_is_real && a_is_comp && b_is_real ) ||
		     //( c_is_real && a_is_real && b_is_comp ) ||
		     //FALSE
		     TRUE
		   )
		{
			if (
			     ( c_is_single && a_is_single && b_is_single && mixeddomain ) ||
			     ( c_is_single && a_is_single && b_is_single && comp_single ) ||
			     ( c_is_single && a_is_single && b_is_single && comp_double ) ||
			     ( c_is_single && a_is_single && b_is_double                ) ||
			     ( c_is_single && a_is_double && b_is_single                ) ||
			     ( c_is_double && a_is_single && b_is_single                ) ||
			     ( c_is_single && a_is_double && b_is_double                ) ||
			     ( c_is_double && a_is_single && b_is_double                ) ||
			     ( c_is_double && a_is_double && b_is_single                ) ||
			     ( c_is_double && a_is_double && b_is_double && comp_single ) ||
			     ( c_is_double && a_is_double && b_is_double && comp_double ) ||
			     ( c_is_double && a_is_double && b_is_double && mixeddomain ) ||
			     FALSE
			   )
				bli_gemm_md_front( alpha, a, b, beta, c, cntx, cntl );
			else
				bli_gemm_md_zgemm( alpha, a, b, beta, c, cntx, cntl );
		}
		else
			bli_gemm_md_zgemm( alpha, a, b, beta, c, cntx, cntl );
		return;
	}
#else
#if 0
	// If any of the storage datatypes differ, or if the execution precision
	// differs from the storage precision of C, utilize the mixed datatype
	// code path.
	// NOTE: We could check the exec dt against the storage dt of C, but for
	// now we don't support the caller setting the execution domain
	// explicitly.
	if ( bli_obj_dt( a ) != bli_obj_dt( b ) ||
	     bli_obj_dt( a ) != bli_obj_dt( c ) ||
	     bli_obj_comp_prec( c ) != bli_obj_prec( c ) )
	{
		bli_gemm_md_front( alpha, a, b, beta, c, cntx, cntl );
		return;
	}
#endif
#endif

