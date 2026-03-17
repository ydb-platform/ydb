/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2017 - 2019, Advanced Micro Devices, Inc.

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

void bli_gemm_md
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	const bool a_is_real = bli_obj_is_real( a );
	const bool a_is_comp = bli_obj_is_complex( a );
	const bool b_is_real = bli_obj_is_real( b );
	const bool b_is_comp = bli_obj_is_complex( b );
	const bool c_is_real = bli_obj_is_real( c );
	const bool c_is_comp = bli_obj_is_complex( c );

	if      ( c_is_real && a_is_real && b_is_real )
	{
		// C_real += A_real * B_real
		doms = bli_gemm_md_rrr( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_comp && a_is_comp && b_is_comp )
	{
		// C_complex += A_complex * B_complex
		doms = bli_gemm_md_ccc( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_comp && a_is_comp && b_is_real )
	{
		// C_complex += A_complex * B_real
		doms = bli_gemm_md_ccr( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_comp && a_is_real && b_is_comp )
	{
		// C_complex += A_real * B_complex
		doms = bli_gemm_md_crc( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_real && a_is_comp && b_is_comp )
	{
		// C_real += A_complex * B_complex
		doms = bli_gemm_md_rcc( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_comp && a_is_real && b_is_real )
	{
		// C_complex += A_real * B_real
		doms = bli_gemm_md_crr( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_real && a_is_comp && b_is_real )
	{
		// C_real += A_complex * B_real
		doms = bli_gemm_md_rcr( a, b, beta, c, cntx_local, cntx );
	}
	else if ( c_is_real && a_is_real && b_is_comp )
	{
		// C_real += A_real * B_complex
		doms = bli_gemm_md_rrc( a, b, beta, c, cntx_local, cntx );
	}
	else
	{
		doms.comp = BLIS_REAL;
		doms.exec = BLIS_REAL;

		// This should never execute.
		bli_abort();
	}

	// Extract the computation and execution domains from the struct
	// returned above.
	dom_t dom_comp = doms.comp;
	dom_t dom_exec = doms.exec;

	// Inspect the computation precision of C. (The user may have set
	// this explicitly to request the precision in which the computation
	// should take place.)
	prec_t prec_comp = bli_obj_comp_prec( c );

	// The computation precision tells us the target precision of A and B.
	// NOTE: We don't set the target domain here. The target domain would
	// either be unchanged, or would have been changed in one of the eight
	// domain cases above.
	bli_obj_set_target_prec( prec_comp, a );
	bli_obj_set_target_prec( prec_comp, b );

	// Combine the execution domain with the computation precision to form
	// the execution datatype. (The computation precision and execution
	// precision are always equal.)
	num_t dt_exec = dom_exec | prec_comp;

	// Set the execution datatypes of A, B, and C.
	bli_obj_set_exec_dt( dt_exec, a );
	bli_obj_set_exec_dt( dt_exec, b );
	bli_obj_set_exec_dt( dt_exec, c );

	// Combine the computation precision and computation domain to form the
	// computation datatype.
	num_t dt_comp = dom_comp | prec_comp;

	// Set the computation datatypes of A, B, and C.
	bli_obj_set_comp_dt( dt_comp, a );
	bli_obj_set_comp_dt( dt_comp, b );
	bli_obj_set_comp_dt( dt_comp, c );

}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_ccr
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	// We assume that the requested computation domain is complex.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_COMPLEX;

	// For ccr, the computation (ukernel) will be real, but the execution
	// will appear complex to other parts of the implementation.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_COMPLEX;

	// Here we construct the computation datatype, which for the ccr case
	// is equal to the real projection of the execution datatype, and use
	// that computation datatype to query the corresponding ukernel output
	// preference.
	const num_t dt = BLIS_REAL | bli_obj_comp_prec( c );
	const bool  row_pref
	      = bli_cntx_l3_nat_ukr_prefers_rows_dt( dt, BLIS_GEMM_UKR, *cntx );

	// We can only perform this case of mixed-domain gemm, C += A*B where
	// B is real, if the microkernel prefers column output. If it prefers
	// row output, we must induce a transposition and perform C += A*B
	// where A (formerly B) is real.
	if ( row_pref )
	{
		bli_obj_swap( a, b );

		bli_obj_induce_trans( a );
		bli_obj_induce_trans( b );
		bli_obj_induce_trans( c );

		return bli_gemm_md_crc( a, b, beta, c, cntx_local, cntx );
	}

	// Create a local copy of the context and then prepare to use this
	// context instead of the one passed in.
	*cntx_local = **cntx;
	*cntx = cntx_local;

	// Copy the real domain blocksizes into the slots of their complex
	// counterparts.
	blksz_t* blksz_mr = bli_cntx_get_blksz( BLIS_MR, *cntx );
	blksz_t* blksz_nr = bli_cntx_get_blksz( BLIS_NR, *cntx );
	blksz_t* blksz_mc = bli_cntx_get_blksz( BLIS_MC, *cntx );
	blksz_t* blksz_nc = bli_cntx_get_blksz( BLIS_NC, *cntx );
	blksz_t* blksz_kc = bli_cntx_get_blksz( BLIS_KC, *cntx );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mr, BLIS_SCOMPLEX, blksz_mr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mr, BLIS_DCOMPLEX, blksz_mr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nr, BLIS_SCOMPLEX, blksz_nr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nr, BLIS_DCOMPLEX, blksz_nr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mc, BLIS_SCOMPLEX, blksz_mc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mc, BLIS_DCOMPLEX, blksz_mc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nc, BLIS_SCOMPLEX, blksz_nc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nc, BLIS_DCOMPLEX, blksz_nc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_kc, BLIS_SCOMPLEX, blksz_kc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_kc, BLIS_DCOMPLEX, blksz_kc );

	// Halve both the real and complex MR's (which are both real MR's).
	bli_blksz_scale_def_max( 1, 2, BLIS_FLOAT,    blksz_mr );
	bli_blksz_scale_def_max( 1, 2, BLIS_DOUBLE,   blksz_mr );
	bli_blksz_scale_def_max( 1, 2, BLIS_SCOMPLEX, blksz_mr );
	bli_blksz_scale_def_max( 1, 2, BLIS_DCOMPLEX, blksz_mr );

	// Halve both the real and complex MC's (which are both real MC's).
	bli_blksz_scale_def_max( 1, 2, BLIS_FLOAT,    blksz_mc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DOUBLE,   blksz_mc );
	bli_blksz_scale_def_max( 1, 2, BLIS_SCOMPLEX, blksz_mc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DCOMPLEX, blksz_mc );

	// Use the default pack schemas in the context.

	// static func_t* bli_cntx_get_l3_vir_ukrs( l3ukr_t ukr_id, cntx_t* cntx )
	func_t* l3_vir_ukrs = bli_cntx_get_l3_vir_ukrs( BLIS_GEMM_UKR, *cntx );

	// Rather than check which complex datatype dt_comp refers to, we set
	// the mixed-domain virtual microkernel for both types.
	bli_func_set_dt( bli_cgemm_md_c2r_ref, BLIS_SCOMPLEX, l3_vir_ukrs );
	bli_func_set_dt( bli_zgemm_md_c2r_ref, BLIS_DCOMPLEX, l3_vir_ukrs );

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_crc
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	// We assume that the requested computation domain is complex.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_COMPLEX;

	// For crc, the computation (ukernel) will be real, but the execution
	// will appear complex to other parts of the implementation.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_COMPLEX;

	// Here we construct the computation datatype, which for the crc case
	// is equal to the real projection of the execution datatype, and use
	// that computation datatype to query the corresponding ukernel output
	// preference.
	const num_t dt = BLIS_REAL | bli_obj_comp_prec( c );
	const bool  col_pref
	      = bli_cntx_l3_nat_ukr_prefers_cols_dt( dt, BLIS_GEMM_UKR, *cntx );

	// We can only perform this case of mixed-domain gemm, C += A*B where
	// A is real, if the microkernel prefers row output. If it prefers
	// column output, we must induce a transposition and perform C += A*B
	// where B (formerly A) is real.
	if ( col_pref )
	{
		bli_obj_swap( a, b );

		bli_obj_induce_trans( a );
		bli_obj_induce_trans( b );
		bli_obj_induce_trans( c );

		return bli_gemm_md_ccr( a, b, beta, c, cntx_local, cntx );
	}

	// Create a local copy of the context and then prepare to use this
	// context instead of the one passed in.
	*cntx_local = **cntx;
	*cntx = cntx_local;

	// Copy the real domain blocksizes into the slots of their complex
	// counterparts.
	blksz_t* blksz_mr = bli_cntx_get_blksz( BLIS_MR, *cntx );
	blksz_t* blksz_nr = bli_cntx_get_blksz( BLIS_NR, *cntx );
	blksz_t* blksz_mc = bli_cntx_get_blksz( BLIS_MC, *cntx );
	blksz_t* blksz_nc = bli_cntx_get_blksz( BLIS_NC, *cntx );
	blksz_t* blksz_kc = bli_cntx_get_blksz( BLIS_KC, *cntx );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mr, BLIS_SCOMPLEX, blksz_mr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mr, BLIS_DCOMPLEX, blksz_mr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nr, BLIS_SCOMPLEX, blksz_nr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nr, BLIS_DCOMPLEX, blksz_nr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mc, BLIS_SCOMPLEX, blksz_mc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mc, BLIS_DCOMPLEX, blksz_mc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nc, BLIS_SCOMPLEX, blksz_nc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nc, BLIS_DCOMPLEX, blksz_nc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_kc, BLIS_SCOMPLEX, blksz_kc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_kc, BLIS_DCOMPLEX, blksz_kc );

	// Halve both the real and complex NR's (which are both real NR's).
	bli_blksz_scale_def_max( 1, 2, BLIS_FLOAT,    blksz_nr );
	bli_blksz_scale_def_max( 1, 2, BLIS_DOUBLE,   blksz_nr );
	bli_blksz_scale_def_max( 1, 2, BLIS_SCOMPLEX, blksz_nr );
	bli_blksz_scale_def_max( 1, 2, BLIS_DCOMPLEX, blksz_nr );

	// Halve both the real and complex NC's (which are both real NC's).
	bli_blksz_scale_def_max( 1, 2, BLIS_FLOAT,    blksz_nc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DOUBLE,   blksz_nc );
	bli_blksz_scale_def_max( 1, 2, BLIS_SCOMPLEX, blksz_nc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DCOMPLEX, blksz_nc );

	// Use the default pack schemas in the context.

	// static func_t* bli_cntx_get_l3_vir_ukrs( l3ukr_t ukr_id, cntx_t* cntx )
	func_t* l3_vir_ukrs = bli_cntx_get_l3_vir_ukrs( BLIS_GEMM_UKR, *cntx );

	// Rather than check which complex datatype dt_comp refers to, we set
	// the mixed-domain virtual microkernel for both types.
	bli_func_set_dt( bli_cgemm_md_c2r_ref, BLIS_SCOMPLEX, l3_vir_ukrs );
	bli_func_set_dt( bli_zgemm_md_c2r_ref, BLIS_DCOMPLEX, l3_vir_ukrs );

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_rcc
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	// We assume that the requested computation domain is complex.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_COMPLEX;

	// For rcc, the computation (ukernel) will be real, and since the output
	// matrix C is also real, so must be the execution domain.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_REAL;

	// Create a local copy of the context and then prepare to use this
	// context instead of the one passed in.
	*cntx_local = **cntx;
	*cntx = cntx_local;

	// Copy the real domain blocksizes into the slots of their complex
	// counterparts.
	blksz_t* blksz_mr = bli_cntx_get_blksz( BLIS_MR, *cntx );
	blksz_t* blksz_nr = bli_cntx_get_blksz( BLIS_NR, *cntx );
	blksz_t* blksz_mc = bli_cntx_get_blksz( BLIS_MC, *cntx );
	blksz_t* blksz_nc = bli_cntx_get_blksz( BLIS_NC, *cntx );
	blksz_t* blksz_kc = bli_cntx_get_blksz( BLIS_KC, *cntx );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mr, BLIS_SCOMPLEX, blksz_mr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mr, BLIS_DCOMPLEX, blksz_mr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nr, BLIS_SCOMPLEX, blksz_nr );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nr, BLIS_DCOMPLEX, blksz_nr );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_mc, BLIS_SCOMPLEX, blksz_mc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_mc, BLIS_DCOMPLEX, blksz_mc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_nc, BLIS_SCOMPLEX, blksz_nc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_nc, BLIS_DCOMPLEX, blksz_nc );

	bli_blksz_copy_dt( BLIS_FLOAT,  blksz_kc, BLIS_SCOMPLEX, blksz_kc );
	bli_blksz_copy_dt( BLIS_DOUBLE, blksz_kc, BLIS_DCOMPLEX, blksz_kc );

	// Halve both the real and complex KC's (which are both real KC's).
	bli_blksz_scale_def_max( 1, 2, BLIS_FLOAT,    blksz_kc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DOUBLE,   blksz_kc );
	bli_blksz_scale_def_max( 1, 2, BLIS_SCOMPLEX, blksz_kc );
	bli_blksz_scale_def_max( 1, 2, BLIS_DCOMPLEX, blksz_kc );

	// Use the 1r pack schema for both A and B with the conjugation
	// of A or B toggled (to produce ar * br - ai * bi).
	bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_1R, *cntx );
	bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_1R, *cntx );

	bli_obj_toggle_conj( b );

	// We also need to copy over the packm kernels from the 1m
	// context. We query the address of that context here.
	// NOTE: This is needed for situations where the rcc case does not
	// involve any casting to different precisions, since currently
	// bli_packm_blk_var1() is coded to hand off control to
	// bli_packm_blk_var1_md() only when the storage datatype differs from
	// the target datatype. (The packm_blk_var1_md() function has "built-in"
	// support for packing to 1r (and 1e) schemas, whereas the
	// packm_blk_var1() function relies on packm kernels for packing to 1r.
	const num_t dt_complex = bli_obj_dt( a );
	cntx_t* cntx_1m = bli_gks_query_ind_cntx( BLIS_1M, dt_complex );

	func_t* cntx_funcs    = bli_cntx_packm_kers_buf( *cntx );
	func_t* cntx_1m_funcs = bli_cntx_packm_kers_buf( cntx_1m );

	for ( dim_t i = 0; i <= BLIS_PACKM_31XK_KER; ++i )
	{
		cntx_funcs[ i ] = cntx_1m_funcs[ i ];
	}

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_crr
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;
#ifndef BLIS_ENABLE_GEMM_MD_EXTRA_MEM
	obj_t  c_real;
#endif

	// We assume that the requested computation domain is real.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_REAL;

	// For crr, the computation (ukernel) will be real, and since we will
	// be updating only the real part of the output matrix C, the exectuion
	// domain is also real.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_REAL;

	// Since the A*B product is real, we can update only the real part of
	// C. Thus, we convert the obj_t for the complex matrix to one that
	// represents only the real part. HOWEVER, there are two situations in
	// which we forgo this trick:
	// - If extra memory optimizations are enabled, we should leave C alone
	//   since we'll be computing A*B to a temporary matrix and accumulating
	//   that result back to C, and in order for that to work, we need to
	//   allow that code to continue accessing C as a complex matrix.
	// - Even if extra memory optimizations are diabled, logically projecting
	//   C as a real matrix can still cause problems if beta is non-unit. In
	//   that situation, the implementation won't get a chance to scale the
	//   imaginary components of C by beta, and thus it would compute the
	//   wrong answer. Thus, if beta is non-unit, we must leave C alone.
#ifndef BLIS_ENABLE_GEMM_MD_EXTRA_MEM
	if ( bli_obj_equals( beta, &BLIS_ONE ) )
	{
		bli_obj_real_part( c, &c_real );

		// Overwrite the complex obj_t with its real-only alias.
		*c = c_real;
	}
#endif

	// Use the default pack schemas in the context.

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_rcr
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;
	obj_t  a_real;

	// We assume that the requested computation domain is real.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_REAL;

	// For rcr, the computation (ukernel) will be real, and since the output
	// matrix C is also real, so must be the execution domain.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_REAL;

	// Convert the obj_t for the complex matrix to one that represents only
	// the real part.
	bli_obj_real_part( a, &a_real );

	// Overwrite the complex obj_t with its real-only alias.
	*a = a_real;

	// Use the default pack schemas in the context.

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_rrc
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;
	obj_t  b_real;

	// We assume that the requested computation domain is real.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_REAL;

	// For rcr, the computation (ukernel) will be real, and since the output
	// matrix C is also real, so must be the execution domain.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_REAL;

	// Convert the obj_t for the complex matrix to one that represents only
	// the real part.
	bli_obj_real_part( b, &b_real );

	// Overwrite the complex obj_t with its real-only alias.
	*b = b_real;

	// Use the default pack schemas in the context.

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_rrr
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	// We assume that the requested computation domain is real.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_REAL;

	// For rrr, the computation (ukernel) and execution domains are both
	// real.
	doms.comp = BLIS_REAL;
	doms.exec = BLIS_REAL;

	// Use the default pack schemas in the context.

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

//                 cab
mddm_t bli_gemm_md_ccc
     (
       obj_t*   a,
       obj_t*   b,
       obj_t*   beta,
       obj_t*   c,
       cntx_t*  cntx_local,
       cntx_t** cntx
     )
{
	mddm_t doms;

	// We assume that the requested computation domain is complex.
	//dom_t dom_comp_in = bli_obj_comp_domain( c );
	//dom_t dom_comp_in = BLIS_COMPLEX;

	// For ccc, the computation (ukernel) and execution domains are both
	// complex.
	doms.comp = BLIS_COMPLEX;
	doms.exec = BLIS_COMPLEX;

	// Use the default pack schemas in the context.

	// Return the computation and execution domains.
	return doms;
}

// -----------------------------------------------------------------------------

#if 0
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
     )
{
	bli_init_once();

	obj_t   a_local;
	obj_t   b_local;
	obj_t   c_local;

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
	    bli_gemm_check( alpha, a, b, beta, c, cntx );

	// If alpha is zero, scale by beta and return.
	if ( bli_obj_equals( alpha, &BLIS_ZERO ) )
	{
		bli_scalm( beta, c );
		return;
	}

	// Alias A, B, and C in case we need to apply transformations.
	bli_obj_alias_to( a, &a_local );
	bli_obj_alias_to( b, &b_local );
	bli_obj_alias_to( c, &c_local );

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
	}

	cntx_t cntx_local;

	// Handle mixed domain cases in bli_gemm_md(), which may modify
	// the objects or the context. (If the context is modified, cntx
	// is adjusted to point to cntx_local.)
	bli_gemm_md( &a_local, &b_local, beta, &c_local, &cntx_local, &cntx );

	// Record the threading for each level within the context.
	bli_rntm_set_ways_for_op
	(
	  BLIS_GEMM,
	  BLIS_LEFT, // ignored for gemm/hemm/symm
	  bli_obj_length( &c_local ),
	  bli_obj_width( &c_local ),
	  bli_obj_width( &a_local ),
	  rntm
	);

	// Invoke the internal back-end via the thread handler.
	bli_l3_thread_decorator
	(
	  bli_gemm_int,
	  BLIS_GEMM, // operation family id
	  alpha,
	  &a_local,
	  &b_local,
	  beta,
	  &c_local,
	  cntx,
	  rntm,
	  cntl
	);
}

// -----------------------------------------------------------------------------

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
     )
{
	bli_init_once();

	obj_t   a_local;
	obj_t   b_local;
	obj_t   c_local;

#if 1
	obj_t   am, bm, cm;
	obj_t*  c_orig;

	//if ( is_md == TRUE )
	{
		//num_t dt_c2 = bli_obj_dt( c );
		//num_t dt_c1 = bli_dt_proj_to_complex( dt_c2 );
		//num_t dt_c  = bli_dt_proj_to_double_prec( dt_c1 );
		//num_t dt_c = bli_obj_dt_proj_to_complex( c );
		num_t dt_c = BLIS_DCOMPLEX;

		if ( bli_obj_is_single_prec( c ) ) dt_c = BLIS_SCOMPLEX;
		else                               dt_c = BLIS_DCOMPLEX;

		if ( bli_obj_is_real( a ) &&
		     bli_obj_is_real( b ) &&
		     bli_obj_is_real( c ) ) dt_c = bli_dt_proj_to_real( dt_c );

		dim_t m = bli_obj_length( c );
		dim_t n = bli_obj_width( c );
		dim_t k = bli_obj_width_after_trans( a );

		bli_obj_create( dt_c, m, k, 0, 0, &am );
		bli_obj_create( dt_c, k, n, 0, 0, &bm );
		bli_obj_create( dt_c, m, n, 0, 0, &cm );

		//bli_projm( a, &am );
		//bli_projm( b, &bm );
		//bli_projm( c, &cm );
		bli_castm( a, &am );
		bli_castm( b, &bm );
		bli_castm( c, &cm );

		c_orig = c;

		a = &am;
		b = &bm;
		c = &cm;
	}
#endif

	// Check parameters.
	if ( bli_error_checking_is_enabled() )
		bli_gemm_check( alpha, a, b, beta, c, cntx );

	// If alpha is zero, scale by beta and return.
	if ( bli_obj_equals( alpha, &BLIS_ZERO ) )
	{
		bli_scalm( beta, c );
		return;
	}

	// Alias A, B, and C in case we need to apply transformations.
	bli_obj_alias_to( a, &a_local );
	bli_obj_alias_to( b, &b_local );
	bli_obj_alias_to( c, &c_local );

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
	}

	{
		// A sort of hack for communicating the desired pach schemas for A and B
		// to bli_gemm_cntl_create() (via bli_l3_thread_decorator() and
		// bli_l3_cntl_create_if()). This allows us to access the schemas from
		// the control tree, which hopefully reduces some confusion, particularly
		// in bli_packm_init().
		if ( bli_cntx_method( cntx ) == BLIS_NAT )
		{
			bli_obj_set_pack_schema( BLIS_PACKED_ROW_PANELS, &a_local );
			bli_obj_set_pack_schema( BLIS_PACKED_COL_PANELS, &b_local );
		}
		else // if ( bli_cntx_method( cntx ) != BLIS_NAT )
		{
			pack_t schema_a = bli_cntx_schema_a_block( cntx );
			pack_t schema_b = bli_cntx_schema_b_panel( cntx );

			bli_obj_set_pack_schema( schema_a, &a_local );
			bli_obj_set_pack_schema( schema_b, &b_local );
		}
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

	// Invoke the internal back-end via the thread handler.
	bli_l3_thread_decorator
	(
	  bli_gemm_int,
	  BLIS_GEMM, // operation family id
	  alpha,
	  &a_local,
	  &b_local,
	  beta,
	  &c_local,
	  cntx,
	  rntm,
	  cntl
	);

#if 1
	//if ( is_md == TRUE )
	{
		//bli_projm( &cm, c_orig );
		bli_castm( &cm, c_orig );

		bli_obj_free( &am );
		bli_obj_free( &bm );
		bli_obj_free( &cm );
	}
#endif
}
#endif

#endif
