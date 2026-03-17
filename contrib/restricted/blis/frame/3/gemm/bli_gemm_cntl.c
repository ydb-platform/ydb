/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018 - 2019, Advanced Micro Devices, Inc.

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

cntl_t* bli_gemm_cntl_create
     (
       rntm_t* rntm,
       opid_t  family,
       pack_t  schema_a,
       pack_t  schema_b
     )
{
	return bli_gemmbp_cntl_create( rntm, family, schema_a, schema_b );
}

// -----------------------------------------------------------------------------

cntl_t* bli_gemmbp_cntl_create
     (
       rntm_t* rntm,
       opid_t  family,
       pack_t  schema_a,
       pack_t  schema_b
     )
{
	void_fp macro_kernel_fp;
	void_fp packa_fp;
	void_fp packb_fp;

	// Use the function pointers to the macrokernels that use slab
	// assignment of micropanels to threads in the jr and ir loops.
	if      ( family == BLIS_GEMM ) macro_kernel_fp = bli_gemm_ker_var2;
	else if ( family == BLIS_HERK ) macro_kernel_fp = bli_herk_x_ker_var2;
	else if ( family == BLIS_TRMM ) macro_kernel_fp = bli_trmm_xx_ker_var2;
	else /* should never execute */ macro_kernel_fp = NULL;

	packa_fp = bli_packm_blk_var1;
	packb_fp = bli_packm_blk_var1;

	// Create two nodes for the macro-kernel.
	cntl_t* gemm_cntl_bu_ke = bli_gemm_cntl_create_node
	(
	  rntm,    // the thread's runtime structure
	  family,  // the operation family
	  BLIS_MR, // needed for bli_thrinfo_rgrow()
	  NULL,    // variant function pointer not used
	  NULL     // no sub-node; this is the leaf of the tree.
	);

	cntl_t* gemm_cntl_bp_bu = bli_gemm_cntl_create_node
	(
	  rntm,    // the thread's runtime structure
	  family,
	  BLIS_NR, // not used by macro-kernel, but needed for bli_thrinfo_rgrow()
	  macro_kernel_fp,
	  gemm_cntl_bu_ke
	);

	// Create a node for packing matrix A.
	cntl_t* gemm_cntl_packa = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_gemm_packa,  // pack the left-hand operand
	  packa_fp,
	  BLIS_MR,
	  BLIS_KR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_a, // normally BLIS_PACKED_ROW_PANELS
	  BLIS_BUFFER_FOR_A_BLOCK,
	  gemm_cntl_bp_bu
	);

	// Create a node for partitioning the m dimension by MC.
	cntl_t* gemm_cntl_op_bp = bli_gemm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_MC,
	  bli_gemm_blk_var1,
	  gemm_cntl_packa
	);

	// Create a node for packing matrix B.
	cntl_t* gemm_cntl_packb = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_gemm_packb,  // pack the right-hand operand
	  packb_fp,
	  BLIS_KR,
	  BLIS_NR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_b, // normally BLIS_PACKED_COL_PANELS
	  BLIS_BUFFER_FOR_B_PANEL,
	  gemm_cntl_op_bp
	);

	// Create a node for partitioning the k dimension by KC.
	cntl_t* gemm_cntl_mm_op = bli_gemm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_KC,
	  bli_gemm_blk_var3,
	  gemm_cntl_packb
	);

	// Create a node for partitioning the n dimension by NC.
	cntl_t* gemm_cntl_vl_mm = bli_gemm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NC,
	  bli_gemm_blk_var2,
	  gemm_cntl_mm_op
	);

	return gemm_cntl_vl_mm;
}

// -----------------------------------------------------------------------------

// This control tree creation function is disabled because it is no longer used.
// (It was originally created in the run up to publishing the 1m journal article,
// but was disabled to reduce complexity.)
#if 0
cntl_t* bli_gemmpb_cntl_create
     (
       opid_t family
     )
{
	void_fp macro_kernel_p = bli_gemm_ker_var1;

	// Change the macro-kernel if the operation family is herk or trmm.
	//if      ( family == BLIS_HERK ) macro_kernel_p = bli_herk_x_ker_var2;
	//else if ( family == BLIS_TRMM ) macro_kernel_p = bli_trmm_xx_ker_var2;

	// Create two nodes for the macro-kernel.
	cntl_t* gemm_cntl_ub_ke = bli_gemm_cntl_create_node
	(
	  family,  // the operation family
	  BLIS_MR, // needed for bli_thrinfo_rgrow()
	  NULL,    // variant function pointer not used
	  NULL     // no sub-node; this is the leaf of the tree.
	);

	cntl_t* gemm_cntl_pb_ub = bli_gemm_cntl_create_node
	(
	  family,
	  BLIS_NR, // not used by macro-kernel, but needed for bli_thrinfo_rgrow()
	  macro_kernel_p,
	  gemm_cntl_ub_ke
	);

	// Create a node for packing matrix A (which is really the right-hand
	// operand "B").
	cntl_t* gemm_cntl_packb = bli_packm_cntl_create_node
	(
	  bli_gemm_packb,  // pack the right-hand operand
	  bli_packm_blk_var1,
	  BLIS_KR,
	  BLIS_MR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  BLIS_PACKED_COL_PANELS,
	  BLIS_BUFFER_FOR_A_BLOCK,
	  gemm_cntl_pb_ub
	);

	// Create a node for partitioning the n dimension by MC.
	cntl_t* gemm_cntl_op_pb = bli_gemm_cntl_create_node
	(
	  family,
	  BLIS_MC,
	  bli_gemm_blk_var2,
	  gemm_cntl_packb
	);

	// Create a node for packing matrix B (which is really the left-hand
	// operand "A").
	cntl_t* gemm_cntl_packa = bli_packm_cntl_create_node
	(
	  bli_gemm_packa,  // pack the left-hand operand
	  bli_packm_blk_var1,
	  BLIS_NR,
	  BLIS_KR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  BLIS_PACKED_ROW_PANELS,
	  BLIS_BUFFER_FOR_B_PANEL,
	  gemm_cntl_op_pb
	);

	// Create a node for partitioning the k dimension by KC.
	cntl_t* gemm_cntl_mm_op = bli_gemm_cntl_create_node
	(
	  family,
	  BLIS_KC,
	  bli_gemm_blk_var3,
	  gemm_cntl_packa
	);

	// Create a node for partitioning the m dimension by NC.
	cntl_t* gemm_cntl_vl_mm = bli_gemm_cntl_create_node
	(
	  family,
	  BLIS_NC,
	  bli_gemm_blk_var1,
	  gemm_cntl_mm_op
	);

	return gemm_cntl_vl_mm;
}
#endif

// -----------------------------------------------------------------------------

void bli_gemm_cntl_free
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	bli_cntl_free( rntm, cntl, thread );
}

// -----------------------------------------------------------------------------

cntl_t* bli_gemm_cntl_create_node
     (
       rntm_t* rntm,
       opid_t  family,
       bszid_t bszid,
       void_fp var_func,
       cntl_t* sub_node
     )
{
	return bli_cntl_create_node( rntm, family, bszid, var_func, NULL, sub_node );
}

