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

cntl_t* bli_trsm_cntl_create
     (
       rntm_t* rntm,
       side_t  side,
       pack_t  schema_a,
       pack_t  schema_b
     )
{
	if ( bli_is_left( side ) )
		return bli_trsm_l_cntl_create( rntm, schema_a, schema_b );
	else
		return bli_trsm_r_cntl_create( rntm, schema_a, schema_b );
}

cntl_t* bli_trsm_l_cntl_create
     (
       rntm_t* rntm,
       pack_t  schema_a,
       pack_t  schema_b
     )
{
	void_fp macro_kernel_p;
	void_fp packa_fp;
	void_fp packb_fp;

	// Use the function pointer to the macrokernels that use slab
	// assignment of micropanels to threads in the jr and ir loops.
	macro_kernel_p = bli_trsm_xx_ker_var2;

	packa_fp = bli_packm_blk_var1;
	packb_fp = bli_packm_blk_var1;

	const opid_t family = BLIS_TRSM;

	//
	// Create nodes for packing A and the macro-kernel (gemm branch).
	//

	cntl_t* gemm_cntl_bu_ke = bli_trsm_cntl_create_node
	(
	  rntm,    // the thread's runtime structure
	  family,  // the operation family
	  BLIS_MR, // needed for bli_thrinfo_rgrow()
	  NULL,    // variant function pointer not used
	  NULL     // no sub-node; this is the leaf of the tree.
	);

	cntl_t* gemm_cntl_bp_bu = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NR, // not used by macro-kernel, but needed for bli_thrinfo_rgrow()
	  macro_kernel_p,
	  gemm_cntl_bu_ke
	);

	// Create a node for packing matrix A.
	cntl_t* gemm_cntl_packa = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_trsm_packa, // trsm operation's packm function for A.
	  packa_fp,
	  BLIS_MR,
	  BLIS_MR,
	  FALSE,   // do NOT invert diagonal
	  TRUE,    // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_a, // normally BLIS_PACKED_ROW_PANELS
	  BLIS_BUFFER_FOR_A_BLOCK,
	  gemm_cntl_bp_bu
	);

	//
	// Create nodes for packing A and the macro-kernel (trsm branch).
	//

	cntl_t* trsm_cntl_bu_ke = bli_trsm_cntl_create_node
	(
	  rntm,    // the thread's runtime structure
	  family,  // the operation family
	  BLIS_MR, // needed for bli_thrinfo_rgrow()
	  NULL,    // variant function pointer not used
	  NULL     // no sub-node; this is the leaf of the tree.
	);

	cntl_t* trsm_cntl_bp_bu = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NR, // not used by macro-kernel, but needed for bli_thrinfo_rgrow()
	  macro_kernel_p,
	  trsm_cntl_bu_ke
	);

	// Create a node for packing matrix A.
	cntl_t* trsm_cntl_packa = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_trsm_packa, // trsm operation's packm function for A.
	  packa_fp,
	  BLIS_MR,
	  BLIS_MR,
#ifdef BLIS_ENABLE_TRSM_PREINVERSION
	  TRUE,    // invert diagonal
#else
	  FALSE,   // do NOT invert diagonal
#endif
	  TRUE,    // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_a, // normally BLIS_PACKED_ROW_PANELS
	  BLIS_BUFFER_FOR_A_BLOCK,
	  trsm_cntl_bp_bu
	);

	// -------------------------------------------------------------------------

	// Create a node for partitioning the m dimension by MC.
	// NOTE: We attach the gemm sub-tree as the main branch.
	cntl_t* trsm_cntl_op_bp = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_MC,
	  bli_trsm_blk_var1,
	  gemm_cntl_packa
	);

	// Attach the trsm sub-tree as the auxiliary "prenode" branch.
	bli_cntl_set_sub_prenode( trsm_cntl_packa, trsm_cntl_op_bp );

	// -------------------------------------------------------------------------

	// Create a node for packing matrix B.
	cntl_t* trsm_cntl_packb = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_trsm_packb,
	  packb_fp,
	  BLIS_MR,
	  BLIS_NR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_b, // normally BLIS_PACKED_COL_PANELS
	  BLIS_BUFFER_FOR_B_PANEL,
	  trsm_cntl_op_bp
	);

	// Create a node for partitioning the k dimension by KC.
	cntl_t* trsm_cntl_mm_op = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_KC,
	  bli_trsm_blk_var3,
	  trsm_cntl_packb
	);

	// Create a node for partitioning the n dimension by NC.
	cntl_t* trsm_cntl_vl_mm = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NC,
	  bli_trsm_blk_var2,
	  trsm_cntl_mm_op
	);

	return trsm_cntl_vl_mm;
}

cntl_t* bli_trsm_r_cntl_create
     (
	   rntm_t* rntm,
       pack_t  schema_a,
       pack_t  schema_b
     )
{
	// NOTE: trsm macrokernels are presently disabled for right-side execution.
	void_fp macro_kernel_p = bli_trsm_xx_ker_var2;

	void_fp packa_fp = bli_packm_blk_var1;
	void_fp packb_fp = bli_packm_blk_var1;

	const opid_t family = BLIS_TRSM;

	// Create two nodes for the macro-kernel.
	cntl_t* trsm_cntl_bu_ke = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_MR, // needed for bli_thrinfo_rgrow()
	  NULL,    // variant function pointer not used
	  NULL     // no sub-node; this is the leaf of the tree.
	);

	cntl_t* trsm_cntl_bp_bu = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NR, // not used by macro-kernel, but needed for bli_thrinfo_rgrow()
	  macro_kernel_p,
	  trsm_cntl_bu_ke
	);

	// Create a node for packing matrix A.
	cntl_t* trsm_cntl_packa = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_trsm_packa,
	  packa_fp,
	  BLIS_NR,
	  BLIS_MR,
	  FALSE,   // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  FALSE,   // reverse iteration if lower?
	  schema_a, // normally BLIS_PACKED_ROW_PANELS
	  BLIS_BUFFER_FOR_A_BLOCK,
	  trsm_cntl_bp_bu
	);

	// Create a node for partitioning the m dimension by MC.
	cntl_t* trsm_cntl_op_bp = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_MC,
	  bli_trsm_blk_var1,
	  trsm_cntl_packa
	);

	// Create a node for packing matrix B.
	cntl_t* trsm_cntl_packb = bli_packm_cntl_create_node
	(
	  rntm,
	  bli_trsm_packb,
	  packb_fp,
	  BLIS_MR,
	  BLIS_MR,
	  TRUE,    // do NOT invert diagonal
	  FALSE,   // reverse iteration if upper?
	  TRUE,    // reverse iteration if lower?
	  schema_b, // normally BLIS_PACKED_COL_PANELS
	  BLIS_BUFFER_FOR_B_PANEL,
	  trsm_cntl_op_bp
	);

	// Create a node for partitioning the k dimension by KC.
	cntl_t* trsm_cntl_mm_op = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_KC,
	  bli_trsm_blk_var3,
	  trsm_cntl_packb
	);

	// Create a node for partitioning the n dimension by NC.
	cntl_t* trsm_cntl_vl_mm = bli_trsm_cntl_create_node
	(
	  rntm,
	  family,
	  BLIS_NC,
	  bli_trsm_blk_var2,
	  trsm_cntl_mm_op
	);

	return trsm_cntl_vl_mm;
}

void bli_trsm_cntl_free
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     )
{
	bli_cntl_free( rntm, cntl, thread );
}

// -----------------------------------------------------------------------------

cntl_t* bli_trsm_cntl_create_node
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

