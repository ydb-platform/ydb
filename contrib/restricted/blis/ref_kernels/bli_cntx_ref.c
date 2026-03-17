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

// -- Instantiate kernel prototypes for the current architecture ---------------

// Define macros to construct the full symbol name from the operation name.
#undef  GENARNAME             // architecture, _ref (no bli_)
#define GENARNAME(opname)     PASTECH2(opname,BLIS_CNAME_INFIX,BLIS_REF_SUFFIX)
#undef  GENBARNAME            // bli_, architecture, _ref
#define GENBARNAME(opname)    PASTEMAC2(opname,BLIS_CNAME_INFIX,BLIS_REF_SUFFIX)
#undef  GENBAINAME            // bli_, architecture, _ind
#define GENBAINAME(opname)    PASTEMAC2(opname,BLIS_CNAME_INFIX,BLIS_IND_SUFFIX)

// -- Level-3 native micro-kernel prototype redefinitions ----------------------

// -- prototypes for completely generic level-3 microkernels --

#undef  gemm_ukr_name
#define gemm_ukr_name       GENARNAME(gemm)
#undef  gemmtrsm_l_ukr_name
#define gemmtrsm_l_ukr_name GENARNAME(gemmtrsm_l)
#undef  gemmtrsm_u_ukr_name
#define gemmtrsm_u_ukr_name GENARNAME(gemmtrsm_u)
#undef  trsm_l_ukr_name
#define trsm_l_ukr_name     GENARNAME(trsm_l)
#undef  trsm_u_ukr_name
#define trsm_u_ukr_name     GENARNAME(trsm_u)

// Instantiate prototypes for above functions via the native micro-kernel API
// template.
#include "bli_l3_ukr.h"

// -- Level-3 virtual micro-kernel prototype redefinitions ---------------------

// -- 3mh --

#undef  gemm3mh_ukr_name
#define gemm3mh_ukr_name       GENARNAME(gemm3mh)

// -- 3m1 --

#undef  gemm3m1_ukr_name
#define gemm3m1_ukr_name       GENARNAME(gemm3m1)
#undef  gemmtrsm3m1_l_ukr_name
#define gemmtrsm3m1_l_ukr_name GENARNAME(gemmtrsm3m1_l)
#undef  gemmtrsm3m1_u_ukr_name
#define gemmtrsm3m1_u_ukr_name GENARNAME(gemmtrsm3m1_u)
#undef  trsm3m1_l_ukr_name
#define trsm3m1_l_ukr_name     GENARNAME(trsm3m1_l)
#undef  trsm3m1_u_ukr_name
#define trsm3m1_u_ukr_name     GENARNAME(trsm3m1_u)

// -- 4mh --

#undef  gemm4mh_ukr_name
#define gemm4mh_ukr_name       GENARNAME(gemm4mh)

// -- 4mb --

#undef  gemm4mb_ukr_name
#define gemm4mb_ukr_name       GENARNAME(gemm4mb)

// -- 4m1 --

#undef  gemm4m1_ukr_name
#define gemm4m1_ukr_name       GENARNAME(gemm4m1)
#undef  gemmtrsm4m1_l_ukr_name
#define gemmtrsm4m1_l_ukr_name GENARNAME(gemmtrsm4m1_l)
#undef  gemmtrsm4m1_u_ukr_name
#define gemmtrsm4m1_u_ukr_name GENARNAME(gemmtrsm4m1_u)
#undef  trsm4m1_l_ukr_name
#define trsm4m1_l_ukr_name     GENARNAME(trsm4m1_l)
#undef  trsm4m1_u_ukr_name
#define trsm4m1_u_ukr_name     GENARNAME(trsm4m1_u)

// -- 1m --

#undef  gemm1m_ukr_name
#define gemm1m_ukr_name        GENARNAME(gemm1m)
#undef  gemmtrsm1m_l_ukr_name
#define gemmtrsm1m_l_ukr_name  GENARNAME(gemmtrsm1m_l)
#undef  gemmtrsm1m_u_ukr_name
#define gemmtrsm1m_u_ukr_name  GENARNAME(gemmtrsm1m_u)
#undef  trsm1m_l_ukr_name
#define trsm1m_l_ukr_name      GENARNAME(trsm1m_l)
#undef  trsm1m_u_ukr_name
#define trsm1m_u_ukr_name      GENARNAME(trsm1m_u)

// Instantiate prototypes for above functions via the virtual micro-kernel API
// template.
#include "bli_l3_ind_ukr.h"

// -- Level-3 small/unpacked micro-kernel prototype definitions ----------------

// NOTE: This results in redundant prototypes for gemmsup_r and gemmsup_c
// kernels, but since they will be identical the compiler won't complain.

#undef  gemmsup_rv_ukr_name
#define gemmsup_rv_ukr_name   GENARNAME(gemmsup_r)
#undef  gemmsup_rg_ukr_name
#define gemmsup_rg_ukr_name   GENARNAME(gemmsup_r)
#undef  gemmsup_cv_ukr_name
#define gemmsup_cv_ukr_name   GENARNAME(gemmsup_c)
#undef  gemmsup_cg_ukr_name
#define gemmsup_cg_ukr_name   GENARNAME(gemmsup_c)

#undef  gemmsup_gx_ukr_name
#define gemmsup_gx_ukr_name   GENARNAME(gemmsup_g)

// Include the small/unpacked kernel API template.
#include "bli_l3_sup_ker.h"

// -- Level-1m (packm/unpackm) kernel prototype redefinitions ------------------

#undef  packm_2xk_ker_name
#define packm_2xk_ker_name  GENARNAME(packm_2xk)
#undef  packm_3xk_ker_name
#define packm_3xk_ker_name  GENARNAME(packm_3xk)
#undef  packm_4xk_ker_name
#define packm_4xk_ker_name  GENARNAME(packm_4xk)
#undef  packm_6xk_ker_name
#define packm_6xk_ker_name  GENARNAME(packm_6xk)
#undef  packm_8xk_ker_name
#define packm_8xk_ker_name  GENARNAME(packm_8xk)
#undef  packm_10xk_ker_name
#define packm_10xk_ker_name GENARNAME(packm_10xk)
#undef  packm_12xk_ker_name
#define packm_12xk_ker_name GENARNAME(packm_12xk)
#undef  packm_14xk_ker_name
#define packm_14xk_ker_name GENARNAME(packm_14xk)
#undef  packm_16xk_ker_name
#define packm_16xk_ker_name GENARNAME(packm_16xk)
#undef  packm_24xk_ker_name
#define packm_24xk_ker_name GENARNAME(packm_24xk)

#undef  unpackm_2xk_ker_name
#define unpackm_2xk_ker_name  GENARNAME(unpackm_2xk)
#undef  unpackm_4xk_ker_name
#define unpackm_4xk_ker_name  GENARNAME(unpackm_4xk)
#undef  unpackm_6xk_ker_name
#define unpackm_6xk_ker_name  GENARNAME(unpackm_6xk)
#undef  unpackm_8xk_ker_name
#define unpackm_8xk_ker_name  GENARNAME(unpackm_8xk)
#undef  unpackm_10xk_ker_name
#define unpackm_10xk_ker_name GENARNAME(unpackm_10xk)
#undef  unpackm_12xk_ker_name
#define unpackm_12xk_ker_name GENARNAME(unpackm_12xk)
#undef  unpackm_14xk_ker_name
#define unpackm_14xk_ker_name GENARNAME(unpackm_14xk)
#undef  unpackm_16xk_ker_name
#define unpackm_16xk_ker_name GENARNAME(unpackm_16xk)

#undef  packm_2xk_3mis_ker_name
#define packm_2xk_3mis_ker_name  GENARNAME(packm_2xk_3mis)
#undef  packm_4xk_3mis_ker_name
#define packm_4xk_3mis_ker_name  GENARNAME(packm_4xk_3mis)
#undef  packm_6xk_3mis_ker_name
#define packm_6xk_3mis_ker_name  GENARNAME(packm_6xk_3mis)
#undef  packm_8xk_3mis_ker_name
#define packm_8xk_3mis_ker_name  GENARNAME(packm_8xk_3mis)
#undef  packm_10xk_3mis_ker_name
#define packm_10xk_3mis_ker_name GENARNAME(packm_10xk_3mis)
#undef  packm_12xk_3mis_ker_name
#define packm_12xk_3mis_ker_name GENARNAME(packm_12xk_3mis)
#undef  packm_14xk_3mis_ker_name
#define packm_14xk_3mis_ker_name GENARNAME(packm_14xk_3mis)
#undef  packm_16xk_3mis_ker_name
#define packm_16xk_3mis_ker_name GENARNAME(packm_16xk_3mis)

#undef  packm_2xk_4mi_ker_name
#define packm_2xk_4mi_ker_name  GENARNAME(packm_2xk_4mi)
#undef  packm_3xk_4mi_ker_name
#define packm_3xk_4mi_ker_name  GENARNAME(packm_3xk_4mi)
#undef  packm_4xk_4mi_ker_name
#define packm_4xk_4mi_ker_name  GENARNAME(packm_4xk_4mi)
#undef  packm_6xk_4mi_ker_name
#define packm_6xk_4mi_ker_name  GENARNAME(packm_6xk_4mi)
#undef  packm_8xk_4mi_ker_name
#define packm_8xk_4mi_ker_name  GENARNAME(packm_8xk_4mi)
#undef  packm_10xk_4mi_ker_name
#define packm_10xk_4mi_ker_name GENARNAME(packm_10xk_4mi)
#undef  packm_12xk_4mi_ker_name
#define packm_12xk_4mi_ker_name GENARNAME(packm_12xk_4mi)
#undef  packm_14xk_4mi_ker_name
#define packm_14xk_4mi_ker_name GENARNAME(packm_14xk_4mi)
#undef  packm_16xk_4mi_ker_name
#define packm_16xk_4mi_ker_name GENARNAME(packm_16xk_4mi)

#undef  packm_2xk_rih_ker_name
#define packm_2xk_rih_ker_name  GENARNAME(packm_2xk_rih)
#undef  packm_4xk_rih_ker_name
#define packm_4xk_rih_ker_name  GENARNAME(packm_4xk_rih)
#undef  packm_6xk_rih_ker_name
#define packm_6xk_rih_ker_name  GENARNAME(packm_6xk_rih)
#undef  packm_8xk_rih_ker_name
#define packm_8xk_rih_ker_name  GENARNAME(packm_8xk_rih)
#undef  packm_10xk_rih_ker_name
#define packm_10xk_rih_ker_name GENARNAME(packm_10xk_rih)
#undef  packm_12xk_rih_ker_name
#define packm_12xk_rih_ker_name GENARNAME(packm_12xk_rih)
#undef  packm_14xk_rih_ker_name
#define packm_14xk_rih_ker_name GENARNAME(packm_14xk_rih)
#undef  packm_16xk_rih_ker_name
#define packm_16xk_rih_ker_name GENARNAME(packm_16xk_rih)

#undef  packm_2xk_1er_ker_name
#define packm_2xk_1er_ker_name  GENARNAME(packm_2xk_1er)
#undef  packm_4xk_1er_ker_name
#define packm_4xk_1er_ker_name  GENARNAME(packm_4xk_1er)
#undef  packm_6xk_1er_ker_name
#define packm_6xk_1er_ker_name  GENARNAME(packm_6xk_1er)
#undef  packm_8xk_1er_ker_name
#define packm_8xk_1er_ker_name  GENARNAME(packm_8xk_1er)
#undef  packm_10xk_1er_ker_name
#define packm_10xk_1er_ker_name GENARNAME(packm_10xk_1er)
#undef  packm_12xk_1er_ker_name
#define packm_12xk_1er_ker_name GENARNAME(packm_12xk_1er)
#undef  packm_14xk_1er_ker_name
#define packm_14xk_1er_ker_name GENARNAME(packm_14xk_1er)
#undef  packm_16xk_1er_ker_name
#define packm_16xk_1er_ker_name GENARNAME(packm_16xk_1er)

// Instantiate prototypes for above functions via the level-1m kernel API
// template.
#include "bli_l1m_ker.h"

// -- Level-1f kernel prototype redefinitions ----------------------------------

#undef  axpy2v_ker_name
#define axpy2v_ker_name     GENARNAME(axpy2v)
#undef  dotaxpyv_ker_name
#define dotaxpyv_ker_name   GENARNAME(dotaxpyv)
#undef  axpyf_ker_name
#define axpyf_ker_name      GENARNAME(axpyf)
#undef  dotxf_ker_name
#define dotxf_ker_name      GENARNAME(dotxf)
#undef  dotxaxpyf_ker_name
#define dotxaxpyf_ker_name  GENARNAME(dotxaxpyf)

// Instantiate prototypes for above functions via the level-1f kernel API
// template.
#include "bli_l1f_ker.h"

// -- Level-1v kernel prototype redefinitions ----------------------------------

// -- prototypes for completely generic level-1v kernels --

#undef  addv_ker_name
#define addv_ker_name      GENARNAME(addv)
#undef  amaxv_ker_name
#define amaxv_ker_name     GENARNAME(amaxv)
#undef  axpbyv_ker_name
#define axpbyv_ker_name    GENARNAME(axpbyv)
#undef  axpyv_ker_name
#define axpyv_ker_name     GENARNAME(axpyv)
#undef  copyv_ker_name
#define copyv_ker_name     GENARNAME(copyv)
#undef  dotv_ker_name
#define dotv_ker_name      GENARNAME(dotv)
#undef  dotxv_ker_name
#define dotxv_ker_name     GENARNAME(dotxv)
#undef  invertv_ker_name
#define invertv_ker_name   GENARNAME(invertv)
#undef  scalv_ker_name
#define scalv_ker_name     GENARNAME(scalv)
#undef  scal2v_ker_name
#define scal2v_ker_name    GENARNAME(scal2v)
#undef  setv_ker_name
#define setv_ker_name      GENARNAME(setv)
#undef  subv_ker_name
#define subv_ker_name      GENARNAME(subv)
#undef  swapv_ker_name
#define swapv_ker_name     GENARNAME(swapv)
#undef  xpbyv_ker_name
#define xpbyv_ker_name     GENARNAME(xpbyv)

// Instantiate prototypes for above functions via the level-1v kernel API
// template.
#include "bli_l1v_ker.h"

// -- Macros to help concisely instantiate bli_func_init() ---------------------

#define gen_func_init_co( func_p, opname ) \
{ \
	bli_func_init( func_p, NULL,               NULL, \
	                       PASTEMAC(c,opname), PASTEMAC(z,opname) ); \
}

#define gen_func_init( func_p, opname ) \
{ \
	bli_func_init( func_p, PASTEMAC(s,opname), PASTEMAC(d,opname), \
	                       PASTEMAC(c,opname), PASTEMAC(z,opname) ); \
}

#define gen_sup_func_init( func0_p, func1_p, opname ) \
{ \
	bli_func_init( func0_p, PASTEMAC(s,opname), PASTEMAC(d,opname), \
	                        PASTEMAC(c,opname), PASTEMAC(z,opname) ); \
	bli_func_init( func1_p, PASTEMAC(s,opname), PASTEMAC(d,opname), \
	                        PASTEMAC(c,opname), PASTEMAC(z,opname) ); \
}



// -----------------------------------------------------------------------------

void GENBARNAME(cntx_init)
     (
       cntx_t* cntx
     )
{
	blksz_t  blkszs[ BLIS_NUM_BLKSZS ];
	blksz_t  thresh[ BLIS_NUM_THRESH ];
	func_t*  funcs;
	mbool_t* mbools;
	dim_t    i;
	void**   vfuncs;


	// -- Clear the context ----------------------------------------------------

	bli_cntx_clear( cntx );


	// -- Set blocksizes -------------------------------------------------------

	//                                          s     d     c     z
	bli_blksz_init_easy( &blkszs[ BLIS_KR ],    1,    1,    1,    1 );
	bli_blksz_init_easy( &blkszs[ BLIS_MR ],    4,    4,    4,    4 );
	bli_blksz_init_easy( &blkszs[ BLIS_NR ],   16,    8,    8,    4 );
	bli_blksz_init_easy( &blkszs[ BLIS_MC ],  256,  128,  128,   64 );
	bli_blksz_init_easy( &blkszs[ BLIS_KC ],  256,  256,  256,  256 );
	bli_blksz_init_easy( &blkszs[ BLIS_NC ], 4096, 4096, 4096, 4096 );
	bli_blksz_init_easy( &blkszs[ BLIS_M2 ], 1000, 1000, 1000, 1000 );
	bli_blksz_init_easy( &blkszs[ BLIS_N2 ], 1000, 1000, 1000, 1000 );
	bli_blksz_init_easy( &blkszs[ BLIS_AF ],    8,    8,    8,    8 );
	bli_blksz_init_easy( &blkszs[ BLIS_DF ],    6,    6,    6,    6 );
	bli_blksz_init_easy( &blkszs[ BLIS_XF ],    4,    4,    4,    4 );

	// Initialize the context with the default blocksize objects and their
	// multiples.
	bli_cntx_set_blkszs
	(
	  BLIS_NAT, 11,
	  BLIS_NC, &blkszs[ BLIS_NC ], BLIS_NR,
	  BLIS_KC, &blkszs[ BLIS_KC ], BLIS_KR,
	  BLIS_MC, &blkszs[ BLIS_MC ], BLIS_MR,
	  BLIS_NR, &blkszs[ BLIS_NR ], BLIS_NR,
	  BLIS_MR, &blkszs[ BLIS_MR ], BLIS_MR,
	  BLIS_KR, &blkszs[ BLIS_KR ], BLIS_KR,
	  BLIS_M2, &blkszs[ BLIS_M2 ], BLIS_M2,
	  BLIS_N2, &blkszs[ BLIS_N2 ], BLIS_N2,
	  BLIS_AF, &blkszs[ BLIS_AF ], BLIS_AF,
	  BLIS_DF, &blkszs[ BLIS_DF ], BLIS_DF,
	  BLIS_XF, &blkszs[ BLIS_XF ], BLIS_XF,
	  cntx
	);


	// -- Set level-3 virtual micro-kernels ------------------------------------

	funcs = bli_cntx_l3_vir_ukrs_buf( cntx );

	// NOTE: We set the virtual micro-kernel slots to contain the addresses
	// of the native micro-kernels. In general, the ukernels in the virtual
	// ukernel slots are always called, and if the function called happens to
	// be a virtual micro-kernel, it will then know to find its native
	// ukernel in the native ukernel slots.
	gen_func_init( &funcs[ BLIS_GEMM_UKR ],       gemm_ukr_name       );
	gen_func_init( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm_l_ukr_name );
	gen_func_init( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm_u_ukr_name );
	gen_func_init( &funcs[ BLIS_TRSM_L_UKR ],     trsm_l_ukr_name     );
	gen_func_init( &funcs[ BLIS_TRSM_U_UKR ],     trsm_u_ukr_name     );


	// -- Set level-3 native micro-kernels and preferences ---------------------

	funcs  = bli_cntx_l3_nat_ukrs_buf( cntx );
	mbools = bli_cntx_l3_nat_ukrs_prefs_buf( cntx );

	gen_func_init( &funcs[ BLIS_GEMM_UKR ],       gemm_ukr_name       );
	gen_func_init( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm_l_ukr_name );
	gen_func_init( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm_u_ukr_name );
	gen_func_init( &funcs[ BLIS_TRSM_L_UKR ],     trsm_l_ukr_name     );
	gen_func_init( &funcs[ BLIS_TRSM_U_UKR ],     trsm_u_ukr_name     );

	//                                                  s      d      c      z
	bli_mbool_init( &mbools[ BLIS_GEMM_UKR ],        TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_GEMMTRSM_L_UKR ], FALSE, FALSE, FALSE, FALSE );
	bli_mbool_init( &mbools[ BLIS_GEMMTRSM_U_UKR ], FALSE, FALSE, FALSE, FALSE );
	bli_mbool_init( &mbools[ BLIS_TRSM_L_UKR ],     FALSE, FALSE, FALSE, FALSE );
	bli_mbool_init( &mbools[ BLIS_TRSM_U_UKR ],     FALSE, FALSE, FALSE, FALSE );


	// -- Set level-3 small/unpacked thresholds --------------------------------

	// NOTE: The default thresholds are set to zero so that the sup framework
	// does not activate by default. Note that the semantic meaning of the
	// thresholds is that the sup code path is executed if a dimension is
	// strictly less than its corresponding threshold. So actually, the
	// thresholds specify the minimum dimension size that will still dispatch
	// the non-sup/large code path. This "strictly less than" behavior was
	// chosen over "less than or equal to" so that threshold values of 0 would
	// effectively disable sup (even for matrix dimensions of 0).
	//                                          s     d     c     z
	bli_blksz_init_easy( &thresh[ BLIS_MT ],    0,    0,    0,    0 );
	bli_blksz_init_easy( &thresh[ BLIS_NT ],    0,    0,    0,    0 );
	bli_blksz_init_easy( &thresh[ BLIS_KT ],    0,    0,    0,    0 );

	// Initialize the context with the default thresholds.
	bli_cntx_set_l3_sup_thresh
	(
	  3,
	  BLIS_MT, &thresh[ BLIS_MT ],
	  BLIS_NT, &thresh[ BLIS_NT ],
	  BLIS_KT, &thresh[ BLIS_KT ],
	  cntx
	);


	// -- Set level-3 small/unpacked handlers ----------------------------------

	vfuncs = bli_cntx_l3_sup_handlers_buf( cntx );

	// Initialize all of the function pointers to NULL;
	for ( i = 0; i < BLIS_NUM_LEVEL3_OPS; ++i ) vfuncs[ i ] = NULL;

	// The level-3 sup handlers are oapi-based, so we only set one slot per
	// operation.

	// Set the gemm slot to the default gemm sup handler.
	vfuncs[ BLIS_GEMM ]  = bli_gemmsup_ref;
	vfuncs[ BLIS_GEMMT ] = bli_gemmtsup_ref;


	// -- Set level-3 small/unpacked micro-kernels and preferences -------------

	funcs  = bli_cntx_l3_sup_kers_buf( cntx );
	mbools = bli_cntx_l3_sup_kers_prefs_buf( cntx );

#if 0
	// Adhere to the small/unpacked ukernel mappings:
	// - rv -> rrr, rcr
	// - rg -> rrc, rcc
	// - cv -> ccr, ccc
	// - cg -> crr, crc
	gen_sup_func_init( &funcs[ BLIS_RRR ],
	                   &funcs[ BLIS_RCR ], gemmsup_rv_ukr_name );
	gen_sup_func_init( &funcs[ BLIS_RRC ],
	                   &funcs[ BLIS_RCC ], gemmsup_rg_ukr_name );
	gen_sup_func_init( &funcs[ BLIS_CCR ],
	                   &funcs[ BLIS_CCC ], gemmsup_cv_ukr_name );
	gen_sup_func_init( &funcs[ BLIS_CRR ],
	                   &funcs[ BLIS_CRC ], gemmsup_cg_ukr_name );
#endif
	gen_func_init( &funcs[ BLIS_RRR ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_RRC ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_RCR ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_RCC ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_CRR ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_CRC ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_CCR ], gemmsup_rv_ukr_name );
	gen_func_init( &funcs[ BLIS_CCC ], gemmsup_rv_ukr_name );

	// Register the general-stride/generic ukernel to the "catch-all" slot
	// associated with the BLIS_XXX enum value. This slot will be queried if
	// *any* operand is stored with general stride.
	gen_func_init( &funcs[ BLIS_XXX ], gemmsup_gx_ukr_name );


	// Set the l3 sup ukernel storage preferences.
	//                                       s      d      c      z
	bli_mbool_init( &mbools[ BLIS_RRR ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_RRC ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_RCR ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_RCC ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_CRR ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_CRC ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_CCR ],  TRUE,  TRUE,  TRUE,  TRUE );
	bli_mbool_init( &mbools[ BLIS_CCC ],  TRUE,  TRUE,  TRUE,  TRUE );

	bli_mbool_init( &mbools[ BLIS_XXX ],  TRUE,  TRUE,  TRUE,  TRUE );


	// -- Set level-1f kernels -------------------------------------------------

	funcs = bli_cntx_l1f_kers_buf( cntx );

	gen_func_init( &funcs[ BLIS_AXPY2V_KER ],    axpy2v_ker_name    );
	gen_func_init( &funcs[ BLIS_DOTAXPYV_KER ],  dotaxpyv_ker_name  );
	gen_func_init( &funcs[ BLIS_AXPYF_KER ],     axpyf_ker_name     );
	gen_func_init( &funcs[ BLIS_DOTXF_KER ],     dotxf_ker_name     );
	gen_func_init( &funcs[ BLIS_DOTXAXPYF_KER ], dotxaxpyf_ker_name );


	// -- Set level-1v kernels -------------------------------------------------

	funcs = bli_cntx_l1v_kers_buf( cntx );

	gen_func_init( &funcs[ BLIS_ADDV_KER ],    addv_ker_name    );
	gen_func_init( &funcs[ BLIS_AMAXV_KER ],   amaxv_ker_name   );
	gen_func_init( &funcs[ BLIS_AXPBYV_KER ],  axpbyv_ker_name  );
	gen_func_init( &funcs[ BLIS_AXPYV_KER ],   axpyv_ker_name   );
	gen_func_init( &funcs[ BLIS_COPYV_KER ],   copyv_ker_name   );
	gen_func_init( &funcs[ BLIS_DOTV_KER ],    dotv_ker_name    );
	gen_func_init( &funcs[ BLIS_DOTXV_KER ],   dotxv_ker_name   );
	gen_func_init( &funcs[ BLIS_INVERTV_KER ], invertv_ker_name );
	gen_func_init( &funcs[ BLIS_SCALV_KER ],   scalv_ker_name   );
	gen_func_init( &funcs[ BLIS_SCAL2V_KER ],  scal2v_ker_name  );
	gen_func_init( &funcs[ BLIS_SETV_KER ],    setv_ker_name    );
	gen_func_init( &funcs[ BLIS_SUBV_KER ],    subv_ker_name    );
	gen_func_init( &funcs[ BLIS_SWAPV_KER ],   swapv_ker_name   );
	gen_func_init( &funcs[ BLIS_XPBYV_KER ],   xpbyv_ker_name   );


	// -- Set level-1m (packm/unpackm) kernels ---------------------------------

	funcs = bli_cntx_packm_kers_buf( cntx );

	// Initialize all packm kernel func_t entries to NULL.
	for ( i = BLIS_PACKM_0XK_KER; i <= BLIS_PACKM_31XK_KER; ++i )
	{
		bli_func_init_null( &funcs[ i ] );
	}

	gen_func_init( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_3XK_KER ],  packm_3xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_ker_name );
	gen_func_init( &funcs[ BLIS_PACKM_24XK_KER ], packm_24xk_ker_name );

	funcs = bli_cntx_unpackm_kers_buf( cntx );

	// Initialize all packm kernel func_t entries to NULL.
	for ( i = BLIS_UNPACKM_0XK_KER; i <= BLIS_UNPACKM_31XK_KER; ++i )
	{
		bli_func_init_null( &funcs[ i ] );
	}

	gen_func_init( &funcs[ BLIS_UNPACKM_2XK_KER ],  unpackm_2xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_4XK_KER ],  unpackm_4xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_6XK_KER ],  unpackm_6xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_8XK_KER ],  unpackm_8xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_10XK_KER ], unpackm_10xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_12XK_KER ], unpackm_12xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_14XK_KER ], unpackm_14xk_ker_name );
	gen_func_init( &funcs[ BLIS_UNPACKM_16XK_KER ], unpackm_16xk_ker_name );


	// -- Set miscellaneous fields ---------------------------------------------

	bli_cntx_set_method( BLIS_NAT, cntx );

	bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS, cntx );
	bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS, cntx );
	bli_cntx_set_schema_c_panel( BLIS_NOT_PACKED,        cntx );

	//bli_cntx_set_anti_pref( FALSE, cntx );

	//bli_cntx_set_membrk( bli_membrk_query(), cntx );
}

// -----------------------------------------------------------------------------

void GENBAINAME(cntx_init)
     (
       ind_t   method,
       num_t   dt,
       cntx_t* cntx
     )
{
	func_t* funcs;
	dim_t   i;

	// This function is designed to modify a copy of an existing native
	// context to enable computation via an induced method for complex
	// domain level-3 operations. It is called by bli_gks_query_ind_cntx()
	// on a context after its contexts are set by copying from the
	// architecture's native context.

	// -- Set induced method level-3 virtual micro-kernels ---------------------

	funcs = bli_cntx_l3_vir_ukrs_buf( cntx );

	// 3mh, 4mh, and 4mb do not not support trsm.
	bli_func_init_null( &funcs[ BLIS_GEMMTRSM_L_UKR ] );
	bli_func_init_null( &funcs[ BLIS_GEMMTRSM_U_UKR ] );
	bli_func_init_null( &funcs[ BLIS_TRSM_L_UKR ] );
	bli_func_init_null( &funcs[ BLIS_TRSM_U_UKR ] );

	if      ( method == BLIS_3MH )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm3mh_ukr_name       );
	}
	else if ( method == BLIS_3M1 )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm3m1_ukr_name       );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm3m1_l_ukr_name );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm3m1_u_ukr_name );
		gen_func_init_co( &funcs[ BLIS_TRSM_L_UKR ],     trsm3m1_l_ukr_name     );
		gen_func_init_co( &funcs[ BLIS_TRSM_U_UKR ],     trsm3m1_u_ukr_name     );
	}
	else if ( method == BLIS_4MH )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm4mh_ukr_name       );
	}
	else if ( method == BLIS_4M1B )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm4mb_ukr_name       );
	}
	else if ( method == BLIS_4M1A )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm4m1_ukr_name       );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm4m1_l_ukr_name );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm4m1_u_ukr_name );
		gen_func_init_co( &funcs[ BLIS_TRSM_L_UKR ],     trsm4m1_l_ukr_name     );
		gen_func_init_co( &funcs[ BLIS_TRSM_U_UKR ],     trsm4m1_u_ukr_name     );
	}
	else if ( method == BLIS_1M )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm1m_ukr_name       );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm1m_l_ukr_name );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm1m_u_ukr_name );
		gen_func_init_co( &funcs[ BLIS_TRSM_L_UKR ],     trsm1m_l_ukr_name     );
		gen_func_init_co( &funcs[ BLIS_TRSM_U_UKR ],     trsm1m_u_ukr_name     );
	}
	else // if ( method == BLIS_NAT )
	{
		gen_func_init_co( &funcs[ BLIS_GEMM_UKR ],       gemm_ukr_name       );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_L_UKR ], gemmtrsm_l_ukr_name );
		gen_func_init_co( &funcs[ BLIS_GEMMTRSM_U_UKR ], gemmtrsm_u_ukr_name );
		gen_func_init_co( &funcs[ BLIS_TRSM_L_UKR ],     trsm_l_ukr_name     );
		gen_func_init_co( &funcs[ BLIS_TRSM_U_UKR ],     trsm_u_ukr_name     );
	}

	// For 1m, we employ an optimization which requires that we copy the native
	// real domain gemm ukernel function pointers to the corresponding real
	// domain slots in the virtual gemm ukernel func_t.
	if ( method == BLIS_1M )
	{
		func_t* gemm_nat_ukrs = bli_cntx_get_l3_nat_ukrs( BLIS_GEMM_UKR, cntx );
		func_t* gemm_vir_ukrs = bli_cntx_get_l3_vir_ukrs( BLIS_GEMM_UKR, cntx );

		bli_func_copy_dt( BLIS_FLOAT,  gemm_nat_ukrs, BLIS_FLOAT,  gemm_vir_ukrs );
		bli_func_copy_dt( BLIS_DOUBLE, gemm_nat_ukrs, BLIS_DOUBLE, gemm_vir_ukrs );
	}


	// -- Set induced method packm kernels -------------------------------------

	funcs = bli_cntx_packm_kers_buf( cntx );

	// Initialize all packm kernel func_t entries to NULL.
	for ( i = BLIS_PACKM_0XK_KER; i <= BLIS_PACKM_31XK_KER; ++i )
	{
		bli_func_init_null( &funcs[ i ] );
	}

	if ( method == BLIS_3MH || method == BLIS_4MH )
	{
		gen_func_init_co( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_rih_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_rih_ker_name );
	}
	else if ( method == BLIS_3M1 )
	{
		gen_func_init_co( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_3mis_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_3mis_ker_name );
	}
	else if ( method == BLIS_4M1A || method == BLIS_4M1B )
	{
		gen_func_init_co( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_4mi_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_4mi_ker_name );
	}
	else if ( method == BLIS_1M )
	{
		gen_func_init_co( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_1er_ker_name );
		gen_func_init_co( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_1er_ker_name );
	}
	else // if ( method == BLIS_NAT )
	{
		gen_func_init( &funcs[ BLIS_PACKM_2XK_KER ],  packm_2xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_3XK_KER ],  packm_3xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_4XK_KER ],  packm_4xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_6XK_KER ],  packm_6xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_8XK_KER ],  packm_8xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_10XK_KER ], packm_10xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_12XK_KER ], packm_12xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_14XK_KER ], packm_14xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_16XK_KER ], packm_16xk_ker_name );
		gen_func_init( &funcs[ BLIS_PACKM_24XK_KER ], packm_24xk_ker_name );
	}


	// -- Set induced method cache and register blocksizes ---------------------

	// Modify the context with cache and register blocksizes (and multiples)
	// appropriate for the current induced method.
	if      ( method == BLIS_3MH )
	{
		bli_cntx_set_ind_blkszs
		(
		  method, 6,
		  BLIS_NC, 1.0, 1.0,
		  BLIS_KC, 1.0, 1.0,
		  BLIS_MC, 1.0, 1.0,
		  BLIS_NR, 1.0, 1.0,
		  BLIS_MR, 1.0, 1.0,
		  BLIS_KR, 1.0, 1.0,
		  cntx
		);
	}
	else if ( method == BLIS_3M1 )
	{
		bli_cntx_set_ind_blkszs
		(
		  method, 6,
		  BLIS_NC, 1.0, 1.0,
		  BLIS_KC, 3.0, 3.0,
		  BLIS_MC, 1.0, 1.0,
		  BLIS_NR, 1.0, 1.0,
		  BLIS_MR, 1.0, 1.0,
		  BLIS_KR, 1.0, 1.0,
		  cntx
		);
	}
	else if ( method == BLIS_4MH )
	{
		bli_cntx_set_ind_blkszs
		(
		  method, 6,
		  BLIS_NC, 1.0, 1.0,
		  BLIS_KC, 1.0, 1.0,
		  BLIS_MC, 1.0, 1.0,
		  BLIS_NR, 1.0, 1.0,
		  BLIS_MR, 1.0, 1.0,
		  BLIS_KR, 1.0, 1.0,
		  cntx
		);
	}
	else if ( method == BLIS_4M1B )
	{
		bli_cntx_set_ind_blkszs
		(
		  method, 6,
		  BLIS_NC, 2.0, 2.0,
		  BLIS_KC, 1.0, 1.0,
		  BLIS_MC, 2.0, 2.0,
		  BLIS_NR, 1.0, 1.0,
		  BLIS_MR, 1.0, 1.0,
		  BLIS_KR, 1.0, 1.0,
		  cntx
		);
	}
	else if ( method == BLIS_4M1A )
	{
		bli_cntx_set_ind_blkszs
		(
		  method, 6,
		  BLIS_NC, 1.0, 1.0,
		  BLIS_KC, 2.0, 2.0,
		  BLIS_MC, 1.0, 1.0,
		  BLIS_NR, 1.0, 1.0,
		  BLIS_MR, 1.0, 1.0,
		  BLIS_KR, 1.0, 1.0,
		  cntx
		);
	}
	else if ( method == BLIS_1M )
	{
		const bool is_pb = FALSE;

		// We MUST set the induced method in the context prior to calling
		// bli_cntx_l3_ukr_prefers_cols_dt() because that function queries
		// the induced method. It needs the induced method value in order
		// to determine whether to evaluate the "prefers column storage"
		// predicate using the storage preference of the kernel for dt, or
		// the storage preference of the kernel for the real projection of
		// dt. Failing to set the induced method here can lead to strange
		// undefined behavior at runtime if the native complex kernel's
		// storage preference happens to not equal that of the native real
		// kernel.
		bli_cntx_set_method( method, cntx );

		// Initialize the blocksizes according to the micro-kernel preference as
		// well as the algorithm.
		if ( bli_cntx_l3_vir_ukr_prefers_cols_dt( dt, BLIS_GEMM_UKR, cntx ) )
		{
			// This branch is used for algorithms 1m_c_bp, 1m_r_pb.

			// Set the pack_t schemas for the c_bp or r_pb algorithms.
			if ( !is_pb )
			{
				bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_1E, cntx );
				bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_1R, cntx );
			}
			else // if ( is_pb )
			{
				bli_cntx_set_schema_b_panel( BLIS_PACKED_ROW_PANELS_1R, cntx );
				bli_cntx_set_schema_a_block( BLIS_PACKED_COL_PANELS_1E, cntx );
			}

			bli_cntx_set_ind_blkszs
			(
			  method, 6,
			  BLIS_NC, 1.0, 1.0,
			  BLIS_KC, 2.0, 2.0, // halve kc...
			  BLIS_MC, 2.0, 2.0, // halve mc...
			  BLIS_NR, 1.0, 1.0,
			  BLIS_MR, 2.0, 1.0, // ...and mr (but NOT packmr)
			  BLIS_KR, 1.0, 1.0,
			  cntx
			);
		}
		else // if ( bli_cntx_l3_vir_ukr_prefers_rows_dt( dt, BLIS_GEMM_UKR, cntx ) )
		{
			// This branch is used for algorithms 1m_r_bp, 1m_c_pb.

			// Set the pack_t schemas for the r_bp or c_pb algorithms.
			if ( !is_pb )
			{
			    bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_1R, cntx );
			    bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_1E, cntx );
			}
			else // if ( is_pb )
			{
			    bli_cntx_set_schema_b_panel( BLIS_PACKED_ROW_PANELS_1E, cntx );
			    bli_cntx_set_schema_a_block( BLIS_PACKED_COL_PANELS_1R, cntx );
			}

			bli_cntx_set_ind_blkszs
			(
			  method, 6,
			  BLIS_NC, 2.0, 2.0, // halve nc...
			  BLIS_KC, 2.0, 2.0, // halve kc...
			  BLIS_MC, 1.0, 1.0,
			  BLIS_NR, 2.0, 1.0, // ...and nr (but NOT packnr)
			  BLIS_MR, 1.0, 1.0,
			  BLIS_KR, 1.0, 1.0,
			  cntx
			);
		}
	}
	else // if ( method == BLIS_NAT )
	{
		// No change in blocksizes needed for native execution.
	}


	// -- Set misc. other fields -----------------------------------------------

	if      ( method == BLIS_3MH )
	{
		// Schemas vary with _stage().
	}
	else if ( method == BLIS_3M1 )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_3MI, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_3MI, cntx );
	}
	else if ( method == BLIS_4MH )
	{
		// Schemas vary with _stage().
	}
	else if ( method == BLIS_4M1A || method == BLIS_4M1B )
	{
		bli_cntx_set_schema_a_block( BLIS_PACKED_ROW_PANELS_4MI, cntx );
		bli_cntx_set_schema_b_panel( BLIS_PACKED_COL_PANELS_4MI, cntx );
	}
	else if ( method == BLIS_1M )
	{
		//const bool is_pb = FALSE;

		// Set the anti-preference field to TRUE when executing a panel-block
		// algorithm, and FALSE otherwise. This will cause higher-level generic
		// code to establish (if needed) disagreement between the storage of C and
		// the micro-kernel output preference so that the two will come back into
		// agreement in the panel-block macro-kernel (which implemented in terms
		// of the block-panel macro-kernel with some induced transpositions).
		//bli_cntx_set_anti_pref( is_pb, cntx );
	}
	else // if ( method == BLIS_NAT )
	{
	}
}

