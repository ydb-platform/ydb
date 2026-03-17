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


//
// Define template prototypes for level-1m kernels.
//

// Note: Instead of defining function prototype macro templates and then
// instantiating those macros to define the individual function prototypes,
// we simply alias the official operations' prototypes as defined in
// bli_l1m_ker_prot.h.

// native packm kernels

#undef  GENTPROT
#define GENTPROT PACKM_KER_PROT

INSERT_GENTPROT_BASIC0( packm_2xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_3xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_4xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_6xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_8xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_10xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_12xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_14xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_16xk_ker_name )
INSERT_GENTPROT_BASIC0( packm_24xk_ker_name )


// native unpackm kernels

#undef  GENTPROT
#define GENTPROT UNPACKM_KER_PROT

INSERT_GENTPROT_BASIC0( unpackm_2xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_4xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_6xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_8xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_10xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_12xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_14xk_ker_name )
INSERT_GENTPROT_BASIC0( unpackm_16xk_ker_name )


// 3mis packm kernels

#undef  GENTPROT
#define GENTPROT PACKM_3MIS_KER_PROT

INSERT_GENTPROT_BASIC0( packm_2xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_4xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_6xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_8xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_10xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_12xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_14xk_3mis_ker_name )
INSERT_GENTPROT_BASIC0( packm_16xk_3mis_ker_name )


// 4mi packm kernels

#undef  GENTPROT
#define GENTPROT PACKM_4MI_KER_PROT

INSERT_GENTPROT_BASIC0( packm_2xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_4xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_6xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_8xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_10xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_12xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_14xk_4mi_ker_name )
INSERT_GENTPROT_BASIC0( packm_16xk_4mi_ker_name )


// rih packm kernels

#undef  GENTPROT
#define GENTPROT PACKM_RIH_KER_PROT

INSERT_GENTPROT_BASIC0( packm_2xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_4xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_6xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_8xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_10xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_12xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_14xk_rih_ker_name )
INSERT_GENTPROT_BASIC0( packm_16xk_rih_ker_name )


// 1e/1r packm kernels

#undef  GENTPROT
#define GENTPROT PACKM_1ER_KER_PROT

INSERT_GENTPROT_BASIC0( packm_2xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_4xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_6xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_8xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_10xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_12xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_14xk_1er_ker_name )
INSERT_GENTPROT_BASIC0( packm_16xk_1er_ker_name )

