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

#undef  GENTPROT2
#define GENTPROT2( ctype_c, ctype_p, chc, chp, varname ) \
\
void PASTEMAC2(chc,chp,varname) \
     ( \
       conj_t            conjc, \
       pack_t            schema, \
       dim_t             m_panel, \
       dim_t             n_panel, \
       dim_t             m_panel_max, \
       dim_t             n_panel_max, \
       ctype_p* restrict kappa, \
       ctype_c* restrict c, inc_t rs_c, inc_t cs_c, \
       ctype_p* restrict p, inc_t rs_p, inc_t cs_p, \
                            inc_t is_p, \
       cntx_t*           cntx  \
     );

INSERT_GENTPROT2_BASIC0( packm_struc_cxk_md )
INSERT_GENTPROT2_MIXDP0( packm_struc_cxk_md )


#undef  GENTPROT2
#define GENTPROT2( ctype_a, ctype_p, cha, chp, opname ) \
\
void PASTEMAC2(cha,chp,opname) \
     ( \
       conj_t            conja, \
       dim_t             m, \
       dim_t             n, \
       ctype_p* restrict kappa, \
       ctype_a* restrict a, inc_t inca, inc_t lda, \
       ctype_p* restrict p,             inc_t ldp  \
     );

INSERT_GENTPROT2_BASIC0( packm_cxk_1e_md )
INSERT_GENTPROT2_MIXDP0( packm_cxk_1e_md )

INSERT_GENTPROT2_BASIC0( packm_cxk_1r_md )
INSERT_GENTPROT2_MIXDP0( packm_cxk_1r_md )

