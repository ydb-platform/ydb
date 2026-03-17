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


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       trans_t transa, \
       trans_t transb, \
       dim_t   m, \
       dim_t   n, \
       dim_t   k, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( gemm3mh )
INSERT_GENTPROT_BASIC0( gemm3m1 )
INSERT_GENTPROT_BASIC0( gemm4mh )
INSERT_GENTPROT_BASIC0( gemm4mb )
INSERT_GENTPROT_BASIC0( gemm4m1 )
INSERT_GENTPROT_BASIC0( gemm1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       side_t  side, \
       uplo_t  uploa, \
       conj_t  conja, \
       trans_t transb, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( hemm3mh )
INSERT_GENTPROT_BASIC0( hemm3m1 )
INSERT_GENTPROT_BASIC0( hemm4mh )
INSERT_GENTPROT_BASIC0( hemm4m1 )
INSERT_GENTPROT_BASIC0( hemm1m )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       uplo_t   uploc, \
       trans_t  transa, \
       trans_t  transb, \
       dim_t    m, \
       dim_t    k, \
       ctype*   alpha, \
       ctype*   a, inc_t rs_a, inc_t cs_a, \
       ctype*   b, inc_t rs_b, inc_t cs_b, \
       ctype_r* beta, \
       ctype*   c, inc_t rs_c, inc_t cs_c, \
       cntx_t*  cntx, \
       rntm_t*  rntmx  \
     );

INSERT_GENTPROTR_BASIC0( her2k3mh )
INSERT_GENTPROTR_BASIC0( her2k3m1 )
INSERT_GENTPROTR_BASIC0( her2k4mh )
INSERT_GENTPROTR_BASIC0( her2k4m1 )
INSERT_GENTPROTR_BASIC0( her2k1m )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       uplo_t   uploc, \
       trans_t  transa, \
       dim_t    m, \
       dim_t    k, \
       ctype_r* alpha, \
       ctype*   a, inc_t rs_a, inc_t cs_a, \
       ctype_r* beta, \
       ctype*   c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntmx  \
     );

INSERT_GENTPROTR_BASIC0( herk3mh )
INSERT_GENTPROTR_BASIC0( herk3m1 )
INSERT_GENTPROTR_BASIC0( herk4mh )
INSERT_GENTPROTR_BASIC0( herk4m1 )
INSERT_GENTPROTR_BASIC0( herk1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       side_t  side, \
       uplo_t  uploa, \
       conj_t  conja, \
       trans_t transb, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( symm3mh )
INSERT_GENTPROT_BASIC0( symm3m1 )
INSERT_GENTPROT_BASIC0( symm4mh )
INSERT_GENTPROT_BASIC0( symm4m1 )
INSERT_GENTPROT_BASIC0( symm1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       uplo_t  uploc, \
       trans_t transa, \
       trans_t transb, \
       dim_t   m, \
       dim_t   k, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( syr2k3mh )
INSERT_GENTPROT_BASIC0( syr2k3m1 )
INSERT_GENTPROT_BASIC0( syr2k4mh )
INSERT_GENTPROT_BASIC0( syr2k4m1 )
INSERT_GENTPROT_BASIC0( syr2k1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       uplo_t  uploc, \
       trans_t transa, \
       dim_t   m, \
       dim_t   k, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( syrk3mh )
INSERT_GENTPROT_BASIC0( syrk3m1 )
INSERT_GENTPROT_BASIC0( syrk4mh )
INSERT_GENTPROT_BASIC0( syrk4m1 )
INSERT_GENTPROT_BASIC0( syrk1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       side_t  side, \
       uplo_t  uploa, \
       trans_t transa, \
       diag_t  diaga, \
       trans_t transb, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       ctype*  beta, \
       ctype*  c, inc_t rs_c, inc_t cs_c, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( trmm33mh )
INSERT_GENTPROT_BASIC0( trmm33m1 )
INSERT_GENTPROT_BASIC0( trmm34mh )
INSERT_GENTPROT_BASIC0( trmm34m1 )
INSERT_GENTPROT_BASIC0( trmm31m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       side_t  side, \
       uplo_t  uploa, \
       trans_t transa, \
       diag_t  diaga, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( trmm3m1 )
INSERT_GENTPROT_BASIC0( trmm4m1 )
INSERT_GENTPROT_BASIC0( trmm1m )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       side_t  side, \
       uplo_t  uploa, \
       trans_t transa, \
       diag_t  diaga, \
       dim_t   m, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  a, inc_t rs_a, inc_t cs_a, \
       ctype*  b, inc_t rs_b, inc_t cs_b, \
       cntx_t* cntx, \
       rntm_t* rntm  \
     );

INSERT_GENTPROT_BASIC0( trsm3m1 )
INSERT_GENTPROT_BASIC0( trsm4m1 )
INSERT_GENTPROT_BASIC0( trsm1m )

