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
// Prototype BLAS-like interfaces with typed operands.
//

#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* asum  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROTR_BASIC0( asumv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       uplo_t  uploa, \
       dim_t   m, \
       ctype*  a, inc_t rs_a, inc_t cs_a  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROT_BASIC0( mkherm )
INSERT_GENTPROT_BASIC0( mksymm )
INSERT_GENTPROT_BASIC0( mktrim )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* norm  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROTR_BASIC0( norm1v )
INSERT_GENTPROTR_BASIC0( normfv )
INSERT_GENTPROTR_BASIC0( normiv )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t   diagoffx, \
       diag_t   diagx, \
       uplo_t   uplox, \
       dim_t    m, \
       dim_t    n, \
       ctype*   x, inc_t rs_x, inc_t cs_x, \
       ctype_r* norm  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROTR_BASIC0( norm1m )
INSERT_GENTPROTR_BASIC0( normfm )
INSERT_GENTPROTR_BASIC0( normim )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       char*  s1, \
       dim_t  n, \
       void*  x, inc_t incx, \
       char*  format, \
       char*  s2  \
     );

INSERT_GENTPROT_BASIC0_I( printv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       char*  s1, \
       dim_t  m, \
       dim_t  n, \
       void*  x, inc_t rs_x, inc_t cs_x, \
       char*  format, \
       char*  s2  \
     );

INSERT_GENTPROT_BASIC0_I( printm )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROT_BASIC0( randv )
INSERT_GENTPROT_BASIC0( randnv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       doff_t  diagoffx, \
       uplo_t  uplox, \
       dim_t   m, \
       dim_t   n, \
       ctype*  x, inc_t rs_x, inc_t cs_x  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROT_BASIC0( randm )
INSERT_GENTPROT_BASIC0( randnm )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t    n, \
       ctype*   x, inc_t incx, \
       ctype_r* scale, \
       ctype_r* sumsq  \
       BLIS_TAPI_EX_PARAMS  \
     );

INSERT_GENTPROTR_BASIC0( sumsqv )


