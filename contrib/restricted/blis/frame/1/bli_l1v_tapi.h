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

#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
      ( \
        conj_t  conjx, \
        dim_t   n, \
        ctype*  x, inc_t incx, \
        ctype*  y, inc_t incy  \
        BLIS_TAPI_EX_PARAMS  \
      );

INSERT_GENTPROT_BASIC0( addv )
INSERT_GENTPROT_BASIC0( copyv )
INSERT_GENTPROT_BASIC0( subv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t   n, \
       ctype*  x, inc_t incx, \
       dim_t*  index  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( amaxv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjx, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t incx, \
       ctype*  beta, \
       ctype*  y, inc_t incy  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( axpbyv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjx, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t incx, \
       ctype*  y, inc_t incy  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( axpyv )
INSERT_GENTPROT_BASIC0( scal2v )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjx, \
       conj_t  conjy, \
       dim_t   n, \
       ctype*  x, inc_t incx, \
       ctype*  y, inc_t incy, \
       ctype*  rho  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( dotv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjx, \
       conj_t  conjy, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t incx, \
       ctype*  y, inc_t incy, \
       ctype*  beta, \
       ctype*  rho  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( dotxv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t   n, \
       ctype*  x, inc_t incx  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( invertv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjalpha, \
       dim_t   n, \
       ctype*  alpha, \
       ctype*  x, inc_t incx  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( scalv )
INSERT_GENTPROT_BASIC0( setv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       dim_t   n, \
       ctype*  x, inc_t incx, \
       ctype*  y, inc_t incy  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( swapv )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC2(ch,opname,EX_SUF) \
     ( \
       conj_t  conjx, \
       dim_t   n, \
       ctype*  x, inc_t incx, \
       ctype*  beta, \
       ctype*  y, inc_t incy  \
       BLIS_TAPI_EX_PARAMS  \
     ); \

INSERT_GENTPROT_BASIC0( xpbyv )
