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
// Define template prototypes for level-1v kernels.
//

#define ADDV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
      ( \
        conj_t           conjx, \
        dim_t            n, \
        ctype*  restrict x, inc_t incx, \
        ctype*  restrict y, inc_t incy, \
        cntx_t* restrict cntx  \
      );


#define AMAXV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       dim_t            n, \
       ctype*  restrict x, inc_t incx, \
       dim_t*  restrict index, \
       cntx_t* restrict cntx  \
     ); \


#define AXPBYV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict beta, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ); \


#define AXPYV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ); \


#define COPYV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
      ( \
        conj_t           conjx, \
        dim_t            n, \
        ctype*  restrict x, inc_t incx, \
        ctype*  restrict y, inc_t incy, \
        cntx_t* restrict cntx  \
      );


#define DOTV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       conj_t           conjy, \
       dim_t            n, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict y, inc_t incy, \
       ctype*  restrict rho, \
       cntx_t* restrict cntx  \
     ); \


#define DOTXV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       conj_t           conjy, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict y, inc_t incy, \
       ctype*  restrict beta, \
       ctype*  restrict rho, \
       cntx_t* restrict cntx  \
     ); \


#define INVERTV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       dim_t            n, \
       ctype*  restrict x, inc_t incx, \
       cntx_t* restrict cntx  \
     ); \


#define SCALV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjalpha, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       cntx_t* restrict cntx  \
     ); \


#define SCAL2V_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ); \


#define SETV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjalpha, \
       dim_t            n, \
       ctype*  restrict alpha, \
       ctype*  restrict x, inc_t incx, \
       cntx_t* restrict cntx  \
     ); \


#define SUBV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
      ( \
        conj_t           conjx, \
        dim_t            n, \
        ctype*  restrict x, inc_t incx, \
        ctype*  restrict y, inc_t incy, \
        cntx_t* restrict cntx  \
      );


#define SWAPV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       dim_t            n, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ); \


#define XPBYV_KER_PROT( ctype, ch, opname ) \
\
void PASTEMAC(ch,opname) \
     ( \
       conj_t           conjx, \
       dim_t            n, \
       ctype*  restrict x, inc_t incx, \
       ctype*  restrict beta, \
       ctype*  restrict y, inc_t incy, \
       cntx_t* restrict cntx  \
     ); \

