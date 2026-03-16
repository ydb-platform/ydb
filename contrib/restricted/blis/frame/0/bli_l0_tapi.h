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
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       conj_t  conjchi, \
       ctype*  chi, \
       ctype*  psi  \
     );

INSERT_GENTPROT_BASIC0( addsc )
INSERT_GENTPROT_BASIC0( divsc )
INSERT_GENTPROT_BASIC0( mulsc )
INSERT_GENTPROT_BASIC0( subsc )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       conj_t  conjchi, \
       ctype*  chi  \
     );

INSERT_GENTPROT_BASIC0( invertsc )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       ctype*   chi, \
       ctype_r* absq  \
     );

INSERT_GENTPROTR_BASIC0( absqsc )
INSERT_GENTPROTR_BASIC0( normfsc )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       ctype*  chi, \
       ctype*  psi  \
     );

INSERT_GENTPROT_BASIC0( sqrtsc )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       ctype*  chi, \
       double* zeta_r, \
       double* zeta_i  \
     );

INSERT_GENTPROT_BASIC0( getsc )


#undef  GENTPROT
#define GENTPROT( ctype, ch, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       double  zeta_r, \
       double  zeta_i, \
       ctype*  chi  \
     );

INSERT_GENTPROT_BASIC0( setsc )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       ctype*   chi, \
       ctype_r* zeta_r, \
       ctype_r* zeta_i  \
     );

INSERT_GENTPROTR_BASIC0( unzipsc )


#undef  GENTPROTR
#define GENTPROTR( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_EXPORT_BLIS void PASTEMAC(ch,opname) \
     ( \
       ctype_r* zeta_r, \
       ctype_r* zeta_i, \
       ctype*   chi  \
     );

INSERT_GENTPROTR_BASIC0( zipsc )

// -----------------------------------------------------------------------------

BLIS_EXPORT_BLIS void bli_igetsc
     (
       dim_t*  chi,
       double* zeta_r,
       double* zeta_i
     );

BLIS_EXPORT_BLIS void bli_isetsc
     (
       double  zeta_r,
       double  zeta_i,
       dim_t*  chi
     );

