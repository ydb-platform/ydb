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

#ifndef BLIS_SUBJS_H
#define BLIS_SUBJS_H

// subjs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_sssubjs( a, y )  bli_ssubjris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dssubjs( a, y )  bli_ssubjris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_cssubjs( a, y )  bli_ssubjris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zssubjs( a, y )  bli_ssubjris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdsubjs( a, y )  bli_dsubjris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddsubjs( a, y )  bli_dsubjris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdsubjs( a, y )  bli_dsubjris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdsubjs( a, y )  bli_dsubjris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scsubjs( a, y )  bli_csubjris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcsubjs( a, y )  bli_csubjris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccsubjs( a, y )  bli_csubjris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcsubjs( a, y )  bli_csubjris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szsubjs( a, y )  bli_zsubjris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzsubjs( a, y )  bli_zsubjris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czsubjs( a, y )  bli_zsubjris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzsubjs( a, y )  bli_zsubjris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scsubjs( a, y )  { (y) -=      (a); }
#define bli_dcsubjs( a, y )  { (y) -=      (a); }
#define bli_ccsubjs( a, y )  { (y) -= conjf(a); }
#define bli_zcsubjs( a, y )  { (y) -= conj (a); }

#define bli_szsubjs( a, y )  { (y) -=      (a); }
#define bli_dzsubjs( a, y )  { (y) -=      (a); }
#define bli_czsubjs( a, y )  { (y) -= conjf(a); }
#define bli_zzsubjs( a, y )  { (y) -= conj (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_ssubjs( a, y )  bli_sssubjs( a, y )
#define bli_dsubjs( a, y )  bli_ddsubjs( a, y )
#define bli_csubjs( a, y )  bli_ccsubjs( a, y )
#define bli_zsubjs( a, y )  bli_zzsubjs( a, y )


#endif

