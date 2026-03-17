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

#ifndef BLIS_SUBS_H
#define BLIS_SUBS_H

// subs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_sssubs( a, y )  bli_ssubris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dssubs( a, y )  bli_ssubris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_cssubs( a, y )  bli_ssubris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zssubs( a, y )  bli_ssubris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdsubs( a, y )  bli_dsubris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddsubs( a, y )  bli_dsubris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdsubs( a, y )  bli_dsubris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdsubs( a, y )  bli_dsubris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scsubs( a, y )  bli_csubris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcsubs( a, y )  bli_csubris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccsubs( a, y )  bli_csubris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcsubs( a, y )  bli_csubris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szsubs( a, y )  bli_zsubris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzsubs( a, y )  bli_zsubris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czsubs( a, y )  bli_zsubris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzsubs( a, y )  bli_zsubris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scsubs( a, y )  { (y) -= (a); }
#define bli_dcsubs( a, y )  { (y) -= (a); }
#define bli_ccsubs( a, y )  { (y) -= (a); }
#define bli_zcsubs( a, y )  { (y) -= (a); }

#define bli_szsubs( a, y )  { (y) -= (a); }
#define bli_dzsubs( a, y )  { (y) -= (a); }
#define bli_czsubs( a, y )  { (y) -= (a); }
#define bli_zzsubs( a, y )  { (y) -= (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_ssubs( a, y )  bli_sssubs( a, y )
#define bli_dsubs( a, y )  bli_ddsubs( a, y )
#define bli_csubs( a, y )  bli_ccsubs( a, y )
#define bli_zsubs( a, y )  bli_zzsubs( a, y )


#endif

