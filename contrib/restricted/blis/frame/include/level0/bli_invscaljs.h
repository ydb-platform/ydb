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

#ifndef BLIS_INVSCALJS_H
#define BLIS_INVSCALJS_H

// invscaljs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_ssinvscaljs( a, y )  bli_sinvscaljris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dsinvscaljs( a, y )  bli_sinvscaljris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_csinvscaljs( a, y )  bli_sinvscaljris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zsinvscaljs( a, y )  bli_sinvscaljris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdinvscaljs( a, y )  bli_dinvscaljris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddinvscaljs( a, y )  bli_dinvscaljris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdinvscaljs( a, y )  bli_dinvscaljris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdinvscaljs( a, y )  bli_dinvscaljris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scinvscaljs( a, y )  bli_scinvscaljris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcinvscaljs( a, y )  bli_scinvscaljris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccinvscaljs( a, y )   bli_cinvscaljris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcinvscaljs( a, y )   bli_cinvscaljris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szinvscaljs( a, y )  bli_dzinvscaljris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzinvscaljs( a, y )  bli_dzinvscaljris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czinvscaljs( a, y )   bli_zinvscaljris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzinvscaljs( a, y )   bli_zinvscaljris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scinvscaljs( a, y )  { (y) /=      (a); }
#define bli_dcinvscaljs( a, y )  { (y) /=      (a); }
#define bli_ccinvscaljs( a, y )  { (y) /= conjf(a); }
#define bli_zcinvscaljs( a, y )  { (y) /= conj (a); }

#define bli_szinvscaljs( a, y )  { (y) /=      (a); }
#define bli_dzinvscaljs( a, y )  { (y) /=      (a); }
#define bli_czinvscaljs( a, y )  { (y) /= conjf(a); }
#define bli_zzinvscaljs( a, y )  { (y) /= conj (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sinvscaljs( a, y )  bli_ssinvscaljs( a, y )
#define bli_dinvscaljs( a, y )  bli_ddinvscaljs( a, y )
#define bli_cinvscaljs( a, y )  bli_ccinvscaljs( a, y )
#define bli_zinvscaljs( a, y )  bli_zzinvscaljs( a, y )


#endif

