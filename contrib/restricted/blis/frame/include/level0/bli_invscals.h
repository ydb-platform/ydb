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

#ifndef BLIS_INVSCALS_H
#define BLIS_INVSCALS_H

// invscals

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_ssinvscals( a, y )  bli_sinvscalris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dsinvscals( a, y )  bli_sinvscalris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_csinvscals( a, y )  bli_sinvscalris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zsinvscals( a, y )  bli_sinvscalris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdinvscals( a, y )  bli_dinvscalris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddinvscals( a, y )  bli_dinvscalris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdinvscals( a, y )  bli_dinvscalris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdinvscals( a, y )  bli_dinvscalris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scinvscals( a, y )  bli_scinvscalris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcinvscals( a, y )  bli_scinvscalris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccinvscals( a, y )   bli_cinvscalris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcinvscals( a, y )   bli_cinvscalris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szinvscals( a, y )  bli_dzinvscalris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzinvscals( a, y )  bli_dzinvscalris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czinvscals( a, y )   bli_zinvscalris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzinvscals( a, y )   bli_zinvscalris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scinvscals( a, y )  { (y) /= (a); }
#define bli_dcinvscals( a, y )  { (y) /= (a); }
#define bli_ccinvscals( a, y )  { (y) /= (a); }
#define bli_zcinvscals( a, y )  { (y) /= (a); }

#define bli_szinvscals( a, y )  { (y) /= (a); }
#define bli_dzinvscals( a, y )  { (y) /= (a); }
#define bli_czinvscals( a, y )  { (y) /= (a); }
#define bli_zzinvscals( a, y )  { (y) /= (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sinvscals( a, y )  bli_ssinvscals( a, y )
#define bli_dinvscals( a, y )  bli_ddinvscals( a, y )
#define bli_cinvscals( a, y )  bli_ccinvscals( a, y )
#define bli_zinvscals( a, y )  bli_zzinvscals( a, y )


#endif

