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

#ifndef BLIS_SCALJS_H
#define BLIS_SCALJS_H

// scaljs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_ssscaljs( a, y )  bli_sscaljris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dsscaljs( a, y )  bli_sscaljris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_csscaljs( a, y )  bli_sscaljris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zsscaljs( a, y )  bli_sscaljris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdscaljs( a, y )  bli_dscaljris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddscaljs( a, y )  bli_dscaljris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdscaljs( a, y )  bli_dscaljris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdscaljs( a, y )  bli_dscaljris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scscaljs( a, y )  bli_scscaljris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcscaljs( a, y )  bli_scscaljris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccscaljs( a, y )   bli_cscaljris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcscaljs( a, y )   bli_cscaljris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szscaljs( a, y )  bli_dzscaljris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzscaljs( a, y )  bli_dzscaljris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czscaljs( a, y )   bli_zscaljris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzscaljs( a, y )   bli_zscaljris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scscaljs( a, y )  { (y) *=      (a); }
#define bli_dcscaljs( a, y )  { (y) *=      (a); }
#define bli_ccscaljs( a, y )  { (y) *= conjf(a); }
#define bli_zcscaljs( a, y )  { (y) *= conj (a); }

#define bli_szscaljs( a, y )  { (y) *=      (a); }
#define bli_dzscaljs( a, y )  { (y) *=      (a); }
#define bli_czscaljs( a, y )  { (y) *= conjf(a); }
#define bli_zzscaljs( a, y )  { (y) *= conj (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sscaljs( a, y )  bli_ssscaljs( a, y )
#define bli_dscaljs( a, y )  bli_ddscaljs( a, y )
#define bli_cscaljs( a, y )  bli_ccscaljs( a, y )
#define bli_zscaljs( a, y )  bli_zzscaljs( a, y )


#endif

