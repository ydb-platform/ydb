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

#ifndef BLIS_SCALS_H
#define BLIS_SCALS_H

// scals

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of y.

#define bli_ssscals( a, y )  bli_sscalris( bli_sreal(a), bli_simag(a), bli_sreal(y), bli_simag(y) )
#define bli_dsscals( a, y )  bli_sscalris( bli_dreal(a), bli_dimag(a), bli_sreal(y), bli_simag(y) )
#define bli_csscals( a, y )  bli_sscalris( bli_creal(a), bli_cimag(a), bli_sreal(y), bli_simag(y) )
#define bli_zsscals( a, y )  bli_sscalris( bli_zreal(a), bli_zimag(a), bli_sreal(y), bli_simag(y) )

#define bli_sdscals( a, y )  bli_dscalris( bli_sreal(a), bli_simag(a), bli_dreal(y), bli_dimag(y) )
#define bli_ddscals( a, y )  bli_dscalris( bli_dreal(a), bli_dimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_cdscals( a, y )  bli_dscalris( bli_creal(a), bli_cimag(a), bli_dreal(y), bli_dimag(y) )
#define bli_zdscals( a, y )  bli_dscalris( bli_zreal(a), bli_zimag(a), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scscals( a, y )  bli_scscalris( bli_sreal(a), bli_simag(a), bli_creal(y), bli_cimag(y) )
#define bli_dcscals( a, y )  bli_scscalris( bli_dreal(a), bli_dimag(a), bli_creal(y), bli_cimag(y) )
#define bli_ccscals( a, y )   bli_cscalris( bli_creal(a), bli_cimag(a), bli_creal(y), bli_cimag(y) )
#define bli_zcscals( a, y )   bli_cscalris( bli_zreal(a), bli_zimag(a), bli_creal(y), bli_cimag(y) )

#define bli_szscals( a, y )  bli_dzscalris( bli_sreal(a), bli_simag(a), bli_zreal(y), bli_zimag(y) )
#define bli_dzscals( a, y )  bli_dzscalris( bli_dreal(a), bli_dimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_czscals( a, y )   bli_zscalris( bli_creal(a), bli_cimag(a), bli_zreal(y), bli_zimag(y) )
#define bli_zzscals( a, y )   bli_zscalris( bli_zreal(a), bli_zimag(a), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scscals( a, y )  { (y) *= (a); }
#define bli_dcscals( a, y )  { (y) *= (a); }
#define bli_ccscals( a, y )  { (y) *= (a); }
#define bli_zcscals( a, y )  { (y) *= (a); }

#define bli_szscals( a, y )  { (y) *= (a); }
#define bli_dzscals( a, y )  { (y) *= (a); }
#define bli_czscals( a, y )  { (y) *= (a); }
#define bli_zzscals( a, y )  { (y) *= (a); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sscals( a, y )  bli_ssscals( a, y )
#define bli_dscals( a, y )  bli_ddscals( a, y )
#define bli_cscals( a, y )  bli_ccscals( a, y )
#define bli_zscals( a, y )  bli_zzscals( a, y )


#endif

