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

#ifndef BLIS_SCALCJS_H
#define BLIS_SCALCJS_H

// scalcjs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_ssscalcjs( conjx, x, y )  bli_sscalcjris( conjx, bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dsscalcjs( conjx, x, y )  bli_sscalcjris( conjx, bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_csscalcjs( conjx, x, y )  bli_sscalcjris( conjx, bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zsscalcjs( conjx, x, y )  bli_sscalcjris( conjx, bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdscalcjs( conjx, x, y )  bli_dscalcjris( conjx, bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddscalcjs( conjx, x, y )  bli_dscalcjris( conjx, bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdscalcjs( conjx, x, y )  bli_dscalcjris( conjx, bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdscalcjs( conjx, x, y )  bli_dscalcjris( conjx, bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scscalcjs( conjx, x, y )  bli_scscalcjris( conjx, bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dcscalcjs( conjx, x, y )  bli_scscalcjris( conjx, bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ccscalcjs( conjx, x, y )   bli_cscalcjris( conjx, bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zcscalcjs( conjx, x, y )   bli_cscalcjris( conjx, bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szscalcjs( conjx, x, y )  bli_dzscalcjris( conjx, bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzscalcjs( conjx, x, y )  bli_dzscalcjris( conjx, bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czscalcjs( conjx, x, y )   bli_zscalcjris( conjx, bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzscalcjs( conjx, x, y )   bli_zscalcjris( conjx, bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scscalcjs( conjx, x, y )  { (y) *= (x); }
#define bli_dcscalcjs( conjx, x, y )  { (y) *= (x); }
#define bli_ccscalcjs( conjx, x, y )  { (y) *= ( bli_is_conj( conjx ) ? conjf(x) : (x) ); }
#define bli_zcscalcjs( conjx, x, y )  { (y) *= ( bli_is_conj( conjx ) ? conj (x) : (x) ); }

#define bli_szscalcjs( conjx, x, y )  { (y) *= (x); }
#define bli_dzscalcjs( conjx, x, y )  { (y) *= (x); }
#define bli_czscalcjs( conjx, x, y )  { (y) *= ( bli_is_conj( conjx ) ? conjf(x) : (x) ); }
#define bli_zzscalcjs( conjx, x, y )  { (y) *= ( bli_is_conj( conjx ) ? conj (x) : (x) ); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sscalcjs( conjx, x, y )  bli_ssscalcjs( conjx, x, y )
#define bli_dscalcjs( conjx, x, y )  bli_ddscalcjs( conjx, x, y )
#define bli_cscalcjs( conjx, x, y )  bli_ccscalcjs( conjx, x, y )
#define bli_zscalcjs( conjx, x, y )  bli_zzscalcjs( conjx, x, y )


#endif

