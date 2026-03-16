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

#ifndef BLIS_COPYCJS_H
#define BLIS_COPYCJS_H

// copycjs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopycjs( conjx, x, y )  bli_scopycjris( conjx, bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dscopycjs( conjx, x, y )  bli_scopycjris( conjx, bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cscopycjs( conjx, x, y )  bli_scopycjris( conjx, bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zscopycjs( conjx, x, y )  bli_scopycjris( conjx, bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdcopycjs( conjx, x, y )  bli_dcopycjris( conjx, bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddcopycjs( conjx, x, y )  bli_dcopycjris( conjx, bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdcopycjs( conjx, x, y )  bli_dcopycjris( conjx, bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdcopycjs( conjx, x, y )  bli_dcopycjris( conjx, bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_sccopycjs( conjx, x, y )  bli_ccopycjris( conjx, bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccopycjs( conjx, x, y )  bli_ccopycjris( conjx, bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccopycjs( conjx, x, y )  bli_ccopycjris( conjx, bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccopycjs( conjx, x, y )  bli_ccopycjris( conjx, bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcopycjs( conjx, x, y )  bli_zcopycjris( conjx, bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzcopycjs( conjx, x, y )  bli_zcopycjris( conjx, bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czcopycjs( conjx, x, y )  bli_zcopycjris( conjx, bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzcopycjs( conjx, x, y )  bli_zcopycjris( conjx, bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_sccopycjs( conjx, x, y )  { (y) = (x); }
#define bli_dccopycjs( conjx, x, y )  { (y) = (x); }
#define bli_cccopycjs( conjx, x, y )  { (y) = ( bli_is_conj( conjx ) ? conjf(x) : (x) ); }
#define bli_zccopycjs( conjx, x, y )  { (y) = ( bli_is_conj( conjx ) ? conj (x) : (x) ); }

#define bli_szcopycjs( conjx, x, y )  { (y) = (x); }
#define bli_dzcopycjs( conjx, x, y )  { (y) = (x); }
#define bli_czcopycjs( conjx, x, y )  { (y) = ( bli_is_conj( conjx ) ? conjf(x) : (x) ); }
#define bli_zzcopycjs( conjx, x, y )  { (y) = ( bli_is_conj( conjx ) ? conj (x) : (x) ); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_iicopycjs( conjx, x, y )  { (y) = ( gint_t ) (x); }


#define bli_scopycjs( conjx, x, y )  bli_sscopycjs( conjx, x, y )
#define bli_dcopycjs( conjx, x, y )  bli_ddcopycjs( conjx, x, y )
#define bli_ccopycjs( conjx, x, y )  bli_cccopycjs( conjx, x, y )
#define bli_zcopycjs( conjx, x, y )  bli_zzcopycjs( conjx, x, y )
#define bli_icopycjs( conjx, x, y )  bli_iicopycjs( conjx, x, y )


#endif

