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

#ifndef BLIS_COPYNZS_H
#define BLIS_COPYNZS_H

// copynzs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopynzs( x, y )  bli_scopyris( bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dscopynzs( x, y )  bli_scopyris( bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cscopynzs( x, y )  bli_scopyris( bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zscopynzs( x, y )  bli_scopyris( bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdcopynzs( x, y )  bli_dcopyris( bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddcopynzs( x, y )  bli_dcopyris( bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdcopynzs( x, y )  bli_dcopyris( bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdcopynzs( x, y )  bli_dcopyris( bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

// NOTE: Use of scopyris() is so we don't touch the imaginary part of y.
#define bli_sccopynzs( x, y )  bli_scopyris( bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccopynzs( x, y )  bli_scopyris( bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccopynzs( x, y )  bli_ccopyris( bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccopynzs( x, y )  bli_ccopyris( bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// NOTE: Use of dcopyris() is so we don't touch the imaginary part of y.
#define bli_szcopynzs( x, y )  bli_dcopyris( bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzcopynzs( x, y )  bli_dcopyris( bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czcopynzs( x, y )  bli_zcopyris( bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzcopynzs( x, y )  bli_zcopyris( bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )


#define bli_iicopynzs( x, y )  { (y) = ( gint_t ) (x); }


#define bli_scopynzs( x, y )  bli_sscopynzs( x, y )
#define bli_dcopynzs( x, y )  bli_ddcopynzs( x, y )
#define bli_ccopynzs( x, y )  bli_cccopynzs( x, y )
#define bli_zcopynzs( x, y )  bli_zzcopynzs( x, y )
#define bli_icopynzs( x, y )  bli_iicopynzs( x, y )


#endif

