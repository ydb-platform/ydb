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

#ifndef BLIS_COPYS_H
#define BLIS_COPYS_H

// copys

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopys( x, y )  bli_scopyris( bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dscopys( x, y )  bli_scopyris( bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cscopys( x, y )  bli_scopyris( bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zscopys( x, y )  bli_scopyris( bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdcopys( x, y )  bli_dcopyris( bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddcopys( x, y )  bli_dcopyris( bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdcopys( x, y )  bli_dcopyris( bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdcopys( x, y )  bli_dcopyris( bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

// NOTE: Use of ccopyris() means the imaginary part of y will be overwritten with zero.
#define bli_sccopys( x, y )  bli_ccopyris( bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccopys( x, y )  bli_ccopyris( bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccopys( x, y )  bli_ccopyris( bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccopys( x, y )  bli_ccopyris( bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// NOTE: Use of zcopyris() means the imaginary part of y will be overwritten with zero.
#define bli_szcopys( x, y )  bli_zcopyris( bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzcopys( x, y )  bli_zcopyris( bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czcopys( x, y )  bli_zcopyris( bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzcopys( x, y )  bli_zcopyris( bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )


#define bli_iicopys( x, y )  { (y) = ( gint_t ) (x); }


#define bli_scopys( x, y )  bli_sscopys( x, y )
#define bli_dcopys( x, y )  bli_ddcopys( x, y )
#define bli_ccopys( x, y )  bli_cccopys( x, y )
#define bli_zcopys( x, y )  bli_zzcopys( x, y )
#define bli_icopys( x, y )  bli_iicopys( x, y )


#endif

