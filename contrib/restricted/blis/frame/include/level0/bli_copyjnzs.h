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

#ifndef BLIS_COPYJNZS_H
#define BLIS_COPYJNZS_H

// copyjnzs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopyjnzs( x, y )  bli_scopyjris( bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dscopyjnzs( x, y )  bli_scopyjris( bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cscopyjnzs( x, y )  bli_scopyjris( bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zscopyjnzs( x, y )  bli_scopyjris( bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdcopyjnzs( x, y )  bli_dcopyjris( bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddcopyjnzs( x, y )  bli_dcopyjris( bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdcopyjnzs( x, y )  bli_dcopyjris( bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdcopyjnzs( x, y )  bli_dcopyjris( bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

// NOTE: Use of scopyjris() (implemented in terms of scopyris()), is so we
// don't touch the imaginary part of y.
#define bli_sccopyjnzs( x, y )  bli_scopyjris( bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccopyjnzs( x, y )  bli_scopyjris( bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccopyjnzs( x, y )  bli_ccopyjris( bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccopyjnzs( x, y )  bli_ccopyjris( bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// NOTE: Use of dcopyjris() (implemented in terms of dcopyris()), is so we
// don't touch the imaginary part of y.
#define bli_szcopyjnzs( x, y )  bli_dcopyjris( bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzcopyjnzs( x, y )  bli_dcopyjris( bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czcopyjnzs( x, y )  bli_zcopyjris( bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzcopyjnzs( x, y )  bli_zcopyjris( bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )


#define bli_iicopyjnzs( x, y )  { (y) = ( gint_t ) (x); }


#define bli_scopyjnzs( x, y )  bli_sscopyjnzs( x, y )
#define bli_dcopyjnzs( x, y )  bli_ddcopyjnzs( x, y )
#define bli_ccopyjnzs( x, y )  bli_cccopyjnzs( x, y )
#define bli_zcopyjnzs( x, y )  bli_zzcopyjnzs( x, y )
#define bli_icopyjnzs( x, y )  bli_iicopyjnzs( x, y )


#endif

