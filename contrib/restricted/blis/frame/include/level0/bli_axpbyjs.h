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

#ifndef BLIS_AXPBYJS_H
#define BLIS_AXPBYJS_H

// axpbyjs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of b.
// - The fourth char encodes the type of y.

// -- (axby) = (???s) ----------------------------------------------------------

#define bli_ssssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dsssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_csssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zsssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_scssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_szssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_czssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzssaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )

#define bli_ssdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dsdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_csdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zsdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sddsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dddsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cddsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zddsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_scdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzdsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )

#define bli_sscsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dscsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cscsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zscsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sccsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dccsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cccsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zccsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzcsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )

#define bli_sszsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dszsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cszsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zszsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sczsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dczsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cczsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zczsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzzsaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )

// -- (axby) = (???d) ----------------------------------------------------------

#define bli_sssdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dssdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cssdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zssdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_scsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzsdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )

#define bli_ssddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dsddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_csddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zsddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_scddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzddaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sscdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dscdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cscdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zscdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sccdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dccdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cccdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zccdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzcdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sszdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dszdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cszdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zszdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sczdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dczdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cczdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zczdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzzdaxpbyjs( a, x, b, y )  bli_rxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axby) = (???c) ----------------------------------------------------------

#define bli_ssscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_csscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_scscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_szscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_czscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzscaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )

#define bli_ssdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_csdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sddcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dddcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cddcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zddcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_scdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzdcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )

#define bli_ssccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_csccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_scccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzccaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )

#define bli_sszcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dszcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cszcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zszcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sczcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dczcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cczcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zczcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzzcaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )

// -- (axby) = (???z) ----------------------------------------------------------

#define bli_ssszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzszaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )

#define bli_ssdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sddzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dddzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cddzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zddzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzdzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_ssczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzczaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sszzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dszzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cszzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zszzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sczzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dczzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cczzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zczzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzzzaxpbyjs( a, x, b, y )  bli_cxaxpbyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axby) = (???c) ----------------------------------------------------------

#define bli_ssscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdscaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scscaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcscaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccscaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcscaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szscaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzscaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czscaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzscaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_ssdcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsdcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csdcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsdcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sddcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dddcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cddcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zddcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scdcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcdcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccdcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcdcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szdcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzdcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czdcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzdcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_ssccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdccaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scccaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcccaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccccaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcccaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szccaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzccaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czccaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzccaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_sszcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dszcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cszcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zszcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdzcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddzcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdzcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdzcaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sczcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dczcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_cczcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zczcaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szzcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzzcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czzcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzzcaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

// -- (axby) = (???z) ----------------------------------------------------------

#define bli_ssszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdszaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scszaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcszaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccszaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcszaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szszaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzszaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czszaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzszaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_ssdzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsdzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csdzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsdzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sddzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dddzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cddzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zddzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scdzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcdzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccdzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcdzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szdzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzdzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czdzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzdzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_ssczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dsczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_csczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zsczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdczaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_scczaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dcczaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_ccczaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zcczaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szczaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzczaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czczaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzczaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#define bli_sszzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_dszzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cszzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zszzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sdzzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_ddzzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_cdzzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_zdzzaxpbyjs( a, x, b, y )  { (y) = (a) *      (x) + (b) * (y); }
#define bli_sczzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_dczzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_cczzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_zczzaxpbyjs( a, x, b, y )  { (y) = (a) * conjf(x) + (b) * (y); }
#define bli_szzzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_dzzzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_czzzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }
#define bli_zzzzaxpbyjs( a, x, b, y )  { (y) = (a) *  conj(x) + (b) * (y); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_saxpbyjs( a, x, b, y )  bli_ssssaxpbyjs( a, x, b, y )
#define bli_daxpbyjs( a, x, b, y )  bli_ddddaxpbyjs( a, x, b, y )
#define bli_caxpbyjs( a, x, b, y )  bli_ccccaxpbyjs( a, x, b, y )
#define bli_zaxpbyjs( a, x, b, y )  bli_zzzzaxpbyjs( a, x, b, y )


#endif

