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

#ifndef BLIS_AXPBYS_H
#define BLIS_AXPBYS_H

// axpbys

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of b.
// - The fourth char encodes the type of y.

// -- (axby) = (???s) ----------------------------------------------------------

#define bli_ssssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dsssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_csssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zsssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_scssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_szssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_czssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzssaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )

#define bli_ssdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dsdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_csdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zsdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sddsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dddsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cddsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zddsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_scdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzdsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )

#define bli_sscsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dscsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cscsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zscsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sccsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dccsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cccsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zccsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzcsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )

#define bli_sszsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dszsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cszsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zszsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sdzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_sczsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dczsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cczsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zczsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_szzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzzsaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )

// -- (axby) = (???d) ----------------------------------------------------------

#define bli_sssdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dssdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cssdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zssdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_scsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzsdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )

#define bli_ssddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dsddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_csddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zsddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_scddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzddaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sscdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dscdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cscdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zscdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sccdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dccdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cccdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zccdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzcdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sszdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dszdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cszdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zszdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sdzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ddzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cdzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zdzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_sczdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dczdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cczdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zczdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_szzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzzdaxpbys( a, x, b, y )  bli_rxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axby) = (???c) ----------------------------------------------------------

#define bli_ssscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_csscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_scscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_szscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_czscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzscaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )

#define bli_ssdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_csdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sddcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dddcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cddcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zddcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_scdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzdcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )

#define bli_ssccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dsccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_csccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zsccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_scccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dcccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ccccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zcccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzccaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )

#define bli_sszcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dszcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cszcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zszcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sdzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_sczcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dczcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cczcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zczcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_szzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzzcaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )

// -- (axby) = (???z) ----------------------------------------------------------

#define bli_ssszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzszaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )

#define bli_ssdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sddzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dddzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cddzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zddzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzdzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_ssczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dsczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_csczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zsczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_scczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dcczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ccczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zcczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzczaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sszzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dszzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cszzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zszzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sdzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_sczzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dczzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cczzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zczzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_szzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzzzaxpbys( a, x, b, y )  bli_cxaxpbyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axby) = (???c) ----------------------------------------------------------

#define bli_ssscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzscaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_ssdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sddcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dddcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cddcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zddcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzdcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_ssccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzccaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_sszcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dszcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cszcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zszcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sczcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dczcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cczcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zczcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzzcaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

// -- (axby) = (???z) ----------------------------------------------------------

#define bli_ssszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzszaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_ssdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sddzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dddzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cddzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zddzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzdzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_ssczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dsczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_csczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zsczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_scczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dcczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ccczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zcczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzczaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#define bli_sszzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dszzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cszzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zszzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sdzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_ddzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cdzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zdzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_sczzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dczzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_cczzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zczzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_szzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_dzzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_czzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }
#define bli_zzzzaxpbys( a, x, b, y )  { (y) = (a) * (x) + (b) * (y); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_saxpbys( a, x, b, y )  bli_ssssaxpbys( a, x, b, y )
#define bli_daxpbys( a, x, b, y )  bli_ddddaxpbys( a, x, b, y )
#define bli_caxpbys( a, x, b, y )  bli_ccccaxpbys( a, x, b, y )
#define bli_zaxpbys( a, x, b, y )  bli_zzzzaxpbys( a, x, b, y )


#endif

