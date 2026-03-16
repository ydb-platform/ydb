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

#ifndef BLIS_DOTJS_H
#define BLIS_DOTJS_H

// dotjs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.
// - The third char encodes the type of rho.
// - x is used in conjugated form.


#define bli_sssdotjs( x, y, a )  bli_sssaxpyjs( y, x, a )
#define bli_dssdotjs( x, y, a )  bli_sdsaxpyjs( y, x, a )
#define bli_cssdotjs( x, y, a )  bli_scsaxpyjs( y, x, a )
#define bli_zssdotjs( x, y, a )  bli_szsaxpyjs( y, x, a )

#define bli_sdsdotjs( x, y, a )  bli_dssaxpyjs( y, x, a )
#define bli_ddsdotjs( x, y, a )  bli_ddsaxpyjs( y, x, a )
#define bli_cdsdotjs( x, y, a )  bli_dcsaxpyjs( y, x, a )
#define bli_zdsdotjs( x, y, a )  bli_dzsaxpyjs( y, x, a )

#define bli_scsdotjs( x, y, a )  bli_cssaxpyjs( y, x, a )
#define bli_dcsdotjs( x, y, a )  bli_cdsaxpyjs( y, x, a )
#define bli_ccsdotjs( x, y, a )  bli_ccsaxpyjs( y, x, a )
#define bli_zcsdotjs( x, y, a )  bli_czsaxpyjs( y, x, a )

#define bli_szsdotjs( x, y, a )  bli_zssaxpyjs( y, x, a )
#define bli_dzsdotjs( x, y, a )  bli_zdsaxpyjs( y, x, a )
#define bli_czsdotjs( x, y, a )  bli_zcsaxpyjs( y, x, a )
#define bli_zzsdotjs( x, y, a )  bli_zzsaxpyjs( y, x, a )


#define bli_ssddotjs( x, y, a )  bli_ssdaxpyjs( y, x, a )
#define bli_dsddotjs( x, y, a )  bli_sddaxpyjs( y, x, a )
#define bli_csddotjs( x, y, a )  bli_scdaxpyjs( y, x, a )
#define bli_zsddotjs( x, y, a )  bli_szdaxpyjs( y, x, a )

#define bli_sdddotjs( x, y, a )  bli_dsdaxpyjs( y, x, a )
#define bli_ddddotjs( x, y, a )  bli_dddaxpyjs( y, x, a )
#define bli_cdddotjs( x, y, a )  bli_dcdaxpyjs( y, x, a )
#define bli_zdddotjs( x, y, a )  bli_dzdaxpyjs( y, x, a )

#define bli_scddotjs( x, y, a )  bli_csdaxpyjs( y, x, a )
#define bli_dcddotjs( x, y, a )  bli_cddaxpyjs( y, x, a )
#define bli_ccddotjs( x, y, a )  bli_ccdaxpyjs( y, x, a )
#define bli_zcddotjs( x, y, a )  bli_czdaxpyjs( y, x, a )

#define bli_szddotjs( x, y, a )  bli_zsdaxpyjs( y, x, a )
#define bli_dzddotjs( x, y, a )  bli_zddaxpyjs( y, x, a )
#define bli_czddotjs( x, y, a )  bli_zcdaxpyjs( y, x, a )
#define bli_zzddotjs( x, y, a )  bli_zzdaxpyjs( y, x, a )


#define bli_sscdotjs( x, y, a )  bli_sscaxpyjs( y, x, a )
#define bli_dscdotjs( x, y, a )  bli_sdcaxpyjs( y, x, a )
#define bli_cscdotjs( x, y, a )  bli_sccaxpyjs( y, x, a )
#define bli_zscdotjs( x, y, a )  bli_szcaxpyjs( y, x, a )

#define bli_sdcdotjs( x, y, a )  bli_dscaxpyjs( y, x, a )
#define bli_ddcdotjs( x, y, a )  bli_ddcaxpyjs( y, x, a )
#define bli_cdcdotjs( x, y, a )  bli_dccaxpyjs( y, x, a )
#define bli_zdcdotjs( x, y, a )  bli_dzcaxpyjs( y, x, a )

#define bli_sccdotjs( x, y, a )  bli_cscaxpyjs( y, x, a )
#define bli_dccdotjs( x, y, a )  bli_cdcaxpyjs( y, x, a )
#define bli_cccdotjs( x, y, a )  bli_cccaxpyjs( y, x, a )
#define bli_zccdotjs( x, y, a )  bli_czcaxpyjs( y, x, a )

#define bli_szcdotjs( x, y, a )  bli_zscaxpyjs( y, x, a )
#define bli_dzcdotjs( x, y, a )  bli_zdcaxpyjs( y, x, a )
#define bli_czcdotjs( x, y, a )  bli_zccaxpyjs( y, x, a )
#define bli_zzcdotjs( x, y, a )  bli_zzcaxpyjs( y, x, a )


#define bli_sszdotjs( x, y, a )  bli_sszaxpyjs( y, x, a )
#define bli_dszdotjs( x, y, a )  bli_sdzaxpyjs( y, x, a )
#define bli_cszdotjs( x, y, a )  bli_sczaxpyjs( y, x, a )
#define bli_zszdotjs( x, y, a )  bli_szzaxpyjs( y, x, a )

#define bli_sdzdotjs( x, y, a )  bli_dszaxpyjs( y, x, a )
#define bli_ddzdotjs( x, y, a )  bli_ddzaxpyjs( y, x, a )
#define bli_cdzdotjs( x, y, a )  bli_dczaxpyjs( y, x, a )
#define bli_zdzdotjs( x, y, a )  bli_dzzaxpyjs( y, x, a )

#define bli_sczdotjs( x, y, a )  bli_cszaxpyjs( y, x, a )
#define bli_dczdotjs( x, y, a )  bli_cdzaxpyjs( y, x, a )
#define bli_cczdotjs( x, y, a )  bli_cczaxpyjs( y, x, a )
#define bli_zczdotjs( x, y, a )  bli_czzaxpyjs( y, x, a )

#define bli_szzdotjs( x, y, a )  bli_zszaxpyjs( y, x, a )
#define bli_dzzdotjs( x, y, a )  bli_zdzaxpyjs( y, x, a )
#define bli_czzdotjs( x, y, a )  bli_zczaxpyjs( y, x, a )
#define bli_zzzdotjs( x, y, a )  bli_zzzaxpyjs( y, x, a )





#define bli_sdotjs( x, y, a )  bli_sssdotjs( x, y, a )
#define bli_ddotjs( x, y, a )  bli_ddddotjs( x, y, a )
#define bli_cdotjs( x, y, a )  bli_cccdotjs( x, y, a )
#define bli_zdotjs( x, y, a )  bli_zzzdotjs( x, y, a )


#endif

