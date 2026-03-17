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

#ifndef BLIS_DOTS_H
#define BLIS_DOTS_H

// dots

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.
// - The third char encodes the type of rho.


#define bli_sssdots( x, y, a )  bli_sssaxpys( x, y, a )
#define bli_dssdots( x, y, a )  bli_dssaxpys( x, y, a )
#define bli_cssdots( x, y, a )  bli_cssaxpys( x, y, a )
#define bli_zssdots( x, y, a )  bli_zssaxpys( x, y, a )

#define bli_sdsdots( x, y, a )  bli_sdsaxpys( x, y, a )
#define bli_ddsdots( x, y, a )  bli_ddsaxpys( x, y, a )
#define bli_cdsdots( x, y, a )  bli_cdsaxpys( x, y, a )
#define bli_zdsdots( x, y, a )  bli_zdsaxpys( x, y, a )

#define bli_scsdots( x, y, a )  bli_scsaxpys( x, y, a )
#define bli_dcsdots( x, y, a )  bli_dcsaxpys( x, y, a )
#define bli_ccsdots( x, y, a )  bli_ccsaxpys( x, y, a )
#define bli_zcsdots( x, y, a )  bli_zcsaxpys( x, y, a )

#define bli_szsdots( x, y, a )  bli_szsaxpys( x, y, a )
#define bli_dzsdots( x, y, a )  bli_dzsaxpys( x, y, a )
#define bli_czsdots( x, y, a )  bli_czsaxpys( x, y, a )
#define bli_zzsdots( x, y, a )  bli_zzsaxpys( x, y, a )



#define bli_ssddots( x, y, a )  bli_ssdaxpys( x, y, a )
#define bli_dsddots( x, y, a )  bli_dsdaxpys( x, y, a )
#define bli_csddots( x, y, a )  bli_csdaxpys( x, y, a )
#define bli_zsddots( x, y, a )  bli_zsdaxpys( x, y, a )

#define bli_sdddots( x, y, a )  bli_sddaxpys( x, y, a )
#define bli_ddddots( x, y, a )  bli_dddaxpys( x, y, a )
#define bli_cdddots( x, y, a )  bli_cddaxpys( x, y, a )
#define bli_zdddots( x, y, a )  bli_zddaxpys( x, y, a )

#define bli_scddots( x, y, a )  bli_scdaxpys( x, y, a )
#define bli_dcddots( x, y, a )  bli_dcdaxpys( x, y, a )
#define bli_ccddots( x, y, a )  bli_ccdaxpys( x, y, a )
#define bli_zcddots( x, y, a )  bli_zcdaxpys( x, y, a )

#define bli_szddots( x, y, a )  bli_szdaxpys( x, y, a )
#define bli_dzddots( x, y, a )  bli_dzdaxpys( x, y, a )
#define bli_czddots( x, y, a )  bli_czdaxpys( x, y, a )
#define bli_zzddots( x, y, a )  bli_zzdaxpys( x, y, a )



#define bli_sscdots( x, y, a )  bli_sscaxpys( x, y, a )
#define bli_dscdots( x, y, a )  bli_dscaxpys( x, y, a )
#define bli_cscdots( x, y, a )  bli_cscaxpys( x, y, a )
#define bli_zscdots( x, y, a )  bli_zscaxpys( x, y, a )

#define bli_sdcdots( x, y, a )  bli_sdcaxpys( x, y, a )
#define bli_ddcdots( x, y, a )  bli_ddcaxpys( x, y, a )
#define bli_cdcdots( x, y, a )  bli_cdcaxpys( x, y, a )
#define bli_zdcdots( x, y, a )  bli_zdcaxpys( x, y, a )

#define bli_sccdots( x, y, a )  bli_sccaxpys( x, y, a )
#define bli_dccdots( x, y, a )  bli_dccaxpys( x, y, a )
#define bli_cccdots( x, y, a )  bli_cccaxpys( x, y, a )
#define bli_zccdots( x, y, a )  bli_zccaxpys( x, y, a )

#define bli_szcdots( x, y, a )  bli_szcaxpys( x, y, a )
#define bli_dzcdots( x, y, a )  bli_dzcaxpys( x, y, a )
#define bli_czcdots( x, y, a )  bli_czcaxpys( x, y, a )
#define bli_zzcdots( x, y, a )  bli_zzcaxpys( x, y, a )



#define bli_sszdots( x, y, a )  bli_sszaxpys( x, y, a )
#define bli_dszdots( x, y, a )  bli_dszaxpys( x, y, a )
#define bli_cszdots( x, y, a )  bli_cszaxpys( x, y, a )
#define bli_zszdots( x, y, a )  bli_zszaxpys( x, y, a )

#define bli_sdzdots( x, y, a )  bli_sdzaxpys( x, y, a )
#define bli_ddzdots( x, y, a )  bli_ddzaxpys( x, y, a )
#define bli_cdzdots( x, y, a )  bli_cdzaxpys( x, y, a )
#define bli_zdzdots( x, y, a )  bli_zdzaxpys( x, y, a )

#define bli_sczdots( x, y, a )  bli_sczaxpys( x, y, a )
#define bli_dczdots( x, y, a )  bli_dczaxpys( x, y, a )
#define bli_cczdots( x, y, a )  bli_cczaxpys( x, y, a )
#define bli_zczdots( x, y, a )  bli_zczaxpys( x, y, a )

#define bli_szzdots( x, y, a )  bli_szzaxpys( x, y, a )
#define bli_dzzdots( x, y, a )  bli_dzzaxpys( x, y, a )
#define bli_czzdots( x, y, a )  bli_czzaxpys( x, y, a )
#define bli_zzzdots( x, y, a )  bli_zzzaxpys( x, y, a )



#define bli_sdots( x, y, a )  bli_sssdots( x, y, a )
#define bli_ddots( x, y, a )  bli_ddddots( x, y, a )
#define bli_cdots( x, y, a )  bli_cccdots( x, y, a )
#define bli_zdots( x, y, a )  bli_zzzdots( x, y, a )


#endif

