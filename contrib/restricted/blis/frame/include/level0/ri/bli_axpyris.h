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

#ifndef BLIS_AXPYRIS_H
#define BLIS_AXPYRIS_H

// axpyris

#define bli_rxaxpyris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr); \
}

#define bli_cxaxpyris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr) - (ai) * (xi); \
	(yi) += (ai) * (xr) + (ar) * (xi); \
}

#define bli_roaxpyris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr) - (ai) * (xi); \
}

#define bli_craxpyris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr); \
	(yi) += (ar) * (xi); \
}

#define bli_rcaxpyris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr); \
	(yi) += (ai) * (xr); \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssaxpyris  bli_rxaxpyris
#define bli_dssaxpyris  bli_rxaxpyris
#define bli_cssaxpyris  bli_rxaxpyris
#define bli_zssaxpyris  bli_rxaxpyris

#define bli_sdsaxpyris  bli_rxaxpyris
#define bli_ddsaxpyris  bli_rxaxpyris
#define bli_cdsaxpyris  bli_rxaxpyris
#define bli_zdsaxpyris  bli_rxaxpyris

#define bli_scsaxpyris  bli_rxaxpyris
#define bli_dcsaxpyris  bli_rxaxpyris
#define bli_ccsaxpyris  bli_roaxpyris
#define bli_zcsaxpyris  bli_roaxpyris

#define bli_szsaxpyris  bli_rxaxpyris
#define bli_dzsaxpyris  bli_rxaxpyris
#define bli_czsaxpyris  bli_roaxpyris
#define bli_zzsaxpyris  bli_roaxpyris

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdaxpyris  bli_rxaxpyris
#define bli_dsdaxpyris  bli_rxaxpyris
#define bli_csdaxpyris  bli_rxaxpyris
#define bli_zsdaxpyris  bli_rxaxpyris

#define bli_sddaxpyris  bli_rxaxpyris
#define bli_dddaxpyris  bli_rxaxpyris
#define bli_cddaxpyris  bli_rxaxpyris
#define bli_zddaxpyris  bli_rxaxpyris

#define bli_scdaxpyris  bli_rxaxpyris
#define bli_dcdaxpyris  bli_rxaxpyris
#define bli_ccdaxpyris  bli_roaxpyris
#define bli_zcdaxpyris  bli_roaxpyris

#define bli_szdaxpyris  bli_rxaxpyris
#define bli_dzdaxpyris  bli_rxaxpyris
#define bli_czdaxpyris  bli_roaxpyris
#define bli_zzdaxpyris  bli_roaxpyris

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpyris  bli_rxaxpyris
#define bli_dscaxpyris  bli_rxaxpyris
#define bli_cscaxpyris  bli_rcaxpyris
#define bli_zscaxpyris  bli_rcaxpyris

#define bli_sdcaxpyris  bli_rxaxpyris
#define bli_ddcaxpyris  bli_rxaxpyris
#define bli_cdcaxpyris  bli_rcaxpyris
#define bli_zdcaxpyris  bli_rcaxpyris

#define bli_sccaxpyris  bli_craxpyris
#define bli_dccaxpyris  bli_craxpyris
#define bli_cccaxpyris  bli_cxaxpyris
#define bli_zccaxpyris  bli_cxaxpyris

#define bli_szcaxpyris  bli_craxpyris
#define bli_dzcaxpyris  bli_craxpyris
#define bli_czcaxpyris  bli_cxaxpyris
#define bli_zzcaxpyris  bli_cxaxpyris

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpyris  bli_rxaxpyris
#define bli_dszaxpyris  bli_rxaxpyris
#define bli_cszaxpyris  bli_rcaxpyris
#define bli_zszaxpyris  bli_rcaxpyris

#define bli_sdzaxpyris  bli_rxaxpyris
#define bli_ddzaxpyris  bli_rxaxpyris
#define bli_cdzaxpyris  bli_rcaxpyris
#define bli_zdzaxpyris  bli_rcaxpyris

#define bli_sczaxpyris  bli_craxpyris
#define bli_dczaxpyris  bli_craxpyris
#define bli_cczaxpyris  bli_cxaxpyris
#define bli_zczaxpyris  bli_cxaxpyris

#define bli_szzaxpyris  bli_craxpyris
#define bli_dzzaxpyris  bli_craxpyris
#define bli_czzaxpyris  bli_cxaxpyris
#define bli_zzzaxpyris  bli_cxaxpyris



#define bli_saxpyris    bli_sssaxpyris
#define bli_daxpyris    bli_dddaxpyris
#define bli_caxpyris    bli_cccaxpyris
#define bli_zaxpyris    bli_zzzaxpyris

#endif

