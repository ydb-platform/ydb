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

#ifndef BLIS_AXPYJRIS_H
#define BLIS_AXPYJRIS_H

// axpyjris

#define bli_rxaxpyjris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr); \
}

#define bli_cxaxpyjris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr) + (ai) * (xi); \
	(yi) += (ai) * (xr) - (ar) * (xi); \
}

#define bli_roaxpyjris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr) + (ai) * (xi); \
}

#define bli_craxpyjris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) *  (xr); \
	(yi) += (ar) * -(xi); \
}

#define bli_rcaxpyjris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) += (ar) * (xr); \
	(yi) += (ai) * (xr); \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssaxpyjris  bli_rxaxpyjris
#define bli_dssaxpyjris  bli_rxaxpyjris
#define bli_cssaxpyjris  bli_rxaxpyjris
#define bli_zssaxpyjris  bli_rxaxpyjris

#define bli_sdsaxpyjris  bli_rxaxpyjris
#define bli_ddsaxpyjris  bli_rxaxpyjris
#define bli_cdsaxpyjris  bli_rxaxpyjris
#define bli_zdsaxpyjris  bli_rxaxpyjris

#define bli_scsaxpyjris  bli_rxaxpyjris
#define bli_dcsaxpyjris  bli_rxaxpyjris
#define bli_ccsaxpyjris  bli_roaxpyjris
#define bli_zcsaxpyjris  bli_roaxpyjris

#define bli_szsaxpyjris  bli_rxaxpyjris
#define bli_dzsaxpyjris  bli_rxaxpyjris
#define bli_czsaxpyjris  bli_roaxpyjris
#define bli_zzsaxpyjris  bli_roaxpyjris

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdaxpyjris  bli_rxaxpyjris
#define bli_dsdaxpyjris  bli_rxaxpyjris
#define bli_csdaxpyjris  bli_rxaxpyjris
#define bli_zsdaxpyjris  bli_rxaxpyjris

#define bli_sddaxpyjris  bli_rxaxpyjris
#define bli_dddaxpyjris  bli_rxaxpyjris
#define bli_cddaxpyjris  bli_rxaxpyjris
#define bli_zddaxpyjris  bli_rxaxpyjris

#define bli_scdaxpyjris  bli_rxaxpyjris
#define bli_dcdaxpyjris  bli_rxaxpyjris
#define bli_ccdaxpyjris  bli_roaxpyjris
#define bli_zcdaxpyjris  bli_roaxpyjris

#define bli_szdaxpyjris  bli_rxaxpyjris
#define bli_dzdaxpyjris  bli_rxaxpyjris
#define bli_czdaxpyjris  bli_roaxpyjris
#define bli_zzdaxpyjris  bli_roaxpyjris

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpyjris  bli_rxaxpyjris
#define bli_dscaxpyjris  bli_rxaxpyjris
#define bli_cscaxpyjris  bli_rcaxpyjris
#define bli_zscaxpyjris  bli_rcaxpyjris

#define bli_sdcaxpyjris  bli_rxaxpyjris
#define bli_ddcaxpyjris  bli_rxaxpyjris
#define bli_cdcaxpyjris  bli_rcaxpyjris
#define bli_zdcaxpyjris  bli_rcaxpyjris

#define bli_sccaxpyjris  bli_craxpyjris
#define bli_dccaxpyjris  bli_craxpyjris
#define bli_cccaxpyjris  bli_cxaxpyjris
#define bli_zccaxpyjris  bli_cxaxpyjris

#define bli_szcaxpyjris  bli_craxpyjris
#define bli_dzcaxpyjris  bli_craxpyjris
#define bli_czcaxpyjris  bli_cxaxpyjris
#define bli_zzcaxpyjris  bli_cxaxpyjris

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpyjris  bli_rxaxpyjris
#define bli_dszaxpyjris  bli_rxaxpyjris
#define bli_cszaxpyjris  bli_rcaxpyjris
#define bli_zszaxpyjris  bli_rcaxpyjris

#define bli_sdzaxpyjris  bli_rxaxpyjris
#define bli_ddzaxpyjris  bli_rxaxpyjris
#define bli_cdzaxpyjris  bli_rcaxpyjris
#define bli_zdzaxpyjris  bli_rcaxpyjris

#define bli_sczaxpyjris  bli_craxpyjris
#define bli_dczaxpyjris  bli_craxpyjris
#define bli_cczaxpyjris  bli_cxaxpyjris
#define bli_zczaxpyjris  bli_cxaxpyjris

#define bli_szzaxpyjris  bli_craxpyjris
#define bli_dzzaxpyjris  bli_craxpyjris
#define bli_czzaxpyjris  bli_cxaxpyjris
#define bli_zzzaxpyjris  bli_cxaxpyjris



#define bli_saxpyjris    bli_sssaxpyjris
#define bli_daxpyjris    bli_dddaxpyjris
#define bli_caxpyjris    bli_cccaxpyjris
#define bli_zaxpyjris    bli_zzzaxpyjris

#endif

