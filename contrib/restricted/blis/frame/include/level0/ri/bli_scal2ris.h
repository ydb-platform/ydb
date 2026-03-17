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

#ifndef BLIS_SCAL2RIS_H
#define BLIS_SCAL2RIS_H

// scal2ris

#define bli_rxscal2ris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr); \
}

#define bli_cxscal2ris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr) - (ai) * (xi); \
	(yi) = (ai) * (xr) + (ar) * (xi); \
}

#define bli_roscal2ris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr) - (ai) * (xi); \
}

#define bli_crscal2ris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr); \
	(yi) = (ar) * (xi); \
}

#define bli_rcscal2ris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr); \
	(yi) = (ai) * (xr); \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal2ris  bli_rxscal2ris
#define bli_dssscal2ris  bli_rxscal2ris
#define bli_cssscal2ris  bli_rxscal2ris
#define bli_zssscal2ris  bli_rxscal2ris

#define bli_sdsscal2ris  bli_rxscal2ris
#define bli_ddsscal2ris  bli_rxscal2ris
#define bli_cdsscal2ris  bli_rxscal2ris
#define bli_zdsscal2ris  bli_rxscal2ris

#define bli_scsscal2ris  bli_rxscal2ris
#define bli_dcsscal2ris  bli_rxscal2ris
#define bli_ccsscal2ris  bli_roscal2ris
#define bli_zcsscal2ris  bli_roscal2ris

#define bli_szsscal2ris  bli_rxscal2ris
#define bli_dzsscal2ris  bli_rxscal2ris
#define bli_czsscal2ris  bli_roscal2ris
#define bli_zzsscal2ris  bli_roscal2ris

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal2ris  bli_rxscal2ris
#define bli_dsdscal2ris  bli_rxscal2ris
#define bli_csdscal2ris  bli_rxscal2ris
#define bli_zsdscal2ris  bli_rxscal2ris

#define bli_sddscal2ris  bli_rxscal2ris
#define bli_dddscal2ris  bli_rxscal2ris
#define bli_cddscal2ris  bli_rxscal2ris
#define bli_zddscal2ris  bli_rxscal2ris

#define bli_scdscal2ris  bli_rxscal2ris
#define bli_dcdscal2ris  bli_rxscal2ris
#define bli_ccdscal2ris  bli_roscal2ris
#define bli_zcdscal2ris  bli_roscal2ris

#define bli_szdscal2ris  bli_rxscal2ris
#define bli_dzdscal2ris  bli_rxscal2ris
#define bli_czdscal2ris  bli_roscal2ris
#define bli_zzdscal2ris  bli_roscal2ris

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2ris  bli_rxscal2ris
#define bli_dscscal2ris  bli_rxscal2ris
#define bli_cscscal2ris  bli_rcscal2ris
#define bli_zscscal2ris  bli_rcscal2ris

#define bli_sdcscal2ris  bli_rxscal2ris
#define bli_ddcscal2ris  bli_rxscal2ris
#define bli_cdcscal2ris  bli_rcscal2ris
#define bli_zdcscal2ris  bli_rcscal2ris

#define bli_sccscal2ris  bli_crscal2ris
#define bli_dccscal2ris  bli_crscal2ris
#define bli_cccscal2ris  bli_cxscal2ris
#define bli_zccscal2ris  bli_cxscal2ris

#define bli_szcscal2ris  bli_crscal2ris
#define bli_dzcscal2ris  bli_crscal2ris
#define bli_czcscal2ris  bli_cxscal2ris
#define bli_zzcscal2ris  bli_cxscal2ris

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2ris  bli_rxscal2ris
#define bli_dszscal2ris  bli_rxscal2ris
#define bli_cszscal2ris  bli_rcscal2ris
#define bli_zszscal2ris  bli_rcscal2ris

#define bli_sdzscal2ris  bli_rxscal2ris
#define bli_ddzscal2ris  bli_rxscal2ris
#define bli_cdzscal2ris  bli_rcscal2ris
#define bli_zdzscal2ris  bli_rcscal2ris

#define bli_sczscal2ris  bli_crscal2ris
#define bli_dczscal2ris  bli_crscal2ris
#define bli_cczscal2ris  bli_cxscal2ris
#define bli_zczscal2ris  bli_cxscal2ris

#define bli_szzscal2ris  bli_crscal2ris
#define bli_dzzscal2ris  bli_crscal2ris
#define bli_czzscal2ris  bli_cxscal2ris
#define bli_zzzscal2ris  bli_cxscal2ris



#define bli_sscal2ris    bli_sssscal2ris
#define bli_dscal2ris    bli_dddscal2ris
#define bli_cscal2ris    bli_cccscal2ris
#define bli_zscal2ris    bli_zzzscal2ris

#endif

