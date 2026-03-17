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

#ifndef BLIS_SCAL2JRIS_H
#define BLIS_SCAL2JRIS_H

// scal2jris

#define bli_rxscal2jris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr); \
}

#define bli_cxscal2jris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr) + (ai) * (xi); \
	(yi) = (ai) * (xr) - (ar) * (xi); \
}

#define bli_roscal2jris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr) + (ai) * (xi); \
}

#define bli_crscal2jris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) *  (xr); \
	(yi) = (ar) * -(xi); \
}

#define bli_rcscal2jris( ar, ai, xr, xi, yr, yi ) \
{ \
	(yr) = (ar) * (xr); \
	(yi) = (ai) * (xr); \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dssscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cssscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zssscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sdsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_ddsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cdsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zdsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_scsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dcsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_ccsscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zcsscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_szsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dzsscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_czsscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zzsscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dsdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_csdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zsdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sddscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dddscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cddscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zddscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_scdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dcdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_ccdscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zcdscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_szdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dzdscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_czdscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zzdscal2jris( ar, ai, xr, xi, yr, yi ) bli_roscal2jris( ar, ai, xr, xi, yr, yi )

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dscscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cscscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zscscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sdcscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_ddcscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cdcscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zdcscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sccscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dccscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cccscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zccscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_szcscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dzcscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_czcscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zzcscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dszscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cszscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zszscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sdzscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_ddzscal2jris( ar, ai, xr, xi, yr, yi ) bli_rxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cdzscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zdzscal2jris( ar, ai, xr, xi, yr, yi ) bli_rcscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_sczscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dczscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cczscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zczscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )

#define bli_szzscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dzzscal2jris( ar, ai, xr, xi, yr, yi ) bli_crscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_czzscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zzzscal2jris( ar, ai, xr, xi, yr, yi ) bli_cxscal2jris( ar, ai, xr, xi, yr, yi )



#define bli_sscal2jris( ar, ai, xr, xi, yr, yi ) bli_sssscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_dscal2jris( ar, ai, xr, xi, yr, yi ) bli_dddscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_cscal2jris( ar, ai, xr, xi, yr, yi ) bli_cccscal2jris( ar, ai, xr, xi, yr, yi )
#define bli_zscal2jris( ar, ai, xr, xi, yr, yi ) bli_zzzscal2jris( ar, ai, xr, xi, yr, yi )

#endif

