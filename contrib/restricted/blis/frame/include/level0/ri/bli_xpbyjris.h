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

#ifndef BLIS_XPBYJRIS_H
#define BLIS_XPBYJRIS_H

// xpbyjris

#define bli_rxxpbyjris( xr, xi, br, bi, yr, yi ) \
{ \
	(yr) = (xr) + (br) * (yr); \
}

#define bli_cxxpbyjris( xr, xi, br, bi, yr, yi ) \
{ \
	const __typeof__(yr) yt_r =  (xr) + (br) * (yr) - (bi) * (yi); \
	const __typeof__(yi) yt_i = -(xi) + (bi) * (yr) + (br) * (yi); \
	(yr) = yt_r; \
	(yi) = yt_i; \
}

#define bli_crxpbyjris( xr, xi, br, bi, yr, yi ) \
{ \
	const __typeof__(yr) yt_r =  (xr) + (br) * (yr); \
	const __typeof__(yi) yt_i = -(xi) + (br) * (yi); \
	(yr) = yt_r; \
	(yi) = yt_i; \
}

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of b.
// - The third char encodes the type of y.

// -- (xby) = (??s) ------------------------------------------------------------

#define bli_sssxpbyjris  bli_rxxpbyjris
#define bli_dssxpbyjris  bli_rxxpbyjris
#define bli_cssxpbyjris  bli_rxxpbyjris
#define bli_zssxpbyjris  bli_rxxpbyjris

#define bli_sdsxpbyjris  bli_rxxpbyjris
#define bli_ddsxpbyjris  bli_rxxpbyjris
#define bli_cdsxpbyjris  bli_rxxpbyjris
#define bli_zdsxpbyjris  bli_rxxpbyjris

#define bli_scsxpbyjris  bli_rxxpbyjris
#define bli_dcsxpbyjris  bli_rxxpbyjris
#define bli_ccsxpbyjris  bli_rxxpbyjris
#define bli_zcsxpbyjris  bli_rxxpbyjris

#define bli_szsxpbyjris  bli_rxxpbyjris
#define bli_dzsxpbyjris  bli_rxxpbyjris
#define bli_czsxpbyjris  bli_rxxpbyjris
#define bli_zzsxpbyjris  bli_rxxpbyjris

// -- (xby) = (??d) ------------------------------------------------------------

#define bli_ssdxpbyjris  bli_rxxpbyjris
#define bli_dsdxpbyjris  bli_rxxpbyjris
#define bli_csdxpbyjris  bli_rxxpbyjris
#define bli_zsdxpbyjris  bli_rxxpbyjris

#define bli_sddxpbyjris  bli_rxxpbyjris
#define bli_dddxpbyjris  bli_rxxpbyjris
#define bli_cddxpbyjris  bli_rxxpbyjris
#define bli_zddxpbyjris  bli_rxxpbyjris

#define bli_scdxpbyjris  bli_rxxpbyjris
#define bli_dcdxpbyjris  bli_rxxpbyjris
#define bli_ccdxpbyjris  bli_rxxpbyjris
#define bli_zcdxpbyjris  bli_rxxpbyjris

#define bli_szdxpbyjris  bli_rxxpbyjris
#define bli_dzdxpbyjris  bli_rxxpbyjris
#define bli_czdxpbyjris  bli_rxxpbyjris
#define bli_zzdxpbyjris  bli_rxxpbyjris

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbyjris  bli_rxxpbyjris
#define bli_dscxpbyjris  bli_rxxpbyjris
#define bli_cscxpbyjris  bli_crxpbyjris
#define bli_zscxpbyjris  bli_crxpbyjris

#define bli_sdcxpbyjris  bli_rxxpbyjris
#define bli_ddcxpbyjris  bli_rxxpbyjris
#define bli_cdcxpbyjris  bli_crxpbyjris
#define bli_zdcxpbyjris  bli_crxpbyjris

#define bli_sccxpbyjris  bli_cxxpbyjris
#define bli_dccxpbyjris  bli_cxxpbyjris
#define bli_cccxpbyjris  bli_cxxpbyjris
#define bli_zccxpbyjris  bli_cxxpbyjris

#define bli_szcxpbyjris  bli_cxxpbyjris
#define bli_dzcxpbyjris  bli_cxxpbyjris
#define bli_czcxpbyjris  bli_cxxpbyjris
#define bli_zzcxpbyjris  bli_cxxpbyjris

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbyjris  bli_rxxpbyjris
#define bli_dszxpbyjris  bli_rxxpbyjris
#define bli_cszxpbyjris  bli_crxpbyjris
#define bli_zszxpbyjris  bli_crxpbyjris

#define bli_sdzxpbyjris  bli_rxxpbyjris
#define bli_ddzxpbyjris  bli_rxxpbyjris
#define bli_cdzxpbyjris  bli_crxpbyjris
#define bli_zdzxpbyjris  bli_crxpbyjris

#define bli_sczxpbyjris  bli_cxxpbyjris
#define bli_dczxpbyjris  bli_cxxpbyjris
#define bli_cczxpbyjris  bli_cxxpbyjris
#define bli_zczxpbyjris  bli_cxxpbyjris

#define bli_szzxpbyjris  bli_cxxpbyjris
#define bli_dzzxpbyjris  bli_cxxpbyjris
#define bli_czzxpbyjris  bli_cxxpbyjris
#define bli_zzzxpbyjris  bli_cxxpbyjris



#define bli_sxpbyjris    bli_sssxpbyjris
#define bli_dxpbyjris    bli_dddxpbyjris
#define bli_cxpbyjris    bli_cccxpbyjris
#define bli_zxpbyjris    bli_zzzxpbyjris

#endif

