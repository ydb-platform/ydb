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

#ifndef BLIS_XPBYRIS_H
#define BLIS_XPBYRIS_H

// xpbyris

#define bli_rxxpbyris( xr, xi, br, bi, yr, yi ) \
{ \
	(yr) = (xr) + (br) * (yr); \
}

#define bli_cxxpbyris( xr, xi, br, bi, yr, yi ) \
{ \
	const __typeof__(yr) yt_r = (xr) + (br) * (yr) - (bi) * (yi); \
	const __typeof__(yi) yt_i = (xi) + (bi) * (yr) + (br) * (yi); \
	(yr) = yt_r; \
	(yi) = yt_i; \
}

#define bli_crxpbyris( xr, xi, br, bi, yr, yi ) \
{ \
	const __typeof__(yr) yt_r = (xr) + (br) * (yr); \
	const __typeof__(yi) yt_i = (xi) + (br) * (yi); \
	(yr) = yt_r; \
	(yi) = yt_i; \
}

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of b.
// - The third char encodes the type of y.

// -- (xby) = (??s) ------------------------------------------------------------

#define bli_sssxpbyris  bli_rxxpbyris
#define bli_dssxpbyris  bli_rxxpbyris
#define bli_cssxpbyris  bli_rxxpbyris
#define bli_zssxpbyris  bli_rxxpbyris

#define bli_sdsxpbyris  bli_rxxpbyris
#define bli_ddsxpbyris  bli_rxxpbyris
#define bli_cdsxpbyris  bli_rxxpbyris
#define bli_zdsxpbyris  bli_rxxpbyris

#define bli_scsxpbyris  bli_rxxpbyris
#define bli_dcsxpbyris  bli_rxxpbyris
#define bli_ccsxpbyris  bli_rxxpbyris
#define bli_zcsxpbyris  bli_rxxpbyris

#define bli_szsxpbyris  bli_rxxpbyris
#define bli_dzsxpbyris  bli_rxxpbyris
#define bli_czsxpbyris  bli_rxxpbyris
#define bli_zzsxpbyris  bli_rxxpbyris

// -- (xby) = (??d) ------------------------------------------------------------

#define bli_ssdxpbyris  bli_rxxpbyris
#define bli_dsdxpbyris  bli_rxxpbyris
#define bli_csdxpbyris  bli_rxxpbyris
#define bli_zsdxpbyris  bli_rxxpbyris

#define bli_sddxpbyris  bli_rxxpbyris
#define bli_dddxpbyris  bli_rxxpbyris
#define bli_cddxpbyris  bli_rxxpbyris
#define bli_zddxpbyris  bli_rxxpbyris

#define bli_scdxpbyris  bli_rxxpbyris
#define bli_dcdxpbyris  bli_rxxpbyris
#define bli_ccdxpbyris  bli_rxxpbyris
#define bli_zcdxpbyris  bli_rxxpbyris

#define bli_szdxpbyris  bli_rxxpbyris
#define bli_dzdxpbyris  bli_rxxpbyris
#define bli_czdxpbyris  bli_rxxpbyris
#define bli_zzdxpbyris  bli_rxxpbyris

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbyris  bli_rxxpbyris
#define bli_dscxpbyris  bli_rxxpbyris
#define bli_cscxpbyris  bli_crxpbyris
#define bli_zscxpbyris  bli_crxpbyris

#define bli_sdcxpbyris  bli_rxxpbyris
#define bli_ddcxpbyris  bli_rxxpbyris
#define bli_cdcxpbyris  bli_crxpbyris
#define bli_zdcxpbyris  bli_crxpbyris

#define bli_sccxpbyris  bli_cxxpbyris
#define bli_dccxpbyris  bli_cxxpbyris
#define bli_cccxpbyris  bli_cxxpbyris
#define bli_zccxpbyris  bli_cxxpbyris

#define bli_szcxpbyris  bli_cxxpbyris
#define bli_dzcxpbyris  bli_cxxpbyris
#define bli_czcxpbyris  bli_cxxpbyris
#define bli_zzcxpbyris  bli_cxxpbyris

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbyris  bli_rxxpbyris
#define bli_dszxpbyris  bli_rxxpbyris
#define bli_cszxpbyris  bli_crxpbyris
#define bli_zszxpbyris  bli_crxpbyris

#define bli_sdzxpbyris  bli_rxxpbyris
#define bli_ddzxpbyris  bli_rxxpbyris
#define bli_cdzxpbyris  bli_crxpbyris
#define bli_zdzxpbyris  bli_crxpbyris

#define bli_sczxpbyris  bli_cxxpbyris
#define bli_dczxpbyris  bli_cxxpbyris
#define bli_cczxpbyris  bli_cxxpbyris
#define bli_zczxpbyris  bli_cxxpbyris

#define bli_szzxpbyris  bli_cxxpbyris
#define bli_dzzxpbyris  bli_cxxpbyris
#define bli_czzxpbyris  bli_cxxpbyris
#define bli_zzzxpbyris  bli_cxxpbyris



#define bli_sxpbyris    bli_sssxpbyris
#define bli_dxpbyris    bli_dddxpbyris
#define bli_cxpbyris    bli_cccxpbyris
#define bli_zxpbyris    bli_zzzxpbyris

#endif

