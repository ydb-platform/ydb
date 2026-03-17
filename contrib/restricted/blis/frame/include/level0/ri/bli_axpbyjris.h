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

#ifndef BLIS_AXPBYJRIS_H
#define BLIS_AXPBYJRIS_H

// axpbyjris

#define bli_rxaxpbyjris( ar, ai, xr, xi, br, bi, yr, yi ) \
{ \
    (yr) = (ar) * (xr) + (br) * (yr); \
}

#define bli_cxaxpbyjris( ar, ai, xr, xi, br, bi, yr, yi ) \
{ \
    const __typeof__(yr) yt_r = (ar) * (xr) + (ai) * (xi) + (br) * (yr) - (bi) * (yi); \
    const __typeof__(yi) yt_i = (ai) * (xr) - (ar) * (xi) + (bi) * (yr) + (br) * (yi); \
    (yr) = yt_r; \
    (yi) = yt_i; \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of b.
// - The fourth char encodes the type of y.

// -- (axby) = (??ss) ----------------------------------------------------------

#define bli_ssssxpbyjris  bli_rxxpbyjris
#define bli_dsssxpbyjris  bli_rxxpbyjris
#define bli_csssxpbyjris  bli_rxxpbyjris
#define bli_zsssxpbyjris  bli_rxxpbyjris

#define bli_sdssxpbyjris  bli_rxxpbyjris
#define bli_ddssxpbyjris  bli_rxxpbyjris
#define bli_cdssxpbyjris  bli_rxxpbyjris
#define bli_zdssxpbyjris  bli_rxxpbyjris

#define bli_scssxpbyjris  bli_rxxpbyjris
#define bli_dcssxpbyjris  bli_rxxpbyjris
#define bli_ccssxpbyjris  bli_rxxpbyjris
#define bli_zcssxpbyjris  bli_rxxpbyjris

#define bli_szssxpbyjris  bli_rxxpbyjris
#define bli_dzssxpbyjris  bli_rxxpbyjris
#define bli_czssxpbyjris  bli_rxxpbyjris
#define bli_zzssxpbyjris  bli_rxxpbyjris

// NOTE: This series needs to be finished for all other char values for (by), but
// not until something in BLIS actually needs mixed-datatype axpbyjris.


#define bli_saxpbyjris    bli_ssssaxpbyjris
#define bli_daxpbyjris    bli_ddddaxpbyjris
#define bli_caxpbyjris    bli_ccccaxpbyjris
#define bli_zaxpbyjris    bli_zzzzaxpbyjris

#endif

