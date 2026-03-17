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

#ifndef BLIS_AXPBYRIS_H
#define BLIS_AXPBYRIS_H

// axpbyris

#define bli_rxaxpbyris( ar, ai, xr, xi, br, bi, yr, yi ) \
{ \
    (yr) = (ar) * (xr) + (br) * (yr); \
}

#define bli_cxaxpbyris( ar, ai, xr, xi, br, bi, yr, yi ) \
{ \
    const __typeof__(yr) yt_r = (ar) * (xr) - (ai) * (xi) + (br) * (yr) - (bi) * (yi); \
    const __typeof__(yi) yt_i = (ai) * (xr) + (ar) * (xi) + (bi) * (yr) + (br) * (yi); \
    (yr) = yt_r; \
    (yi) = yt_i; \
}

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of b.
// - The fourth char encodes the type of y.

// -- (axby) = (??ss) ----------------------------------------------------------

#define bli_ssssxpbyris  bli_rxxpbyris
#define bli_dsssxpbyris  bli_rxxpbyris
#define bli_csssxpbyris  bli_rxxpbyris
#define bli_zsssxpbyris  bli_rxxpbyris

#define bli_sdssxpbyris  bli_rxxpbyris
#define bli_ddssxpbyris  bli_rxxpbyris
#define bli_cdssxpbyris  bli_rxxpbyris
#define bli_zdssxpbyris  bli_rxxpbyris

#define bli_scssxpbyris  bli_rxxpbyris
#define bli_dcssxpbyris  bli_rxxpbyris
#define bli_ccssxpbyris  bli_rxxpbyris
#define bli_zcssxpbyris  bli_rxxpbyris

#define bli_szssxpbyris  bli_rxxpbyris
#define bli_dzssxpbyris  bli_rxxpbyris
#define bli_czssxpbyris  bli_rxxpbyris
#define bli_zzssxpbyris  bli_rxxpbyris

// NOTE: This series needs to be finished for all other char values for (by), but
// not until something in BLIS actually needs mixed-datatype axpbyris.


#define bli_saxpbyris    bli_ssssaxpbyris
#define bli_daxpbyris    bli_ddddaxpbyris
#define bli_caxpbyris    bli_ccccaxpbyris
#define bli_zaxpbyris    bli_zzzzaxpbyris

#endif

