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

#ifndef BLIS_SETS_H
#define BLIS_SETS_H

// sets

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sssets( xr, xi, y )  { (y) = (xr); }
#define bli_dssets( xr, xi, y )  { (y) = (xr); }
#define bli_cssets( xr, xi, y )  { (y) = (xr); }
#define bli_zssets( xr, xi, y )  { (y) = (xr); }
#define bli_issets( xr, xi, y )  { (y) = (xr); }

#define bli_sdsets( xr, xi, y )  { (y) = (xr); }
#define bli_ddsets( xr, xi, y )  { (y) = (xr); }
#define bli_cdsets( xr, xi, y )  { (y) = (xr); }
#define bli_zdsets( xr, xi, y )  { (y) = (xr); }
#define bli_idsets( xr, xi, y )  { (y) = (xr); }

#ifndef BLIS_ENABLE_C99_COMPLEX 

#define bli_scsets( xr, xi, y ) { bli_creal(y) = (xr); bli_cimag(y) = (xi); }
#define bli_dcsets( xr, xi, y ) { bli_creal(y) = (xr); bli_cimag(y) = (xi); }
#define bli_ccsets( xr, xi, y ) { bli_creal(y) = (xr); bli_cimag(y) = (xi); }
#define bli_zcsets( xr, xi, y ) { bli_creal(y) = (xr); bli_cimag(y) = (xi); }
#define bli_icsets( xr, xi, y ) { bli_creal(y) = (xr); bli_cimag(y) = (xi); }

#define bli_szsets( xr, xi, y ) { bli_zreal(y) = (xr); bli_zimag(y) = (xi); }
#define bli_dzsets( xr, xi, y ) { bli_zreal(y) = (xr); bli_zimag(y) = (xi); }
#define bli_czsets( xr, xi, y ) { bli_zreal(y) = (xr); bli_zimag(y) = (xi); }
#define bli_zzsets( xr, xi, y ) { bli_zreal(y) = (xr); bli_zimag(y) = (xi); }
#define bli_izsets( xr, xi, y ) { bli_zreal(y) = (xr); bli_zimag(y) = (xi); }

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_dcsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_ccsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_zcsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }

#define bli_szsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_dzsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_czsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }
#define bli_zzsets( xr, xi, y )  { (y) = (xr) + (xi) * (I); }

#endif // BLIS_ENABLE_C99_COMPLEX

#define bli_sisets( xr, xi, y ) { (y) = bli_sreal(xr); }
#define bli_disets( xr, xi, y ) { (y) = bli_dreal(xr); }
#define bli_cisets( xr, xi, y ) { (y) = bli_creal(xr); }
#define bli_zisets( xr, xi, y ) { (y) = bli_zreal(xr); }
#define bli_iisets( xr, xi, y ) { (y) =          (xr); }


#define bli_ssets( xr, xi, y )  bli_sssets( xr, xi, y )
#define bli_dsets( xr, xi, y )  bli_ddsets( xr, xi, y )
#define bli_csets( xr, xi, y )  bli_scsets( xr, xi, y )
#define bli_zsets( xr, xi, y )  bli_dzsets( xr, xi, y )
#define bli_isets( xr, xi, y )  bli_disets( xr, xi, y )


#endif

