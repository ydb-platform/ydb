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

#ifndef BLIS_SWAPS_H
#define BLIS_SWAPS_H

// swaps

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.


#define bli_ssswaps( x, y ) \
{ \
	float    w; \
	bli_sscopys( (y), (w) ); \
	bli_sscopys( (x), (y) ); \
	bli_sscopys( (w), (x) ); \
}
#define bli_dsswaps( x, y ) \
{ \
	double   w; \
	bli_sdcopys( (y), (w) ); \
	bli_dscopys( (x), (y) ); \
	bli_ddcopys( (w), (x) ); \
}
#define bli_csswaps( x, y ) \
{ \
	scomplex w; \
	bli_sccopys( (y), (w) ); \
	bli_cscopys( (x), (y) ); \
	bli_cccopys( (w), (x) ); \
}
#define bli_zsswaps( x, y ) \
{ \
	dcomplex w; \
	bli_szcopys( (y), (w) ); \
	bli_zscopys( (x), (y) ); \
	bli_zzcopys( (w), (x) ); \
}


#define bli_sdswaps( x, y ) \
{ \
	float    w; \
	bli_dscopys( (y), (w) ); \
	bli_sdcopys( (x), (y) ); \
	bli_sscopys( (w), (x) ); \
}
#define bli_ddswaps( x, y ) \
{ \
	double   w; \
	bli_ddcopys( (y), (w) ); \
	bli_ddcopys( (x), (y) ); \
	bli_ddcopys( (w), (x) ); \
}
#define bli_cdswaps( x, y ) \
{ \
	scomplex w; \
	bli_dccopys( (y), (w) ); \
	bli_cdcopys( (x), (y) ); \
	bli_cccopys( (w), (x) ); \
}
#define bli_zdswaps( x, y ) \
{ \
	dcomplex w; \
	bli_dzcopys( (y), (w) ); \
	bli_zdcopys( (x), (y) ); \
	bli_zzcopys( (w), (x) ); \
}


#define bli_scswaps( x, y ) \
{ \
	float    w; \
	bli_cscopys( (y), (w) ); \
	bli_sccopys( (x), (y) ); \
	bli_sscopys( (w), (x) ); \
}
#define bli_dcswaps( x, y ) \
{ \
	double   w; \
	bli_cdcopys( (y), (w) ); \
	bli_dccopys( (x), (y) ); \
	bli_ddcopys( (w), (x) ); \
}
#define bli_ccswaps( x, y ) \
{ \
	scomplex w; \
	bli_cccopys( (y), (w) ); \
	bli_cccopys( (x), (y) ); \
	bli_cccopys( (w), (x) ); \
}
#define bli_zcswaps( x, y ) \
{ \
	dcomplex w; \
	bli_czcopys( (y), (w) ); \
	bli_zccopys( (x), (y) ); \
	bli_zzcopys( (w), (x) ); \
}


#define bli_szswaps( x, y ) \
{ \
	float    w; \
	bli_zscopys( (y), (w) ); \
	bli_szcopys( (x), (y) ); \
	bli_sscopys( (w), (x) ); \
}
#define bli_dzswaps( x, y ) \
{ \
	double   w; \
	bli_zdcopys( (y), (w) ); \
	bli_dzcopys( (x), (y) ); \
	bli_ddcopys( (w), (x) ); \
}
#define bli_czswaps( x, y ) \
{ \
	scomplex w; \
	bli_zccopys( (y), (w) ); \
	bli_czcopys( (x), (y) ); \
	bli_cccopys( (w), (x) ); \
}
#define bli_zzswaps( x, y ) \
{ \
	dcomplex w; \
	bli_zzcopys( (y), (w) ); \
	bli_zzcopys( (x), (y) ); \
	bli_zzcopys( (w), (x) ); \
}


#define bli_sswaps( x, y )  bli_ssswaps( x, y )
#define bli_dswaps( x, y )  bli_ddswaps( x, y )
#define bli_cswaps( x, y )  bli_ccswaps( x, y )
#define bli_zswaps( x, y )  bli_zzswaps( x, y )


#endif
