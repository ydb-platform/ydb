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

#ifndef BLIS_COPYS_MXN_H
#define BLIS_COPYS_MXN_H

// copys_mxn

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

// xy = ?s

BLIS_INLINE void bli_sscopys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sscopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sscopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sscopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dscopys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dscopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dscopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dscopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cscopys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cscopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cscopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cscopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zscopys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zscopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zscopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zscopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?d

BLIS_INLINE void bli_sdcopys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sdcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sdcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sdcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_ddcopys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ddcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_ddcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ddcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cdcopys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cdcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cdcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cdcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zdcopys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zdcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zdcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zdcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?c

BLIS_INLINE void bli_sccopys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sccopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sccopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sccopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dccopys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dccopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dccopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dccopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cccopys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cccopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cccopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cccopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zccopys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zccopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zccopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zccopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?c

BLIS_INLINE void bli_szcopys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_szcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dzcopys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dzcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_czcopys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_czcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zzcopys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                           dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzcopys( *(x + ii + jj*cs_x),
		             *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zzcopys( *(x + ii*rs_x + jj),
		             *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzcopys( *(x + ii*rs_x + jj*cs_x),
		             *(y + ii*rs_y + jj*cs_y) );
	}
}


BLIS_INLINE void bli_scopys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_sscopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_dcopys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_ddcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_ccopys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_cccopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_zcopys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_zzcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}

#endif
