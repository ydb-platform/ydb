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

#ifndef BLIS_ADDS_MXN_H
#define BLIS_ADDS_MXN_H

// adds_mxn

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.


// xy = ?s

BLIS_INLINE void bli_ssadds_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ssadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_ssadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ssadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dsadds_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dsadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dsadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dsadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_csadds_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_csadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_csadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_csadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zsadds_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zsadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zsadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zsadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?d

BLIS_INLINE void bli_sdadds_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sdadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sdadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sdadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_ddadds_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ddadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_ddadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ddadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cdadds_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cdadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cdadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cdadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zdadds_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zdadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zdadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zdadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?c

BLIS_INLINE void bli_scadds_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_scadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_scadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_scadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dcadds_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dcadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dcadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dcadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_ccadds_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ccadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_ccadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_ccadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zcadds_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zcadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zcadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zcadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}

// xy = ?z

BLIS_INLINE void bli_szadds_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_szadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dzadds_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dzadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_czadds_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_czadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zzadds_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzadds( *(x + ii + jj*cs_x),
		            *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zzadds( *(x + ii*rs_x + jj),
		            *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzadds( *(x + ii*rs_x + jj*cs_x),
		            *(y + ii*rs_y + jj*cs_y) );
	}
}



BLIS_INLINE void bli_sadds_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                         float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_ssadds_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_dadds_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                         double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_ddadds_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_cadds_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                         scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_ccadds_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}
BLIS_INLINE void bli_zadds_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                         dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_zzadds_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
}


#endif
