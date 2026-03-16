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

#ifndef BLIS_XPBYS_MXN_H
#define BLIS_XPBYS_MXN_H

// xpbys_mxn

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of b.
// - The third char encodes the type of y.


// -- (xby) = (?ss) ------------------------------------------------------------

BLIS_INLINE void bli_sssxpbys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            float*    restrict beta,
                                                            float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_seq0( *beta ) )
	{
		bli_sscopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sssxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sssxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sssxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dssxpbys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            float*    restrict beta,
                                                            float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_seq0( *beta ) )
	{
		bli_dscopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dssxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dssxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dssxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cssxpbys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            float*    restrict beta,
                                                            float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_seq0( *beta ) )
	{
		bli_cscopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cssxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cssxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cssxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zssxpbys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            float*    restrict beta,
                                                            float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_seq0( *beta ) )
	{
		bli_zscopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zssxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zssxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zssxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}

// -- (xby) = (?dd) ------------------------------------------------------------

BLIS_INLINE void bli_sddxpbys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            double*   restrict beta,
                                                            double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_deq0( *beta ) )
	{
		bli_sdcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sddxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sddxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sddxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dddxpbys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            double*   restrict beta,
                                                            double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_deq0( *beta ) )
	{
		bli_ddcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dddxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dddxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dddxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cddxpbys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            double*   restrict beta,
                                                            double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_deq0( *beta ) )
	{
		bli_cdcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cddxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cddxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cddxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zddxpbys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            double*   restrict beta,
                                                            double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_deq0( *beta ) )
	{
		bli_zdcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zddxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zddxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zddxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}

// -- (xby) = (?cc) ------------------------------------------------------------

BLIS_INLINE void bli_sccxpbys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            scomplex* restrict beta,
                                                            scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_ceq0( *beta ) )
	{
		bli_sccopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sccxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_sccxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_sccxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dccxpbys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            scomplex* restrict beta,
                                                            scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_ceq0( *beta ) )
	{
		bli_dccopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dccxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dccxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dccxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_cccxpbys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            scomplex* restrict beta,
                                                            scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_ceq0( *beta ) )
	{
		bli_cccopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cccxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_cccxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_cccxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zccxpbys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            scomplex* restrict beta,
                                                            scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_ceq0( *beta ) )
	{
		bli_zccopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zccxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zccxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zccxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}

// -- (xby) = (?zz) ------------------------------------------------------------

BLIS_INLINE void bli_szzxpbys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            dcomplex* restrict beta,
                                                            dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_zeq0( *beta ) )
	{
		bli_szcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szzxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_szzxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_szzxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_dzzxpbys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            dcomplex* restrict beta,
                                                            dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_zeq0( *beta ) )
	{
		bli_dzcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzzxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_dzzxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_dzzxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_czzxpbys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            dcomplex* restrict beta,
                                                            dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_zeq0( *beta ) )
	{
		bli_czcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czzxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_czzxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_czzxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}
BLIS_INLINE void bli_zzzxpbys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                            dcomplex* restrict beta,
                                                            dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	// If beta is zero, overwrite y with x (in case y has infs or NaNs).
	if ( bli_zeq0( *beta ) )
	{
		bli_zzcopys_mxn( m, n, x, rs_x, cs_x, y, rs_y, cs_y );
		return;
	}

#ifdef BLIS_ENABLE_CR_CASES
	if ( rs_x == 1 && rs_y == 1 )
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzzxpbys( *(x + ii + jj*cs_x), *beta,
		              *(y + ii + jj*cs_y) );
	}
	else if ( cs_x == 1 && cs_y == 1 )
	{
		for ( dim_t ii = 0; ii < m; ++ii )
		for ( dim_t jj = 0; jj < n; ++jj )
		bli_zzzxpbys( *(x + ii*rs_x + jj), *beta,
		              *(y + ii*rs_y + jj) );
	}
	else
#endif
	{
		for ( dim_t jj = 0; jj < n; ++jj )
		for ( dim_t ii = 0; ii < m; ++ii )
		bli_zzzxpbys( *(x + ii*rs_x + jj*cs_x), *beta,
		              *(y + ii*rs_y + jj*cs_y) );
	}
}



BLIS_INLINE void bli_sxpbys_mxn( const dim_t m, const dim_t n, float*    restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          float*    restrict beta,
                                                          float*    restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_sssxpbys_mxn( m, n, x, rs_x, cs_x, beta, y, rs_y, cs_y );
}
BLIS_INLINE void bli_dxpbys_mxn( const dim_t m, const dim_t n, double*   restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          double*   restrict beta,
                                                          double*   restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_dddxpbys_mxn( m, n, x, rs_x, cs_x, beta, y, rs_y, cs_y );
}
BLIS_INLINE void bli_cxpbys_mxn( const dim_t m, const dim_t n, scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          scomplex* restrict beta,
                                                          scomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_cccxpbys_mxn( m, n, x, rs_x, cs_x, beta, y, rs_y, cs_y );
}
BLIS_INLINE void bli_zxpbys_mxn( const dim_t m, const dim_t n, dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
                                                          dcomplex* restrict beta,
                                                          dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y )
{
	bli_zzzxpbys_mxn( m, n, x, rs_x, cs_x, beta, y, rs_y, cs_y );
}


#endif
