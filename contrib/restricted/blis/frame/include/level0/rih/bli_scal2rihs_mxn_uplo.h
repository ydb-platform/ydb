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

#ifndef BLIS_SCAL2RIHS_MXN_UPLO_H
#define BLIS_SCAL2RIHS_MXN_UPLO_H

// scal2rihs_mxn_uplo

#define bli_cscal2rihs_mxn_uplo( schema, uplo, conjx, m, a, x, rs_x, cs_x, y_r, rs_y, cs_y ) \
{ \
	dim_t _i, _j; \
\
	/* Handle ro, io, and rpi separately. */ \
	if ( bli_is_ro_packed( schema ) ) \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2jros( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2ros( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2jros( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2ros( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_io_packed( schema ) ) \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2jios( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2ios( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2jios( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2ios( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_rpi_packed( schema ) ) */ \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2jrpis( *(a), \
					                 *(x   + _i*rs_x + _j*cs_x), \
					                 *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_cscal2rpis( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2jrpis( *(a), \
					                 *(x   + _i*rs_x + _j*cs_x), \
					                 *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_cscal2rpis( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
}

#define bli_zscal2rihs_mxn_uplo( schema, uplo, conjx, m, a, x, rs_x, cs_x, y_r, rs_y, cs_y ) \
{ \
	dim_t _i, _j; \
\
	/* Handle ro, io, and rpi separately. */ \
	if ( bli_is_ro_packed( schema ) ) \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2jros( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2ros( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2jros( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2ros( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
	else if ( bli_is_io_packed( schema ) ) \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2jios( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2ios( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2jios( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2ios( *(a), \
					               *(x   + _i*rs_x + _j*cs_x), \
					               *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_rpi_packed( schema ) ) */ \
	{ \
		if ( bli_is_lower( uplo ) ) \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2jrpis( *(a), \
					                 *(x   + _i*rs_x + _j*cs_x), \
					                 *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = _j; _i < m; ++_i ) \
				{ \
					bli_zscal2rpis( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
		else /* if ( bli_is_upper( uplo ) ) */ \
		{ \
			if ( bli_is_conj( conjx ) ) \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2jrpis( *(a), \
					                 *(x   + _i*rs_x + _j*cs_x), \
					                 *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
			else /* if ( bli_is_noconj( conjx ) ) */ \
			{ \
				for ( _j = 0; _j < m; ++_j ) \
				for ( _i = 0; _i < _j + 1; ++_i ) \
				{ \
					bli_zscal2rpis( *(a), \
					                *(x   + _i*rs_x + _j*cs_x), \
					                *(y_r + _i*rs_y + _j*cs_y) ); \
				} \
			} \
		} \
	} \
}

#endif
