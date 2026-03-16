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

#ifndef BLIS_SETRIHS_MXN_DIAG_H
#define BLIS_SETRIHS_MXN_DIAG_H

// setrihs_mxn_diag

#define bli_csetrihs_mxn_diag( schema, m, n, a, y_r, rs_y, cs_y ) \
{ \
	const float  a_r     = bli_zreal( *a ); \
	const float  a_i     = bli_zimag( *a ); \
	dim_t        min_m_n = bli_min( m, n ); \
	dim_t        _i; \
\
	/* Handle ro, io, and rpi separately. */ \
	if ( bli_is_ro_packed( schema ) ) \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_scopys(  (a_r), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
	else if ( bli_is_io_packed( schema ) ) \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_scopys(  (a_i), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
	else /* if ( bli_is_rpi_packed( schema ) ) */ \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_sadd3s(  (a_r), \
			             (a_i), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
}

#define bli_zsetrihs_mxn_diag( schema, m, n, a, y_r, rs_y, cs_y ) \
{ \
	const double a_r     = bli_zreal( *a ); \
	const double a_i     = bli_zimag( *a ); \
	dim_t        min_m_n = bli_min( m, n ); \
	dim_t        _i; \
\
	/* Handle ro, io, and rpi separately. */ \
	if ( bli_is_ro_packed( schema ) ) \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_dcopys(  (a_r), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
	else if ( bli_is_io_packed( schema ) ) \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_dcopys(  (a_i), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
	else /* if ( bli_is_rpi_packed( schema ) ) */ \
	{ \
		for ( _i = 0; _i < min_m_n; ++_i ) \
		{ \
			bli_dadd3s(  (a_r), \
			             (a_i), \
			            *(y_r + _i*rs_y + _i*cs_y) ); \
		} \
	} \
}

#endif
