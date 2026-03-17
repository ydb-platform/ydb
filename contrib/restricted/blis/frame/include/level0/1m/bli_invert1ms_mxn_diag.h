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

#ifndef BLIS_INVERT1MS_MXN_DIAG_H
#define BLIS_INVERT1MS_MXN_DIAG_H

// invert1ms_mxn_diag

#define bli_cinvert1ms_mxn_diag( schema, offm, offn, m, n, y, rs_y, cs_y, ld_y ) \
{ \
	dim_t min_m_n = bli_min( m, n ); \
	dim_t i; \
\
	/* Handle 1e and 1r separately. */ \
	if ( bli_is_1e_packed( schema ) ) \
	{ \
		scomplex* restrict y_off_ri = y + (offm  )*rs_y \
		                                + (offn  )*cs_y; \
		scomplex* restrict y_off_ir = y + (offm  )*rs_y \
		                                + (offn  )*cs_y + ld_y/2; \
\
		for ( i = 0; i < min_m_n; ++i ) \
		{ \
			bli_cinvert1es( *(y_off_ri + i*rs_y + i*cs_y), \
			                *(y_off_ir + i*rs_y + i*cs_y) ); \
		} \
	} \
	else /* if ( bli_is_1r_packed( schema ) ) */ \
	{ \
		inc_t rs_y2 = rs_y; \
		inc_t cs_y2 = cs_y; \
\
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */ \
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; } \
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; } \
\
		float*    restrict y_cast  = ( float* )y; \
		float*    restrict y_off_r = y_cast + (offm  )*rs_y2 \
		                                    + (offn  )*cs_y2; \
		float*    restrict y_off_i = y_cast + (offm  )*rs_y2 \
		                                    + (offn  )*cs_y2 + ld_y; \
\
		for ( i = 0; i < min_m_n; ++i ) \
		{ \
			bli_cinvert1rs( *(y_off_r + i*rs_y2 + i*cs_y2), \
			                *(y_off_i + i*rs_y2 + i*cs_y2) ); \
		} \
	} \
}

#define bli_zinvert1ms_mxn_diag( schema, offm, offn, m, n, y, rs_y, cs_y, ld_y ) \
{ \
	dim_t min_m_n = bli_min( m, n ); \
	dim_t i; \
\
	/* Handle 1e and 1r separately. */ \
	if ( bli_is_1e_packed( schema ) ) \
	{ \
		dcomplex* restrict y_off_ri = y + (offm  )*rs_y \
		                                + (offn  )*cs_y; \
		dcomplex* restrict y_off_ir = y + (offm  )*rs_y \
		                                + (offn  )*cs_y + ld_y/2; \
\
		for ( i = 0; i < min_m_n; ++i ) \
		{ \
			bli_zinvert1es( *(y_off_ri + i*rs_y + i*cs_y), \
			                *(y_off_ir + i*rs_y + i*cs_y) ); \
		} \
	} \
	else /* if ( bli_is_1r_packed( schema ) ) */ \
	{ \
		inc_t rs_y2 = rs_y; \
		inc_t cs_y2 = cs_y; \
\
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */ \
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; } \
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; } \
\
		double*   restrict y_cast  = ( double* )y; \
		double*   restrict y_off_r = y_cast + (offm  )*rs_y2 \
		                                    + (offn  )*cs_y2; \
		double*   restrict y_off_i = y_cast + (offm  )*rs_y2 \
		                                    + (offn  )*cs_y2 + ld_y; \
\
		for ( i = 0; i < min_m_n; ++i ) \
		{ \
			bli_zinvert1rs( *(y_off_r + i*rs_y2 + i*cs_y2), \
			                *(y_off_i + i*rs_y2 + i*cs_y2) ); \
		} \
	} \
}

#endif
