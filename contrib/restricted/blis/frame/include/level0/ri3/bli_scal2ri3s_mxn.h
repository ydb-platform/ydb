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

#ifndef BLIS_SCAL2RI3S_MXN_H
#define BLIS_SCAL2RI3S_MXN_H

// scal2ri3s_mxn

BLIS_INLINE void bli_cscal2ri3s_mxn
     (
       const conj_t       conjx,
       const dim_t        m,
       const dim_t        n,
       scomplex* restrict alpha,
       scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
       scomplex* restrict y, const inc_t rs_y, const inc_t cs_y, const inc_t is_y
     )
{
	float*  restrict alpha_r = ( float*  )alpha; \
	float*  restrict alpha_i = ( float*  )alpha + 1; \
	float*  restrict x_r     = ( float*  )x; \
	float*  restrict x_i     = ( float*  )x + 1; \
	float*  restrict y_r     = ( float*  )y; \
	float*  restrict y_i     = ( float*  )y +   is_y; \
	float*  restrict y_rpi   = ( float*  )y + 2*is_y; \
	const dim_t      incx2   = 2*rs_x; \
	const dim_t      ldx2    = 2*cs_x; \

	/* Treat the micro-panel as panel_dim x panel_len and column-stored
	   (unit row stride). */ \

	if ( bli_is_conj( conjx ) )
	{
		for ( dim_t j = 0; j < n; ++j )
		for ( dim_t i = 0; i < m; ++i )
		{
			float*  restrict chi11_r   = x_r   + (i  )*incx2 + (j  )*ldx2;
			float*  restrict chi11_i   = x_i   + (i  )*incx2 + (j  )*ldx2;
			float*  restrict psi11_r   = y_r   + (i  )*1     + (j  )*cs_y;
			float*  restrict psi11_i   = y_i   + (i  )*1     + (j  )*cs_y;
			float*  restrict psi11_rpi = y_rpi + (i  )*1     + (j  )*cs_y;

			bli_cscal2jri3s
			(
			  *alpha_r,
			  *alpha_i,
			  *chi11_r,
			  *chi11_i,
			  *psi11_r,
			  *psi11_i,
			  *psi11_rpi
			);
		}
	}
	else /* if ( bli_is_noconj( conjx ) ) */
	{
		for ( dim_t j = 0; j < n; ++j )
		for ( dim_t i = 0; i < m; ++i )
		{
			float*  restrict chi11_r   = x_r   + (i  )*incx2 + (j  )*ldx2;
			float*  restrict chi11_i   = x_i   + (i  )*incx2 + (j  )*ldx2;
			float*  restrict psi11_r   = y_r   + (i  )*1     + (j  )*cs_y;
			float*  restrict psi11_i   = y_i   + (i  )*1     + (j  )*cs_y;
			float*  restrict psi11_rpi = y_rpi + (i  )*1     + (j  )*cs_y;

			bli_cscal2ri3s
			(
			  *alpha_r,
			  *alpha_i,
			  *chi11_r,
			  *chi11_i,
			  *psi11_r,
			  *psi11_i,
			  *psi11_rpi
			);
		}
	}
}

BLIS_INLINE void bli_zscal2ri3s_mxn
     (
       const conj_t       conjx,
       const dim_t        m,
       const dim_t        n,
       dcomplex* restrict alpha,
       dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
       dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y, const inc_t is_y
     )
{
	double* restrict alpha_r = ( double* )alpha; \
	double* restrict alpha_i = ( double* )alpha + 1; \
	double* restrict x_r     = ( double* )x; \
	double* restrict x_i     = ( double* )x + 1; \
	double* restrict y_r     = ( double* )y; \
	double* restrict y_i     = ( double* )y +   is_y; \
	double* restrict y_rpi   = ( double* )y + 2*is_y; \
	const dim_t      incx2   = 2*rs_x; \
	const dim_t      ldx2    = 2*cs_x; \

	/* Treat the micro-panel as panel_dim x panel_len and column-stored
	   (unit row stride). */ \

	if ( bli_is_conj( conjx ) )
	{
		for ( dim_t j = 0; j < n; ++j )
		for ( dim_t i = 0; i < m; ++i )
		{
			double* restrict chi11_r   = x_r   + (i  )*incx2 + (j  )*ldx2;
			double* restrict chi11_i   = x_i   + (i  )*incx2 + (j  )*ldx2;
			double* restrict psi11_r   = y_r   + (i  )*1     + (j  )*cs_y;
			double* restrict psi11_i   = y_i   + (i  )*1     + (j  )*cs_y;
			double* restrict psi11_rpi = y_rpi + (i  )*1     + (j  )*cs_y;

			bli_zscal2jri3s
			(
			  *alpha_r,
			  *alpha_i,
			  *chi11_r,
			  *chi11_i,
			  *psi11_r,
			  *psi11_i,
			  *psi11_rpi
			);
		}
	}
	else /* if ( bli_is_noconj( conjx ) ) */
	{
		for ( dim_t j = 0; j < n; ++j )
		for ( dim_t i = 0; i < m; ++i )
		{
			double* restrict chi11_r   = x_r   + (i  )*incx2 + (j  )*ldx2;
			double* restrict chi11_i   = x_i   + (i  )*incx2 + (j  )*ldx2;
			double* restrict psi11_r   = y_r   + (i  )*1     + (j  )*cs_y;
			double* restrict psi11_i   = y_i   + (i  )*1     + (j  )*cs_y;
			double* restrict psi11_rpi = y_rpi + (i  )*1     + (j  )*cs_y;

			bli_zscal2ri3s
			(
			  *alpha_r,
			  *alpha_i,
			  *chi11_r,
			  *chi11_i,
			  *psi11_r,
			  *psi11_i,
			  *psi11_rpi
			);
		}
	}
}


#endif
