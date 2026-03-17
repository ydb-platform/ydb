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

#ifndef BLIS_SCAL2RIHS_MXN_H
#define BLIS_SCAL2RIHS_MXN_H

// scal2rihs_mxn

BLIS_INLINE void bli_cscal2rihs_mxn
     (
       const pack_t       schema,
       const conj_t       conjx,
       const dim_t        m,
       const dim_t        n,
       scomplex* restrict alpha,
       scomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
       scomplex* restrict y, const inc_t rs_y, const inc_t cs_y
     )
{
	scomplex* restrict x_r =            x;
	float*    restrict y_r = ( float*  )y;

	if ( bli_is_ro_packed( schema ) )
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2jros
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2ros
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
	else if ( bli_is_io_packed( schema ) )
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2jios
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2ios
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
	else /* if ( bli_is_rpi_packed( schema ) ) */
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2jrpis
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				scomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				float*    restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_cscal2rpis
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
}

BLIS_INLINE void bli_zscal2rihs_mxn
     (
       const pack_t       schema,
       const conj_t       conjx,
       const dim_t        m,
       const dim_t        n,
       dcomplex* restrict alpha,
       dcomplex* restrict x, const inc_t rs_x, const inc_t cs_x,
       dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y
     )
{
	dcomplex* restrict x_r =            x;
	double*   restrict y_r = ( double* )y;

	if ( bli_is_ro_packed( schema ) )
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2jros
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2ros
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
	else if ( bli_is_io_packed( schema ) )
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2jios
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2ios
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
	else /* if ( bli_is_rpi_packed( schema ) ) */
	{
		if ( bli_is_conj( conjx ) )
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2jrpis
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
		else /* if ( bli_is_noconj( conjx ) ) */
		{
			for ( dim_t j = 0; j < n; ++j )
			for ( dim_t i = 0; i < m; ++i )
			{
				dcomplex* restrict chi11   = x_r + (i  )*rs_x + (j  )*cs_x;
				double*   restrict psi11_r = y_r + (i  )*rs_y + (j  )*cs_y;

				bli_zscal2rpis
				(
				  *alpha,
				  *chi11,
				  *psi11_r 
				);
			}
		}
	}
}


#endif
