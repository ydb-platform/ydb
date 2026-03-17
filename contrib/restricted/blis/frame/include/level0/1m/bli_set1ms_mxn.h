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

#ifndef BLIS_SET1MS_MXN_H
#define BLIS_SET1MS_MXN_H

// set1ms_mxn

#define bli_sset1ms_mxn( schema, offm, offn, m, n, a, y, rs_y, cs_y, ld_y ) \
{ \
	/* Include real domain version to facilitate macro-izing mixed-datatype
	   components of packm. */ \
}

#define bli_dset1ms_mxn( schema, offm, offn, m, n, a, y, rs_y, cs_y, ld_y ) \
{ \
	/* Include real domain version to facilitate macro-izing mixed-datatype
	   components of packm. */ \
}

BLIS_INLINE void bli_cset1ms_mxn
     (
       const pack_t       schema,
       const dim_t        offm,
       const dim_t        offn,
       const dim_t        m,
       const dim_t        n,
       scomplex* restrict alpha,
       scomplex* restrict y, const inc_t rs_y, const inc_t cs_y, const inc_t ld_y
     )
{
	inc_t offm_local = offm;
	inc_t offn_local = offn;
	dim_t m_local    = m;
	dim_t n_local    = n;
	inc_t rs_y1      = rs_y;
	inc_t cs_y1      = cs_y;
	inc_t rs_y2      = rs_y;
	inc_t cs_y2      = cs_y;
	dim_t i, j;

	/* Optimization: The loops walk through y with unit stride if y is
	   column-stored. If y is row-stored, swap the dimensions and strides
	   to preserve unit stride movement. */
	if ( cs_y == 1 )
	{
		bli_swap_incs( &offm_local, &offn_local );
		bli_swap_dims( &m_local, &n_local );
		bli_swap_incs( &rs_y1, &cs_y1 );
		bli_swap_incs( &rs_y2, &cs_y2 );
	}

	/* Handle 1e and 1r separately. */
	if ( bli_is_1e_packed( schema ) )
	{
		scomplex* restrict y_off_ri = y + (offm_local  )*rs_y1
		                                + (offn_local  )*cs_y1;
		scomplex* restrict y_off_ir = y + (offm_local  )*rs_y1
		                                + (offn_local  )*cs_y1 + ld_y/2;

		for ( j = 0; j < n_local; ++j )
		for ( i = 0; i < m_local; ++i )
		{
			bli_ccopy1es( *(alpha),
			              *(y_off_ri + i*rs_y1 + j*cs_y1),
			              *(y_off_ir + i*rs_y1 + j*cs_y1) );
		}
	}
	else /* if ( bli_is_1r_packed( schema ) ) */
	{
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; }
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; }

		float*    restrict y_cast  = ( float* )y;
		float*    restrict y_off_r = y_cast + (offm_local  )*rs_y2
		                                    + (offn_local  )*cs_y2;
		float*    restrict y_off_i = y_cast + (offm_local  )*rs_y2
		                                    + (offn_local  )*cs_y2 + ld_y;

		for ( j = 0; j < n_local; ++j )
		for ( i = 0; i < m_local; ++i )
		{
			bli_ccopy1rs( *(alpha),
			              *(y_off_r + i*rs_y2 + j*cs_y2),
			              *(y_off_i + i*rs_y2 + j*cs_y2) );
		}
	}
}

BLIS_INLINE void bli_zset1ms_mxn
     (
       const pack_t       schema,
       const dim_t        offm,
       const dim_t        offn,
       const dim_t        m,
       const dim_t        n,
       dcomplex* restrict alpha,
       dcomplex* restrict y, const inc_t rs_y, const inc_t cs_y, const inc_t ld_y
     )
{
	inc_t offm_local = offm;
	inc_t offn_local = offn;
	dim_t m_local    = m;
	dim_t n_local    = n;
	inc_t rs_y1      = rs_y;
	inc_t cs_y1      = cs_y;
	inc_t rs_y2      = rs_y;
	inc_t cs_y2      = cs_y;
	dim_t i, j;

	/* Optimization: The loops walk through y with unit stride if y is
	   column-stored. If y is row-stored, swap the dimensions and strides
	   to preserve unit stride movement. */
	if ( cs_y == 1 )
	{
		bli_swap_incs( &offm_local, &offn_local );
		bli_swap_dims( &m_local, &n_local );
		bli_swap_incs( &rs_y1, &cs_y1 );
		bli_swap_incs( &rs_y2, &cs_y2 );
	}

	/* Handle 1e and 1r separately. */
	if ( bli_is_1e_packed( schema ) )
	{
		dcomplex* restrict y_off_ri = y + (offm_local  )*rs_y1
		                                + (offn_local  )*cs_y1;
		dcomplex* restrict y_off_ir = y + (offm_local  )*rs_y1
		                                + (offn_local  )*cs_y1 + ld_y/2;

		for ( j = 0; j < n_local; ++j )
		for ( i = 0; i < m_local; ++i )
		{
			bli_zcopy1es( *(alpha),
			              *(y_off_ri + i*rs_y1 + j*cs_y1),
			              *(y_off_ir + i*rs_y1 + j*cs_y1) );
		}
	}
	else /* if ( bli_is_1r_packed( schema ) ) */
	{
		/* Scale the non-unit stride by two for the 1r loop, which steps
		   in units of real (not complex) values. */
		if         ( rs_y2 == 1 )    { cs_y2 *= 2; }
		else /* if ( cs_y2 == 1 ) */ { rs_y2 *= 2; }

		double*   restrict y_cast  = ( double* )y;
		double*   restrict y_off_r = y_cast + (offm_local  )*rs_y2
		                                    + (offn_local  )*cs_y2;
		double*   restrict y_off_i = y_cast + (offm_local  )*rs_y2
		                                    + (offn_local  )*cs_y2 + ld_y;

		for ( j = 0; j < n_local; ++j )
		for ( i = 0; i < m_local; ++i )
		{
			bli_zcopy1rs( *(alpha),
			              *(y_off_r + i*rs_y2 + j*cs_y2),
			              *(y_off_i + i*rs_y2 + j*cs_y2) );
		}
	}
}

#endif
