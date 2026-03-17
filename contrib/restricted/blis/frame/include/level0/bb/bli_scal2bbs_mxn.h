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

#ifndef BLIS_SCAL2BBS_MXN_H
#define BLIS_SCAL2BBS_MXN_H

// scal2bbs_mxn

#undef  GENTFUNCRO
#define GENTFUNCRO( ctype, ch, opname ) \
\
BLIS_INLINE void PASTEMAC(ch,opname) \
     ( \
       const conj_t       conjx, \
       const dim_t        m, \
       const dim_t        n, \
       ctype*    restrict alpha, \
       ctype*    restrict x, const inc_t incx, const inc_t ldx, \
       ctype*    restrict y, const inc_t incy, const inc_t ldy  \
     ) \
{ \
	/* Assume that the duplication factor is the row stride of y. */ \
	const dim_t d    = incy; \
	const dim_t ds_y = 1; \
\
	if ( bli_is_conj( conjx ) ) \
	{ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict xj = x + j*ldx; \
			ctype* restrict yj = y + j*ldy; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict xij = xj + i*incx; \
				ctype* restrict yij = yj + i*incy; \
\
				PASTEMAC(ch,scal2js)( *alpha, *xij, *yij ); \
\
				for ( dim_t p = 1; p < d; ++p ) \
				{ \
					ctype* restrict yijd = yij + p*ds_y; \
\
					PASTEMAC(ch,copys)( *yij, *yijd ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_noconj( conjx ) ) */ \
	{ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype* restrict xj = x + j*ldx; \
			ctype* restrict yj = y + j*ldy; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype* restrict xij = xj + i*incx; \
				ctype* restrict yij = yj + i*incy; \
\
				PASTEMAC(ch,scal2s)( *alpha, *xij, *yij ); \
\
				for ( dim_t p = 1; p < d; ++p ) \
				{ \
					ctype* restrict yijd = yij + p*ds_y; \
\
					PASTEMAC(ch,copys)( *yij, *yijd ); \
				} \
			} \
		} \
	} \
}

INSERT_GENTFUNCRO_BASIC0( scal2bbs_mxn )


#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname ) \
\
BLIS_INLINE void PASTEMAC(ch,opname) \
     ( \
       const conj_t       conjx, \
       const dim_t        m, \
       const dim_t        n, \
       ctype*    restrict alpha, \
       ctype*    restrict x, const inc_t incx, const inc_t ldx, \
       ctype*    restrict y, const inc_t incy, const inc_t ldy  \
     ) \
{ \
	/* Assume that the duplication factor is the row stride of y. */ \
	const dim_t       d          = incy; \
	const dim_t       ds_y       = 1; \
\
	const inc_t       incx2      = 2 * incx; \
	const inc_t       ldx2       = 2 * ldx; \
\
	const inc_t       incy2      = 2 * incy; \
	const inc_t       ldy2       = 2 * ldy; \
\
	ctype_r* restrict alpha_r    = ( ctype_r* )alpha; \
	ctype_r* restrict alpha_i    = ( ctype_r* )alpha + 1; \
	ctype_r* restrict chi_r      = ( ctype_r* )x; \
	ctype_r* restrict chi_i      = ( ctype_r* )x + 1; \
	ctype_r* restrict psi_r      = ( ctype_r* )y; \
	ctype_r* restrict psi_i      = ( ctype_r* )y + 1*d; \
\
	if ( bli_is_conj( conjx ) ) \
	{ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype_r* restrict chij_r = chi_r + j*ldx2; \
			ctype_r* restrict chij_i = chi_i + j*ldx2; \
			ctype_r* restrict psij_r = psi_r + j*ldy2; \
			ctype_r* restrict psij_i = psi_i + j*ldy2; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype_r* restrict chiij_r = chij_r + i*incx2; \
				ctype_r* restrict chiij_i = chij_i + i*incx2; \
				ctype_r* restrict psiij_r = psij_r + i*incy2; \
				ctype_r* restrict psiij_i = psij_i + i*incy2; \
\
				PASTEMAC(ch,scal2jris)( *alpha_r, *alpha_i, \
				                        *chiij_r, *chiij_i, \
				                        *psiij_r, *psiij_i ); \
\
				for ( dim_t p = 1; p < d; ++p ) \
				{ \
					ctype_r* restrict psiijd_r = psiij_r + p*ds_y; \
					ctype_r* restrict psiijd_i = psiij_i + p*ds_y; \
\
					PASTEMAC(ch,copyris)( *psiij_r,  *psiij_i, \
					                      *psiijd_r, *psiijd_i ); \
				} \
			} \
		} \
	} \
	else /* if ( bli_is_noconj( conjx ) ) */ \
	{ \
		for ( dim_t j = 0; j < n; ++j ) \
		{ \
			ctype_r* restrict chij_r = chi_r + j*ldx2; \
			ctype_r* restrict chij_i = chi_i + j*ldx2; \
			ctype_r* restrict psij_r = psi_r + j*ldy2; \
			ctype_r* restrict psij_i = psi_i + j*ldy2; \
\
			for ( dim_t i = 0; i < m; ++i ) \
			{ \
				ctype_r* restrict chiij_r = chij_r + i*incx2; \
				ctype_r* restrict chiij_i = chij_i + i*incx2; \
				ctype_r* restrict psiij_r = psij_r + i*incy2; \
				ctype_r* restrict psiij_i = psij_i + i*incy2; \
\
				PASTEMAC(ch,scal2ris)( *alpha_r, *alpha_i, \
				                       *chiij_r, *chiij_i, \
				                       *psiij_r, *psiij_i ); \
\
				for ( dim_t p = 1; p < d; ++p ) \
				{ \
					ctype_r* restrict psiijd_r = psiij_r + p*ds_y; \
					ctype_r* restrict psiijd_i = psiij_i + p*ds_y; \
\
					PASTEMAC(ch,copyris)( *psiij_r,  *psiij_i, \
					                      *psiijd_r, *psiijd_i ); \
				} \
			} \
		} \
	} \
}

INSERT_GENTFUNCCO_BASIC0( scal2bbs_mxn )

#endif
