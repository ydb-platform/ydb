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

#include "blis.h"

// -- 6xk, duplication factor 2 ------------------------------------------------

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, mnr, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conja, \
       pack_t           schema, \
       dim_t            cdim, \
       dim_t            n, \
       dim_t            n_max, \
       ctype*  restrict kappa, \
       ctype*  restrict a, inc_t inca, inc_t lda, \
       ctype*  restrict p,             inc_t ldp, \
       cntx_t* restrict cntx \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict alpha1     = a; \
	ctype* restrict pi1        = p; \
\
	const dim_t     dfac       = 2; \
\
	/* Handle the packing of B (column panel schemas) separately from packing
	   of A (row panel schemas). */ \
	if ( bli_is_col_packed( schema ) ) \
	{ \
		if ( cdim == mnr ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 11) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 11) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
			else /* if ( !PASTEMAC(ch,eq1)( *kappa_cast ) ) */ \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 11) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 11) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( cdim < mnr ) */ \
		{ \
			PASTEMAC(ch,scal2bbs_mxn) \
			( \
			  conja, \
			  cdim, \
			  n, \
			  kappa, \
			  a, inca, lda, \
			  p, dfac, ldp  \
			); \
\
			/* if ( cdim < mnr ) */ \
			{ \
				const dim_t     i      = cdim; \
				const dim_t     m_edge = mnr - cdim; \
				const dim_t     n_edge = n_max; \
				ctype* restrict p_cast = p; \
				ctype* restrict p_edge = p_cast + (i  )*dfac; \
\
				PASTEMAC(ch,set0bbs_mxn) \
				( \
				  m_edge, \
				  n_edge, \
				  p_edge, dfac, ldp  \
				); \
			} \
		} \
\
		if ( n < n_max ) \
		{ \
			const dim_t     j      = n; \
			const dim_t     m_edge = mnr; \
			const dim_t     n_edge = n_max - n; \
			ctype* restrict p_cast = p; \
			ctype* restrict p_edge = p_cast + (j  )*ldp; \
\
			PASTEMAC(ch,set0bbs_mxn) \
			( \
			  m_edge, \
			  n_edge, \
			  p_edge, dfac, ldp  \
			); \
		} \
	} \
	else /* if ( bli_is_row_packed( schema ) ) */ \
	{ \
		if ( cdim == mnr ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
			else /* if ( !PASTEMAC(ch,eq1)( *kappa_cast ) ) */ \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( cdim < mnr ) */ \
		{ \
			PASTEMAC(ch,scal2s_mxn) \
			( \
			  conja, \
			  cdim, \
			  n, \
			  kappa, \
			  a, inca, lda, \
			  p, 1,    ldp  \
			); \
\
			/* if ( cdim < mnr ) */ \
			{ \
				const dim_t     i      = cdim; \
				const dim_t     m_edge = mnr - cdim; \
				const dim_t     n_edge = n_max; \
				ctype* restrict p_cast = p; \
				ctype* restrict p_edge = p_cast + (i  )*1; \
\
				PASTEMAC(ch,set0s_mxn) \
				( \
				  m_edge, \
				  n_edge, \
				  p_edge, 1, ldp  \
				); \
			} \
		} \
\
		if ( n < n_max ) \
		{ \
			const dim_t     j      = n; \
			const dim_t     m_edge = mnr; \
			const dim_t     n_edge = n_max - n; \
			ctype* restrict p_cast = p; \
			ctype* restrict p_edge = p_cast + (j  )*ldp; \
\
			PASTEMAC(ch,set0s_mxn) \
			( \
			  m_edge, \
			  n_edge, \
			  p_edge, 1, ldp  \
			); \
		} \
	} \
}

INSERT_GENTFUNC_BASIC3( packm_6xk_bb2, 6, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

// -- 6xk, duplication factor 4 ------------------------------------------------

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, mnr, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conja, \
       pack_t           schema, \
       dim_t            cdim, \
       dim_t            n, \
       dim_t            n_max, \
       ctype*  restrict kappa, \
       ctype*  restrict a, inc_t inca, inc_t lda, \
       ctype*  restrict p,             inc_t ldp, \
       cntx_t* restrict cntx \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict alpha1     = a; \
	ctype* restrict pi1        = p; \
\
	const dim_t     dfac       = 4; \
\
	/* Handle the packing of B (column panel schemas) separately from packing
	   of A (row panel schemas). */ \
	if ( bli_is_col_packed( schema ) ) \
	{ \
		if ( cdim == mnr ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 + 11) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 12) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 13) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 14) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 15) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 16) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 17) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 18) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 19) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 20) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 21) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 22) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 23) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 + 11) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 12) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 13) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 14) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 15) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 16) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 17) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 18) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 19) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 20) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 21) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 22) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 23) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
			else /* if ( !PASTEMAC(ch,eq1)( *kappa_cast ) ) */ \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 11) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 12) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 13) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 14) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 15) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 16) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 17) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 18) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 19) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 20) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 21) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 22) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 23) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  0) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  1) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  2) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 +  3) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  4) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  5) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  6) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 +  7) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  8) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 +  9) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 10) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 11) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 12) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 13) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 14) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 15) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 16) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 17) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 18) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 19) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 20) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 21) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 22) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 23) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( cdim < mnr ) */ \
		{ \
			PASTEMAC(ch,scal2bbs_mxn) \
			( \
			  conja, \
			  cdim, \
			  n, \
			  kappa, \
			  a, inca, lda, \
			  p, dfac, ldp  \
			); \
\
			/* if ( cdim < mnr ) */ \
			{ \
				const dim_t     i      = cdim; \
				const dim_t     m_edge = mnr - cdim; \
				const dim_t     n_edge = n_max; \
				ctype* restrict p_cast = p; \
				ctype* restrict p_edge = p_cast + (i  )*dfac; \
\
				PASTEMAC(ch,set0bbs_mxn) \
				( \
				  m_edge, \
				  n_edge, \
				  p_edge, dfac, ldp  \
				); \
			} \
		} \
\
		if ( n < n_max ) \
		{ \
			const dim_t     j      = n; \
			const dim_t     m_edge = mnr; \
			const dim_t     n_edge = n_max - n; \
			ctype* restrict p_cast = p; \
			ctype* restrict p_edge = p_cast + (j  )*ldp; \
\
			PASTEMAC(ch,set0bbs_mxn) \
			( \
			  m_edge, \
			  n_edge, \
			  p_edge, dfac, ldp  \
			); \
		} \
	} \
	else /* if ( bli_is_row_packed( schema ) ) */ \
	{ \
		if ( cdim == mnr ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjs)( *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,copyjs)( *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copys)( *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,copys)( *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
			else /* if ( !PASTEMAC(ch,eq1)( *kappa_cast ) ) */ \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,scal2js)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
				else /* if ( bli_is_noconj( conja ) ) */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 0*inca), *(pi1 + 0) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 1*inca), *(pi1 + 1) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 2*inca), *(pi1 + 2) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 3*inca), *(pi1 + 3) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 4*inca), *(pi1 + 4) ); \
						PASTEMAC(ch,scal2s)( *kappa_cast, *(alpha1 + 5*inca), *(pi1 + 5) ); \
\
						alpha1 += lda; \
						pi1    += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( cdim < mnr ) */ \
		{ \
			PASTEMAC(ch,scal2s_mxn) \
			( \
			  conja, \
			  cdim, \
			  n, \
			  kappa, \
			  a, inca, lda, \
			  p, 1,    ldp  \
			); \
\
			/* if ( cdim < mnr ) */ \
			{ \
				const dim_t     i      = cdim; \
				const dim_t     m_edge = mnr - cdim; \
				const dim_t     n_edge = n_max; \
				ctype* restrict p_cast = p; \
				ctype* restrict p_edge = p_cast + (i  )*1; \
\
				PASTEMAC(ch,set0s_mxn) \
				( \
				  m_edge, \
				  n_edge, \
				  p_edge, 1, ldp  \
				); \
			} \
		} \
\
		if ( n < n_max ) \
		{ \
			const dim_t     j      = n; \
			const dim_t     m_edge = mnr; \
			const dim_t     n_edge = n_max - n; \
			ctype* restrict p_cast = p; \
			ctype* restrict p_edge = p_cast + (j  )*ldp; \
\
			PASTEMAC(ch,set0s_mxn) \
			( \
			  m_edge, \
			  n_edge, \
			  p_edge, 1, ldp  \
			); \
		} \
	} \
}

INSERT_GENTFUNC_BASIC3( packm_6xk_bb4, 6, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

