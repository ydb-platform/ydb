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

#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_2xk_rih, 2, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_4xk_rih, 4, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_6xk_rih, 6, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 7*inca2), *(pi1_r + 7) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_8xk_rih, 8, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 9*inca2), *(pi1_r + 9) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_10xk_rih, 10, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +11*inca2), *(pi1_r +11) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +11*inca2), *(pi1_r +11) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_12xk_rih, 12, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +13*inca2), *(pi1_r +13) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +13*inca2), *(pi1_r +13) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +13*inca2), *(pi1_r +13) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +12*inca2), -*(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +13*inca2), -*(alpha1_i +13*inca2), *(pi1_r +13) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_14xk_rih, 14, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



#undef  GENTFUNCCO
#define GENTFUNCCO( ctype, ctype_r, ch, chr, opname, mnr, arch, suf ) \
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
       cntx_t* restrict cntx  \
     ) \
{ \
	const inc_t       inca2      = 2 * inca; \
	const inc_t       lda2       = 2 * lda; \
\
	ctype*            kappa_cast = kappa; \
	ctype*   restrict alpha1     = a; \
	ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
	ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
	ctype_r* restrict pi1_r      = ( ctype_r* )p; \
\
\
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_ro_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				/* This works regardless of conja since we are only copying
				   the real part. */ \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_r + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_r + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +13*inca2), *(pi1_r +13) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +14*inca2), *(pi1_r +14) ); \
						PASTEMAC(chr,copys)( *(alpha1_r +15*inca2), *(pi1_r +15) ); \
	\
						alpha1_r += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2jros)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2ros)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else if ( bli_is_io_packed( schema ) ) \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +13*inca2), *(pi1_r +13) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +14*inca2), *(pi1_r +14) ); \
						PASTEMAC(chr,copys)( -*(alpha1_i +15*inca2), *(pi1_r +15) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,copys)( *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,copys)( *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +13*inca2), *(pi1_r +13) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +14*inca2), *(pi1_r +14) ); \
						PASTEMAC(chr,copys)( *(alpha1_i +15*inca2), *(pi1_r +15) ); \
	\
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2jios)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2ios)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_rpi_packed( schema ) ) */ \
		{ \
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), -*(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), -*(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), -*(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), -*(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), -*(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), -*(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), -*(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), -*(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), -*(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), -*(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), -*(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), -*(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +12*inca2), -*(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +13*inca2), -*(alpha1_i +13*inca2), *(pi1_r +13) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +14*inca2), -*(alpha1_i +14*inca2), *(pi1_r +14) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +15*inca2), -*(alpha1_i +15*inca2), *(pi1_r +15) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(chr,add3s)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +14*inca2), *(alpha1_i +14*inca2), *(pi1_r +14) ); \
						PASTEMAC(chr,add3s)( *(alpha1_r +15*inca2), *(alpha1_i +15*inca2), *(pi1_r +15) ); \
	\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2jrpis)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 0*inca), *(pi1_r + 0) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 1*inca), *(pi1_r + 1) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 2*inca), *(pi1_r + 2) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 3*inca), *(pi1_r + 3) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 4*inca), *(pi1_r + 4) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 5*inca), *(pi1_r + 5) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 6*inca), *(pi1_r + 6) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 7*inca), *(pi1_r + 7) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 8*inca), *(pi1_r + 8) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 + 9*inca), *(pi1_r + 9) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +10*inca), *(pi1_r +10) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +11*inca), *(pi1_r +11) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +12*inca), *(pi1_r +12) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +13*inca), *(pi1_r +13) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +14*inca), *(pi1_r +14) ); \
						PASTEMAC(ch,scal2rpis)( *kappa_cast, *(alpha1 +15*inca), *(pi1_r +15) ); \
	\
						alpha1 += lda; \
						pi1_r  += ldp; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal2rihs_mxn) \
		( \
		  schema, \
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
}

INSERT_GENTFUNCCO_BASIC3( packm_16xk_rih, 16, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

