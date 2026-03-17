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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_2xk_1er, 2, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_4xk_1er, 4, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_6xk_1er, 6, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_8xk_1er, 8, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_10xk_1er, 10, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_12xk_1er, 12, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_14xk_1er, 14, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )



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
	if ( cdim == mnr ) \
	{ \
		if ( bli_is_1e_packed( schema ) ) \
		{ \
			const inc_t       inca1      = inca; \
			const inc_t       lda1       = lda; \
			const inc_t       ldp1       = ldp; \
\
			ctype*   restrict kappa_cast = ( ctype* )kappa; \
			ctype*   restrict alpha1_ri  = ( ctype* )a; \
			ctype*   restrict pi1_ri     = ( ctype* )p; \
			ctype*   restrict pi1_ir     = ( ctype* )p + ldp1/2; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +14*inca1), *(pi1_ri +14), *(pi1_ir +14) ); \
						PASTEMAC(ch,copyj1es)( *(alpha1_ri +15*inca1), *(pi1_ri +15), *(pi1_ir +15) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +14*inca1), *(pi1_ri +14), *(pi1_ir +14) ); \
						PASTEMAC(ch,copy1es)( *(alpha1_ri +15*inca1), *(pi1_ri +15), *(pi1_ir +15) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +14*inca1), *(pi1_ri +14), *(pi1_ir +14) ); \
						PASTEMAC(ch,scal2j1es)( *kappa_cast, *(alpha1_ri +15*inca1), *(pi1_ri +15), *(pi1_ir +15) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 0*inca1), *(pi1_ri + 0), *(pi1_ir + 0) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 1*inca1), *(pi1_ri + 1), *(pi1_ir + 1) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 2*inca1), *(pi1_ri + 2), *(pi1_ir + 2) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 3*inca1), *(pi1_ri + 3), *(pi1_ir + 3) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 4*inca1), *(pi1_ri + 4), *(pi1_ir + 4) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 5*inca1), *(pi1_ri + 5), *(pi1_ir + 5) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 6*inca1), *(pi1_ri + 6), *(pi1_ir + 6) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 7*inca1), *(pi1_ri + 7), *(pi1_ir + 7) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 8*inca1), *(pi1_ri + 8), *(pi1_ir + 8) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri + 9*inca1), *(pi1_ri + 9), *(pi1_ir + 9) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +10*inca1), *(pi1_ri +10), *(pi1_ir +10) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +11*inca1), *(pi1_ri +11), *(pi1_ir +11) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +12*inca1), *(pi1_ri +12), *(pi1_ir +12) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +13*inca1), *(pi1_ri +13), *(pi1_ir +13) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +14*inca1), *(pi1_ri +14), *(pi1_ir +14) ); \
						PASTEMAC(ch,scal21es)( *kappa_cast, *(alpha1_ri +15*inca1), *(pi1_ri +15), *(pi1_ir +15) ); \
\
						alpha1_ri += lda1; \
						pi1_ri    += ldp1; \
						pi1_ir    += ldp1; \
					} \
				} \
			} \
		} \
		else /* if ( bli_is_1r_packed( schema ) ) */ \
		{ \
			const inc_t       inca2      = 2 * inca; \
			const inc_t       lda2       = 2 * lda; \
			const inc_t       ldp2       = 2 * ldp; \
\
			ctype*            kappa_cast =             kappa; \
			ctype_r* restrict kappa_r    = ( ctype_r* )kappa; \
			ctype_r* restrict kappa_i    = ( ctype_r* )kappa + 1; \
			ctype_r* restrict alpha1_r   = ( ctype_r* )a; \
			ctype_r* restrict alpha1_i   = ( ctype_r* )a + 1; \
			ctype_r* restrict pi1_r      = ( ctype_r* )p; \
			ctype_r* restrict pi1_i      = ( ctype_r* )p + ldp; \
\
			if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +14*inca2), *(alpha1_i +14*inca2), *(pi1_r +14), *(pi1_i +14) ); \
						PASTEMAC(ch,copyjris)( *(alpha1_r +15*inca2), *(alpha1_i +15*inca2), *(pi1_r +15), *(pi1_i +15) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,copyris)( *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +14*inca2), *(alpha1_i +14*inca2), *(pi1_r +14), *(pi1_i +14) ); \
						PASTEMAC(ch,copyris)( *(alpha1_r +15*inca2), *(alpha1_i +15*inca2), *(pi1_r +15), *(pi1_i +15) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
			else \
			{ \
				if ( bli_is_conj( conja ) ) \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +14*inca2), *(alpha1_i +14*inca2), *(pi1_r +14), *(pi1_i +14) ); \
						PASTEMAC(ch,scal2jris)( *kappa_r, *kappa_i, *(alpha1_r +15*inca2), *(alpha1_i +15*inca2), *(pi1_r +15), *(pi1_i +15) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
				else \
				{ \
					for ( dim_t k = n; k != 0; --k ) \
					{ \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 0*inca2), *(alpha1_i + 0*inca2), *(pi1_r + 0), *(pi1_i + 0) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 1*inca2), *(alpha1_i + 1*inca2), *(pi1_r + 1), *(pi1_i + 1) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 2*inca2), *(alpha1_i + 2*inca2), *(pi1_r + 2), *(pi1_i + 2) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 3*inca2), *(alpha1_i + 3*inca2), *(pi1_r + 3), *(pi1_i + 3) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 4*inca2), *(alpha1_i + 4*inca2), *(pi1_r + 4), *(pi1_i + 4) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 5*inca2), *(alpha1_i + 5*inca2), *(pi1_r + 5), *(pi1_i + 5) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 6*inca2), *(alpha1_i + 6*inca2), *(pi1_r + 6), *(pi1_i + 6) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 7*inca2), *(alpha1_i + 7*inca2), *(pi1_r + 7), *(pi1_i + 7) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 8*inca2), *(alpha1_i + 8*inca2), *(pi1_r + 8), *(pi1_i + 8) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r + 9*inca2), *(alpha1_i + 9*inca2), *(pi1_r + 9), *(pi1_i + 9) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +10*inca2), *(alpha1_i +10*inca2), *(pi1_r +10), *(pi1_i +10) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +11*inca2), *(alpha1_i +11*inca2), *(pi1_r +11), *(pi1_i +11) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +12*inca2), *(alpha1_i +12*inca2), *(pi1_r +12), *(pi1_i +12) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +13*inca2), *(alpha1_i +13*inca2), *(pi1_r +13), *(pi1_i +13) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +14*inca2), *(alpha1_i +14*inca2), *(pi1_r +14), *(pi1_i +14) ); \
						PASTEMAC(ch,scal2ris)( *kappa_r, *kappa_i, *(alpha1_r +15*inca2), *(alpha1_i +15*inca2), *(pi1_r +15), *(pi1_i +15) ); \
\
						alpha1_r += lda2; \
						alpha1_i += lda2; \
						pi1_r    += ldp2; \
						pi1_i    += ldp2; \
					} \
				} \
			} \
		} \
	} \
	else /* if ( cdim < mnr ) */ \
	{ \
		PASTEMAC(ch,scal21ms_mxn) \
		( \
		  schema, \
		  conja, \
		  cdim, \
		  n, \
		  kappa, \
		  a, inca, lda, \
		  p, 1,    ldp, ldp  \
		); \
\
		/* if ( cdim < mnr ) */ \
		{ \
			ctype* restrict zero   = PASTEMAC(ch,0); \
			const dim_t     offm   = cdim; \
			const dim_t     offn   = 0; \
			const dim_t     m_edge = mnr - cdim; \
			const dim_t     n_edge = n_max; \
\
			PASTEMAC(ch,set1ms_mxn) \
			( \
			  schema, \
			  offm, \
			  offn, \
			  m_edge, \
			  n_edge, \
			  zero, \
			  p, 1, ldp, ldp  \
			); \
		} \
	} \
\
	if ( n < n_max ) \
	{ \
		ctype* restrict zero   = PASTEMAC(ch,0); \
		const dim_t     offm   = 0; \
		const dim_t     offn   = n; \
		const dim_t     m_edge = mnr; \
		const dim_t     n_edge = n_max - n; \
\
		PASTEMAC(ch,set1ms_mxn) \
		( \
		  schema, \
		  offm, \
		  offn, \
		  m_edge, \
		  n_edge, \
		  zero, \
		  p, 1, ldp, ldp  \
		); \
	} \
}

INSERT_GENTFUNCCO_BASIC3( packm_16xk_1er, 16, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

