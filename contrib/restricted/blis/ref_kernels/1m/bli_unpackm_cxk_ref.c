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

#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_2xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_4xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_6xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_8xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_10xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_12xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 13), *(alpha1 + 13*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 13), *(alpha1 + 13*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 13), *(alpha1 + 13*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 13), *(alpha1 + 13*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_14xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )




#undef  GENTFUNC
#define GENTFUNC( ctype, ch, opname, arch, suf ) \
\
void PASTEMAC3(ch,opname,arch,suf) \
     ( \
       conj_t           conjp, \
       dim_t            n, \
       void*   restrict kappa, \
       void*   restrict p,             inc_t ldp, \
       void*   restrict a, inc_t inca, inc_t lda, \
       cntx_t* restrict cntx  \
     ) \
{ \
	ctype* restrict kappa_cast = kappa; \
	ctype* restrict pi1        = p; \
	ctype* restrict alpha1     = a; \
\
	if ( PASTEMAC(ch,eq1)( *kappa_cast ) ) \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 13), *(alpha1 + 13*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 14), *(alpha1 + 14*inca) ); \
				PASTEMAC2(ch,ch,copyjs)( *(pi1 + 15), *(alpha1 + 15*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 13), *(alpha1 + 13*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 14), *(alpha1 + 14*inca) ); \
				PASTEMAC2(ch,ch,copys)( *(pi1 + 15), *(alpha1 + 15*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
	else \
	{ \
		if ( bli_is_conj( conjp ) ) \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 13), *(alpha1 + 13*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 14), *(alpha1 + 14*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2js)( *kappa_cast, *(pi1 + 15), *(alpha1 + 15*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
		else \
		{ \
			for ( ; n != 0; --n ) \
			{ \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 0), *(alpha1 + 0*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 1), *(alpha1 + 1*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 2), *(alpha1 + 2*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 3), *(alpha1 + 3*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 4), *(alpha1 + 4*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 5), *(alpha1 + 5*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 6), *(alpha1 + 6*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 7), *(alpha1 + 7*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 8), *(alpha1 + 8*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 9), *(alpha1 + 9*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 10), *(alpha1 + 10*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 11), *(alpha1 + 11*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 12), *(alpha1 + 12*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 13), *(alpha1 + 13*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 14), *(alpha1 + 14*inca) ); \
				PASTEMAC3(ch,ch,ch,scal2s)( *kappa_cast, *(pi1 + 15), *(alpha1 + 15*inca) ); \
\
				pi1    += ldp; \
				alpha1 += lda; \
			} \
		} \
	} \
}

INSERT_GENTFUNC_BASIC2( unpackm_16xk, BLIS_CNAME_INFIX, BLIS_REF_SUFFIX )

