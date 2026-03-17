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

#ifndef BLIS_SQRT2S_H
#define BLIS_SQRT2S_H

// sqrt2s

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of a.

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_sssqrt2s( x, a )  bli_ssqrt2ris( bli_sreal(x), bli_simag(x), bli_sreal(a), bli_simag(a) )
#define bli_dssqrt2s( x, a )  bli_ssqrt2ris( bli_dreal(x), bli_dimag(x), bli_sreal(a), bli_simag(a) )
#define bli_cssqrt2s( x, a )  bli_ssqrt2ris( bli_creal(x), bli_cimag(x), bli_sreal(a), bli_simag(a) )
#define bli_zssqrt2s( x, a )  bli_ssqrt2ris( bli_zreal(x), bli_zimag(x), bli_sreal(a), bli_simag(a) )

#define bli_sdsqrt2s( x, a )  bli_dsqrt2ris( bli_sreal(x), bli_simag(x), bli_dreal(a), bli_dimag(a) )
#define bli_ddsqrt2s( x, a )  bli_dsqrt2ris( bli_dreal(x), bli_dimag(x), bli_dreal(a), bli_dimag(a) )
#define bli_cdsqrt2s( x, a )  bli_dsqrt2ris( bli_creal(x), bli_cimag(x), bli_dreal(a), bli_dimag(a) )
#define bli_zdsqrt2s( x, a )  bli_dsqrt2ris( bli_zreal(x), bli_zimag(x), bli_dreal(a), bli_dimag(a) )

#define bli_scsqrt2s( x, a )  bli_scsqrt2ris( bli_sreal(x), bli_simag(x), bli_creal(a), bli_cimag(a) )
#define bli_dcsqrt2s( x, a )  bli_scsqrt2ris( bli_dreal(x), bli_dimag(x), bli_creal(a), bli_cimag(a) )
#define bli_ccsqrt2s( x, a )   bli_csqrt2ris( bli_creal(x), bli_cimag(x), bli_creal(a), bli_cimag(a) )
#define bli_zcsqrt2s( x, a )   bli_csqrt2ris( bli_zreal(x), bli_zimag(x), bli_creal(a), bli_cimag(a) )

#define bli_szsqrt2s( x, a )  bli_dzsqrt2ris( bli_sreal(x), bli_simag(x), bli_zreal(a), bli_zimag(a) )
#define bli_dzsqrt2s( x, a )  bli_dzsqrt2ris( bli_dreal(x), bli_dimag(x), bli_zreal(a), bli_zimag(a) )
#define bli_czsqrt2s( x, a )   bli_zsqrt2ris( bli_creal(x), bli_cimag(x), bli_zreal(a), bli_zimag(a) )
#define bli_zzsqrt2s( x, a )   bli_zsqrt2ris( bli_zreal(x), bli_zimag(x), bli_zreal(a), bli_zimag(a) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_sssqrt2s( x, a )  { (a) = ( float    )            sqrtf( (x) )  ; }
#define bli_dssqrt2s( x, a )  { (a) = ( float    )            sqrt ( (x) )  ; }
#define bli_cssqrt2s( x, a )  { (a) = ( float    )bli_creal( csqrtf( (x) ) ); }
#define bli_zssqrt2s( x, a )  { (a) = ( float    )bli_zreal( csqrt ( (x) ) ); }

#define bli_sdsqrt2s( x, a )  { (a) = ( double   )            sqrtf( (x) )  ; }
#define bli_ddsqrt2s( x, a )  { (a) = ( double   )            sqrt ( (x) )  ; }
#define bli_cdsqrt2s( x, a )  { (a) = ( double   )bli_creal( csqrtf( (x) ) ); }
#define bli_zdsqrt2s( x, a )  { (a) = ( double   )bli_zreal( csqrt ( (x) ) ); }

#define bli_scsqrt2s( x, a )  { (a) = ( scomplex )            sqrtf( (x) )  ; }
#define bli_dcsqrt2s( x, a )  { (a) = ( scomplex )            sqrt ( (x) )  ; }
#define bli_ccsqrt2s( x, a )  { (a) = ( scomplex )           csqrtf( (x) )  ; }
#define bli_zcsqrt2s( x, a )  { (a) = ( scomplex )           csqrt ( (x) )  ; }

#define bli_szsqrt2s( x, a )  { (a) = ( dcomplex )            sqrtf( (x) )  ; }
#define bli_dzsqrt2s( x, a )  { (a) = ( dcomplex )            sqrt ( (x) )  ; }
#define bli_czsqrt2s( x, a )  { (a) = ( dcomplex )           csqrtf( (x) )  ; }
#define bli_zzsqrt2s( x, a )  { (a) = ( dcomplex )           csqrt ( (x) )  ; }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_ssqrt2s( x, a )  bli_sssqrt2s( x, a )
#define bli_dsqrt2s( x, a )  bli_ddsqrt2s( x, a )
#define bli_csqrt2s( x, a )  bli_ccsqrt2s( x, a )
#define bli_zsqrt2s( x, a )  bli_zzsqrt2s( x, a )


#endif
