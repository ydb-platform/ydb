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

#ifndef BLIS_COPYJS_H
#define BLIS_COPYJS_H

// copyjs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopyjs( x, y )  bli_scopyjris( bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dscopyjs( x, y )  bli_scopyjris( bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cscopyjs( x, y )  bli_scopyjris( bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zscopyjs( x, y )  bli_scopyjris( bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdcopyjs( x, y )  bli_dcopyjris( bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ddcopyjs( x, y )  bli_dcopyjris( bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cdcopyjs( x, y )  bli_dcopyjris( bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zdcopyjs( x, y )  bli_dcopyjris( bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_sccopyjs( x, y )  bli_ccopyjris( bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccopyjs( x, y )  bli_ccopyjris( bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccopyjs( x, y )  bli_ccopyjris( bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccopyjs( x, y )  bli_ccopyjris( bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcopyjs( x, y )  bli_zcopyjris( bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzcopyjs( x, y )  bli_zcopyjris( bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czcopyjs( x, y )  bli_zcopyjris( bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzcopyjs( x, y )  bli_zcopyjris( bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_sccopyjs( x, y )  { (y) =      (x); }
#define bli_dccopyjs( x, y )  { (y) =      (x); }
#define bli_cccopyjs( x, y )  { (y) = conjf(x); }
#define bli_zccopyjs( x, y )  { (y) = conj (x); }

#define bli_szcopyjs( x, y )  { (y) =      (x); }
#define bli_dzcopyjs( x, y )  { (y) =      (x); }
#define bli_czcopyjs( x, y )  { (y) = conjf(x); }
#define bli_zzcopyjs( x, y )  { (y) = conj (x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_iicopyjs( x, y )  { (y) = ( gint_t ) (x); }


#define bli_scopyjs( x, y )  bli_sscopyjs( x, y )
#define bli_dcopyjs( x, y )  bli_ddcopyjs( x, y )
#define bli_ccopyjs( x, y )  bli_cccopyjs( x, y )
#define bli_zcopyjs( x, y )  bli_zzcopyjs( x, y )
#define bli_icopyjs( x, y )  bli_iicopyjs( x, y )


#endif

