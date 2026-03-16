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

#ifndef BLIS_XPBYJS_H
#define BLIS_XPBYJS_H

// xpbyjs

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of b.
// - The third char encodes the type of y.

// -- (xby) = (??s) ------------------------------------------------------------

#define bli_sssxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dssxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_cssxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zssxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )

#define bli_sdsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )

#define bli_scsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )

#define bli_szsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzsxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )

// -- (xby) = (??d) ------------------------------------------------------------

#define bli_ssdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dsdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_csdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zsdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sddxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dddxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cddxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zddxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_scdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_szdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzdxpbyjs( x, b, y )  bli_rxxpbyjris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dscxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_cscxpbyjs( x, b, y )  bli_crxpbyjris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zscxpbyjs( x, b, y )  bli_crxpbyjris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )

#define bli_sdcxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddcxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdcxpbyjs( x, b, y )  bli_crxpbyjris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdcxpbyjs( x, b, y )  bli_crxpbyjris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )

#define bli_sccxpbyjs( x, b, y )  bli_cxxpbyjris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dccxpbyjs( x, b, y )  bli_cxxpbyjris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cccxpbyjs( x, b, y )  bli_cxxpbyjris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zccxpbyjs( x, b, y )  bli_cxxpbyjris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )

#define bli_szcxpbyjs( x, b, y )  bli_cxxpbyjris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzcxpbyjs( x, b, y )  bli_cxxpbyjris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czcxpbyjs( x, b, y )  bli_cxxpbyjris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzcxpbyjs( x, b, y )  bli_cxxpbyjris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dszxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cszxpbyjs( x, b, y )  bli_crxpbyjris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zszxpbyjs( x, b, y )  bli_crxpbyjris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sdzxpbyjs( x, b, y )  bli_rxxpbyjris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddzxpbyjs( x, b, y )  bli_rxxpbyjris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdzxpbyjs( x, b, y )  bli_crxpbyjris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdzxpbyjs( x, b, y )  bli_crxpbyjris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sczxpbyjs( x, b, y )  bli_cxxpbyjris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dczxpbyjs( x, b, y )  bli_cxxpbyjris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cczxpbyjs( x, b, y )  bli_cxxpbyjris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zczxpbyjs( x, b, y )  bli_cxxpbyjris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_szzxpbyjs( x, b, y )  bli_cxxpbyjris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzzxpbyjs( x, b, y )  bli_cxxpbyjris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czzxpbyjs( x, b, y )  bli_cxxpbyjris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzzxpbyjs( x, b, y )  bli_cxxpbyjris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dscxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cscxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zscxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sdcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_ddcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cdcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zdcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sccxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dccxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cccxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zccxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_szcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dzcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_czcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zzcxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dszxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cszxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zszxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sdzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_ddzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cdzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zdzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sczxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dczxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cczxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zczxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_szzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dzzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_czzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zzzxpbyjs( x, b, y )  { (y) = (x) + (b) * (y); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sxpbyjs( x, b, y )  bli_sssxpbyjs( x, b, y )
#define bli_dxpbyjs( x, b, y )  bli_dddxpbyjs( x, b, y )
#define bli_cxpbyjs( x, b, y )  bli_cccxpbyjs( x, b, y )
#define bli_zxpbyjs( x, b, y )  bli_zzzxpbyjs( x, b, y )


#endif

