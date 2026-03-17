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

#ifndef BLIS_XPBYS_H
#define BLIS_XPBYS_H

// xpbys

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of b.
// - The third char encodes the type of y.

// -- (xby) = (??s) ------------------------------------------------------------

#define bli_sssxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_dssxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_cssxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )
#define bli_zssxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_sreal(y), bli_simag(y) )

#define bli_sdsxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ddsxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_cdsxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zdsxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_sreal(y), bli_simag(y) )

#define bli_scsxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dcsxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_ccsxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zcsxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_sreal(y), bli_simag(y) )

#define bli_szsxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_dzsxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_czsxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )
#define bli_zzsxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_sreal(y), bli_simag(y) )

// -- (xby) = (??d) ------------------------------------------------------------

#define bli_ssdxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dsdxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_csdxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zsdxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_dreal(y), bli_dimag(y) )

#define bli_sddxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dddxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_cddxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zddxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_scdxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dcdxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_ccdxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zcdxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_dreal(y), bli_dimag(y) )

#define bli_szdxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_dzdxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_czdxpbys( x, b, y )  bli_rxxpbyris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )
#define bli_zzdxpbys( x, b, y )  bli_rxxpbyris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_dscxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_cscxpbys( x, b, y )  bli_crxpbyris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )
#define bli_zscxpbys( x, b, y )  bli_crxpbyris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_creal(y), bli_cimag(y) )

#define bli_sdcxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_ddcxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cdcxpbys( x, b, y )  bli_crxpbyris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zdcxpbys( x, b, y )  bli_crxpbyris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_creal(y), bli_cimag(y) )

#define bli_sccxpbys( x, b, y )  bli_cxxpbyris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dccxpbys( x, b, y )  bli_cxxpbyris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_cccxpbys( x, b, y )  bli_cxxpbyris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zccxpbys( x, b, y )  bli_cxxpbyris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_creal(y), bli_cimag(y) )

#define bli_szcxpbys( x, b, y )  bli_cxxpbyris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_dzcxpbys( x, b, y )  bli_cxxpbyris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_czcxpbys( x, b, y )  bli_cxxpbyris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )
#define bli_zzcxpbys( x, b, y )  bli_cxxpbyris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_creal(y), bli_cimag(y) )

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dszxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cszxpbys( x, b, y )  bli_crxpbyris( bli_creal(x), bli_cimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zszxpbys( x, b, y )  bli_crxpbyris( bli_zreal(x), bli_zimag(x), bli_sreal(b), bli_simag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sdzxpbys( x, b, y )  bli_rxxpbyris( bli_sreal(x), bli_simag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_ddzxpbys( x, b, y )  bli_rxxpbyris( bli_dreal(x), bli_dimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cdzxpbys( x, b, y )  bli_crxpbyris( bli_creal(x), bli_cimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zdzxpbys( x, b, y )  bli_crxpbyris( bli_zreal(x), bli_zimag(x), bli_dreal(b), bli_dimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_sczxpbys( x, b, y )  bli_cxxpbyris( bli_sreal(x), bli_simag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dczxpbys( x, b, y )  bli_cxxpbyris( bli_dreal(x), bli_dimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_cczxpbys( x, b, y )  bli_cxxpbyris( bli_creal(x), bli_cimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zczxpbys( x, b, y )  bli_cxxpbyris( bli_zreal(x), bli_zimag(x), bli_creal(b), bli_cimag(b), bli_zreal(y), bli_zimag(y) )

#define bli_szzxpbys( x, b, y )  bli_cxxpbyris( bli_sreal(x), bli_simag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_dzzxpbys( x, b, y )  bli_cxxpbyris( bli_dreal(x), bli_dimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_czzxpbys( x, b, y )  bli_cxxpbyris( bli_creal(x), bli_cimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )
#define bli_zzzxpbys( x, b, y )  bli_cxxpbyris( bli_zreal(x), bli_zimag(x), bli_zreal(b), bli_zimag(b), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (xby) = (??c) ------------------------------------------------------------

#define bli_sscxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dscxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cscxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zscxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sdcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_ddcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cdcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zdcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sccxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dccxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cccxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zccxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_szcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dzcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_czcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zzcxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

// -- (xby) = (??z) ------------------------------------------------------------

#define bli_sszxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dszxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cszxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zszxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sdzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_ddzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cdzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zdzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_sczxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dczxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_cczxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zczxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#define bli_szzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_dzzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_czzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }
#define bli_zzzxpbys( x, b, y )  { (y) = (x) + (b) * (y); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sxpbys( x, b, y )  bli_sssxpbys( x, b, y )
#define bli_dxpbys( x, b, y )  bli_dddxpbys( x, b, y )
#define bli_cxpbys( x, b, y )  bli_cccxpbys( x, b, y )
#define bli_zxpbys( x, b, y )  bli_zzzxpbys( x, b, y )


#endif

