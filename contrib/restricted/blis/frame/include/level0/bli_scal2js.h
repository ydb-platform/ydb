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

#ifndef BLIS_SCAL2JS_H
#define BLIS_SCAL2JS_H

// scal2js

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.


// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dssscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_cssscal2js( a, x, y )  bli_rxscal2jris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_zssscal2js( a, x, y )  bli_rxscal2jris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdsscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ddsscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cdsscal2js( a, x, y )  bli_rxscal2jris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zdsscal2js( a, x, y )  bli_rxscal2jris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )

#define bli_scsscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dcsscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ccsscal2js( a, x, y )  bli_roscal2jris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zcsscal2js( a, x, y )  bli_roscal2jris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )

#define bli_szsscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dzsscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_czsscal2js( a, x, y )  bli_roscal2jris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zzsscal2js( a, x, y )  bli_roscal2jris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dsdscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_csdscal2js( a, x, y )  bli_rxscal2jris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zsdscal2js( a, x, y )  bli_rxscal2jris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )

#define bli_sddscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dddscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cddscal2js( a, x, y )  bli_rxscal2jris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zddscal2js( a, x, y )  bli_rxscal2jris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_scdscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dcdscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ccdscal2js( a, x, y )  bli_roscal2jris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zcdscal2js( a, x, y )  bli_roscal2jris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_szdscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dzdscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_czdscal2js( a, x, y )  bli_roscal2jris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zzdscal2js( a, x, y )  bli_roscal2jris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dscscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_cscscal2js( a, x, y )  bli_rcscal2jris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_zscscal2js( a, x, y )  bli_rcscal2jris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )

#define bli_sdcscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ddcscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cdcscal2js( a, x, y )  bli_rcscal2jris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zdcscal2js( a, x, y )  bli_rcscal2jris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )

#define bli_sccscal2js( a, x, y )  bli_crscal2jris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccscal2js( a, x, y )  bli_crscal2jris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccscal2js( a, x, y )  bli_cxscal2jris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccscal2js( a, x, y )  bli_cxscal2jris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcscal2js( a, x, y )  bli_crscal2jris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dzcscal2js( a, x, y )  bli_crscal2jris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_czcscal2js( a, x, y )  bli_cxscal2jris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zzcscal2js( a, x, y )  bli_cxscal2jris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dszscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cszscal2js( a, x, y )  bli_rcscal2jris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zszscal2js( a, x, y )  bli_rcscal2jris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sdzscal2js( a, x, y )  bli_rxscal2jris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_ddzscal2js( a, x, y )  bli_rxscal2jris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cdzscal2js( a, x, y )  bli_rcscal2jris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zdzscal2js( a, x, y )  bli_rcscal2jris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sczscal2js( a, x, y )  bli_crscal2jris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dczscal2js( a, x, y )  bli_crscal2jris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cczscal2js( a, x, y )  bli_cxscal2jris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zczscal2js( a, x, y )  bli_cxscal2jris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_szzscal2js( a, x, y )  bli_crscal2jris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzzscal2js( a, x, y )  bli_crscal2jris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czzscal2js( a, x, y )  bli_cxscal2jris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzzscal2js( a, x, y )  bli_cxscal2jris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_dscscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_cscscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_zscscal2js( a, x, y )  { (y) = (a) * (x); }

#define bli_sdcscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_ddcscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_cdcscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_zdcscal2js( a, x, y )  { (y) = (a) * (x); }

#define bli_sccscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_dccscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_cccscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_zccscal2js( a, x, y )  { (y) = (a) * conjf(x); }

#define bli_szcscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_dzcscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_czcscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_zzcscal2js( a, x, y )  { (y) = (a) * conj(x); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_dszscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_cszscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_zszscal2js( a, x, y )  { (y) = (a) * (x); }

#define bli_sdzscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_ddzscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_cdzscal2js( a, x, y )  { (y) = (a) * (x); }
#define bli_zdzscal2js( a, x, y )  { (y) = (a) * (x); }

#define bli_sczscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_dczscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_cczscal2js( a, x, y )  { (y) = (a) * conjf(x); }
#define bli_zczscal2js( a, x, y )  { (y) = (a) * conjf(x); }

#define bli_szzscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_dzzscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_czzscal2js( a, x, y )  { (y) = (a) * conj(x); }
#define bli_zzzscal2js( a, x, y )  { (y) = (a) * conj(x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sscal2js( a, x, y )  bli_sssscal2js( a, x, y )
#define bli_dscal2js( a, x, y )  bli_dddscal2js( a, x, y )
#define bli_cscal2js( a, x, y )  bli_cccscal2js( a, x, y )
#define bli_zscal2js( a, x, y )  bli_zzzscal2js( a, x, y )


#endif

