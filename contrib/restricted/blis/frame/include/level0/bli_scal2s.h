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

#ifndef BLIS_SCAL2S_H
#define BLIS_SCAL2S_H

// scal2s

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dssscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_cssscal2s( a, x, y )  bli_rxscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_zssscal2s( a, x, y )  bli_rxscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdsscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ddsscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cdsscal2s( a, x, y )  bli_rxscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zdsscal2s( a, x, y )  bli_rxscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )

#define bli_scsscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dcsscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ccsscal2s( a, x, y )  bli_roscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zcsscal2s( a, x, y )  bli_roscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )

#define bli_szsscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dzsscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_czsscal2s( a, x, y )  bli_roscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zzsscal2s( a, x, y )  bli_roscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dsdscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_csdscal2s( a, x, y )  bli_rxscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zsdscal2s( a, x, y )  bli_rxscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )

#define bli_sddscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dddscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cddscal2s( a, x, y )  bli_rxscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zddscal2s( a, x, y )  bli_rxscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_scdscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dcdscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ccdscal2s( a, x, y )  bli_roscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zcdscal2s( a, x, y )  bli_roscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_szdscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dzdscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_czdscal2s( a, x, y )  bli_roscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zzdscal2s( a, x, y )  bli_roscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dscscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_cscscal2s( a, x, y )  bli_rcscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_zscscal2s( a, x, y )  bli_rcscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )

#define bli_sdcscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ddcscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cdcscal2s( a, x, y )  bli_rcscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zdcscal2s( a, x, y )  bli_rcscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )

#define bli_sccscal2s( a, x, y )  bli_crscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccscal2s( a, x, y )  bli_crscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccscal2s( a, x, y )  bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccscal2s( a, x, y )  bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcscal2s( a, x, y )  bli_crscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dzcscal2s( a, x, y )  bli_crscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_czcscal2s( a, x, y )  bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zzcscal2s( a, x, y )  bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dszscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cszscal2s( a, x, y )  bli_rcscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zszscal2s( a, x, y )  bli_rcscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sdzscal2s( a, x, y )  bli_rxscal2ris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_ddzscal2s( a, x, y )  bli_rxscal2ris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cdzscal2s( a, x, y )  bli_rcscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zdzscal2s( a, x, y )  bli_rcscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sczscal2s( a, x, y )  bli_crscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dczscal2s( a, x, y )  bli_crscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cczscal2s( a, x, y )  bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zczscal2s( a, x, y )  bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_szzscal2s( a, x, y )  bli_crscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzzscal2s( a, x, y )  bli_crscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czzscal2s( a, x, y )  bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzzscal2s( a, x, y )  bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dscscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cscscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zscscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_sdcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_ddcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cdcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zdcscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_sccscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dccscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cccscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zccscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_szcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dzcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_czcscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zzcscal2s( a, x, y )  { (y) = (a) * (x); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dszscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cszscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zszscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_sdzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_ddzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cdzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zdzscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_sczscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dczscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_cczscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zczscal2s( a, x, y )  { (y) = (a) * (x); }

#define bli_szzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_dzzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_czzscal2s( a, x, y )  { (y) = (a) * (x); }
#define bli_zzzscal2s( a, x, y )  { (y) = (a) * (x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sscal2s( a, x, y )  bli_sssscal2s( a, x, y )
#define bli_dscal2s( a, x, y )  bli_dddscal2s( a, x, y )
#define bli_cscal2s( a, x, y )  bli_cccscal2s( a, x, y )
#define bli_zscal2s( a, x, y )  bli_zzzscal2s( a, x, y )


#endif

