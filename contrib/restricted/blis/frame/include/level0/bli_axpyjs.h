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

#ifndef BLIS_AXPYJS_H
#define BLIS_AXPYJS_H

// axpyjs

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.


// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dssaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_cssaxpyjs( a, x, y )  bli_saxpyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_zssaxpyjs( a, x, y )  bli_saxpyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdsaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ddsaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cdsaxpyjs( a, x, y )  bli_saxpyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zdsaxpyjs( a, x, y )  bli_saxpyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )

#define bli_scsaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dcsaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ccsaxpyjs( a, x, y )  bli_saxpyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zcsaxpyjs( a, x, y )  bli_saxpyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )

#define bli_szsaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dzsaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_czsaxpyjs( a, x, y )  bli_saxpyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zzsaxpyjs( a, x, y )  bli_saxpyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dsdaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_csdaxpyjs( a, x, y )  bli_daxpyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zsdaxpyjs( a, x, y )  bli_daxpyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )

#define bli_sddaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dddaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cddaxpyjs( a, x, y )  bli_daxpyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zddaxpyjs( a, x, y )  bli_daxpyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_scdaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dcdaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ccdaxpyjs( a, x, y )  bli_daxpyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zcdaxpyjs( a, x, y )  bli_daxpyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_szdaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dzdaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_czdaxpyjs( a, x, y )  bli_daxpyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zzdaxpyjs( a, x, y )  bli_daxpyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dscaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_cscaxpyjs( a, x, y )  bli_caxpyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_zscaxpyjs( a, x, y )  bli_caxpyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )

#define bli_sdcaxpyjs( a, x, y )  bli_saxpyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ddcaxpyjs( a, x, y )  bli_saxpyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cdcaxpyjs( a, x, y )  bli_caxpyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zdcaxpyjs( a, x, y )  bli_caxpyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )

#define bli_sccaxpyjs( a, x, y )  bli_scaxpyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccaxpyjs( a, x, y )  bli_scaxpyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccaxpyjs( a, x, y )   bli_caxpyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccaxpyjs( a, x, y )   bli_caxpyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcaxpyjs( a, x, y )  bli_scaxpyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dzcaxpyjs( a, x, y )  bli_scaxpyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_czcaxpyjs( a, x, y )   bli_caxpyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zzcaxpyjs( a, x, y )   bli_caxpyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dszaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cszaxpyjs( a, x, y )  bli_zaxpyjris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zszaxpyjs( a, x, y )  bli_zaxpyjris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sdzaxpyjs( a, x, y )  bli_daxpyjris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_ddzaxpyjs( a, x, y )  bli_daxpyjris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cdzaxpyjs( a, x, y )  bli_zaxpyjris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zdzaxpyjs( a, x, y )  bli_zaxpyjris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sczaxpyjs( a, x, y )  bli_dzaxpyjris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dczaxpyjs( a, x, y )  bli_dzaxpyjris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cczaxpyjs( a, x, y )   bli_zaxpyjris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zczaxpyjs( a, x, y )   bli_zaxpyjris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_szzaxpyjs( a, x, y )  bli_dzaxpyjris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzzaxpyjs( a, x, y )  bli_dzaxpyjris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czzaxpyjs( a, x, y )   bli_zaxpyjris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzzaxpyjs( a, x, y )   bli_zaxpyjris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_dscaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_cscaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_zscaxpyjs( a, x, y )  { (y) += (a) * (x); }

#define bli_sdcaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_ddcaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_cdcaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_zdcaxpyjs( a, x, y )  { (y) += (a) * (x); }

#define bli_sccaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_dccaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_cccaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_zccaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }

#define bli_szcaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_dzcaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_czcaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_zzcaxpyjs( a, x, y )  { (y) += (a) * conj(x); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_dszaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_cszaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_zszaxpyjs( a, x, y )  { (y) += (a) * (x); }

#define bli_sdzaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_ddzaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_cdzaxpyjs( a, x, y )  { (y) += (a) * (x); }
#define bli_zdzaxpyjs( a, x, y )  { (y) += (a) * (x); }

#define bli_sczaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_dczaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_cczaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }
#define bli_zczaxpyjs( a, x, y )  { (y) += (a) * conjf(x); }

#define bli_szzaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_dzzaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_czzaxpyjs( a, x, y )  { (y) += (a) * conj(x); }
#define bli_zzzaxpyjs( a, x, y )  { (y) += (a) * conj(x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_saxpyjs( a, x, y )  bli_sssaxpyjs( a, x, y )
#define bli_daxpyjs( a, x, y )  bli_dddaxpyjs( a, x, y )
#define bli_caxpyjs( a, x, y )  bli_cccaxpyjs( a, x, y )
#define bli_zaxpyjs( a, x, y )  bli_zzzaxpyjs( a, x, y )


#endif

