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

#ifndef BLIS_AXPYS_H
#define BLIS_AXPYS_H

// axpys

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.


// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dssaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_cssaxpys( a, x, y )  bli_saxpyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_zssaxpys( a, x, y )  bli_saxpyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdsaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ddsaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cdsaxpys( a, x, y )  bli_saxpyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zdsaxpys( a, x, y )  bli_saxpyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )

#define bli_scsaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dcsaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ccsaxpys( a, x, y )  bli_saxpyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zcsaxpys( a, x, y )  bli_saxpyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )

#define bli_szsaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dzsaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_czsaxpys( a, x, y )  bli_saxpyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zzsaxpys( a, x, y )  bli_saxpyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dsdaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_csdaxpys( a, x, y )  bli_daxpyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zsdaxpys( a, x, y )  bli_daxpyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )

#define bli_sddaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dddaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cddaxpys( a, x, y )  bli_daxpyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zddaxpys( a, x, y )  bli_daxpyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_scdaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dcdaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ccdaxpys( a, x, y )  bli_daxpyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zcdaxpys( a, x, y )  bli_daxpyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_szdaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dzdaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_czdaxpys( a, x, y )  bli_daxpyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zzdaxpys( a, x, y )  bli_daxpyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dscaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_cscaxpys( a, x, y )  bli_caxpyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_zscaxpys( a, x, y )  bli_caxpyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )

#define bli_sdcaxpys( a, x, y )  bli_saxpyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ddcaxpys( a, x, y )  bli_saxpyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cdcaxpys( a, x, y )  bli_caxpyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zdcaxpys( a, x, y )  bli_caxpyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )

#define bli_sccaxpys( a, x, y )  bli_scaxpyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccaxpys( a, x, y )  bli_scaxpyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccaxpys( a, x, y )   bli_caxpyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccaxpys( a, x, y )   bli_caxpyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcaxpys( a, x, y )  bli_scaxpyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dzcaxpys( a, x, y )  bli_scaxpyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_czcaxpys( a, x, y )   bli_caxpyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zzcaxpys( a, x, y )   bli_caxpyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dszaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cszaxpys( a, x, y )  bli_zaxpyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zszaxpys( a, x, y )  bli_zaxpyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sdzaxpys( a, x, y )  bli_daxpyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_ddzaxpys( a, x, y )  bli_daxpyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cdzaxpys( a, x, y )  bli_zaxpyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zdzaxpys( a, x, y )  bli_zaxpyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sczaxpys( a, x, y )  bli_dzaxpyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dczaxpys( a, x, y )  bli_dzaxpyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cczaxpys( a, x, y )   bli_zaxpyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zczaxpys( a, x, y )   bli_zaxpyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_szzaxpys( a, x, y )  bli_dzaxpyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzzaxpys( a, x, y )  bli_dzaxpyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czzaxpys( a, x, y )   bli_zaxpyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzzaxpys( a, x, y )   bli_zaxpyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dscaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cscaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zscaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_sdcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_ddcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cdcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zdcaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_sccaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dccaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cccaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zccaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_szcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dzcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_czcaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zzcaxpys( a, x, y )  { (y) += (a) * (x); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dszaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cszaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zszaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_sdzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_ddzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cdzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zdzaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_sczaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dczaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_cczaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zczaxpys( a, x, y )  { (y) += (a) * (x); }

#define bli_szzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_dzzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_czzaxpys( a, x, y )  { (y) += (a) * (x); }
#define bli_zzzaxpys( a, x, y )  { (y) += (a) * (x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_saxpys( a, x, y )  bli_sssaxpys( a, x, y )
#define bli_daxpys( a, x, y )  bli_dddaxpys( a, x, y )
#define bli_caxpys( a, x, y )  bli_cccaxpys( a, x, y )
#define bli_zaxpys( a, x, y )  bli_zzzaxpys( a, x, y )


#endif

