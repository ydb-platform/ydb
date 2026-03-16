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

#ifndef BLIS_AXMYS_H
#define BLIS_AXMYS_H

// axmys

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.


// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_dssaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_cssaxmys( a, x, y )  bli_saxmyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )
#define bli_zssaxmys( a, x, y )  bli_saxmyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_sreal(y), bli_simag(y) )

#define bli_sdsaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ddsaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_cdsaxmys( a, x, y )  bli_saxmyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zdsaxmys( a, x, y )  bli_saxmyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_sreal(y), bli_simag(y) )

#define bli_scsaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dcsaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_ccsaxmys( a, x, y )  bli_saxmyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zcsaxmys( a, x, y )  bli_saxmyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_sreal(y), bli_simag(y) )

#define bli_szsaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_dzsaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_czsaxmys( a, x, y )  bli_saxmyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )
#define bli_zzsaxmys( a, x, y )  bli_saxmyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_sreal(y), bli_simag(y) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dsdaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_csdaxmys( a, x, y )  bli_daxmyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zsdaxmys( a, x, y )  bli_daxmyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_dreal(y), bli_dimag(y) )

#define bli_sddaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dddaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_cddaxmys( a, x, y )  bli_daxmyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zddaxmys( a, x, y )  bli_daxmyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_scdaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dcdaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_ccdaxmys( a, x, y )  bli_daxmyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zcdaxmys( a, x, y )  bli_daxmyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_dreal(y), bli_dimag(y) )

#define bli_szdaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_dzdaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_czdaxmys( a, x, y )  bli_daxmyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )
#define bli_zzdaxmys( a, x, y )  bli_daxmyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_dreal(y), bli_dimag(y) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_dscaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_cscaxmys( a, x, y )  bli_caxmyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )
#define bli_zscaxmys( a, x, y )  bli_caxmyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_creal(y), bli_cimag(y) )

#define bli_sdcaxmys( a, x, y )  bli_saxmyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_ddcaxmys( a, x, y )  bli_saxmyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cdcaxmys( a, x, y )  bli_caxmyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zdcaxmys( a, x, y )  bli_caxmyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_creal(y), bli_cimag(y) )

#define bli_sccaxmys( a, x, y )  bli_scaxmyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dccaxmys( a, x, y )  bli_scaxmyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_cccaxmys( a, x, y )   bli_caxmyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zccaxmys( a, x, y )   bli_caxmyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_creal(y), bli_cimag(y) )

#define bli_szcaxmys( a, x, y )  bli_scaxmyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_dzcaxmys( a, x, y )  bli_scaxmyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_czcaxmys( a, x, y )   bli_caxmyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )
#define bli_zzcaxmys( a, x, y )   bli_caxmyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_creal(y), bli_cimag(y) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dszaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cszaxmys( a, x, y )  bli_zaxmyris( bli_creal(a), bli_cimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zszaxmys( a, x, y )  bli_zaxmyris( bli_zreal(a), bli_zimag(a), bli_sreal(x), bli_simag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sdzaxmys( a, x, y )  bli_daxmyris( bli_sreal(a), bli_simag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_ddzaxmys( a, x, y )  bli_daxmyris( bli_dreal(a), bli_dimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cdzaxmys( a, x, y )  bli_zaxmyris( bli_creal(a), bli_cimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zdzaxmys( a, x, y )  bli_zaxmyris( bli_zreal(a), bli_zimag(a), bli_dreal(x), bli_dimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_sczaxmys( a, x, y )  bli_dzaxmyris( bli_sreal(a), bli_simag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dczaxmys( a, x, y )  bli_dzaxmyris( bli_dreal(a), bli_dimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_cczaxmys( a, x, y )   bli_zaxmyris( bli_creal(a), bli_cimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zczaxmys( a, x, y )   bli_zaxmyris( bli_zreal(a), bli_zimag(a), bli_creal(x), bli_cimag(x), bli_zreal(y), bli_zimag(y) )

#define bli_szzaxmys( a, x, y )  bli_dzaxmyris( bli_sreal(a), bli_simag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_dzzaxmys( a, x, y )  bli_dzaxmyris( bli_dreal(a), bli_dimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_czzaxmys( a, x, y )   bli_zaxmyris( bli_creal(a), bli_cimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )
#define bli_zzzaxmys( a, x, y )   bli_zaxmyris( bli_zreal(a), bli_zimag(a), bli_zreal(x), bli_zimag(x), bli_zreal(y), bli_zimag(y) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dscaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cscaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zscaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_sdcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_ddcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cdcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zdcaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_sccaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dccaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cccaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zccaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_szcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dzcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_czcaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zzcaxmys( a, x, y )  { (y) -= (a) * (x); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dszaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cszaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zszaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_sdzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_ddzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cdzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zdzaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_sczaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dczaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_cczaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zczaxmys( a, x, y )  { (y) -= (a) * (x); }

#define bli_szzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_dzzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_czzaxmys( a, x, y )  { (y) -= (a) * (x); }
#define bli_zzzaxmys( a, x, y )  { (y) -= (a) * (x); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_saxmys( a, x, y )  bli_sssaxmys( a, x, y )
#define bli_daxmys( a, x, y )  bli_dddaxmys( a, x, y )
#define bli_caxmys( a, x, y )  bli_cccaxmys( a, x, y )
#define bli_zaxmys( a, x, y )  bli_zzzaxmys( a, x, y )


#endif

