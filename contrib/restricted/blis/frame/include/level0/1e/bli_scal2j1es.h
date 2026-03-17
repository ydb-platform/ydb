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

#ifndef BLIS_SCAL2J1ES_H
#define BLIS_SCAL2J1ES_H

// scal2j1es

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal2j1es( a, x, yri, yir ) {}
#define bli_sdsscal2j1es( a, x, yri, yir ) {}
#define bli_scsscal2j1es( a, x, yri, yir ) {}
#define bli_szsscal2j1es( a, x, yri, yir ) {}

#define bli_dssscal2j1es( a, x, yri, yir ) {}
#define bli_ddsscal2j1es( a, x, yri, yir ) {}
#define bli_dcsscal2j1es( a, x, yri, yir ) {}
#define bli_dzsscal2j1es( a, x, yri, yir ) {}

#define bli_cssscal2j1es( a, x, yri, yir ) {}
#define bli_cdsscal2j1es( a, x, yri, yir ) {}
#define bli_ccsscal2j1es( a, x, yri, yir ) {}
#define bli_czsscal2j1es( a, x, yri, yir ) {}

#define bli_zssscal2j1es( a, x, yri, yir ) {}
#define bli_zdsscal2j1es( a, x, yri, yir ) {}
#define bli_zcsscal2j1es( a, x, yri, yir ) {}
#define bli_zzsscal2j1es( a, x, yri, yir ) {}

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal2j1es( a, x, yri, yir ) {}
#define bli_sddscal2j1es( a, x, yri, yir ) {}
#define bli_scdscal2j1es( a, x, yri, yir ) {}
#define bli_szdscal2j1es( a, x, yri, yir ) {}

#define bli_dsdscal2j1es( a, x, yri, yir ) {}
#define bli_dddscal2j1es( a, x, yri, yir ) {}
#define bli_dcdscal2j1es( a, x, yri, yir ) {}
#define bli_dzdscal2j1es( a, x, yri, yir ) {}

#define bli_csdscal2j1es( a, x, yri, yir ) {}
#define bli_cddscal2j1es( a, x, yri, yir ) {}
#define bli_ccdscal2j1es( a, x, yri, yir ) {}
#define bli_czdscal2j1es( a, x, yri, yir ) {}

#define bli_zsdscal2j1es( a, x, yri, yir ) {}
#define bli_zddscal2j1es( a, x, yri, yir ) {}
#define bli_zcdscal2j1es( a, x, yri, yir ) {}
#define bli_zzdscal2j1es( a, x, yri, yir ) {}

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal2j1es( a, x, yri, yir ) {}
#define bli_sdcscal2j1es( a, x, yri, yir ) {}
#define bli_sccscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_szcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_dscscal2j1es( a, x, yri, yir ) {}
#define bli_ddcscal2j1es( a, x, yri, yir ) {}
#define bli_dccscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_dzcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_cscscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), -bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_simag(x),  bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cdcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), -bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_dimag(x),  bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cccscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_czcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_zscscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), -bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_simag(x),  bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zdcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), -bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_dimag(x),  bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zccscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zzcscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal2j1es( a, x, yri, yir ) {}
#define bli_sdzscal2j1es( a, x, yri, yir ) {}
#define bli_sczscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_szzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_dszscal2j1es( a, x, yri, yir ) {}
#define bli_ddzscal2j1es( a, x, yri, yir ) {}
#define bli_dczscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_dzzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_cszscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_sreal(x), -bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_simag(x),  bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cdzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_dreal(x), -bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_dimag(x),  bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cczscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_czzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_zszscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_sreal(x), -bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_simag(x),  bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zdzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_dreal(x), -bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_dimag(x),  bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zczscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_creal(x), -bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_cimag(x),  bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zzzscal2j1es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zreal(x), -bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), bli_zimag(x),  bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}



#define bli_cscal2j1es( a, x, yri, yir ) bli_cccscal2j1es( a, x, yri, yir )
#define bli_zscal2j1es( a, x, yri, yir ) bli_zzzscal2j1es( a, x, yri, yir )

#endif

