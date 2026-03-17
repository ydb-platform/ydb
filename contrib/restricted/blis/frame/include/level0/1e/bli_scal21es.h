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

#ifndef BLIS_SCAL21ES_H
#define BLIS_SCAL21ES_H

// scal21es

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of x.
// - The third char encodes the type of y.

// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssscal21es( a, x, yri, yir ) {}
#define bli_sdsscal21es( a, x, yri, yir ) {}
#define bli_scsscal21es( a, x, yri, yir ) {}
#define bli_szsscal21es( a, x, yri, yir ) {}

#define bli_dssscal21es( a, x, yri, yir ) {}
#define bli_ddsscal21es( a, x, yri, yir ) {}
#define bli_dcsscal21es( a, x, yri, yir ) {}
#define bli_dzsscal21es( a, x, yri, yir ) {}

#define bli_cssscal21es( a, x, yri, yir ) {}
#define bli_cdsscal21es( a, x, yri, yir ) {}
#define bli_ccsscal21es( a, x, yri, yir ) {}
#define bli_czsscal21es( a, x, yri, yir ) {}

#define bli_zssscal21es( a, x, yri, yir ) {}
#define bli_zdsscal21es( a, x, yri, yir ) {}
#define bli_zcsscal21es( a, x, yri, yir ) {}
#define bli_zzsscal21es( a, x, yri, yir ) {}

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdscal21es( a, x, yri, yir ) {}
#define bli_sddscal21es( a, x, yri, yir ) {}
#define bli_scdscal21es( a, x, yri, yir ) {}
#define bli_szdscal21es( a, x, yri, yir ) {}

#define bli_dsdscal21es( a, x, yri, yir ) {}
#define bli_dddscal21es( a, x, yri, yir ) {}
#define bli_dcdscal21es( a, x, yri, yir ) {}
#define bli_dzdscal21es( a, x, yri, yir ) {}

#define bli_csdscal21es( a, x, yri, yir ) {}
#define bli_cddscal21es( a, x, yri, yir ) {}
#define bli_ccdscal21es( a, x, yri, yir ) {}
#define bli_czdscal21es( a, x, yri, yir ) {}

#define bli_zsdscal21es( a, x, yri, yir ) {}
#define bli_zddscal21es( a, x, yri, yir ) {}
#define bli_zcdscal21es( a, x, yri, yir ) {}
#define bli_zzdscal21es( a, x, yri, yir ) {}

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscscal21es( a, x, yri, yir ) {}
#define bli_sdcscal21es( a, x, yri, yir ) {}
#define bli_sccscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_szcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_dscscal21es( a, x, yri, yir ) {}
#define bli_ddcscal21es( a, x, yri, yir ) {}
#define bli_dccscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_dzcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_cscscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_sreal(x), bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_simag(x), bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cdcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_dreal(x), bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_dimag(x), bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cccscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_czcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_zscscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_sreal(x), bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_simag(x), bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zdcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_dreal(x), bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_dimag(x), bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zccscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zzcscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszscal21es( a, x, yri, yir ) {}
#define bli_sdzscal21es( a, x, yri, yir ) {}
#define bli_sczscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_szzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_sreal(a), bli_simag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_dszscal21es( a, x, yri, yir ) {}
#define bli_ddzscal21es( a, x, yri, yir ) {}
#define bli_dczscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_dzzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_dreal(a), bli_dimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_cszscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_sreal(x), bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_simag(x), bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cdzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_dreal(x), bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_dimag(x), bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_cczscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_czzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_creal(a), bli_cimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}

#define bli_zszscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_sreal(x), bli_simag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_simag(x), bli_sreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zdzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_dreal(x), bli_dimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_dimag(x), bli_dreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zczscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_creal(x), bli_cimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_cimag(x), bli_creal(x), bli_zreal(yir), bli_zimag(yir) ); \
}
#define bli_zzzscal21es( a, x, yri, yir ) \
{ \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a),  bli_zreal(x), bli_zimag(x), bli_zreal(yri), bli_zimag(yri) ); \
	bli_cxscal2ris( bli_zreal(a), bli_zimag(a), -bli_zimag(x), bli_zreal(x), bli_zreal(yir), bli_zimag(yir) ); \
}



#define bli_cscal21es( a, x, yri, yir ) bli_cccscal21es( a, x, yri, yir )
#define bli_zscal21es( a, x, yri, yir ) bli_zzzscal21es( a, x, yri, yir )

#endif

