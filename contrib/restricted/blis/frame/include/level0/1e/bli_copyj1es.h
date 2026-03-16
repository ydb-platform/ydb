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

#ifndef BLIS_COPYJ1ES_H
#define BLIS_COPYJ1ES_H

// copyj1es

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sscopyj1es( a, bri, bir ) {}
#define bli_dscopyj1es( a, bri, bir ) {}
#define bli_cscopyj1es( a, bri, bir ) {}
#define bli_zscopyj1es( a, bri, bir ) {}

#define bli_sdcopyj1es( a, bri, bir ) {}
#define bli_ddcopyj1es( a, bri, bir ) {}
#define bli_cdcopyj1es( a, bri, bir ) {}
#define bli_zdcopyj1es( a, bri, bir ) {}

#define bli_sccopyj1es( a, bri, bir ) {}
#define bli_dccopyj1es( a, bri, bir ) {}
#define bli_cccopyj1es( a, bri, bir ) \
{ \
	bli_cccopyris( bli_creal(a), -bli_cimag(a), bli_creal(bri), bli_cimag(bri) ); \
	bli_cccopyris( bli_cimag(a),  bli_creal(a), bli_creal(bir), bli_cimag(bir) ); \
}
#define bli_zccopyj1es( a, bri, bir ) \
{ \
	bli_zccopyris( bli_zreal(a), -bli_zimag(a), bli_creal(bri), bli_cimag(bri) ); \
	bli_zccopyris( bli_zimag(a),  bli_zreal(a), bli_creal(bir), bli_cimag(bir) ); \
}

#define bli_szcopyj1es( a, bri, bir ) {}
#define bli_dzcopyj1es( a, bri, bir ) {}
#define bli_czcopyj1es( a, bri, bir ) \
{ \
	bli_czcopyris( bli_creal(a), -bli_cimag(a), bli_zreal(bri), bli_zimag(bri) ); \
	bli_czcopyris( bli_cimag(a),  bli_creal(a), bli_zreal(bir), bli_zimag(bir) ); \
}
#define bli_zzcopyj1es( a, bri, bir ) \
{ \
	bli_zzcopyris( bli_zreal(a), -bli_zimag(a), bli_zreal(bri), bli_zimag(bri) ); \
	bli_zzcopyris( bli_zimag(a),  bli_zreal(a), bli_zreal(bir), bli_zimag(bir) ); \
}


#define bli_ccopyj1es( a, bri, bir ) bli_cccopyj1es( a, bri, bir )
#define bli_zcopyj1es( a, bri, bir ) bli_zzcopyj1es( a, bri, bir )

#endif

