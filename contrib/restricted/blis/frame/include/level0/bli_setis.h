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

#ifndef BLIS_SETIS_H
#define BLIS_SETIS_H

// setis

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.

#define bli_sssetis( xi, y )  { ; }
#define bli_dssetis( xi, y )  { ; }

#define bli_sdsetis( xi, y )  { ; }
#define bli_ddsetis( xi, y )  { ; }

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_scsetis( xi, y )  { bli_cimag(y) = (xi); }
#define bli_dcsetis( xi, y )  { bli_cimag(y) = (xi); }

#define bli_szsetis( xi, y )  { bli_zimag(y) = (xi); }
#define bli_dzsetis( xi, y )  { bli_zimag(y) = (xi); }

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_scsetis( xi, y )  { (y) = bli_creal(y) + (xi) * (I); }
#define bli_dcsetis( xi, y )  { (y) = bli_creal(y) + (xi) * (I); }

#define bli_szsetis( xi, y )  { (y) = bli_zreal(y) + (xi) * (I); }
#define bli_dzsetis( xi, y )  { (y) = bli_zreal(y) + (xi) * (I); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_ssetis( xi, y )  bli_sssetis( xi, y )
#define bli_dsetis( xi, y )  bli_ddsetis( xi, y )
#define bli_csetis( xi, y )  bli_scsetis( xi, y )
#define bli_zsetis( xi, y )  bli_dzsetis( xi, y )


#endif

