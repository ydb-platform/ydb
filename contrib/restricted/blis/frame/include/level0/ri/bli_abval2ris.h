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

#ifndef BLIS_ABVAL2RIS_H
#define BLIS_ABVAL2RIS_H

// abval2ris

#define bli_sabval2ris( xr, xi, ar, ai ) \
{ \
	(ar)       = fabsf(xr); \
}

#define bli_dabval2ris( xr, xi, ar, ai ) \
{ \
	(ar)       = fabs(xr); \
}

#define bli_cabval2ris( xr, xi, ar, ai ) \
{ \
	float  s   = bli_fmaxabs( (xr), (xi) ); \
	float  mag; \
	if ( s == 0.0F ) mag = 0.0F; \
	else \
	{ \
		mag = sqrtf( s ) * \
		      sqrtf( ( (xr) / s ) * (xr) + \
		             ( (xi) / s ) * (xi) ); \
	} \
	(ar)       = mag; \
	(ai)       = 0.0F; \
}

#define bli_zabval2ris( xr, xi, ar, ai ) \
{ \
	double s   = bli_fmaxabs( (xr), (xi) ); \
	double mag; \
	if ( s == 0.0 ) mag = 0.0; \
	else \
	{ \
		mag = sqrt( s ) * \
		      sqrt( ( (xr) / s ) * (xr) + \
		            ( (xi) / s ) * (xi) ); \
	} \
	(ar)       = mag; \
	(ai)       = 0.0; \
}

#endif
