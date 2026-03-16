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

#ifndef BLIS_INVSCALRIS_H
#define BLIS_INVSCALRIS_H

// invscalris

#define bli_sinvscalris( ar, ai, xr, xi ) \
{ \
	(xr) /= (ar); \
}

#define bli_dinvscalris( ar, ai, xr, xi ) \
{ \
	(xr) /= (ar); \
}

#define bli_cinvscalris( ar, ai, xr, xi ) \
{ \
	float  s     = bli_fmaxabs( (ar), (ai) ); \
	float  ar_s  = (ar) / s; \
	float  ai_s  = (ai) / s; \
	float  xrt   = (xr); \
	float  temp  = ( ar_s * (ar) + ai_s * (ai) ); \
	(xr)         = ( (xrt) * ar_s + (xi)  * ai_s ) / temp; \
	(xi)         = ( (xi)  * ar_s - (xrt) * ai_s ) / temp; \
}

#define bli_zinvscalris( ar, ai, xr, xi ) \
{ \
	double s     = bli_fmaxabs( (ar), (ai) ); \
	double ar_s  = (ar) / s; \
	double ai_s  = (ai) / s; \
	double xrt   = (xr); \
	double temp  = ( ar_s * (ar) + ai_s * (ai) ); \
	(xr)         = ( (xrt) * ar_s + (xi)  * ai_s ) / temp; \
	(xi)         = ( (xi)  * ar_s - (xrt) * ai_s ) / temp; \
}

#define bli_scinvscalris( ar, ai, xr, xi ) \
{ \
	(xr) /= (ar); \
	(xi) /= (ar); \
}

#define bli_dzinvscalris( ar, ai, xr, xi ) \
{ \
	(xr) /= (ar); \
	(xi) /= (ar); \
}

#endif
