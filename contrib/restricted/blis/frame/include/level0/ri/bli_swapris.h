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

#ifndef BLIS_SWAPRIS_H
#define BLIS_SWAPRIS_H

// swapris

#define bli_sswapris( ar, ai, br, bi ) \
{ \
	float tr, ti; \
\
	bli_scopyris( (br), (bi), (tr), (ti) ); \
	bli_scopyris( (ar), (ai), (br), (bi) ); \
	bli_scopyris( (tr), (ti), (ar), (ai) ); \
}

#define bli_dswapris( ar, ai, br, bi ) \
{ \
	double tr, ti; \
\
	bli_dcopyris( (br), (bi), (tr), (ti) ); \
	bli_dcopyris( (ar), (ai), (br), (bi) ); \
	bli_dcopyris( (tr), (ti), (ar), (ai) ); \
}

#define bli_cswapris( ar, ai, br, bi ) \
{ \
	scomplex tr, ti; \
\
	bli_ccopyris( (br), (bi), (tr), (ti) ); \
	bli_ccopyris( (ar), (ai), (br), (bi) ); \
	bli_ccopyris( (tr), (ti), (ar), (ai) ); \
}

#define bli_zswapris( ar, ai, br, bi ) \
{ \
	dcomplex tr, ti; \
\
	bli_zcopyris( (br), (bi), (tr), (ti) ); \
	bli_zcopyris( (ar), (ai), (br), (bi) ); \
	bli_zcopyris( (tr), (ti), (ar), (ai) ); \
}

#endif

