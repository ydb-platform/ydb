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

#ifndef BLIS_COPYJRIS_H
#define BLIS_COPYJRIS_H

// copyjris

#define bli_scopyjris( ar, ai, br, bi )  bli_scopyris( (ar), -(ai), (br), (bi) )
#define bli_dcopyjris( ar, ai, br, bi )  bli_dcopyris( (ar), -(ai), (br), (bi) )
#define bli_ccopyjris( ar, ai, br, bi )  bli_ccopyris( (ar), -(ai), (br), (bi) )
#define bli_zcopyjris( ar, ai, br, bi )  bli_zcopyris( (ar), -(ai), (br), (bi) )

#define bli_sscopyjris( ar, ai, br, bi )  bli_scopyjris( ar, 0.0F, br, bi )
#define bli_dscopyjris( ar, ai, br, bi )  bli_scopyjris( ar, 0.0,  br, bi )
#define bli_cscopyjris( ar, ai, br, bi )  bli_scopyjris( ar, ai,   br, bi )
#define bli_zscopyjris( ar, ai, br, bi )  bli_scopyjris( ar, ai,   br, bi )

#define bli_sdcopyjris( ar, ai, br, bi )  bli_dcopyjris( ar, 0.0F, br, bi )
#define bli_ddcopyjris( ar, ai, br, bi )  bli_dcopyjris( ar, 0.0,  br, bi )
#define bli_cdcopyjris( ar, ai, br, bi )  bli_dcopyjris( ar, ai,   br, bi )
#define bli_zdcopyjris( ar, ai, br, bi )  bli_dcopyjris( ar, ai,   br, bi )

#define bli_sccopyjris( ar, ai, br, bi )  bli_ccopyjris( ar, 0.0F, br, bi )
#define bli_dccopyjris( ar, ai, br, bi )  bli_ccopyjris( ar, 0.0,  br, bi )
#define bli_cccopyjris( ar, ai, br, bi )  bli_ccopyjris( ar, ai,   br, bi )
#define bli_zccopyjris( ar, ai, br, bi )  bli_ccopyjris( ar, ai,   br, bi )

#define bli_szcopyjris( ar, ai, br, bi )  bli_zcopyjris( ar, 0.0F, br, bi )
#define bli_dzcopyjris( ar, ai, br, bi )  bli_zcopyjris( ar, 0.0,  br, bi )
#define bli_czcopyjris( ar, ai, br, bi )  bli_zcopyjris( ar, ai,   br, bi )
#define bli_zzcopyjris( ar, ai, br, bi )  bli_zcopyjris( ar, ai,   br, bi )

#endif

