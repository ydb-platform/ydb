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

#ifndef BLIS_ABVAL2S_H
#define BLIS_ABVAL2S_H

// abval2s

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of a.

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_ssabval2s( x, a )              bli_sabval2ris( bli_sreal(x), bli_simag(x), bli_sreal(a), 0.0F         )
#define bli_dsabval2s( x, a )              bli_dabval2ris( bli_dreal(x), bli_dimag(x), bli_sreal(a), 0.0F         )
#define bli_csabval2s( x, a ) { float ti;  bli_cabval2ris( bli_creal(x), bli_cimag(x), bli_sreal(a), ti           ); ( void )ti; }
#define bli_zsabval2s( x, a ) { float ti;  bli_zabval2ris( bli_zreal(x), bli_zimag(x), bli_sreal(a), ti           ); ( void )ti; }

#define bli_sdabval2s( x, a )              bli_sabval2ris( bli_sreal(x), bli_simag(x), bli_dreal(a), 0.0          )
#define bli_ddabval2s( x, a )              bli_dabval2ris( bli_dreal(x), bli_dimag(x), bli_dreal(a), 0.0          )
#define bli_cdabval2s( x, a ) { double ti; bli_cabval2ris( bli_creal(x), bli_cimag(x), bli_dreal(a), ti           ); ( void )ti; }
#define bli_zdabval2s( x, a ) { double ti; bli_zabval2ris( bli_zreal(x), bli_zimag(x), bli_dreal(a), ti           ); ( void )ti; }

#define bli_scabval2s( x, a )              bli_sabval2ris( bli_sreal(x), bli_simag(x), bli_creal(a), bli_cimag(a) )
#define bli_dcabval2s( x, a )              bli_dabval2ris( bli_dreal(x), bli_dimag(x), bli_creal(a), bli_cimag(a) )
#define bli_ccabval2s( x, a )              bli_cabval2ris( bli_creal(x), bli_cimag(x), bli_creal(a), bli_cimag(a) )
#define bli_zcabval2s( x, a )              bli_zabval2ris( bli_zreal(x), bli_zimag(x), bli_creal(a), bli_cimag(a) )

#define bli_szabval2s( x, a )              bli_sabval2ris( bli_sreal(x), bli_simag(x), bli_zreal(a), bli_zimag(a) )
#define bli_dzabval2s( x, a )              bli_dabval2ris( bli_dreal(x), bli_dimag(x), bli_zreal(a), bli_zimag(a) )
#define bli_czabval2s( x, a )              bli_cabval2ris( bli_creal(x), bli_cimag(x), bli_zreal(a), bli_zimag(a) )
#define bli_zzabval2s( x, a )              bli_zabval2ris( bli_zreal(x), bli_zimag(x), bli_zreal(a), bli_zimag(a) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_ssabval2s( x, a )  bli_sssets( fabsf(x), 0.0, (a) )
#define bli_dsabval2s( x, a )  bli_dssets( fabs (x), 0.0, (a) )
#define bli_csabval2s( x, a )  bli_cssets( cabsf(x), 0.0, (a) )
#define bli_zsabval2s( x, a )  bli_zssets( cabs (x), 0.0, (a) )

#define bli_sdabval2s( x, a )  bli_sdsets( fabsf(x), 0.0, (a) )
#define bli_ddabval2s( x, a )  bli_ddsets( fabs (x), 0.0, (a) )
#define bli_cdabval2s( x, a )  bli_cdsets( cabsf(x), 0.0, (a) )
#define bli_zdabval2s( x, a )  bli_zdsets( cabs (x), 0.0, (a) )

#define bli_scabval2s( x, a )  bli_scsets( fabsf(x), 0.0, (a) )
#define bli_dcabval2s( x, a )  bli_dcsets( fabs (x), 0.0, (a) )
#define bli_ccabval2s( x, a )  bli_ccsets( cabsf(x), 0.0, (a) )
#define bli_zcabval2s( x, a )  bli_zcsets( cabs (x), 0.0, (a) )

#define bli_szabval2s( x, a )  bli_szsets( fabsf(x), 0.0, (a) )
#define bli_dzabval2s( x, a )  bli_dzsets( fabs (x), 0.0, (a) )
#define bli_czabval2s( x, a )  bli_czsets( cabsf(x), 0.0, (a) )
#define bli_zzabval2s( x, a )  bli_zzsets( cabs (x), 0.0, (a) )

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sabval2s( x, a )  bli_ssabval2s( x, a )
#define bli_dabval2s( x, a )  bli_ddabval2s( x, a )
#define bli_cabval2s( x, a )  bli_ccabval2s( x, a )
#define bli_zabval2s( x, a )  bli_zzabval2s( x, a )


#endif
