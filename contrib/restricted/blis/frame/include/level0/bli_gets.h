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

#ifndef BLIS_GETS_H
#define BLIS_GETS_H

// gets

// Notes:
// - The first char encodes the type of x.
// - The second char encodes the type of y.


#define bli_ssgets( x, yr, yi )  { (yr) = bli_sreal(x); (yi) = bli_simag(x); }
#define bli_dsgets( x, yr, yi )  { (yr) = bli_dreal(x); (yi) = bli_dimag(x); }
#define bli_csgets( x, yr, yi )  { (yr) = bli_creal(x); (yi) = bli_cimag(x); }
#define bli_zsgets( x, yr, yi )  { (yr) = bli_zreal(x); (yi) = bli_zimag(x); }
#define bli_isgets( x, yr, yi )  { (yr) = ( float )(x); (yi) = 0.0F; }

#define bli_sdgets( x, yr, yi )  { (yr) = bli_sreal(x); (yi) = bli_simag(x); }
#define bli_ddgets( x, yr, yi )  { (yr) = bli_dreal(x); (yi) = bli_dimag(x); }
#define bli_cdgets( x, yr, yi )  { (yr) = bli_creal(x); (yi) = bli_cimag(x); }
#define bli_zdgets( x, yr, yi )  { (yr) = bli_zreal(x); (yi) = bli_zimag(x); }
#define bli_idgets( x, yr, yi )  { (yr) = ( double )(x); (yi) = 0.0; }

#define bli_scgets( x, yr, yi )  { (yr) = bli_sreal(x); (yi) = bli_simag(x); }
#define bli_dcgets( x, yr, yi )  { (yr) = bli_dreal(x); (yi) = bli_dimag(x); }
#define bli_ccgets( x, yr, yi )  { (yr) = bli_creal(x); (yi) = bli_cimag(x); }
#define bli_zcgets( x, yr, yi )  { (yr) = bli_zreal(x); (yi) = bli_zimag(x); }
#define bli_icgets( x, yr, yi )  { (yr) = ( float )(x); (yi) = 0.0F; }

#define bli_szgets( x, yr, yi )  { (yr) = bli_sreal(x); (yi) = bli_simag(x); }
#define bli_dzgets( x, yr, yi )  { (yr) = bli_dreal(x); (yi) = bli_dimag(x); }
#define bli_czgets( x, yr, yi )  { (yr) = bli_creal(x); (yi) = bli_cimag(x); }
#define bli_zzgets( x, yr, yi )  { (yr) = bli_zreal(x); (yi) = bli_zimag(x); }
#define bli_izgets( x, yr, yi )  { (yr) = ( double )(x); (yi) = 0.0; }

#define bli_sigets( x, yr, yi )  { (yr) = bli_sreal(x); (yi) = 0; }
#define bli_digets( x, yr, yi )  { (yr) = bli_dreal(x); (yi) = 0; }
#define bli_cigets( x, yr, yi )  { (yr) = bli_creal(x); (yi) = 0; }
#define bli_zigets( x, yr, yi )  { (yr) = bli_zreal(x); (yi) = 0; }
#define bli_iigets( x, yr, yi )  { (yr) =          (x); (yi) = 0; }


#define bli_sgets( x, yr, yi )  bli_ssgets( x, yr, yi )
#define bli_dgets( x, yr, yi )  bli_ddgets( x, yr, yi )
#define bli_cgets( x, yr, yi )  bli_csgets( x, yr, yi )
#define bli_zgets( x, yr, yi )  bli_zdgets( x, yr, yi )
#define bli_igets( x, yr, yi )  bli_idgets( x, yr, yi )


#endif
