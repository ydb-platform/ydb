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

#ifndef BLIS_ADD3S_H
#define BLIS_ADD3S_H

// add3s

// Notes:
// - The first char encodes the type of a.
// - The second char encodes the type of b.
// - The third char encodes the type of c.


// -- (axy) = (??s) ------------------------------------------------------------

#define bli_sssadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_sreal(b), bli_simag(b), bli_sreal(c), bli_simag(c) )
#define bli_dssadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_sreal(b), bli_simag(b), bli_sreal(c), bli_simag(c) )
#define bli_cssadd3s( a, b, c )  bli_sadd3ris( bli_creal(a), bli_cimag(a), bli_sreal(b), bli_simag(b), bli_sreal(c), bli_simag(c) )
#define bli_zssadd3s( a, b, c )  bli_sadd3ris( bli_zreal(a), bli_zimag(a), bli_sreal(b), bli_simag(b), bli_sreal(c), bli_simag(c) )

#define bli_sdsadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_dreal(b), bli_dimag(b), bli_sreal(c), bli_simag(c) )
#define bli_ddsadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_dreal(b), bli_dimag(b), bli_sreal(c), bli_simag(c) )
#define bli_cdsadd3s( a, b, c )  bli_sadd3ris( bli_creal(a), bli_cimag(a), bli_dreal(b), bli_dimag(b), bli_sreal(c), bli_simag(c) )
#define bli_zdsadd3s( a, b, c )  bli_sadd3ris( bli_zreal(a), bli_zimag(a), bli_dreal(b), bli_dimag(b), bli_sreal(c), bli_simag(c) )

#define bli_scsadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_creal(b), bli_cimag(b), bli_sreal(c), bli_simag(c) )
#define bli_dcsadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_creal(b), bli_cimag(b), bli_sreal(c), bli_simag(c) )
#define bli_ccsadd3s( a, b, c )  bli_sadd3ris( bli_creal(a), bli_cimag(a), bli_creal(b), bli_cimag(b), bli_sreal(c), bli_simag(c) )
#define bli_zcsadd3s( a, b, c )  bli_sadd3ris( bli_zreal(a), bli_zimag(a), bli_creal(b), bli_cimag(b), bli_sreal(c), bli_simag(c) )

#define bli_szsadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_zreal(b), bli_zimag(b), bli_sreal(c), bli_simag(c) )
#define bli_dzsadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_zreal(b), bli_zimag(b), bli_sreal(c), bli_simag(c) )
#define bli_czsadd3s( a, b, c )  bli_sadd3ris( bli_creal(a), bli_cimag(a), bli_zreal(b), bli_zimag(b), bli_sreal(c), bli_simag(c) )
#define bli_zzsadd3s( a, b, c )  bli_sadd3ris( bli_zreal(a), bli_zimag(a), bli_zreal(b), bli_zimag(b), bli_sreal(c), bli_simag(c) )

// -- (axy) = (??d) ------------------------------------------------------------

#define bli_ssdadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_sreal(b), bli_simag(b), bli_dreal(c), bli_dimag(c) )
#define bli_dsdadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_sreal(b), bli_simag(b), bli_dreal(c), bli_dimag(c) )
#define bli_csdadd3s( a, b, c )  bli_dadd3ris( bli_creal(a), bli_cimag(a), bli_sreal(b), bli_simag(b), bli_dreal(c), bli_dimag(c) )
#define bli_zsdadd3s( a, b, c )  bli_dadd3ris( bli_zreal(a), bli_zimag(a), bli_sreal(b), bli_simag(b), bli_dreal(c), bli_dimag(c) )

#define bli_sddadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_dreal(b), bli_dimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_dddadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_dreal(b), bli_dimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_cddadd3s( a, b, c )  bli_dadd3ris( bli_creal(a), bli_cimag(a), bli_dreal(b), bli_dimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_zddadd3s( a, b, c )  bli_dadd3ris( bli_zreal(a), bli_zimag(a), bli_dreal(b), bli_dimag(b), bli_dreal(c), bli_dimag(c) )

#define bli_scdadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_creal(b), bli_cimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_dcdadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_creal(b), bli_cimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_ccdadd3s( a, b, c )  bli_dadd3ris( bli_creal(a), bli_cimag(a), bli_creal(b), bli_cimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_zcdadd3s( a, b, c )  bli_dadd3ris( bli_zreal(a), bli_zimag(a), bli_creal(b), bli_cimag(b), bli_dreal(c), bli_dimag(c) )

#define bli_szdadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_zreal(b), bli_zimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_dzdadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_zreal(b), bli_zimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_czdadd3s( a, b, c )  bli_dadd3ris( bli_creal(a), bli_cimag(a), bli_zreal(b), bli_zimag(b), bli_dreal(c), bli_dimag(c) )
#define bli_zzdadd3s( a, b, c )  bli_dadd3ris( bli_zreal(a), bli_zimag(a), bli_zreal(b), bli_zimag(b), bli_dreal(c), bli_dimag(c) )

#ifndef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_sreal(b), bli_simag(b), bli_creal(c), bli_cimag(c) )
#define bli_dscadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_sreal(b), bli_simag(b), bli_creal(c), bli_cimag(c) )
#define bli_cscadd3s( a, b, c )  bli_cadd3ris( bli_creal(a), bli_cimag(a), bli_sreal(b), bli_simag(b), bli_creal(c), bli_cimag(c) )
#define bli_zscadd3s( a, b, c )  bli_cadd3ris( bli_zreal(a), bli_zimag(a), bli_sreal(b), bli_simag(b), bli_creal(c), bli_cimag(c) )

#define bli_sdcadd3s( a, b, c )  bli_sadd3ris( bli_sreal(a), bli_simag(a), bli_dreal(b), bli_dimag(b), bli_creal(c), bli_cimag(c) )
#define bli_ddcadd3s( a, b, c )  bli_sadd3ris( bli_dreal(a), bli_dimag(a), bli_dreal(b), bli_dimag(b), bli_creal(c), bli_cimag(c) )
#define bli_cdcadd3s( a, b, c )  bli_cadd3ris( bli_creal(a), bli_cimag(a), bli_dreal(b), bli_dimag(b), bli_creal(c), bli_cimag(c) )
#define bli_zdcadd3s( a, b, c )  bli_cadd3ris( bli_zreal(a), bli_zimag(a), bli_dreal(b), bli_dimag(b), bli_creal(c), bli_cimag(c) )

#define bli_sccadd3s( a, b, c )  bli_cadd3ris( bli_sreal(a), bli_simag(a), bli_creal(b), bli_cimag(b), bli_creal(c), bli_cimag(c) )
#define bli_dccadd3s( a, b, c )  bli_cadd3ris( bli_dreal(a), bli_dimag(a), bli_creal(b), bli_cimag(b), bli_creal(c), bli_cimag(c) )
#define bli_cccadd3s( a, b, c )  bli_cadd3ris( bli_creal(a), bli_cimag(a), bli_creal(b), bli_cimag(b), bli_creal(c), bli_cimag(c) )
#define bli_zccadd3s( a, b, c )  bli_cadd3ris( bli_zreal(a), bli_zimag(a), bli_creal(b), bli_cimag(b), bli_creal(c), bli_cimag(c) )

#define bli_szcadd3s( a, b, c )  bli_cadd3ris( bli_sreal(a), bli_simag(a), bli_zreal(b), bli_zimag(b), bli_creal(c), bli_cimag(c) )
#define bli_dzcadd3s( a, b, c )  bli_cadd3ris( bli_dreal(a), bli_dimag(a), bli_zreal(b), bli_zimag(b), bli_creal(c), bli_cimag(c) )
#define bli_czcadd3s( a, b, c )  bli_cadd3ris( bli_creal(a), bli_cimag(a), bli_zreal(b), bli_zimag(b), bli_creal(c), bli_cimag(c) )
#define bli_zzcadd3s( a, b, c )  bli_cadd3ris( bli_zreal(a), bli_zimag(a), bli_zreal(b), bli_zimag(b), bli_creal(c), bli_cimag(c) )

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_sreal(b), bli_simag(b), bli_zreal(c), bli_zimag(c) )
#define bli_dszadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_sreal(b), bli_simag(b), bli_zreal(c), bli_zimag(c) )
#define bli_cszadd3s( a, b, c )  bli_zadd3ris( bli_creal(a), bli_cimag(a), bli_sreal(b), bli_simag(b), bli_zreal(c), bli_zimag(c) )
#define bli_zszadd3s( a, b, c )  bli_zadd3ris( bli_zreal(a), bli_zimag(a), bli_sreal(b), bli_simag(b), bli_zreal(c), bli_zimag(c) )

#define bli_sdzadd3s( a, b, c )  bli_dadd3ris( bli_sreal(a), bli_simag(a), bli_dreal(b), bli_dimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_ddzadd3s( a, b, c )  bli_dadd3ris( bli_dreal(a), bli_dimag(a), bli_dreal(b), bli_dimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_cdzadd3s( a, b, c )  bli_zadd3ris( bli_creal(a), bli_cimag(a), bli_dreal(b), bli_dimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_zdzadd3s( a, b, c )  bli_zadd3ris( bli_zreal(a), bli_zimag(a), bli_dreal(b), bli_dimag(b), bli_zreal(c), bli_zimag(c) )

#define bli_sczadd3s( a, b, c )  bli_zadd3ris( bli_sreal(a), bli_simag(a), bli_creal(b), bli_cimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_dczadd3s( a, b, c )  bli_zadd3ris( bli_dreal(a), bli_dimag(a), bli_creal(b), bli_cimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_cczadd3s( a, b, c )  bli_zadd3ris( bli_creal(a), bli_cimag(a), bli_creal(b), bli_cimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_zczadd3s( a, b, c )  bli_zadd3ris( bli_zreal(a), bli_zimag(a), bli_creal(b), bli_cimag(b), bli_zreal(c), bli_zimag(c) )

#define bli_szzadd3s( a, b, c )  bli_zadd3ris( bli_sreal(a), bli_simag(a), bli_zreal(b), bli_zimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_dzzadd3s( a, b, c )  bli_zadd3ris( bli_dreal(a), bli_dimag(a), bli_zreal(b), bli_zimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_czzadd3s( a, b, c )  bli_zadd3ris( bli_creal(a), bli_cimag(a), bli_zreal(b), bli_zimag(b), bli_zreal(c), bli_zimag(c) )
#define bli_zzzadd3s( a, b, c )  bli_zadd3ris( bli_zreal(a), bli_zimag(a), bli_zreal(b), bli_zimag(b), bli_zreal(c), bli_zimag(c) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

// -- (axy) = (??c) ------------------------------------------------------------

#define bli_sscadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dscadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cscadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zscadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_sdcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_ddcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cdcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zdcadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_sccadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dccadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cccadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zccadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_szcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dzcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_czcadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zzcadd3s( a, b, c )  { (c) = (a) + (b); }

// -- (axy) = (??z) ------------------------------------------------------------

#define bli_sszadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dszadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cszadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zszadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_sdzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_ddzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cdzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zdzadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_sczadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dczadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_cczadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zczadd3s( a, b, c )  { (c) = (a) + (b); }

#define bli_szzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_dzzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_czzadd3s( a, b, c )  { (c) = (a) + (b); }
#define bli_zzzadd3s( a, b, c )  { (c) = (a) + (b); }

#endif // BLIS_ENABLE_C99_COMPLEX


#define bli_sadd3s( a, b, c )  bli_sssadd3s( a, b, c )
#define bli_dadd3s( a, b, c )  bli_dddadd3s( a, b, c )
#define bli_cadd3s( a, b, c )  bli_cccadd3s( a, b, c )
#define bli_zadd3s( a, b, c )  bli_zzzadd3s( a, b, c )


#endif

