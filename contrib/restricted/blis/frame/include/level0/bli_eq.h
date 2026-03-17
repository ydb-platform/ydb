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

#ifndef BLIS_EQ_H
#define BLIS_EQ_H


// eq (passed by value)

#define bli_seq( a, b )  ( (a) == (b) )
#define bli_deq( a, b )  ( (a) == (b) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_ceq( a, b )  ( ( bli_creal(a) == bli_creal(b) ) && ( bli_cimag(a) == bli_cimag(b) ) )
#define bli_zeq( a, b )  ( ( bli_zreal(a) == bli_zreal(b) ) && ( bli_zimag(a) == bli_zimag(b) ) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_ceq( a, b )  ( (a) == (b) )
#define bli_zeq( a, b )  ( (a) == (b) )

#endif // BLIS_ENABLE_C99_COMPLEX

#define bli_ieq( a, b )  ( (a) == (b) )



// eqtori (passed by value)

#define bli_seqtori( a, br, bi )  ( (a) == (br) )
#define bli_deqtori( a, br, bi )  ( (a) == (br) )

#ifndef BLIS_ENABLE_C99_COMPLEX

#define bli_ceqtori( a, br, bi )  ( ( bli_creal(a) == (br) ) && ( bli_cimag(a) == (bi) ) )
#define bli_zeqtori( a, br, bi )  ( ( bli_zreal(a) == (br) ) && ( bli_zimag(a) == (bi) ) )

#else // ifdef BLIS_ENABLE_C99_COMPLEX

#define bli_ceqtori( a, br, bi )  ( (a) == (br) + (bi) * (I) )
#define bli_zeqtori( a, br, bi )  ( (a) == (br) + (bi) * (I) )

#endif // BLIS_ENABLE_C99_COMPLEX



// eqa (passed by address)

#define bli_seqa( a, b )  bli_seq( *(( float*    )(a)), *(( float*    )(b)) )
#define bli_deqa( a, b )  bli_deq( *(( double*   )(a)), *(( double*   )(b)) )
#define bli_ceqa( a, b )  bli_ceq( *(( scomplex* )(a)), *(( scomplex* )(b)) )
#define bli_zeqa( a, b )  bli_zeq( *(( dcomplex* )(a)), *(( dcomplex* )(b)) )
#define bli_ieqa( a, b )  bli_ieq( *(( gint_t*   )(a)), *(( gint_t*   )(b)) )



// eq1

#define bli_seq1( a )  bli_seqtori( (a), 1.0F, 0.0F )
#define bli_deq1( a )  bli_deqtori( (a), 1.0,  0.0  )
#define bli_ceq1( a )  bli_ceqtori( (a), 1.0F, 0.0F )
#define bli_zeq1( a )  bli_zeqtori( (a), 1.0,  0.0  )
#define bli_ieq1( a )  bli_ieq    ( (a), 1          )



// eq0

#define bli_seq0( a )  bli_seqtori( (a), 0.0F, 0.0F )
#define bli_deq0( a )  bli_deqtori( (a), 0.0,  0.0  )
#define bli_ceq0( a )  bli_ceqtori( (a), 0.0F, 0.0F )
#define bli_zeq0( a )  bli_zeqtori( (a), 0.0,  0.0  )
#define bli_ieq0( a )  bli_ieq    ( (a), 0          )



// eqm1

#define bli_seqm1( a )  bli_seqtori( (a), -1.0F, 0.0F )
#define bli_deqm1( a )  bli_deqtori( (a), -1.0,  0.0  )
#define bli_ceqm1( a )  bli_ceqtori( (a), -1.0F, 0.0F )
#define bli_zeqm1( a )  bli_zeqtori( (a), -1.0,  0.0  )
#define bli_ieqm1( a )  bli_ieq    ( (a), -1          )



#endif
