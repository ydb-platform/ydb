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

#ifndef BLIS_RANDNP2S_H
#define BLIS_RANDNP2S_H

// randnp2s


#define bli_srandnp2s( a ) \
{ \
	bli_drandnp2s( a ); \
}

#if 0
#define bli_drandnp2s_prev( a ) \
{ \
	const double m_max  = 3.0; \
	const double m_max2 = m_max + 2.0; \
	double       t; \
	double       r_val; \
\
	/* Compute a narrow-range power of two.

	   For the purposes of commentary, we'll assume that m_max = 4. This
	   represents the largest power of two we will use to generate the
	   random numbers. */ \
\
	/* Generate a random real number t on the interval: [0.0, 6.0]. */ \
	t = ( ( double ) rand() / ( double ) RAND_MAX ) * m_max2; \
\
	/* Modify t to guarantee that is never equal to the upper bound of
	   the interval (in this case, 6.0). */ \
	if ( t == m_max2 ) t = t - 1.0; \
\
	/* Transform the interval into the set of integers, {0,1,2,3,4,5}. */ \
	t = floor( t ); \
\
	/* Map values of t == 0 to a final value of 0. */ \
	if ( t == 0.0 ) r_val = 0.0; \
	else \
	{ \
		/* This case handles values of t = {1,2,3,4,5}. */ \
\
		double s_exp, s_val; \
\
		/* Compute two random numbers to determine the signs of the
		   exponent and the end result. */ \
		PASTEMAC(d,rands)( s_exp ); \
		PASTEMAC(d,rands)( s_val ); \
\
		/* Compute r_val = 2^s where s = +/-(t-1) = {-4,-3,-2,-1,0,1,2,3,4}. */ \
		if ( s_exp < 0.0 ) r_val = pow( 2.0, -(t - 1.0) ); \
		else               r_val = pow( 2.0,   t - 1.0  ); \
\
		/* If our sign value is negative, our random power of two will
		   be negative. */ \
		if ( s_val < 0.0 ) r_val = -r_val; \
	} \
\
	/* Normalize by the largest possible positive value. */ \
	r_val = r_val / pow( 2.0, m_max ); \
\
	/* r_val = 0, or +/-{2^-4, 2^-3, 2^-2, 2^-1, 2^0, 2^1, 2^2, 2^3, 2^4}. */ \
	/* NOTE: For single-precision macros, this assignment results in typecast
	   down to float. */ \
	a = r_val; \
}
#endif

#define bli_drandnp2s( a ) \
{ \
	const double m_max  = 6.0; \
	const double m_max2 = m_max + 2.0; \
	double       t; \
	double       r_val; \
\
	/* Compute a narrow-range power of two.

	   For the purposes of commentary, we'll assume that m_max = 4. This
	   represents the largest power of two we will use to generate the
	   random numbers. */ \
\
	do \
	{ \
		/* Generate a random real number t on the interval: [0.0, 6.0]. */ \
		t = ( ( double ) rand() / ( double ) RAND_MAX ) * m_max2; \
\
		/* Transform the interval into the set of integers, {0,1,2,3,4,5}.
		   Note that 6 is prohibited by the loop guard below. */ \
		t = floor( t ); \
	} \
	/* If t is ever equal to m_max2, we re-randomize. The guard against
	   m_max2 < t is for sanity and shouldn't happen, unless perhaps there
	   is weirdness in the typecasting to double when computing t above. */ \
	while ( m_max2 <= t ); \
\
	/* Map values of t == 0 to a final value of 0. */ \
	if ( t == 0.0 ) r_val = 0.0; \
	else \
	{ \
		/* This case handles values of t = {1,2,3,4,5}. */ \
\
		double s_val; \
\
		/* Compute r_val = 2^s where s = -(t-1) = {-4,-3,-2,-1,0}. */ \
		r_val = pow( 2.0, -(t - 1.0) ); \
\
		/* Compute a random number to determine the sign of the final
		   result. */ \
		PASTEMAC(d,rands)( s_val ); \
\
		/* If our sign value is negative, our random power of two will
		   be negative. */ \
		if ( s_val < 0.0 ) r_val = -r_val; \
	} \
\
	/* r_val = 0, or +/-{2^0, 2^-1, 2^-2, 2^-3, 2^-4}. */ \
	/* NOTE: For single-precision macros, this assignment results in typecast
	   down to float. */ \
	a = r_val; \
}
#define bli_crandnp2s( a ) \
{ \
	float  ar, ai; \
\
	bli_srandnp2s( ar ); \
	bli_srandnp2s( ai ); \
\
	bli_csets( ar, ai, (a) ); \
}
#define bli_zrandnp2s( a ) \
{ \
	double ar, ai; \
\
	bli_drandnp2s( ar ); \
	bli_drandnp2s( ai ); \
\
	bli_zsets( ar, ai, (a) ); \
}


#endif

