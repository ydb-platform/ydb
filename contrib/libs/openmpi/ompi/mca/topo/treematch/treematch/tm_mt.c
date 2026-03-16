/*
   A C-program for MT19937, with improved initialization 2002/1/26.

   This is an optimized version that amortizes the shift/reload cost,
   by Eric Landry 2004-03-15.

   Before using, initialize the state by using init_genrand(seed) or
   init_by_array(init_key, key_length).

   Copyright (C) 1997--2004, Makoto Matsumoto, Takuji Nishimura, and
   Eric Landry; All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions
   are met:

     1. Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.

     2. Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer
        in the documentation and/or other materials provided with the
        distribution.

     3. The names of its contributors may not be used to endorse or
        promote products derived from this software without specific
        prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   Any feedback is very welcome.
   http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/emt.html
   email: m-mat @ math.sci.hiroshima-u.ac.jp (remove space)

   Reference: M. Matsumoto and T. Nishimura, "Mersenne Twister:
   A 623-Dimensionally Equidistributed Uniform Pseudo-Random Number
   Generator", ACM Transactions on Modeling and Computer Simulation,
   Vol. 8, No. 1, January 1998, pp 3--30.
*/

#include "tm_mt.h"

/* Period parameters */
#define N 624
#define M 397
#define MATRIX_A 0x9908b0dfUL   /* constant vector a */
#define UPPER_MASK 0x80000000UL /* most significant w-r bits */
#define LOWER_MASK 0x7fffffffUL /* least significant r bits */

static unsigned long x[N];      /* the array for the state vector  */
static unsigned long *p0, *p1, *pm;

/*
   initialize with a seed

   See Knuth TAOCP Vol2. 3rd Ed. P.106 for multiplier.

   In the previous versions, MSBs of the seed affect only MSBs of
   the state.

   2002-01-09 modified by Makoto Matsumoto
*/
void
init_genrand(unsigned long s)
{
  int i;

  x[0] = s & 0xffffffffUL;
  for (i = 1; i < N; ++i) {
    x[i] = (1812433253UL * (x[i - 1] ^ (x[i - 1] >> 30)) + i)
      & 0xffffffffUL;           /* for >32 bit machines */
  }
  p0 = x;
  p1 = x + 1;
  pm = x + M;
}

/*
   initialize by an array with array-length

   init_key is the array for initializing keys

   key_length is its length

   2004-02-26 slight change for C++
*/
static void
init_by_array(unsigned long init_key[], int key_length)
{
  int i, j, k;

  init_genrand(19650218UL);
  i = 1;
  j = 0;
  for (k = (N > key_length ? N : key_length); k; --k) {
    /* non linear */
    x[i] = ((x[i] ^ ((x[i - 1] ^ (x[i - 1] >> 30)) * 1664525UL))
            + init_key[j] + j) & 0xffffffffUL;  /* for WORDSIZE > 32 machines */
    if (++i >= N) {
      x[0] = x[N - 1];
      i = 1;
    }
    if (++j >= key_length) {
      j = 0;
    }
  }
  for (k = N - 1; k; --k) {
    /* non linear */
    x[i] = ((x[i] ^ ((x[i - 1] ^ (x[i - 1] >> 30)) * 1566083941UL)) - i)
      & 0xffffffffUL;           /* for WORDSIZE > 32 machines */
    if (++i >= N) {
      x[0] = x[N - 1];
      i = 1;
    }
  }
  x[0] = 0x80000000UL;          /* MSB is 1; assuring non-zero initial array */
}

/* generates a random number on the interval [0,0xffffffff] */
unsigned long
genrand_int32(void)
{
  unsigned long y;

  if (!p0) {
    /* Default seed */
    init_genrand(5489UL);
  }
  /* Twisted feedback */
  y = *p0 = *pm++ ^ (((*p0 & UPPER_MASK) | (*p1 & LOWER_MASK)) >> 1)
    ^ (-(*p1 & 1) & MATRIX_A);
  p0 = p1++;
  if (pm == x + N) {
    pm = x;
  }
  if (p1 == x + N) {
    p1 = x;
  }
  /* Temper */
  y ^= y >> 11;
  y ^= y << 7 & 0x9d2c5680UL;
  y ^= y << 15 & 0xefc60000UL;
  y ^= y >> 18;
  return y;
}

/* generates a random number on the interval [0,0x7fffffff] */
long
genrand_int31(void)
{
  return (long) (genrand_int32() >> 1);
}

/* generates a random number on the real interval [0,1] */
double
genrand_real1(void)
{
  return genrand_int32() * (1.0 / 4294967295.0);
  /* divided by 2^32-1 */
}

/* generates a random number on the real interval [0,1) */
double
genrand_real2(void)
{
  return genrand_int32() * (1.0 / 4294967296.0);
  /* divided by 2^32 */
}

/* generates a random number on the real interval (0,1) */
double
genrand_real3(void)
{
  return (((double) genrand_int32()) + 0.5) * (1.0 / 4294967296.0);
  /* divided by 2^32 */
}

/* generates a 53-bit random number on the real interval [0,1) */
double
genrand_res53(void)
{
  unsigned long a = genrand_int32() >> 5, b = genrand_int32() >> 6;

  return (a * 67108864.0 + b) * (1.0 / 9007199254740992.0);
}

/* 2002-01-09 These real versions are due to Isaku Wada */

