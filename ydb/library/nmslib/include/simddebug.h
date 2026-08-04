/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef _SIMD_DEBUG_H_
#define _SIMD_DEBUG_H_

#include "portable_intrinsics.h"


#ifdef PORTABLE_SSE4

#include <iostream>
#include <string>

using std::cout;
using std::endl;
using std::string;

namespace similarity {

// Some SIMD debug printing
inline void print4_ps(__m128 val, const string& desc="") {
  float tmp[4];
  _mm_storeu_ps(tmp, val);
  cout << "SIMD debug 4 floats: " << "(" << desc <<") ";
  for (int i = 0; i < 4; ++i) {
    cout << " " << tmp[i] << " ";
  }
  cout << endl;
}

inline void print4_pd(__m128d val, const string& desc="") {
  double tmp[2];
  _mm_storeu_pd(tmp, val);
  cout << "SIMD debug 2 doubles: " << "(" << desc <<") ";
  for (int i = 0; i < 2; ++i) {
    cout << " " << tmp[i] << " ";
  }
  cout << endl;
}

inline void print4_si(__m128i val, const string& desc="") {
  int tmp[4];

  _mm_storeu_si128(reinterpret_cast<__m128i*>(tmp), val);
  cout << "SIMD debug 4 ints: " << "(" << desc <<") ";
  for (int i = 0; i < 4; ++i) {
    cout << " " << tmp[i] << " ";
  }
  cout << endl;
}

#endif

}  // namespace similarity

#endif
