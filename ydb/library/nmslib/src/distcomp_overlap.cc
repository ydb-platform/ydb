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
#include <algorithm>

#include "distcomp.h"

using namespace std;

namespace similarity {

// This 3-way intersection counter isn't super fast
unsigned IntersectSizeScalar3way(const IdType *A, const size_t lenA, 
                                 const IdType *B, const size_t lenB,
                                 const IdType *C, const size_t lenC) {

  if (lenA == 0 || lenB == 0 || lenC == 0) return 0;
  unsigned res = 0;

  const IdType *endA = A + lenA;
  const IdType *endB = B + lenB;
  const IdType *endC = C + lenC;


  while (A < endA && B < endB && C < endC) {
    IdType minVal = min(*A, min(*B, *C));
    size_t qty = 0;
    while (*A < minVal) {
      if (++A == endA) return res; // reached the end before find value >= minVal
    }
    if (*A == minVal) {
      ++qty; ++A;
    }
    while (*B < minVal) {
      if (++B == endB) return res; // reached the end before find value >= minVal
    }
    if (*B == minVal) {
      ++qty; ++B;
    }
    while (*C < minVal) {
      if (++C == endC) return res; // reached the end before find value >= minVal
    }
    if (*C == minVal) {
      ++qty; ++C;
    }
    if (qty ==3) ++res;
  } 
  return res;
}

/*
 * Finds the size of the intersection:
 * A fast scalar scheme designed by N. Kurz.
 * Adopted from https://github.com/lemire/SIMDCompressionAndIntersection (which is also Apache 2)
 */
unsigned IntersectSizeScalarFast(const IdType *A, const size_t lenA, 
                       const IdType *B, const size_t lenB) {
  if (lenA == 0 || lenB == 0)
    return 0;
  unsigned res = 0;

  const IdType *endA = A + lenA;
  const IdType *endB = B + lenB;

  while (true) {
    while (*A < *B) {
    SKIP_FIRST_COMPARE:
      if (++A == endA)
        return res;
    }
    while (*A > *B) {
      if (++B == endB)
        return res;
    }
    if (*A == *B) {
      ++res;
      if (++A == endA || ++B == endB)
        return res;
    } else {
      goto SKIP_FIRST_COMPARE;
    }
  }

  return res; // NOTREACHED
};

// Text-book algo, but it is less efficient
unsigned IntersectSizeScalarStand(const IdType *A, const size_t lenA, 
                                  const IdType *B, const size_t lenB) {
  if (lenA == 0 || lenB == 0)
    return 0;
  unsigned res = 0;

  const IdType *endA = A + lenA;
  const IdType *endB = B + lenB;

  while (A < endA && B < endB) {
    if (*A < *B) {
      A++;
    } else if (*A > *B) {
      B++;
    } else {
    // *A == *B
      ++res;
      A++; B++;
    }
  }

  return res; 
};

}

