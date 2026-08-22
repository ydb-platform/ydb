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
#ifndef GENRAND_VECT_HPP
#define GENRAND_VECT_HPP

#include "space/space_sparse_vector.h"

namespace similarity {

template <class T> 
inline void Normalize(T* pVect, size_t qty) {
  T sum = 0;
  for (size_t i = 0; i < qty; ++i) {
    sum += pVect[i];
  }
  if (sum != 0) {
    for (size_t i = 0; i < qty; ++i) {
      pVect[i] /= sum;
    }
  }
}


template <class T> 
inline void GenRandVect(T* pVect, size_t qty, T MinElem = T(0), T MaxElem = T(1), bool DoNormalize = false) {
  T sum = 0;
  for (size_t i = 0; i < qty; ++i) {
    pVect[i] = MinElem + (MaxElem - MinElem) * RandomReal<T>();
    sum += fabs(pVect[i]);
  }
  if (DoNormalize && sum != 0) {
    for (size_t i = 0; i < qty; ++i) {
      pVect[i] /= sum;
    }
  }
}

inline void GenRandIntVect(int* pVect, size_t qty) {
  for (size_t i = 0; i < qty; ++i) {
    pVect[i] = RandomInt();
  }
}

template <class T> 
inline void SetRandZeros(T* pVect, size_t qty, double pZero) {
    for (size_t j = 0; j < qty; ++j) if (RandomReal<T>() < pZero) pVect[j] = T(0);
}

template <typename dist_t>
void GenSparseVectZipf(size_t maxSize, vector<SparseVectElem<dist_t>>& res) {
  maxSize = max(maxSize, (size_t)1);

  for (size_t i = 1; i < maxSize; ++i) {
    float f = RandomReal<float>();
    if (f <= sqrt((float)i)/i) { // This is a bit ad hoc, but is ok for testing purposes
      res.push_back(SparseVectElem<dist_t>(i, RandomReal<float>()));
    }
  }
};

}

#endif

