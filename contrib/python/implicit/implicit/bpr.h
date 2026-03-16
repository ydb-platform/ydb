// Copyright 2018 Ben Frederickson

#ifndef IMPLICIT_BPR_H_
#define IMPLICIT_BPR_H_

// We need to get the thread number to figure out which RNG to use,
// but this will fail on OSX etc if we have no openmp enabled compiler.
// Cython won't let me #ifdef this, so I'm doing it here

#ifdef _OPENMP
#include <omp.h>
#endif

namespace implicit {
#ifdef _OPENMP
inline int get_thread_num() { return omp_get_thread_num(); }
#else
inline int get_thread_num() { return 0; }
#endif
}  // namespace implicit
#endif  // IMPLICIT_BPR_H_
