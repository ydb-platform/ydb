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
#ifndef _PERM_TYPE_H
#define _PERM_TYPE_H

#include <vector>
#include <cstdint>

namespace similarity {


/* 
 * Should be a (signed) int32_t type!!!
 * We don't use integer of a smaller range, due to possible overflow.
 *
 * Don't change it, or the code will be broken.
 * In particular, SIMD-based correlation functions won't work
 */
typedef int32_t PivotIdType;

typedef std::vector<PivotIdType> Permutation;


}
#endif
