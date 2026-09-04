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
#ifndef _PERMUTATION_UTILS_H_
#define _PERMUTATION_UTILS_H_

#include <algorithm>
#include <iostream>
#include <unordered_set>

#include "space.h"
#include "rangequery.h"
#include "knnquery.h"
#include "permutation_type.h"
#include "utils.h"

namespace similarity {

using std::max;

template <typename dist_t>
using DistInt = std::pair<dist_t, PivotIdType>;     // <dist,      pivot_id>

typedef std::pair<PivotIdType, size_t> IntInt;      // <perm-dist, object_id>

template <typename dist_t>
void GetPermutationPivot(const ObjectVector& data,
                         const Space<dist_t>& space,
                         const size_t num_pivot,
                         ObjectVector* pivot,
                         std::vector<IdType>* pivot_pos = NULL) {
  if (num_pivot >= data.size()) {
    throw runtime_error("The data set in the space " + space.StrDesc() +
                        "is to small to select enough pivots");
  }
  std::unordered_set<int> pivot_idx;
  for (size_t i = 0; i < num_pivot; ++i) {
    int p = RandomInt() % data.size();
    for (size_t rep = 0; pivot_idx.count(p) != 0; ++rep) {
      if (rep > MAX_RAND_ITER_BEFORE_GIVE_UP) {
        throw runtime_error("Cannot find a unique pivot, perhaps, the data set is too small.");
      }
      p = RandomInt() % data.size();
    }
    pivot_idx.insert(p);
    if (pivot_pos != NULL) pivot_pos->push_back(p);
    pivot->push_back(data[p]);
  }
}

template <typename dist_t>
void GetPermutation(const ObjectVector& pivot, const Space<dist_t>& space,
                    const Object* object, Permutation* p) {
  // get pivot id
  std::vector<DistInt<dist_t>> dists;
  for (size_t i = 0; i < pivot.size(); ++i) {
    dists.push_back(std::make_pair(space.IndexTimeDistance(pivot[i], object), static_cast<PivotIdType>(i)));
  }
  std::sort(dists.begin(), dists.end());
  // dists.second = pivot id    i.e.  \Pi_o(i)

  // get pivot id's pos         i.e. position in \Pi_o(i) = \Pi^{-1}(i)
  std::vector<IntInt> pivot_idx;
  for (size_t i = 0; i < pivot.size(); ++i) {
    pivot_idx.push_back(std::make_pair(dists[i].second, static_cast<PivotIdType>(i)));
  }
  std::sort(pivot_idx.begin(), pivot_idx.end());
  // pivot_idx.second = pos of pivot  (which is needed for computing the Rho func)
  for (size_t i = 0; i < pivot.size(); ++i) {
    p->push_back(static_cast<PivotIdType>(pivot_idx[i].second));
  }
}

template <typename dist_t>
void GetPermutation(const ObjectVector& pivot,
                    const Query<dist_t>* query,
                    Permutation* p) {
  std::vector<DistInt<dist_t>> dists;
  for (size_t i = 0; i < pivot.size(); ++i) {
    /* Distance can be asymmetric, pivot should be always on the left side */
    dists.push_back(std::make_pair(query->DistanceObjLeft(pivot[i]),
                                   static_cast<PivotIdType>(i)));
  }
  std::sort(dists.begin(), dists.end());
  std::vector<IntInt> pivot_idx;
  for (size_t i = 0; i < pivot.size(); ++i) {
    pivot_idx.push_back(std::make_pair(dists[i].second,
                                       static_cast<PivotIdType>(i)));
  }
  std::sort(pivot_idx.begin(), pivot_idx.end());
  for (size_t i = 0; i < pivot.size(); ++i) {
	  p->push_back(static_cast<PivotIdType>(pivot_idx[i].second));
  }
}




// Permutation Prefix Index

template <typename dist_t>
void GetPermutationPPIndex(const ObjectVector& pivot,
                           const Space<dist_t>& space,
                           const Object* object,
                           Permutation* p) {
  // get pivot id
  std::vector<DistInt<dist_t>> dists;
  for (size_t i = 0; i < pivot.size(); ++i) {
    dists.push_back(std::make_pair(space.IndexTimeDistance(pivot[i], object),
                                   static_cast<PivotIdType>(i)));
  }
  std::sort(dists.begin(), dists.end());
  // dists.second = pivot id    i.e.  \Pi_o(i)

  for (size_t i = 0; i < pivot.size(); ++i) {
    p->push_back(dists[i].second);
  }
}

template <template<typename> class QueryType, typename dist_t>
void GetPermutationPPIndex(const ObjectVector& pivot,
                           QueryType<dist_t>* query,
                           Permutation* p) {
  std::vector<DistInt<dist_t>> dists;
  for (size_t i = 0; i < pivot.size(); ++i) {
    /* Distance can be asymmetric, pivot should be always on the left side */
    dists.push_back(std::make_pair(query->DistanceObjLeft(pivot[i]),
                                   static_cast<PivotIdType>(i)));
  }
  std::sort(dists.begin(), dists.end());
  for (size_t i = 0; i < pivot.size(); ++i) {
    p->push_back(dists[i].second);
  }
}

/*
 * Create a binary version of the permutation.
 */
inline void Binarize(const vector<PivotIdType> &perm, const PivotIdType thresh, vector<uint32_t>&bin_perm) {
  size_t bin_perm_word_qty = (perm.size() + 31)/32;

  bin_perm.resize(bin_perm_word_qty);
  fill(bin_perm.begin(), bin_perm.end(), 0);

  for (size_t i = 0; i < perm.size(); ++i) {
    bool b =perm[i] >= thresh;

    if (b) {
      bin_perm[i/32] |= (1<<(i%32)) ;
    }
  }
}

inline void Binarize(const vector<PivotIdType> &perm, const PivotIdType thresh, vector<uint64_t>&bin_perm) {
  size_t bin_perm_word_qty = (perm.size() + 63)/64;

  bin_perm.resize(bin_perm_word_qty);
  fill(bin_perm.begin(), bin_perm.end(), 0);

  for (size_t i = 0; i < perm.size(); ++i) {
    bool b =perm[i] >= thresh;

    if (b) {
      bin_perm[i/64] |= (1<<(i%64)) ;
    }
  }
}

}  // namespace similarity

#endif     // _PERMUTATION_UTILS_H_

