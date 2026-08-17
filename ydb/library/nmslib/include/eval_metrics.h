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
#ifndef EVAL_METRICS_H
#define EVAL_METRICS_H

#include <vector>
#include <iomanip>
#include <algorithm>
#include <iostream>
#include <limits>
#include <unordered_set>

#include "object.h"
#include "gold_standard.h"

namespace similarity {

using std::vector;
using std::sort;
using std::ostream;
using std::unordered_set;

template <class dist_t>
bool ApproxEqualElem(const ResultEntry<dist_t>& elem1, const ResultEntry<dist_t>& elem2) {
  return elem1.mId == elem2.mId || ApproxEqual(elem1.mDist, elem2.mDist);
}

template <class dist_t>
struct EvalMetricsBase {
  virtual double operator()(double ExactResultSize,
                      const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
                      const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
                      ) const = 0;
  /* 
   * An auxilliar function that aligns exact and approximate answers.
   * It used to compute error approximation metrics.
   */
  template <class AccumObj>
  static void iterate(AccumObj& obj,
               const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
               const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
               ) {
      for (size_t k = 0, p = 0; k < ApproxEntries.size() && p < SortedAllEntries.size(); ++k) {
        const auto& elemApprox = ApproxEntries[k];
        const auto& elemExact  = SortedAllEntries[p];
        /*
         * There is no guarantee that the floating point arithmetic
         * produces consistent results. For instance, we can call the distance
         * function twice with the same object pointers, but get slightly
         * different results.
         */
        dist_t diffApproxExact = elemApprox.mDist -  elemExact.mDist;
        if (diffApproxExact < 0
           && !ApproxEqualElem(elemApprox, elemExact)
            ) {
          double mx = std::abs(std::max(ApproxEntries[k].mDist, SortedAllEntries[p].mDist));
          double mn = std::abs(std::min(ApproxEntries[k].mDist, SortedAllEntries[p].mDist));
  
          for (size_t i = 0; i < std::min(SortedAllEntries.size(), ApproxEntries.size()); ++i ) {
            LOG(LIB_INFO) << "Ex: " << SortedAllEntries[i].mDist << " id = " << SortedAllEntries[i].mId <<
                         " -> Apr: "<< ApproxEntries[i].mDist << " id = " << ApproxEntries[i].mId << 
                         " 1 - ratio: " << (1 - mn/mx) << " diff: " << (mx - mn);
          }
          LOG(LIB_FATAL) << "bug: the approximate query should not return objects "
                         << "that are closer to the query than object returned by "
                         << "(exact) sequential searching!"
                         << std::setprecision(std::numeric_limits<dist_t>::digits10)
                         << " Approx: " << elemApprox.mDist << " id = " << elemApprox.mId 
                         << " Exact: "  << elemExact.mDist  << " id = " << elemExact.mId
                         << " difference: " << diffApproxExact;
        }
        // At this point the distance to the true answer is either <= than the distance to the approximate answer,
        // or the distance to the true answer is slightly larger due to non-determinism of floating point arithmetic
        size_t LastEqualP = p;
        if (p < SortedAllEntries.size() && 
            ApproxEqualElem(elemApprox, elemExact)) {
          ++p;
        } else {
          while (p < SortedAllEntries.size() && 
                 SortedAllEntries[p].mDist < ApproxEntries[k].mDist &&
                 !ApproxEqualElem(SortedAllEntries[p], ApproxEntries[k]))
                 {
            ++p;
            ++LastEqualP;
          }
        }
        if (p < k) {
          for (size_t i = 0; i < std::min(SortedAllEntries.size(), ApproxEntries.size()); ++i ) {
            LOG(LIB_INFO) << "E: " << SortedAllEntries[i].mDist << " -> " << ApproxEntries[i].mDist;
          }
          LOG(LIB_FATAL) << "bug: p = " << p << " k = " << k;
        }
        CHECK(p >= k);
        obj(k, LastEqualP);
      }
    }
};


template <class dist_t>
struct EvalRecall : public EvalMetricsBase<dist_t> {
  /*
   * A classic recall measure
   */
  double operator()(double ExactResultSize,
                    const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
                    const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
                    ) const {
    if (ExactResultIds.empty()) return 1.0;
    double recall = 0.0;
    for (auto it = ApproxResultIds.begin(); it != ApproxResultIds.end(); ++it) {
      recall += ExactResultIds.count(*it);
    }
    return recall / ExactResultSize;
  }
};

template <class dist_t>
struct EvalNumberCloser : public EvalMetricsBase<dist_t> {
  /*
   * Number of the nearest neighbors or range search answers that are closer to the query
   * than the closest element returned by the search.
   */
  double operator()(double ExactResultSize,
                    const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
                    const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
                   ) const {
    if (ExactResultIds.empty()) return 0.0;
    if (ApproxEntries.empty()) return min(ExactResultSize, static_cast<double>(SortedAllEntries.size()));
    
    double NumberCloser = 0;

    // 2. Compute the number of points closer to the 1-NN then the first result.
    CHECK(!ApproxEntries.empty());
    for (size_t p = 0; p < SortedAllEntries.size(); ++p) {
      if (SortedAllEntries[p].mDist >= ApproxEntries[0].mDist || ApproxEqualElem(SortedAllEntries[p], ApproxEntries[0])) break;
      ++NumberCloser;
    }

    return NumberCloser;
  }
};

template <class dist_t>
struct EvalPrecisionOfApprox : public EvalMetricsBase<dist_t> {
   /*
    * Precision of approximation.
    *
    * Proposed in:
    * Zezula, P., Savino, P., Amato, G., Rabitti, F., 
    * Approximate similarity retrieval with m-trees. 
    * The VLDB Journal 7(4) (December 1998) 275-293
    *
    * Formally, the precision of approximation is equal to:
    * 1/K sum_{i=1}^K   i/pos(i)
    */
  struct AccumPrecisionOfApprox {
    double PrecisionOfApprox_ = 0;
    void operator()(size_t k, size_t LastEqualP) {
       PrecisionOfApprox_ += static_cast<double>(k + 1) / (LastEqualP + 1);
    }
  };

  double operator()(double ExactResultSize,
                    const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
                    const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
                    ) const {
    if (ExactResultIds.empty()) return 1.0;
    if (ApproxEntries.empty()) return 0.0;

    AccumPrecisionOfApprox res;

    EvalMetricsBase<dist_t>::iterate(res, SortedAllEntries, ExactResultIds, ApproxEntries, ApproxResultIds);
    
    return res.PrecisionOfApprox_ / ApproxEntries.size();
  }
};

template <class dist_t>
struct EvalLogRelPosError : public EvalMetricsBase<dist_t> {
  struct AccumLogRelPossError {
    double LogRelPosError_ = 0;
    void operator()(size_t k, size_t LastEqualP) {
       LogRelPosError_    += log(static_cast<double>(LastEqualP + 1) / (k + 1));
    }
  };

  double operator()(double ExactResultSize,
                    const vector<ResultEntry<dist_t>>& SortedAllEntries, const unordered_set<IdType>& ExactResultIds,
                    const vector<ResultEntry<dist_t>>& ApproxEntries, const unordered_set<IdType>& ApproxResultIds
                    ) const {
    if (ExactResultIds.empty()) return 0.0;
    if (ApproxEntries.empty()) return log(min(ExactResultSize, static_cast<double>(SortedAllEntries.size())));

    AccumLogRelPossError res;

    EvalMetricsBase<dist_t>::iterate(res, SortedAllEntries, ExactResultIds, ApproxEntries, ApproxResultIds);
    
    return res.LogRelPosError_ / ApproxEntries.size();
  }
};

}

#endif
