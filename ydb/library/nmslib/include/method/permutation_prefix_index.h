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
#ifndef _PERMUTATION_PREFIX_INDEX_H_
#define _PERMUTATION_PREFIX_INDEX_H_

#include <string>
#include "index.h"
#include "permutation_utils.h"

#define METH_PERMUTATION_PREFIX_IND     "perm_prefix"
#define METH_PERMUTATION_PREFIX_IND_SYN "pp-index"

namespace similarity {

/*
 * Permutation Prefix Index (PP-index): 
 * Esuli, 2012.
 * Using Permutation Prefixes for Efficient and Scalable Approximate Similarity Search.
 */

template <typename dist_t>
class Space;

class PrefixTree;

template <typename dist_t>
class PermutationPrefixIndex : public Index<dist_t> {
 public:
  PermutationPrefixIndex(bool  PrintProgress,
                        const Space<dist_t>& space,
                        const ObjectVector& data);

  void CreateIndex(const AnyParams& IndexParams) override;
  ~PermutationPrefixIndex();

  const std::string StrDesc() const override;
  void Search(RangeQuery<dist_t>* query, IdType) const override;
  void Search(KNNQuery<dist_t>* query, IdType) const override;

  void SetQueryTimeParams(const AnyParams& QueryTimeParams) override;
  virtual bool DuplicateData() const override { return chunkBucket_; }
 private:

  size_t computeDbScan(size_t K) const {
    if (knn_amp_) { return min(K * knn_amp_, this->data_.size()); }
    return static_cast<size_t>(min(min_candidate_, this->data_.size()));
  }


  template <typename QueryType>
  void GenSearch(QueryType* query, size_t K) const;

  const Space<dist_t>&  space_;
  bool                  PrintProgress_;

  // permutation prefix length (l in the original paper) in (0, num_pivot]
  size_t num_pivot_;
  size_t prefix_length_;
  size_t min_candidate_;
  size_t knn_amp_;
  // min # of candidates to be selected (z in the original paper)
  ObjectVector pivot_;
  unique_ptr<PrefixTree> prefixtree_;
  bool                   chunkBucket_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(PermutationPrefixIndex);
};

}  // namespace similarity

#endif     // _PERMUTATION_PREFIX_INDEX_H_

