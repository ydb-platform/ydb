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
#ifndef _SEQSEARCH_H_
#define _SEQSEARCH_H_

#include <string>

#include "index.h"

#define METH_SEQ_SEARCH                 "brute_force"
#define METH_SEQ_SEARCH_SYN             "seq_search"

namespace similarity {

using std::string;
using std::vector;

// Sequential search

template <typename dist_t>
class SeqSearch : public Index<dist_t> {
 public:
  SeqSearch(Space<dist_t>& space, const ObjectVector& data);
  void CreateIndex(const AnyParams& ) override;
  virtual ~SeqSearch();

  const std::string StrDesc() const override { return "Sequential search"; }

  void Search(RangeQuery<dist_t>* query, IdType) const override;
  void Search(KNNQuery<dist_t>* query, IdType) const override;

  void SetQueryTimeParams(const AnyParams& params) override {}

  size_t GetSize() const override { return getData().size(); }
 private:
  Space<dist_t>&          space_;
  char*                   cacheOptimizedBucket_;

  ObjectVector*           pData_;
  bool                    multiThread_;
  IdTypeUnsign            threadQty_;
  vector<ObjectVector>    vvThreadData;

  const ObjectVector& getData() const { return pData_ != NULL ? *pData_ : this->data_; }
  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(SeqSearch);
};

}   // namespace similarity

#endif     // _SEQSEARCH_H_
