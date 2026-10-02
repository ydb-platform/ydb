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
#include <thread>

#include "space.h"
#include "rangequery.h"
#include "knnquery.h"
#include "knnqueue.h"
#include "method/seqsearch.h"

namespace similarity {

template <typename dist_t, typename QueryType>
struct SearchThreadParamSeqSearch {
  const Space<dist_t>&      space_;
  const ObjectVector&       data_;
  IdTypeUnsign              threadId_;
  QueryType&                query_;

  SearchThreadParamSeqSearch(
      const Space<dist_t>&             space,
      const ObjectVector&              data,
      IdTypeUnsign                     threadId,
      QueryType&                       query
  ) :
      space_(space),
      data_(data),
      threadId_(threadId),
      query_(query) {}
};

template <typename dist_t, typename QueryType>
struct SearchThreadSeqSearch {
  void operator()(SearchThreadParamSeqSearch<dist_t, QueryType> &prm) {
    for (const Object* o: prm.data_) {
      prm.query_.CheckAndAddToResult(o);
    }
  }
};


template <typename dist_t>
SeqSearch<dist_t>::SeqSearch(Space<dist_t>& space, const ObjectVector& origData) :
                      Index<dist_t>(origData), space_(space), cacheOptimizedBucket_(NULL), pData_(NULL) {
}

template <typename dist_t>
void SeqSearch<dist_t>::CreateIndex(const AnyParams& IndexParams) {
  AnyParamManager pmgr(IndexParams);

  bool bCopyMem;
  pmgr.GetParamOptional("copyMem", bCopyMem, false);
  pmgr.GetParamOptional("multiThread", multiThread_, false);
  pmgr.GetParamOptional("threadQty", threadQty_, thread::hardware_concurrency()/2);
  if (threadQty_ < 2) multiThread_ = false;
  pmgr.CheckUnused();

  LOG(LIB_INFO) << "copyMem       = " << bCopyMem;
  LOG(LIB_INFO) << "multiThread   = " << multiThread_;

  if (multiThread_) {
    CHECK(threadQty_ > 1);
    const ObjectVector& data = getData();
    vvThreadData.resize(threadQty_);
    for (size_t i = 0; i < threadQty_; ++i)
      vvThreadData.reserve((data.size() + threadQty_ - 1)/threadQty_);

    size_t D = (data.size() + threadQty_ - 1)/threadQty_;

    for (size_t i = 0; i < data.size(); ++i) {
      //vvThreadData[i % threadQty_].push_back(data[i]);
      vvThreadData[i / D].push_back(data[i]); // This version seems to be working much better
    }
    /*
    for (size_t i = 0; i < threadQty_; ++i) {
      cout << "#######@@@" << vvThreadData[i].size() << endl;
    }
     */
    LOG(LIB_INFO) << "threadQty     = " << threadQty_;
  }

  SetQueryTimeParams(getEmptyParams());

  if (bCopyMem) {
    CreateCacheOptimizedBucket(this->data_, cacheOptimizedBucket_, pData_);
  }
}

template <typename dist_t>
SeqSearch<dist_t>::~SeqSearch() {
  if (cacheOptimizedBucket_ != NULL) {
    ClearBucket(cacheOptimizedBucket_, pData_);
  }
}

template <typename dist_t>
void SeqSearch<dist_t>::Search(RangeQuery<dist_t>* query, IdType) const {
  const ObjectVector& data = getData();

  if (!multiThread_) {
    for (size_t i = 0; i < data.size(); ++i) {
      query->CheckAndAddToResult(data[i]);
    }
  } else {
    vector<unique_ptr<RangeQuery<dist_t>>>                    vQueries(threadQty_);
    vector<thread>                                            vThreads(threadQty_);
    vector<unique_ptr<SearchThreadParamSeqSearch<dist_t, RangeQuery<dist_t>>>>    vThreadParams(threadQty_);

    for (size_t i = 0; i < threadQty_; ++i) {
      vQueries[i].reset(new RangeQuery<dist_t>(space_, query->QueryObject(), query->Radius()));
      vThreadParams[i].reset(new SearchThreadParamSeqSearch<dist_t,RangeQuery<dist_t>>(space_, vvThreadData[i], i, *vQueries[i]));
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      vThreads[i] = thread(SearchThreadSeqSearch<dist_t,RangeQuery<dist_t>>(), ref(*vThreadParams[i]));
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      vThreads[i].join();
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      RangeQuery<dist_t>& threadQuery = vThreadParams[i]->query_;
      const ObjectVector& res          = *threadQuery.Result();
      const std::vector<dist_t>& dists = *threadQuery.ResultDists();
      query->AddDistanceComputations(threadQuery.DistanceComputations());
      for (size_t k = 0; k < res.size(); ++k) {
        query->CheckAndAddToResult(dists[k], res[k]);
      }
    }
  }
}

template <typename dist_t>
void SeqSearch<dist_t>::Search(KNNQuery<dist_t>* query, IdType) const {
  const ObjectVector& data = getData();

  if (!multiThread_) {
    for (size_t i = 0; i < data.size(); ++i) {
      query->CheckAndAddToResult(data[i]);
    }
  } else {
    vector<unique_ptr<KNNQuery<dist_t>>> vQueries(threadQty_);
    vector<thread>                       vThreads(threadQty_);
    vector<unique_ptr<SearchThreadParamSeqSearch<dist_t, KNNQuery<dist_t>>>>    vThreadParams(threadQty_);

    for (size_t i = 0; i < threadQty_; ++i) {
      vQueries[i].reset(new KNNQuery<dist_t>(space_, query->QueryObject(), query->GetK(), query->GetEPS()));
      vThreadParams[i].reset(new SearchThreadParamSeqSearch<dist_t,KNNQuery<dist_t>>(space_, vvThreadData[i], i, *vQueries[i]));
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      vThreads[i] = thread(SearchThreadSeqSearch<dist_t,KNNQuery<dist_t>>(), ref(*vThreadParams[i]));
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      vThreads[i].join();
    }
    for (size_t i = 0; i < threadQty_; ++i) {
      KNNQuery<dist_t>& threadQuery = vThreadParams[i]->query_;
      unique_ptr<KNNQueue<dist_t>> ResQ(threadQuery.Result()->Clone());
      query->AddDistanceComputations(threadQuery.DistanceComputations());
      while(!ResQ->Empty()) {
        const Object *pObj = reinterpret_cast<const Object *>(ResQ->TopObject());
        query->CheckAndAddToResult(ResQ->TopDistance(), pObj);
        ResQ->Pop();
      }
    }
  }
}

template class SeqSearch<float>;
template class SeqSearch<int>;

}
