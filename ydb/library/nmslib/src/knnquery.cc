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
#include <iostream>
#include <algorithm>
#include <memory>
#include <stdexcept>

#include "knnqueue.h"
#include "utils.h"
#include "space.h"
#include "index.h"
#include "rangequery.h"
#include "knnquery.h"

namespace similarity {

using std::unique_ptr;

template <typename dist_t>
KNNQuery<dist_t>::KNNQuery(const Space<dist_t>& space, const Object* query_object, const unsigned K, float eps)
    : Query<dist_t>(space, query_object),
      K_(K), eps_(eps),
      result_(new KNNQueue<dist_t>(K)) {
}

template <typename dist_t>
KNNQuery<dist_t>::~KNNQuery() {
  delete result_;
}

template <typename dist_t>
void KNNQuery<dist_t>::Reset() {
  this->ResetStats();
  result_->Reset();
}

template <typename dist_t>
const KNNQueue<dist_t>* KNNQuery<dist_t>::Result() const {
  return result_;
}

template <typename dist_t>
dist_t KNNQuery<dist_t>::Radius() const {
  return result_->Size() < static_cast<size_t>(K_)
      ? DistMax<dist_t>()
      : static_cast<dist_t>(result_->TopDistance() / (static_cast<dist_t>(1) + eps_));
}

template <typename dist_t>
unsigned KNNQuery<dist_t>::ResultSize() const {
  return result_->Size();
}

template <typename dist_t>
bool KNNQuery<dist_t>::CheckAndAddToResult(const dist_t distance,
                                           const Object* object) {
  if (result_->Size() < static_cast<size_t>(K_) ||
      distance < result_->TopDistance()) {
    result_->Push(distance, object);
    return true;
  }
  return false;
}

template <typename dist_t>
bool KNNQuery<dist_t>::CheckAndAddToResult(const Object* object) {
  return this->CheckAndAddToResult(this->DistanceObjLeft(object), object);
}

template <typename dist_t>
size_t KNNQuery<dist_t>::CheckAndAddToResult(const ObjectVector& bucket) {
  size_t res = 0;
  for (size_t i = 0; i < bucket.size(); ++i) {
    if (CheckAndAddToResult(bucket[i])) {
      ++res;
    }
  }
  return res;
}

template <typename dist_t>
bool KNNQuery<dist_t>::Equals(const KNNQuery<dist_t>* other) const {
  bool equal = true;
  unique_ptr<KNNQueue<dist_t>> first(result_->Clone());
  unique_ptr<KNNQueue<dist_t>> second(other->result_->Clone());
  while (equal && !first->Empty() && !second->Empty()) {
    equal = equal && ApproxEqual(first->TopDistance(), second->TopDistance());
    if (!equal) {
      std::cerr << "Equality check failed: "
                << first->TopDistance() <<  " != "
                << second->TopDistance() << std::endl;
    }
    first->Pop();
    second->Pop();
  }
  equal = equal && first->Empty() && second->Empty();   // both should be empty
  return equal;
}

template <typename dist_t>
void KNNQuery<dist_t>::Print() const {
  unique_ptr<KNNQueue<dist_t>> clone(Result()->Clone());
  std::cerr << "queryID = " << this->query_object_->id()
            << " size = " << ResultSize()
            << " (k=" << GetK()
            << " dc=" << this->DistanceComputations()
            << ") ";
  while (!clone->Empty()) {
    if (clone->TopObject() == NULL) {
      std::cerr << "null (" << clone->TopDistance() << ")";
    } else {
      const Object* object = reinterpret_cast<const Object*>(clone->TopObject());
      std::cerr << object->id() << "("
                << clone->TopDistance() << " "
                << this->space_.IndexTimeDistance(object, this->QueryObject()) << ") ";
    }
    clone->Pop();
  }
  std::cerr << std::endl;
}

template class KNNQuery<float>;
template class KNNQuery<int>;
template class KNNQuery<short int>;

}    // namespace similarity
