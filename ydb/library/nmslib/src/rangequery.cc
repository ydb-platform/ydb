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
#include <set>
#include <iterator>

#include "space.h"
#include "object.h"
#include "utils.h"
#include "rangequery.h"

namespace similarity {

using namespace std;

template <typename dist_t>
RangeQuery<dist_t>::RangeQuery(const Space<dist_t>& space,
                               const Object* query_object,
                               const dist_t radius)
    : Query<dist_t>(space, query_object),
      radius_(radius) {
}

template <typename dist_t>
const ObjectVector* RangeQuery<dist_t>::Result() const {
  return &result_;
}

template <typename dist_t>
std::set<const Object*> RangeQuery<dist_t>::ResultSet() const {
  std::set<const Object*> res;
  for (auto it = result_.begin(); it != result_.end(); ++it) {
    res.insert(*it);
  }
  return res;
}

template <typename dist_t>
dist_t RangeQuery<dist_t>::Radius() const {
  return radius_;
}

template <typename dist_t>
unsigned RangeQuery<dist_t>::ResultSize() const {
  return static_cast<unsigned>(result_.size());
}

template <typename dist_t>
void RangeQuery<dist_t>::Reset() {
  this->ResetStats();
  result_.clear();
}

template <typename dist_t>
bool RangeQuery<dist_t>::CheckAndAddToResult(const dist_t distance,
                                             const Object* object) {
  if (distance <= radius_) {
    result_.push_back(object);
    resultDists_.push_back(distance);
    return true;
  }
  return false;
}

template <typename dist_t>
bool RangeQuery<dist_t>::CheckAndAddToResult(const Object* object) {
  // Distance can be asymmetric, but query is on the left side here
  return CheckAndAddToResult(this->DistanceObjLeft(object), object);
}

template <typename dist_t>
size_t RangeQuery<dist_t>::CheckAndAddToResult(const ObjectVector& bucket) {
  size_t res = 0;
  for (size_t i = 0; i < bucket.size(); ++i) {
    if (CheckAndAddToResult(bucket[i])) {
      ++res;
    }
  }
  return res;
}

template <typename dist_t>
bool RangeQuery<dist_t>::Equals(const RangeQuery<dist_t>* query) const {
  std::set<const Object*> res1, res2;
  copy(result_.begin(), result_.end(), inserter(res1, res1.end()));
  copy(query->result_.begin(), query->result_.end(), inserter(res2, res2.end()));
  return res1 == res2;
}

template <typename dist_t>
void RangeQuery<dist_t>::Print() const {
  std::cerr << "queryID = " << this->QueryObject()->id()
         << "size = " << ResultSize() << std::endl;
  for (auto iter = result_.begin(); iter != result_.end(); ++iter) {
    const Object* object = *iter;
    std::cerr << object->id() << "("
        << this->space_.HiddenDistance(this->QueryObject(), object) << ") ";
  }
  std::cerr << std::endl;
}

template class RangeQuery<float>;
template class RangeQuery<int>;
template class RangeQuery<short int>;

}    // namespace similarity
