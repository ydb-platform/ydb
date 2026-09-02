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
#include <stdio.h>
#include <algorithm>

#include "space.h"
#include "object.h"
#include "utils.h"
#include "query.h"

namespace similarity {

template <typename dist_t>
Query<dist_t>::Query(const Space<dist_t>& space, const Object* query_object)
    : space_(space),
      query_object_(query_object),
      distance_computations_(0) {
}

template <typename dist_t>
Query<dist_t>::~Query() {
}

template <typename dist_t>
const Object* Query<dist_t>::QueryObject() const {
  return query_object_;
}

template <typename dist_t>
uint64_t Query<dist_t>::DistanceComputations() const {
  return distance_computations_;
}

template <typename dist_t>
void Query<dist_t>::ResetStats() {
  distance_computations_ = 0;
}

template <typename dist_t>
dist_t Query<dist_t>::Distance(
    const Object* object1,
    const Object* object2) const {
  ++distance_computations_;
  return space_.HiddenDistance(object1, object2);
}

template <typename dist_t>
dist_t Query<dist_t>::DistanceObjLeft(const Object* object) const {
  return Distance(object, query_object_);
}

template <typename dist_t>
dist_t Query<dist_t>::DistanceObjRight(const Object* object) const {
  return Distance(query_object_, object);
}

template class Query<float>;
template class Query<int>;
template class Query<short int>;

}    // namespace similarity
