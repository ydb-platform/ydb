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
#ifndef _QUERY_CREATOR_H_
#define _QUERY_CREATOR_H_

#include "rangequery.h"
#include "knnquery.h"


namespace similarity {

template <typename dist_t>
class RangeCreator {
public:
  RangeCreator(dist_t radius) : radius_(radius){}
  RangeQuery<dist_t>* operator()(const Space<dist_t>& space,
                                 const Object* query_object) const {
    return new RangeQuery<dist_t>(space, query_object, radius_);
  }
  std::string ParamsForPrint() const {
    std::stringstream str;
    str << "Radius = " << radius_;
    return str.str();
  }
  dist_t radius_;
};

template <typename dist_t>
class KNNCreator {
public:
  KNNCreator(size_t K, float eps) : K_(K), eps_(eps) {}
  KNNQuery<dist_t>* operator()(const Space<dist_t>& space,
                               const Object* query_object) const {
    return new KNNQuery<dist_t>(space, query_object, K_, eps_);
  }

  std::string ParamsForPrint() const {
    std::stringstream str;
    str << "K = " << K_ << " Epsilon = " << eps_;
    return str.str();
  }
  dist_t K_;
  float  eps_;
};

}     // namespace similarity

#endif   // _QUERY_H_
