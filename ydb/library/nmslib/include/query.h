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
#ifndef _QUERY_H_
#define _QUERY_H_

#include "object.h"

namespace similarity {

template <typename dist_t>
class Space;

template <typename dist_t>
class Query {
 public:
  Query(const Space<dist_t>& space, const Object* query_object);
  virtual ~Query();

  const Object* QueryObject() const;
  uint64_t DistanceComputations() const;
  void AddDistanceComputations(uint64_t DistComp) { distance_computations_ += DistComp; }

  void ResetStats();
  virtual dist_t Distance(const Object* object1, const Object* object2) const;
  // Distance can be asymmetric!
  virtual dist_t DistanceObjLeft(const Object* object) const;
  virtual dist_t DistanceObjRight(const Object* object) const;

  virtual void Reset() = 0;
  virtual dist_t Radius() const = 0;
  virtual unsigned ResultSize() const = 0;
  virtual bool CheckAndAddToResult(const dist_t distance, const Object* object) = 0;
  virtual void Print() const = 0;

 protected:
  const Space<dist_t>& space_;
  const Object* query_object_;
  mutable uint64_t distance_computations_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(Query);
};

}     // namespace similarity

#endif   // _QUERY_H_
