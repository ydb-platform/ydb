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
#ifndef _RANGEQUERY_H_
#define _RANGEQUERY_H_

#include <set>
#include "object.h"
#include "query.h"

namespace similarity {

template <typename dist_t>
class RangeQuery : public Query<dist_t> {
 public:
  RangeQuery(const Space<dist_t>& space, const Object* query_object, const dist_t radius);

  const ObjectVector* Result() const;
  const std::vector<dist_t>* ResultDists() const { return &resultDists_; }
  std::set<const Object*> ResultSet() const ;
  dist_t Radius() const;
  unsigned ResultSize() const;

  void Reset();
  bool CheckAndAddToResult(const dist_t distance, const Object* object);
  bool CheckAndAddToResult(const Object* object);
  size_t CheckAndAddToResult(const ObjectVector& bucket);
  bool Equals(const RangeQuery<dist_t>* query) const;
  void Print() const;
  static std::string Type() { return "RANGE"; }

 private:
  dist_t               radius_;
  ObjectVector         result_;
  std::vector<dist_t>  resultDists_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(RangeQuery);
};

}     // namespace similarity

#endif   // _RANGEQUERY_H_
