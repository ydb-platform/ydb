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
#ifndef _KNNQUERY_H_
#define _KNNQUERY_H_

#include "object.h"
#include "query.h"

namespace similarity {

template <typename dist_t>
class KNNQueue;

template <typename dist_t>
class Index;

template <typename dist_t>
class Space;

template <typename dist_t>
class KNNQuery : public Query<dist_t> {
 public:
  ~KNNQuery();
  KNNQuery(const Space<dist_t>& space, const Object* query_object, const unsigned K, float eps = 0);

  const KNNQueue<dist_t>* Result() const;
  virtual dist_t Radius() const;
  unsigned ResultSize() const;
  unsigned GetK() const { return K_; }
  float GetEPS() const { return eps_; }

  void Reset();
  bool CheckAndAddToResult(const dist_t distance, const Object* object);
  bool CheckAndAddToResult(const Object* object);
  size_t CheckAndAddToResult(const ObjectVector& bucket);

  bool Equals(const KNNQuery<dist_t>* query) const;
  void Print() const;
  static std::string Type() { return "K-NN"; }

 protected:
  unsigned K_;
  float eps_;
  KNNQueue<dist_t>* result_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(KNNQuery);
};

}     // namespace similarity

#endif   // _QUERY_TYPES_H_
