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
#ifndef _RANK_CORREL_SPACE_H_
#define _RANK_CORREL_SPACE_H_

#include <string>
#include <map>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_vector.h"
#include "permutation_type.h"

namespace similarity {

template <PivotIdType (*RankCorrelDistFunc)(const PivotIdType*, const PivotIdType*, size_t)>
class RankCorrelVectorSpace : public VectorSpace<PivotIdType> {
 public:
  explicit RankCorrelVectorSpace() {}

  virtual std::string StrDesc() const { return "rank correlation vector space"; }
 protected:
  // Should not be directly accessible
  virtual PivotIdType HiddenDistance(const Object* obj1, const Object* obj2) const {
    CHECK(obj1->datalength() > 0);
    CHECK(obj1->datalength() == obj2->datalength());
    const PivotIdType* x = reinterpret_cast<const PivotIdType*>(obj1->data());
    const PivotIdType* y = reinterpret_cast<const PivotIdType*>(obj2->data());
    const size_t length = obj1->datalength() / sizeof(size_t);

    return RankCorrelDistFunc(x, y, length);
  }
  DISABLE_COPY_AND_ASSIGN(RankCorrelVectorSpace);
};

}  // namespace similarity

#endif
