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
#include <cmath>
#include <fstream>
#include <string>
#include <sstream>

#include "space/space_js.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
Object* SpaceJSBase<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  if (type_ == kJSSlow) {
    return new Object(id, label, InpVect.size() * sizeof(dist_t), &InpVect[0]);
  }
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}


template <typename dist_t>
dist_t SpaceJSBase<dist_t>::JensenShannonFunc(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());

  size_t length = obj1->datalength() / sizeof(dist_t);

  if (type_ != kJSSlow) length /= 2;

  dist_t val = 0;

  switch (type_) {
    case kJSSlow:               val = JSStandard(x, y, length); break;
    case kJSFastPrecomp:        val = JSPrecomp(x, y, length); break;
    case kJSFastPrecompApprox:  val = JSPrecompSIMDApproxLog(x, y, length); break;
    default: {
               PREPARE_RUNTIME_ERR(err) << "Unknown JS function type code: " << type_;
               THROW_RUNTIME_ERR(err);
             }
  }

  return val;
}

template class SpaceJSBase<float>;

template class SpaceJSDiv<float>;

template class SpaceJSMetric<float>;

}  // namespace similarity
