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

#include "space/space_sparse_lp.h"
#include "space/space_sparse_vector.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
dist_t SpaceSparseLp<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  return SpaceSparseVectorSimpleStorage<dist_t>::ComputeDistanceHelper(obj1, obj2, distObj_);
}

template <typename dist_t>
std::string SpaceSparseLp<dist_t>::StrDesc() const {
  std::stringstream stream;
  stream << "SpaceSparseLp: p = " << distObj_.getP() << " (custom implement.) = " << distObj_.getCustom();
  return stream.str();
}

template class SpaceSparseLp<float>;

}  // namespace similarity
