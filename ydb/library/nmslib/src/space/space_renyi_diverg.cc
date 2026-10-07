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

#include "space/space_renyi_diverg.h"
#include "logging.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
dist_t SpaceRenyiDivergSlow<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
  const size_t length = obj1->datalength() / sizeof(dist_t);

  return renyiDivergenceSlow(x, y, length, alpha_);
}

template <typename dist_t>
std::string SpaceRenyiDivergSlow<dist_t>::StrDesc() const {
  std::stringstream stream;
  stream << SPACE_RENYI_DIVERG_SLOW << ":alpha=" << alpha_;
  return stream.str();
}

template class SpaceRenyiDivergSlow<float>;

template <typename dist_t>
dist_t SpaceRenyiDivergFast<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
  const size_t length = obj1->datalength() / sizeof(dist_t);

  return renyiDivergenceFast(x, y, length, alpha_);
}

template <typename dist_t>
std::string SpaceRenyiDivergFast<dist_t>::StrDesc() const {
  std::stringstream stream;
  stream << SPACE_RENYI_DIVERG_FAST << ":alpha=" << alpha_;
  return stream.str();
}

template class SpaceRenyiDivergFast<float>;

}  // namespace similarity
