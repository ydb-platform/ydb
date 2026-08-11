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
#ifndef _SPACE_RENYI_DIVERG_H_
#define _SPACE_RENYI_DIVERG_H_

#include <string>
#include <limits>
#include <map>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_vector.h"
#include "distcomp.h"

#define SPACE_RENYI_DIVERG_SLOW "renyidiv_slow"
#define SPACE_RENYI_DIVERG_FAST "renyidiv_fast"

namespace similarity {



/*
 * Renyi divergences.
 * 
 */

template <typename dist_t>
class SpaceRenyiDivergSlow : public VectorSpaceSimpleStorage<dist_t> {
 public:
  explicit SpaceRenyiDivergSlow(float alpha) : alpha_(alpha) {}
  virtual ~SpaceRenyiDivergSlow() {}

  virtual std::string StrDesc() const override;
 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const override;
 private:
  DISABLE_COPY_AND_ASSIGN(SpaceRenyiDivergSlow);
  float alpha_;
};

template <typename dist_t>
class SpaceRenyiDivergFast : public VectorSpaceSimpleStorage<dist_t> {
 public:
  explicit SpaceRenyiDivergFast(float alpha) : alpha_(alpha) {}
  virtual ~SpaceRenyiDivergFast() {}

  virtual std::string StrDesc() const override;
 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const override;
 private:
  DISABLE_COPY_AND_ASSIGN(SpaceRenyiDivergFast);
  float alpha_;
};


}  // namespace similarity

#endif 
