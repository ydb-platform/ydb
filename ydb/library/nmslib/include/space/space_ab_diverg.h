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
#ifndef _SPACE_AB_DIVERG_H_
#define _SPACE_AB_DIVERG_H_

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

#define SPACE_AB_DIVERG_SLOW     "abdiv_slow"
#define SPACE_AB_DIVERG_FAST     "abdiv_fast"

namespace similarity {



/*
 * Alpha-beta divergence.
 * 
 * Póczos, Barnabás, Liang Xiong, Dougal J. Sutherland, and Jeff Schneider (2012). “Nonparametric kernel
 * estimators for image classification”. In: Computer Vision and Pattern Recognition (CVPR), 2012 IEEE
 * Conference on, pages 2989–2996
 */

template <typename dist_t>
class SpaceAlphaBetaDivergSlow : public VectorSpaceSimpleStorage<dist_t> {
 public:
  explicit SpaceAlphaBetaDivergSlow(float alpha, float beta) : alpha_(alpha), beta_(beta) {}
  virtual ~SpaceAlphaBetaDivergSlow() {}

  virtual std::string StrDesc() const override;
 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const override;
  virtual dist_t ProxyDistance(const Object* obj1, const Object* obj2) const override;
 private:
  DISABLE_COPY_AND_ASSIGN(SpaceAlphaBetaDivergSlow);
  float alpha_, beta_;
};

template <typename dist_t>
class SpaceAlphaBetaDivergFast : public VectorSpaceSimpleStorage<dist_t> {
 public:
  explicit SpaceAlphaBetaDivergFast(float alpha, float beta) : alpha_(alpha), beta_(beta) {}
  virtual ~SpaceAlphaBetaDivergFast() {}

  virtual std::string StrDesc() const override;
 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const override;
  virtual dist_t ProxyDistance(const Object* obj1, const Object* obj2) const override;
 private:
  DISABLE_COPY_AND_ASSIGN(SpaceAlphaBetaDivergFast);
  float alpha_, beta_;
};


}  // namespace similarity

#endif 
