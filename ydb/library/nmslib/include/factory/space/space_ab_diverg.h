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
#ifndef FACTORY_SPACE_AB_DIVERG_H
#define FACTORY_SPACE_AB_DIVERG_H

#include <space/space_ab_diverg.h>

namespace similarity {

template <typename dist_t>
Space<dist_t>* CreateAlphaBetaDivergSlow(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  float alpha = 1.0, beta = 1.0;

  pmgr.GetParamOptional("alpha",  alpha, alpha);
  pmgr.GetParamOptional("beta",   beta,  beta);

  return new SpaceAlphaBetaDivergSlow<dist_t>(alpha, beta);
}

template <typename dist_t>
Space<dist_t>* CreateAlphaBetaDivergFast(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  float alpha = 1.0, beta = 1.0;

  pmgr.GetParamOptional("alpha",  alpha, alpha);
  pmgr.GetParamOptional("beta",   beta,  beta);

  return new SpaceAlphaBetaDivergFast<dist_t>(alpha, beta);
}

/*
 * End of creating functions.
 */

}

#endif
