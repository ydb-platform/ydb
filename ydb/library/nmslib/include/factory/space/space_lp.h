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
#ifndef FACTORY_SPACE_LP_H
#define FACTORY_SPACE_LP_H

#include <space/space_lp.h>
#include <space/space_l2sqr_sift.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateLINF(const AnyParams& /* ignoring params */) {
  // Chebyshev Distance
  return new SpaceLp<dist_t>(-1);

}
template <typename dist_t>
Space<dist_t>* CreateL1(const AnyParams& /* ignoring params */) {
  return new SpaceLp<dist_t>(1);
}
template <typename dist_t>
Space<dist_t>* CreateL2(const AnyParams& /* ignoring params */) {
  return new SpaceLp<dist_t>(2);
}

template <typename dist_t>
Space<dist_t>* CreateL(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  dist_t p;

  pmgr.GetParamRequired("p",  p);

  return new SpaceLp<dist_t>(p);
}

Space<DistTypeSIFT>* CreateL2SqrSIFT(const AnyParams& ){
  return new SpaceL2SqrSift();
}

/*
 * End of creating functions.
 */

}

#endif
