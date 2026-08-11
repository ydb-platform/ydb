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
#ifndef FACTORY_SPACE_SQFD_H
#define FACTORY_SPACE_SQFD_H

#if !defined(_MSC_VER)

#include "space/space_sqfd.h"

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateSqfdHeuristicFunc(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  dist_t alpha;
  pmgr.GetParamRequired("alpha", alpha);

  return new SpaceSqfd<dist_t>(new SqfdHeuristicFunction<dist_t>(alpha));
}

template <typename dist_t>
Space<dist_t>* CreateSqfdMinusFunc(const AnyParams& ignore) {
  return new SpaceSqfd<dist_t>(new SqfdMinusFunction<dist_t>);
}

template <typename dist_t>
Space<dist_t>* CreateSqfdGaussianFunc(const AnyParams& AllParams) {
  AnyParamManager pmgr(AllParams);

  dist_t alpha;
  pmgr.GetParamRequired("alpha", alpha);

  return new SpaceSqfd<dist_t>(new SqfdGaussianFunction<dist_t>(alpha));
}

/*
 * End of creating functions.
 */

}

#endif

#endif
