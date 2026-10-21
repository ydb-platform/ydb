/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/searchivarius/NonMetricSpaceLib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef FACTORY_SPACE_SPARSE_DENSE_FUSION_H
#define FACTORY_SPACE_SPARSE_DENSE_FUSION_H

#include <space/space_sparse_dense_fusion.h>

namespace similarity {

/*
 * Creating functions.
 */

Space<float>* createSparseDenseFusion(const AnyParams& AllParams) {
 AnyParamManager pmgr(AllParams);

  string weightFileName;

  pmgr.GetParamRequired("weightfilename",  weightFileName);
  pmgr.CheckUnused();

  return new SpaceSparseDenseFusion(weightFileName);
}

/*
 * End of creating functions.
 */

}

#endif
