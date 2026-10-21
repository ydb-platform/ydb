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
#ifndef FACTORY_SPACE_SCALAR_H
#define FACTORY_SPACE_SCALAR_H

#include <space/space_scalar.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateCosineSimilarity(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceCosineSimilarity<dist_t>();
}


template <typename dist_t>
Space<dist_t>* CreateAngularDistance(const AnyParams& /* ignoring params */) {
  // Angular distance
  return new SpaceAngularDistance<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateNegativeScalarProduct(const AnyParams& /* ignoring params */) {
  // Negative inner/scalar product
  return new SpaceNegativeScalarProduct<dist_t>();
}

/*
 * End of creating functions.
 */

}

#endif
