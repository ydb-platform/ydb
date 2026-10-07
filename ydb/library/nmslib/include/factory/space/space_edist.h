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
#ifndef FACTORY_SPACE_EDIST_H
#define FACTORY_SPACE_EDIST_H

#include <space/space_leven.h>

namespace similarity {

/*
 * Creating functions.
 */

Space<int>* CreateLevenshtein(const AnyParams& AllParams) {
  return new SpaceLevenshtein();
}

Space<float>* CreateLevenshteinNormalized(const AnyParams& AllParams) {
  return new SpaceLevenshteinNormalized();
}

/*
 * End of creating functions.
 */

}

#endif
