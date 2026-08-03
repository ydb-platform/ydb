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
#ifndef FACTORY_SPACE_BREGMAN_H
#define FACTORY_SPACE_BREGMAN_H

#include <space/space_bregman.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateKLDivFast(const AnyParams& /* ignoring params */) {
  return new KLDivFast<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateKLDivFastRightQuery(const AnyParams& /* ignoring params */) {
  return new KLDivFastRightQuery<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateKLDivGenFast(const AnyParams& /* ignoring params */) {
  return new KLDivGenFast<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateKLDivGenFastRightQuery(const AnyParams& /* ignoring params */) {
  return new KLDivGenFastRightQuery<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateKLDivGenSlow(const AnyParams& /* ignoring params */) {
  return new KLDivGenSlow<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateItakuraSaitoFast(const AnyParams& /* ignoring params */) {
  return new ItakuraSaitoFast<dist_t>();
}


/*
 * End of creating functions.
 */


}

#endif
