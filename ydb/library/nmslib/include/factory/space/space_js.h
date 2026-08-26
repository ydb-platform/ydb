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
#ifndef FACTORY_SPACE_JS_H
#define FACTORY_SPACE_JS_H

#include <space/space_js.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Space<dist_t>* CreateJSDivSlow(const AnyParams& /* ignoring params */) {
  return new SpaceJSDiv<dist_t>(SpaceJSDiv<dist_t>::kJSSlow);
}

template <typename dist_t>
Space<dist_t>* CreateJSDivFastPrecomp(const AnyParams& /* ignoring params */) {
  return new SpaceJSDiv<dist_t>(SpaceJSDiv<dist_t>::kJSFastPrecomp);
}

template <typename dist_t>
Space<dist_t>* CreateJSDivFastPrecompApprox(const AnyParams& /* ignoring params */) {
  return new SpaceJSDiv<dist_t>(SpaceJSDiv<dist_t>::kJSFastPrecompApprox);
}


template <typename dist_t>
Space<dist_t>* CreateJSMetricSlow(const AnyParams& /* ignoring params */) {
  return new SpaceJSMetric<dist_t>(SpaceJSMetric<dist_t>::kJSSlow);
}

template <typename dist_t>
Space<dist_t>* CreateJSMetricFastPrecomp(const AnyParams& /* ignoring params */) {
  return new SpaceJSMetric<dist_t>(SpaceJSMetric<dist_t>::kJSFastPrecomp);
}

template <typename dist_t>
Space<dist_t>* CreateJSMetricFastPrecompApprox(const AnyParams& /* ignoring params */) {
  return new SpaceJSMetric<dist_t>(SpaceJSMetric<dist_t>::kJSFastPrecompApprox);
}



/*
 * End of creating functions.
 */


}

#endif
