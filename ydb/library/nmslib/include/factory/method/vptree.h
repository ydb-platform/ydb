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
#ifndef _FACTORY_VPTREE_H_
#define _FACTORY_VPTREE_H_

#include "searchoracle.h"
#include <method/vptree.h>

namespace similarity {

/* 
 * We have different creating functions, 
 * b/c there are different pruning methods.
 */
template <typename dist_t>
Index<dist_t>* CreateVPTree(bool PrintProgress,
                           const string& SpaceType,
                           Space<dist_t>& space,
                           const ObjectVector& DataObjects) {
    return new VPTree<dist_t,PolynomialPruner<dist_t>>(PrintProgress, space, DataObjects);
}

/*
 * End of creating functions.
 */
}

#endif
