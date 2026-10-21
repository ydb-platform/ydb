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
#ifndef _FACTORY_PROJ_VPTREE_H
#define _FACTORY_PROJ_VPTREE_H

#include <method/proj_vptree.h>
#include <space/space_sparse_scalar.h>
#include <space/space_sparse_scalar_fast.h>

namespace similarity {

/*
 * Creating functions.
 */

template <typename dist_t>
Index<dist_t>* CreateProjVPTree(bool PrintProgress,
                           const string& SpaceType,
                           Space<dist_t>& space,
                           const ObjectVector& DataObjects) {
    return new ProjectionVPTree<dist_t>(PrintProgress, space, DataObjects);
}

/*
 * End of creating functions.
 */

}

#endif
