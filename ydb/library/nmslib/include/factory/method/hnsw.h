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
 /*
 *
 * A Hierarchical Navigable Small World (HNSW) approach.
 *
 * The main publication is (available on arxiv: http://arxiv.org/abs/1603.09320):
 * "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs" by Yu. A. Malkov, D. A. Yashunin
 * This code was contributed by Yu. A. Malkov. It also was used in tests from the paper.
 *
 *
 */

#pragma once

#include <method/hnsw.h>

namespace similarity {

/*
 * Creating functions.
 */

    template <typename dist_t>
    Index<dist_t>* CreateHnsw(bool PrintProgress,
        const string& SpaceType,
        Space<dist_t>& space,
        const ObjectVector& DataObjects) {
        return new Hnsw<dist_t>(PrintProgress, space, DataObjects);
    }

/*
 * End of creating functions.
 */

}


