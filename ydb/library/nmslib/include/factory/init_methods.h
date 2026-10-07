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
#ifndef INIT_METHODS_H
#define INIT_METHODS_H

#include "methodfactory.h"

// Some extra stuff
#include "factory/method/dummy.h"
#include "factory/method/pivot_neighb_invindx.h"
#include "factory/method/seqsearch.h"
#include "factory/method/small_world_rand.h"
#include "factory/method/hnsw.h"
#include "factory/method/vptree.h"
#include "factory/method/simple_inverted_index.h"

namespace similarity {


inline void initMethods() {
  // a dummy method
  REGISTER_METHOD_CREATOR(float,  METH_DUMMY, CreateDummy)
  REGISTER_METHOD_CREATOR(int,    METH_DUMMY, CreateDummy)

  // Inverted index over permutation-based neighborhoods
  REGISTER_METHOD_CREATOR(float,  METH_PIVOT_NEIGHB_INVINDEX, CreatePivotNeighbInvertedIndex)
  REGISTER_METHOD_CREATOR(int,    METH_PIVOT_NEIGHB_INVINDEX, CreatePivotNeighbInvertedIndex)
  REGISTER_METHOD_CREATOR(float,  METH_PIVOT_NEIGHB_INVINDEX_SYN, CreatePivotNeighbInvertedIndex)
  REGISTER_METHOD_CREATOR(int,    METH_PIVOT_NEIGHB_INVINDEX_SYN, CreatePivotNeighbInvertedIndex)

  // Just sequential searching
  REGISTER_METHOD_CREATOR(float,  METH_SEQ_SEARCH, CreateSeqSearch)
  REGISTER_METHOD_CREATOR(int,    METH_SEQ_SEARCH, CreateSeqSearch)
  REGISTER_METHOD_CREATOR(float,  METH_SEQ_SEARCH_SYN, CreateSeqSearch)
  REGISTER_METHOD_CREATOR(int,    METH_SEQ_SEARCH_SYN, CreateSeqSearch)

  // Small-word (KNN-graph) with randomly generated neighborhood-networks
  REGISTER_METHOD_CREATOR(float,  METH_SMALL_WORLD_RAND, CreateSmallWorldRand)
  REGISTER_METHOD_CREATOR(int,    METH_SMALL_WORLD_RAND, CreateSmallWorldRand)
  REGISTER_METHOD_CREATOR(float,  METH_SMALL_WORLD_RAND_SYN, CreateSmallWorldRand)
  REGISTER_METHOD_CREATOR(int,    METH_SMALL_WORLD_RAND_SYN, CreateSmallWorldRand)

  REGISTER_METHOD_CREATOR(float,  METH_HNSW, CreateHnsw)
  REGISTER_METHOD_CREATOR(int,    METH_HNSW, CreateHnsw)

  // VP-tree, piecewise-polynomial approximation of the decision rule
  REGISTER_METHOD_CREATOR(int,    METH_VPTREE, CreateVPTree)
  REGISTER_METHOD_CREATOR(float,  METH_VPTREE, CreateVPTree)


  // Classic DAAT inverted index
  REGISTER_METHOD_CREATOR(float,  METH_SIMPLE_INV_INDEX, CreateSimplInvIndex)
}



}

#endif
