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
#ifndef FACTORY_SPACE_SPARSE_SCALAR_H
#define FACTORY_SPACE_SPARSE_SCALAR_H

#include <space/space_sparse_scalar.h>
#include <space/space_sparse_scalar_fast.h>
#include <space/space_sparse_scalar_bin_fast.h>

namespace similarity {

/*
 * Creating functions.
 */

/* Regular sparse spaces */

template <typename dist_t>
Space<dist_t>* CreateSparseCosineSimilarity(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseCosineSimilarity<dist_t>();
}


template <typename dist_t>
Space<dist_t>* CreateSparseAngularDistance(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseAngularDistance<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateSparseNegativeScalarProduct(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseNegativeScalarProduct<dist_t>();
}

template <typename dist_t>
Space<dist_t>* CreateSparseQueryNormNegativeScalarProduct(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseQueryNormNegativeScalarProduct<dist_t>();
}

/* Fast sparse spaces */

Space<float>* CreateSparseCosineSimilarityFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseCosineSimilarityFast();
}

Space<float>* CreateSparseCosineSimilarityBinFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseCosineSimilarityBinFast();
}

Space<float>* CreateSparseNegativeScalarProductFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseNegativeScalarProductFast();
}

Space<float>* CreateSparseQueryNormNegativeScalarProductFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseQueryNormNegativeScalarProductFast();
}

Space<float>* CreateSparseNegativeScalarProductBinFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseNegativeScalarProductBinFast();
}

Space<float>* CreateSparseAngularDistanceFast(const AnyParams& /* ignoring params */) {
  // Cosine Similarity
  return new SpaceSparseAngularDistanceFast();
}

/*
 * End of creating functions.
 */

}

#endif
