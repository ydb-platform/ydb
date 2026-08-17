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
#ifndef INIT_SPACES_H
#define INIT_SPACES_H

#include "spacefactory.h"

#include "factory/space/space_edist.h"
#include "factory/space/space_bit_hamming.h"
#include "factory/space/space_bit_jaccard.h"
#include "factory/space/space_bregman.h"
#include "factory/space/space_dummy.h"
#include "factory/space/space_js.h"
#include "factory/space/space_lp.h"
#include "factory/space/space_scalar.h"
#include "factory/space/space_sparse_lp.h"
#include "factory/space/space_sparse_scalar.h"
#include "factory/space/space_word_embed.h"
#include "factory/space/space_ab_diverg.h"
#include "factory/space/space_renyi_diverg.h"
#include "factory/space/space_sparse_jaccard.h"
#include "factory/space/space_sparse_dense_fusion.h"
#if defined(WITH_EXTRAS)
#include "factory/space/space_sqfd.h"
#endif

namespace similarity {

inline void initSpaces() {
  // Registering a dummy space
  REGISTER_SPACE_CREATOR(int,    SPACE_DUMMY,  CreateDummy)
  REGISTER_SPACE_CREATOR(float,  SPACE_DUMMY,  CreateDummy)

  // Registering binary/bit Hamming/Jaccard
  SpaceFactoryRegistry<int>::CreateFuncPtr bit_hamming_func_ptr = CreateBitHamming<int,uint32_t>;
  REGISTER_SPACE_CREATOR(int,    SPACE_BIT_HAMMING, bit_hamming_func_ptr )
  SpaceFactoryRegistry<float>::CreateFuncPtr bit_jaccard_func_ptr = CreateBitJaccard<float,uint32_t>;
  REGISTER_SPACE_CREATOR(float, SPACE_BIT_JACCARD,  bit_jaccard_func_ptr )

  // Registering the Levensthein-distance: regular and normalized
  REGISTER_SPACE_CREATOR(int,   SPACE_LEVENSHTEIN,  CreateLevenshtein)
  REGISTER_SPACE_CREATOR(float, SPACE_LEVENSHTEIN_NORM,  CreateLevenshteinNormalized)

  // Registering Bregman divergences
  REGISTER_SPACE_CREATOR(float,  SPACE_KLDIV_FAST, CreateKLDivFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_KLDIV_FAST_RIGHT_QUERY, CreateKLDivFastRightQuery)
  REGISTER_SPACE_CREATOR(float,  SPACE_KLDIVGEN_FAST, CreateKLDivGenFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_KLDIVGEN_SLOW, CreateKLDivGenSlow)
  REGISTER_SPACE_CREATOR(float,  SPACE_KLDIVGEN_FAST_RIGHT_QUERY, CreateKLDivGenFastRightQuery)
  REGISTER_SPACE_CREATOR(float,  SPACE_ITAKURASAITO_FAST, CreateItakuraSaitoFast)

  // Jensen-Shannon functions and their derivatives
  REGISTER_SPACE_CREATOR(float,  SPACE_JS_DIV_SLOW, CreateJSDivSlow)
  REGISTER_SPACE_CREATOR(float,  SPACE_JS_DIV_FAST, CreateJSDivFastPrecomp)
  REGISTER_SPACE_CREATOR(float,  SPACE_JS_DIV_FAST_APPROX, CreateJSDivFastPrecompApprox)

  REGISTER_SPACE_CREATOR(float,  SPACE_JS_METR_SLOW, CreateJSMetricSlow)
  REGISTER_SPACE_CREATOR(float,  SPACE_JS_METR_FAST, CreateJSMetricFastPrecomp)
  REGISTER_SPACE_CREATOR(float,  SPACE_JS_METR_FAST_APPROX, CreateJSMetricFastPrecompApprox)

  // Word embeddings
  REGISTER_SPACE_CREATOR(float,  SPACE_WORD_EMBED,  CreateWordEmbed)

  // LP spaces
  REGISTER_SPACE_CREATOR(float,  SPACE_L,  CreateL)
  REGISTER_SPACE_CREATOR(float,  SPACE_LINF, CreateLINF)
  REGISTER_SPACE_CREATOR(float,  SPACE_L1, CreateL1)
  REGISTER_SPACE_CREATOR(float,  SPACE_L2, CreateL2)

  // Scalar product and related distances
  // Dense
  REGISTER_SPACE_CREATOR(float,  SPACE_COSINE_SIMILARITY, CreateCosineSimilarity)
  REGISTER_SPACE_CREATOR(float,  SPACE_ANGULAR_DISTANCE, CreateAngularDistance)
  REGISTER_SPACE_CREATOR(float,  SPACE_NEGATIVE_SCALAR, CreateNegativeScalarProduct)

  // Sparse
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_L, CreateSparseL)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_LINF, CreateSparseLINF)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_L1, CreateSparseL1)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_L2, CreateSparseL2)

  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_COSINE_SIMILARITY, CreateSparseCosineSimilarity)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_ANGULAR_DISTANCE, CreateSparseAngularDistance)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_NEGATIVE_SCALAR, CreateSparseNegativeScalarProduct)

  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_COSINE_SIMILARITY_FAST, CreateSparseCosineSimilarityFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_COSINE_SIMILARITY_BIN_FAST, CreateSparseCosineSimilarityBinFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_ANGULAR_DISTANCE_FAST, CreateSparseAngularDistanceFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_NEGATIVE_SCALAR_FAST, CreateSparseNegativeScalarProductFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_NEGATIVE_SCALAR_PROD_BIN_FAST, CreateSparseNegativeScalarProductBinFast)
  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_QUERY_NORM_NEGATIVE_SCALAR_FAST, CreateSparseQueryNormNegativeScalarProductFast)

  REGISTER_SPACE_CREATOR(float,  SPACE_SPARSE_JACCARD,  CreateSpaceSparseJaccard)

#if defined(WITH_EXTRAS)
  // Signature Quadratic Form Distance
  REGISTER_SPACE_CREATOR(float,  SPACE_SQFD_HEURISTIC_FUNC, CreateSqfdHeuristicFunc)
  REGISTER_SPACE_CREATOR(float,  SPACE_SQFD_MINUS_FUNC, CreateSqfdMinusFunc)
  REGISTER_SPACE_CREATOR(float,  SPACE_SQFD_GAUSSIAN_FUNC, CreateSqfdGaussianFunc)
#endif

  REGISTER_SPACE_CREATOR(float,  SPACE_AB_DIVERG_SLOW,  CreateAlphaBetaDivergSlow)
  REGISTER_SPACE_CREATOR(float,  SPACE_AB_DIVERG_FAST,  CreateAlphaBetaDivergFast)

  REGISTER_SPACE_CREATOR(float,  SPACE_RENYI_DIVERG_SLOW,  CreateRenyiDivergSlow)
  REGISTER_SPACE_CREATOR(float,  SPACE_RENYI_DIVERG_FAST,  CreateRenyiDivergFast)

  REGISTER_SPACE_CREATOR(int,    SPACE_L2SQR_SIFT,  CreateL2SqrSIFT)
  REGISTER_SPACE_CREATOR(float, SPACE_SPARSE_DENSE_FUSION, createSparseDenseFusion)
}

}

#endif
