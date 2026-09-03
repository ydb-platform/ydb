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
#ifndef _SPACE_SPARSE_SCALAR_H_
#define _SPACE_SPARSE_SCALAR_H_

#include <string>
#include <map>
#include <cmath>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_sparse_vector.h"
#include "distcomp.h"
#include "my_isnan_isinf.h"

#define SPACE_SPARSE_COSINE_SIMILARITY          "cosinesimil_sparse"
#define SPACE_SPARSE_ANGULAR_DISTANCE           "angulardist_sparse"
#define SPACE_SPARSE_NEGATIVE_SCALAR            "negdotprod_sparse"
#define SPACE_SPARSE_QUERY_NORM_NEGATIVE_SCALAR "querynorm_negdotprod_sparse"

namespace similarity {

template <typename dist_t>
class SpaceSparseAngularDistance : public SpaceSparseVectorSimpleStorage<dist_t> {
 public:
  explicit SpaceSparseAngularDistance() {}
  virtual ~SpaceSparseAngularDistance() {}

  virtual std::string StrDesc() const {
    return SPACE_SPARSE_ANGULAR_DISTANCE;
  }

 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const {
    return SpaceSparseVectorSimpleStorage<dist_t>::
                        ComputeDistanceHelper(obj1, obj2, distObjAngular_);
  }
 private:
  struct SpaceAngularDist {
    dist_t operator()(const dist_t* x, const dist_t* y, size_t length) const {
      dist_t val = AngularDistance(x, y, length);
      // This shouldn't normally happen, but let's keep this check
      if (my_isnan(val)) throw runtime_error("SpaceAngularDist Bug: NAN dist!!!!");
      return val;
    }
  };
  SpaceAngularDist        distObjAngular_;
  DISABLE_COPY_AND_ASSIGN(SpaceSparseAngularDistance);
};

template <typename dist_t>
class SpaceSparseCosineSimilarity : public SpaceSparseVectorSimpleStorage<dist_t> {
 public:
  explicit SpaceSparseCosineSimilarity() {}
  virtual ~SpaceSparseCosineSimilarity() {}

  virtual std::string StrDesc() const {
    return SPACE_SPARSE_COSINE_SIMILARITY;
  }

 protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const {
    return SpaceSparseVectorSimpleStorage<dist_t>::
                  ComputeDistanceHelper(obj1, obj2, distObjCosineSimilarity_);
  }
 private:
  struct SpaceCosineSimilarityDist {
    dist_t operator()(const dist_t* x, const dist_t* y, size_t length) const {
      dist_t val = CosineSimilarity(x, y, length);
      // This shouldn't normally happen, but let's keep this check
      if (my_isnan(val)) throw runtime_error("SpaceCosineSimilarityDist Bug: NAN dist!!!!");
      return val;
    }
  };
  SpaceCosineSimilarityDist        distObjCosineSimilarity_;
  DISABLE_COPY_AND_ASSIGN(SpaceSparseCosineSimilarity);
};

template <typename dist_t>
class SpaceSparseNegativeScalarProduct : public SpaceSparseVectorSimpleStorage<dist_t> {
public:
  explicit SpaceSparseNegativeScalarProduct() {}
  virtual ~SpaceSparseNegativeScalarProduct() {}

  virtual std::string StrDesc() const {
    return SPACE_SPARSE_NEGATIVE_SCALAR;
  }

protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const {
    return SpaceSparseVectorSimpleStorage<dist_t>::
    ComputeDistanceHelper(obj1, obj2, distObjNegativeScalarProduct_);
  }
private:
  struct SpaceNegativeScalarDist {
    dist_t operator()(const dist_t* x, const dist_t* y, size_t length) const {
      return -ScalarProductSIMD(x, y, length);
    }
  };
  SpaceNegativeScalarDist        distObjNegativeScalarProduct_;
  DISABLE_COPY_AND_ASSIGN(SpaceSparseNegativeScalarProduct);
};

template <typename dist_t>
class SpaceSparseQueryNormNegativeScalarProduct : public SpaceSparseVectorSimpleStorage<dist_t> {
public:
  explicit SpaceSparseQueryNormNegativeScalarProduct() {}
  virtual ~SpaceSparseQueryNormNegativeScalarProduct() {}

  virtual std::string StrDesc() const {
    return SPACE_SPARSE_QUERY_NORM_NEGATIVE_SCALAR;
  }

protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const {
    return SpaceSparseVectorSimpleStorage<dist_t>::
    ComputeDistanceHelper(obj1, obj2, distObjQueryNormNegativeScalarProduct_);
  }
private:
  struct SpaceNegativeQueryNormScalarDist {
    dist_t operator()(const dist_t* x, const dist_t* y, size_t length) const {
      // This shouldn't normally happen, but let's keep this check
      dist_t val = QueryNormScalarProduct(x, y, length);
      if (my_isnan(val)) throw runtime_error("SpaceNegativeQueryNormScalarDist Bug: NAN dist!!!!");
      return -val;
    }
  };
  SpaceNegativeQueryNormScalarDist        distObjQueryNormNegativeScalarProduct_;
  DISABLE_COPY_AND_ASSIGN(SpaceSparseQueryNormNegativeScalarProduct);
};


}  // namespace similarity

#endif 
