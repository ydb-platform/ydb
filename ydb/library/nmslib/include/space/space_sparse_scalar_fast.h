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
#ifndef _SPACE_SPARSE_SCALAR_FAST_H_
#define _SPACE_SPARSE_SCALAR_FAST_H_

#include <string>
#include <limits>
#include <map>
#include <stdexcept>
#include <algorithm>

#include <string.h>
#include "global.h"
#include "object.h"
#include "query.h"
#include "inmem_inv_index.h"
#include "utils.h"
#include "space.h"
#include "space_sparse_vector_inter.h"
#include "distcomp.h"


#define SPACE_SPARSE_COSINE_SIMILARITY_FAST  "cosinesimil_sparse_fast"
#define SPACE_SPARSE_ANGULAR_DISTANCE_FAST   "angulardist_sparse_fast"

#define SPACE_SPARSE_NEGATIVE_SCALAR_FAST             "negdotprod_sparse_fast"
#define SPACE_SPARSE_QUERY_NORM_NEGATIVE_SCALAR_FAST  "querynorm_negdotprod_sparse_fast"


namespace similarity {

// An implementation of efficient all-pivot distance computation
class SpaceDotProdPivotIndexBase: public PivotIndex<float> {
public:
  // If hashTrickDim > 0, we use the hashing trick as a simple means to reduce dimensionality
  // at the expense of accuracy loss
  SpaceDotProdPivotIndexBase(const Space<float>& space,
                             const ObjectVector& pivots,
                             bool  bNormData,
                             bool  bNormQuery,
                             size_t hashTrickDim = 0) :
    space_(space), pivots_(pivots), bNormData_(bNormData), bNormQuery_(bNormQuery), hashTrickDim_(hashTrickDim) {
    createIndex();
  }
  virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const override;
  virtual void ComputePivotDistancesQueryTime(const Query<float>* pQuery, vector<float>& vResDist) const override {
    ComputePivotDistancesIndexTime(pQuery->QueryObject(), vResDist);
  }
private:
  InMemInvIndex         invIndex_;
  const Space<float>&   space_;
  ObjectVector          pivots_;
  bool                  bNormData_;
  bool                  bNormQuery_;
  size_t                hashTrickDim_;

  void createIndex();
  void GenVectElems(const Object& obj, bool bNorm, vector<SparseVectElem<float>>& pivElems) const;
};

class SpaceSparseCosineSimilarityFast : public SpaceSparseVectorInter<float> {
public:
  explicit SpaceSparseCosineSimilarityFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_COSINE_SIMILARITY_FAST;
  }
  virtual PivotIndex<float>* CreatePivotIndex(const ObjectVector& pivots, size_t hashTrickDim = 0) const override {
    return new PivotIndexLocal(*this, pivots, hashTrickDim);
  }
protected:
  virtual float HiddenDistance(const Object* obj1, const Object* obj2) const override;

  class PivotIndexLocal : public SpaceDotProdPivotIndexBase {
  public:
    PivotIndexLocal(const Space<float>& space,
                    const ObjectVector& pivots,
                    size_t hashTrickDim) :
        SpaceDotProdPivotIndexBase(space, pivots, true, true, hashTrickDim) {}
    virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const override {
      SpaceDotProdPivotIndexBase::ComputePivotDistancesIndexTime(pObj, vResDist);
      for (float &val : vResDist) val = max(0.0f, 1.0f - val); // converting dot-product to cosine distance
    }
  };

  DISABLE_COPY_AND_ASSIGN(SpaceSparseCosineSimilarityFast);
};

class SpaceSparseAngularDistanceFast : public SpaceSparseVectorInter<float> {
public:
  explicit SpaceSparseAngularDistanceFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_ANGULAR_DISTANCE_FAST;
  }

  virtual PivotIndex<float>* CreatePivotIndex(const ObjectVector& pivots, size_t hashTrickDim = 0) const override {
    return new PivotIndexLocal(*this, pivots, hashTrickDim);
  }
protected:
  virtual float HiddenDistance(const Object* obj1, const Object* obj2) const override;

  class PivotIndexLocal : public SpaceDotProdPivotIndexBase {
  public:
    PivotIndexLocal(const Space<float>& space,
                    const ObjectVector& pivots,
                    size_t hashTrickDim) :
      SpaceDotProdPivotIndexBase(space, pivots, true, true, hashTrickDim) {}
    virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const override {
      SpaceDotProdPivotIndexBase::ComputePivotDistancesIndexTime(pObj, vResDist);
      // NormSparseScalarProductFast ensures ret value is in [-1,1]
      for (float &val : vResDist) val = acos(val); // converting normalized dot-product to angular distance
    }
  };

  DISABLE_COPY_AND_ASSIGN(SpaceSparseAngularDistanceFast);
};

class SpaceSparseNegativeScalarProductFast : public SpaceSparseVectorInter<float> {
public:
  explicit SpaceSparseNegativeScalarProductFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_NEGATIVE_SCALAR_FAST;
  }

  virtual PivotIndex<float>* CreatePivotIndex(const ObjectVector& pivots, size_t hashTrickDim = 0) const override {
    return new PivotIndexLocal(*this, pivots, hashTrickDim);
  }
protected:
  virtual float HiddenDistance(const Object* obj1, const Object* obj2) const override;

  class PivotIndexLocal : public SpaceDotProdPivotIndexBase {
  public:
    PivotIndexLocal(const Space<float>& space,
                    const ObjectVector& pivots,
                    size_t hashTrickDim) :
      SpaceDotProdPivotIndexBase(space, pivots, false, false, hashTrickDim) {}
    virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const override {
      SpaceDotProdPivotIndexBase::ComputePivotDistancesIndexTime(pObj, vResDist);
      for (float &val : vResDist) val = -val;
    }
  };

  DISABLE_COPY_AND_ASSIGN(SpaceSparseNegativeScalarProductFast);
};

class SpaceSparseQueryNormNegativeScalarProductFast : public SpaceSparseVectorInter<float> {
public:
  explicit SpaceSparseQueryNormNegativeScalarProductFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_QUERY_NORM_NEGATIVE_SCALAR_FAST;
  }

  virtual PivotIndex<float>* CreatePivotIndex(const ObjectVector& pivots, size_t hashTrickDim = 0) const override {
    return new PivotIndexLocal(*this, pivots, hashTrickDim);
  }
protected:
  virtual float HiddenDistance(const Object* obj1, const Object* obj2) const override;

  class PivotIndexLocal : public SpaceDotProdPivotIndexBase {
  public:
    PivotIndexLocal(const Space<float>& space,
                    const ObjectVector& pivots,
                    size_t hashTrickDim) :
      SpaceDotProdPivotIndexBase(space, pivots, false, true /* only query normalization */, hashTrickDim) {}
    virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const override {
      SpaceDotProdPivotIndexBase::ComputePivotDistancesIndexTime(pObj, vResDist);
      for (float &val : vResDist) val = -val;
    }
  };

  DISABLE_COPY_AND_ASSIGN(SpaceSparseQueryNormNegativeScalarProductFast);
};


}  // namespace similarity

#endif 
