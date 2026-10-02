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
#include <memory>
#include <cmath>
#include <fstream>
#include <vector>
#include <limits>
#include <cstddef>

#include "utils.h"
#include "space/space_sparse_scalar_fast.h"
#include "experimentconf.h"

namespace similarity {

using namespace std;

float 
SpaceSparseCosineSimilarityFast::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj2->datalength() > 0);

  float val = 1 - NormSparseScalarProductFast(obj1->data(), obj1->datalength(),
                                              obj2->data(), obj2->datalength());

  return max(val, float(0));
}

float 
SpaceSparseAngularDistanceFast::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj2->datalength() > 0);

  // NormSparseScalarProductFast ensures ret value is in [-1,1]
  return acos(NormSparseScalarProductFast(obj1->data(), obj1->datalength(),
                                          obj2->data(), obj2->datalength()));
}

float
SpaceSparseNegativeScalarProductFast::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj2->datalength() > 0);

  return -SparseScalarProductFast(obj1->data(), obj1->datalength(),
                                  obj2->data(), obj2->datalength());

}

float
SpaceSparseQueryNormNegativeScalarProductFast::HiddenDistance(const Object* obj1, const Object* obj2) const {
  CHECK(obj1->datalength() > 0);
  CHECK(obj2->datalength() > 0);

  return -QueryNormSparseScalarProductFast(obj1->data(), obj1->datalength(),
                                           obj2->data(), obj2->datalength());

}


void SpaceDotProdPivotIndexBase::GenVectElems(const Object& obj, bool bNorm, vector<SparseVectElem<float>>& pivElems) const {
  pivElems.clear();
  if (hashTrickDim_) {
    vector<float> tmp(hashTrickDim_);
    space_.CreateDenseVectFromObj(&obj, &tmp[0], hashTrickDim_);
    for (size_t id = 0; id < hashTrickDim_; ++id)
    if (fabs(tmp[id]) > numeric_limits<float>::min()) {
      pivElems.push_back(SparseVectElem<float>(id, tmp[id]));
    }
  } else {
    UnpackSparseElements(obj.data(), obj.datalength(), pivElems);
  }
  if (bNorm) {
    size_t            blockQty = 0;
    float             SqSum = 0;
    float             normCoeff = 0;
    const size_t*     pBlockQty = NULL;
    const size_t*     pBlockOff = NULL;

    const char* pBlockBegin = NULL;

    ParseSparseElementHeader(obj.data(), blockQty,
                             SqSum, normCoeff,
                             pBlockQty,
                             pBlockOff,
                             pBlockBegin);
    CHECK(ptrdiff_t(obj.datalength()) >= (pBlockBegin-reinterpret_cast<const char*>(obj.data())));
    for (SparseVectElem<float> & e : pivElems) {
      e.val_ *= normCoeff;
    }
  }
}

void SpaceDotProdPivotIndexBase::createIndex() {
  LOG(LIB_INFO) << "Creating an index, hash trick dim: " << hashTrickDim_ << " norm. data?: " << bNormData_ << " norm. query?: " << bNormQuery_;
  for (size_t pivId = 0; pivId < pivots_.size(); ++pivId) {
    vector<SparseVectElem<float>> pivElems;
    GenVectElems(*pivots_[pivId], bNormData_, pivElems);
    for (size_t i = 0; i < pivElems.size(); ++i) {
      SparseVectElem<float> e = pivElems[i];
      invIndex_.addEntry(e.id_, SimpleInvEntry(pivId, e.val_));
    }
  }
  // Sorting of inverted index posting lists can be done, but it is not really needed for fast all-pivot distance computation
  //invIndex_.sort(); //
}

void SpaceDotProdPivotIndexBase::ComputePivotDistancesIndexTime(const Object* pObj, vector<float>& vResDist) const {
  vector<SparseVectElem<float>> queryElems;
  GenVectElems(*pObj, bNormQuery_, queryElems);

  vResDist.resize(pivots_.size());
  for (size_t pivId = 0; pivId < pivots_.size(); ++pivId)
    vResDist[pivId] = 0;

  for (auto qe : queryElems) {
    const vector<SimpleInvEntry> *pEntries = invIndex_.getDict(qe.id_);
    if (pEntries != nullptr) {
      for (auto pe : *pEntries)
        vResDist[pe.id_] += qe.val_ * pe.weight_;
    }
  }
}
}  // namespace similarity
