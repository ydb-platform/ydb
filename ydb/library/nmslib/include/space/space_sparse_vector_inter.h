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
#ifndef _SPACE_SPARSE_VECTOR_INTER_H_
#define _SPACE_SPARSE_VECTOR_INTER_H_

#include <cstdint>
#include <string>
#include <map>
#include <cmath>
#include <stdexcept>
#include <algorithm>
#include <limits>
#include <vector>
#include <cstddef>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "distcomp.h"
#include "space_sparse_vector.h"

namespace similarity {

using std::vector;

struct OverlapInfo {
  uint32_t  overlap_qty_ = 0; // The number of shared dimension, i.e., a vectors' overlap
  // The dot product of elements from the overlap, normalized by vector norms,
  // it is also equal to 1 - cosine distance
  float     overlap_dotprod_norm_ = 0;

  // Overlap statistics for the left vector (left argument of the cosine distance)
  float     overlap_sum_left_ = 0;  // The sum of elements from the overlap 
  float     overlap_mean_left_ = 0; // The sample mean of elements from the overlap
  float     overlap_std_left_ = 0;  // The sample standard deviation of elements from the overlap

  // Difference statistics for the left vector (left argument of the cosine)
  float     diff_sum_left_ = 0;    // The sum of elements for the left vector dimensions NOT in the overlap 
  float     diff_mean_left_ = 0;   // The sample mean of ...
  float     diff_std_left_ = 0;    // The sample standard deviation of ...

  // Overlap statistics for the right vector (right argument of the cosine distance)
  // Naming conventions are identical to those for the left vector
  float     overlap_sum_right_ = 0;
  float     overlap_mean_right_ = 0;
  float     overlap_std_right_ = 0;

  // Difference statistics for the right vector (right argument of the cosine)
  // Naming conventions are identical to those for the left vector
  float     diff_sum_right_ = 0;
  float     diff_mean_right_ = 0;
  float     diff_std_right_ = 0;
};

/*
 *
 * This helper base class is different from the SpaceSparseVectorSimpleStorage class
 * in that it stores sparse vectors in a special format (divided into blocks).
 *
 * This makes it possible to quickly compute distances, 
 * where computations involves elements present in both vectors.
 *
 * One example is the scalar product.
 *
 */
template <typename dist_t>
class SpaceSparseVectorInter : public SpaceSparseVector<dist_t> {
 public:
  explicit SpaceSparseVectorInter(){}
  typedef SparseVectElem<dist_t> ElemType;

  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                     size_t nElem) const;
  virtual Object* CreateObjFromVect(IdType id, LabelType label, const vector<ElemType>& InpVect) const;
  virtual void CreateVectFromObj(const Object* obj, vector<ElemType>& v) const ;

  virtual size_t GetElemQty(const Object* object) const;

  size_t ComputeOverlap(const Object* pObj1, const Object* pObj2) const;
  size_t ComputeOverlap(const Object* pObj1, const Object* pObj2, const Object* pObj3) const;

  static OverlapInfo ComputeOverlapInfo(const Object* pObj1, const Object* pObj2);
  static OverlapInfo ComputeOverlapInfo(const vector<SparseVectElem<dist_t>>& elemsA, 
                                 const vector<SparseVectElem<dist_t>>& elemsB);
 protected:
  DISABLE_COPY_AND_ASSIGN(SpaceSparseVectorInter);

  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const = 0;
};

/* 
 *
 * Modifying ids, so we don't have ids for which id % 65536 == 0
 *
 * The SIMD intersection algorithm won't be able to handle such ids, 
 * because a faster version of _mm_cmpistrm requires that all ids 
 * are non-zero.
 *
 */
inline  size_t removeBlockZeros(size_t id) {
  // This way we don't have ids for which id % 65536 == 0
  return (id / 65535) * 65536 + (id % 65535) + 1;
};

/* 
 * A conversion that reverts removeBlockZeros
 * It works only for the numbers where id % 65536 != 0,
 * which makes sense as removeBlockZeros can't produce a multiple of 65536.
 *
 *
 * One may doubt if this reverse conversion actually works (we 
 * do have a unit test to verify this). However, it's not 
 * hard to see why the conversion is correct using basic
 * modulo and integer division arithmetic (see below).
 *
 * Note 1:
 * One way to represent id produced by removeBlockZeros(origId):
 * id = (origId / 65535) * 65536 + term_smaller_than_65536
 * Therefore: origId / 65536 is equal to  id / 65536
 * This deletion clearly "removes" the term smaller than 65536.
 * 
 * Note 2:
 * Another way to represent id obtained as removeBlockZeros(origId):
 * id = a_multiple_of_65536 + (origId % 65535) + 1
 * Note that the sum of second and the third terms is smaller than 65536.
 * Therefore id % 65536 = (origId % 65535) + 1
 *
 * Combining Note 1 & 2, we obtain that:
 * addBlockZeros(id) = (origId / 65535) + origId % 65535 = origId
 * 
 * Q.E.D.
 */
inline size_t addBlockZeros(size_t id) {
  return (id / 65536) * 65535 + (id % 65536) - 1;
}

template <typename dist_t>
inline  void ParseSparseElementHeader(const char*     pBuff, 
                                      size_t&         rBlockQty,
                                      dist_t&         rSqSum,
                                      float&          rNormCoeff,
                                      const size_t*&  rpBlockQtys,
                                      const size_t*&  rpBlockOffs, 
                                      const char*&    rpBlockBegin) {
  const size_t*   pQty = reinterpret_cast<const size_t*>(pBuff);
  rBlockQty = *pQty;
  const dist_t*   pSqSum = reinterpret_cast<const dist_t*>(pQty + 1);
  rSqSum = *pSqSum;
  const float* pNormCoeff = reinterpret_cast<const float*>(pSqSum + 1);
  rNormCoeff = *pNormCoeff;
  rpBlockQtys = reinterpret_cast<const size_t*>(pNormCoeff + 1);
  rpBlockOffs = rpBlockQtys + rBlockQty;

  rpBlockBegin = reinterpret_cast<const char*>(rpBlockOffs + rBlockQty);
}  

template <typename dist_t>
inline  void UnpackSparseElements(const char* pBuff, size_t dataLen,
                                  vector<SparseVectElem<dist_t>>& OutVect)
{
  typedef SparseVectElem<dist_t> ElemType;

  OutVect.clear(); // just in case

  size_t            blockQty = 0;
  dist_t            SqSum = 0;
  float             normCoeff = 0;
  const size_t*     pBlockQty = NULL;
  const size_t*     pBlockOff = NULL;

  const char* pBlockBegin = NULL;

  ParseSparseElementHeader(pBuff, blockQty,
                           SqSum, normCoeff,
                           pBlockQty, 
                           pBlockOff, 
                           pBlockBegin);

  size_t elemSize = 2 + sizeof(dist_t);

  for (size_t i = 0; i < blockQty; ++i) {
    size_t qty    = pBlockQty[i];
    size_t offset = pBlockOff[i];

    const uint16_t* pBlockIds = reinterpret_cast<const uint16_t*>(pBlockBegin);
    const dist_t* pValBegin = reinterpret_cast<const dist_t*>(pBlockIds + qty);

    for (size_t k = 0; k < qty; ++k) {
      OutVect.push_back(ElemType(addBlockZeros(pBlockIds[k] + offset), pValBegin[k]));
    }

    pBlockBegin += elemSize * qty;
  }

  CHECK(pBlockBegin - pBuff == (ptrdiff_t)dataLen);
}

template <typename dist_t>
inline  void PackSparseElements(const vector<SparseVectElem<dist_t>>& InpVect, 
                                char*& prBuff, size_t& dataSize) {
  typedef SparseVectElem<dist_t> ElemType;

  vector<vector<ElemType>>    blocks;
  vector<size_t>              blockOffsets;

  uint32_t  prevBlockId = numeric_limits<uint32_t>::max();

  vector<ElemType> currBlock;

  size_t totQty = 0;
  dist_t sqSum = 0;

  for (size_t i = 0; i < InpVect.size(); ++i) {
	  uint32_t id = static_cast<uint32_t> // Blocks are never too large
						(removeBlockZeros(InpVect[i].id_));

	
    uint32_t blockId = static_cast<uint32_t> // Blocks are never too large
						(id / 65536);

    id &= 65535; // in-block id will contain only the last two bytes

    if (blockId != prevBlockId && !currBlock.empty()) {
      blocks.push_back(currBlock);
      blockOffsets.push_back(prevBlockId * 65536);
      currBlock.clear();
    }

    prevBlockId = blockId;
    currBlock.push_back(ElemType(id, InpVect[i].val_));
    sqSum += InpVect[i].val_ * InpVect[i].val_;
    ++totQty;
  }
  blocks.push_back(currBlock);
  blockOffsets.push_back(prevBlockId * 65536);

  CHECK(InpVect.size() == totQty);

  /* 
   * How much memory do we need?
   *
   * i)   A header will store the number of blocks. 
   * ii)  For each block we keep the # of elements
   * iii) The sum of squared element values.
   * iv)  Each element has a 2-byte id and a sizeof(dist_t) value
   *
   */
  size_t elemSize = 2 + sizeof(dist_t);
  dataSize = sizeof(size_t) + // number of blocks
              sizeof(dist_t) + // sum of squared elements
              sizeof(float) + // (sum of squared elements)^(-0.5)
              2 * sizeof(size_t) * (blocks.size()) + // block qtys & offsets
              elemSize * InpVect.size();  

  prBuff = new char[dataSize]; 

  size_t*   pQty = reinterpret_cast<size_t*>(prBuff);

  /*
   * Store meta information.
   */
  *pQty = blocks.size(); 

  dist_t*   pSqSum = reinterpret_cast<dist_t*>(pQty + 1);
  *pSqSum = sqSum;
  float*    pNormCoeff = reinterpret_cast<float*>(pSqSum + 1);
  *pNormCoeff = 1.0f/sqrt(static_cast<float>(sqSum));
  size_t*   pBlockQtyOff = reinterpret_cast<size_t*>(pNormCoeff + 1);

  for (size_t i = 0; i < blocks.size(); ++i) {
    *pBlockQtyOff++ = blocks[i].size(); // qty
  }
  for (size_t i = 0; i < blocks.size(); ++i) {
    *pBlockQtyOff++ = blockOffsets[i]; // offset
  }
  /*
   * Store block data.
   */
  uint16_t* pBlockIds = reinterpret_cast<uint16_t*>(pBlockQtyOff);

  for (size_t i = 0; i < blocks.size(); ++i) {
    dist_t* pBlockVals = reinterpret_cast<dist_t*>(pBlockIds + blocks[i].size());

    for (size_t k = 0; k < blocks[i].size(); ++k) {
      *pBlockIds++ = blocks[i][k].id_;
      *pBlockVals++ = blocks[i][k].val_;
    }

    pBlockIds = reinterpret_cast<uint16_t*>(pBlockVals);
  }

  CHECK(reinterpret_cast<char*>(pBlockIds) - prBuff == (ptrdiff_t)dataSize); 
}

}  // namespace similarity

#endif
