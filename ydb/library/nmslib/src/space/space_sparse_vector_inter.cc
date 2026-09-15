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
#include <cmath>
#include <fstream>
#include <string>
#include <sstream>
#include <random>

#include "space/space_sparse_vector_inter.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
void
SpaceSparseVectorInter<dist_t>::CreateDenseVectFromObj(const Object* obj, 
                                                       dist_t* pVect,
                                                       size_t nElem) const {
  static std::hash<size_t>   indexHash;
  fill(pVect, pVect + nElem, static_cast<dist_t>(0));

  vector<SparseVectElem<dist_t>> target;
  UnpackSparseElements(obj->data(), obj->datalength(), target);

  for (SparseVectElem<dist_t> e: target) {
    size_t idx = indexHash(e.id_) % nElem;
    pVect[idx] += e.val_;
  }
}

template <typename dist_t>
void
SpaceSparseVectorInter<dist_t>::CreateVectFromObj(const Object* obj, vector<ElemType>& v) const {
  UnpackSparseElements(obj->data(), obj->datalength(), v);
}

template <typename dist_t>
Object* SpaceSparseVectorInter<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<ElemType>& InpVect) const {
  char    *pData = NULL;
  size_t  dataLen = 0;

  PackSparseElements(InpVect, pData, dataLen);
  unique_ptr<char[]>  data(pData);
  unique_ptr<Object> obj(new Object(id, label, dataLen, data.get()));
  return obj.release();
}

template <typename dist_t>
size_t
SpaceSparseVectorInter<dist_t>::ComputeOverlap(const Object* obj1, 
                                               const Object* obj2) const {
  vector<SparseVectElem<dist_t>> elems1, elems2;
  UnpackSparseElements(obj1->data(), obj1->datalength(), elems1);
  UnpackSparseElements(obj2->data(), obj2->datalength(), elems2);
  vector<IdType> ids1, ids2;
  for (SparseVectElem<dist_t> e: elems1)
    ids1.push_back(e.id_);
  for (SparseVectElem<dist_t> e: elems2)
    ids2.push_back(e.id_);
  return IntersectSizeScalarFast(&ids1[0], ids1.size(), &ids2[0], ids2.size());

}

template <typename dist_t>
OverlapInfo 
SpaceSparseVectorInter<dist_t>::ComputeOverlapInfo(const Object* objA, const Object* objB) {
  vector<SparseVectElem<dist_t>> elemsA, elemsB;
  UnpackSparseElements(objA->data(), objA->datalength(), elemsA);
  UnpackSparseElements(objB->data(), objB->datalength(), elemsB);
  return ComputeOverlapInfo(elemsA, elemsB);
}

template <typename dist_t>
OverlapInfo 
SpaceSparseVectorInter<dist_t>::ComputeOverlapInfo(const vector<SparseVectElem<dist_t>>& elemsA, const vector<SparseVectElem<dist_t>>& elemsB) {
  OverlapInfo res;

  // 1. Compute norms

  float norm_left = 0;
  {
    for (uint32_t A = 0; A < elemsA.size(); ++A)
      norm_left += elemsA[A].val_*elemsA[A].val_;
    norm_left = sqrt(norm_left);
  }

  float norm_right = 0;
  {
    for (uint32_t B = 0; B < elemsB.size(); ++B)
      norm_right += elemsB[B].val_*elemsB[B].val_;
    norm_right = sqrt(norm_right);
  }

  // 2. Compute basic overlap statistics and some of values
  //    in matching and non-matching vector parts
   
  uint32_t A = 0, B = 0;

  while (A < elemsA.size() && B < elemsB.size()) {
    uint32_t idA = elemsA[A].id_;
    uint32_t idB = elemsB[B].id_;
    if (idA < idB) {
      res.diff_sum_left_ += elemsA[A].val_; 
      A++;
    } else if (idA > idB) {
      res.diff_sum_right_ += elemsB[B].val_;
      B++;
    } else {
    // *A == *B
      res.overlap_dotprod_norm_ += elemsA[A].val_*elemsB[B].val_; 
      res.overlap_sum_left_ += elemsA[A].val_; 
      res.overlap_sum_right_ += elemsB[B].val_;
      res.overlap_qty_++;
      A++; B++;
    }
  }

  while (A < elemsA.size()) {
    res.diff_sum_left_ += elemsA[A].val_; 
    A++;
  }

  while (B < elemsB.size()) {
    res.diff_sum_right_ += elemsB[B].val_;
    B++;
  }

  // 3. Convert sums into mean values

  if (res.overlap_qty_ > 0) {
    res.overlap_mean_left_  = res.overlap_sum_left_ / res.overlap_qty_;
    res.overlap_mean_right_ = res.overlap_sum_right_ / res.overlap_qty_;
  }
  size_t left_diff_qty = elemsA.size() - res.overlap_qty_;
  if (left_diff_qty) {
    res.diff_mean_left_ = res.diff_sum_left_ / left_diff_qty;
  }
  size_t right_diff_qty = elemsB.size() - res.overlap_qty_;
  if (right_diff_qty) {
    res.diff_mean_right_ = res.diff_sum_right_ / right_diff_qty;
  }

  // 4. Compute standard deviations by iterating over arrays once again

  A = 0; B = 0;

  while (A < elemsA.size() && B < elemsB.size()) {
    uint32_t idA = elemsA[A].id_;
    uint32_t idB = elemsB[B].id_;
    if (idA < idB) {
      float vA = elemsA[A].val_ - res.diff_mean_left_; 
      res.diff_std_left_ += vA*vA; 
      A++;
    } else if (idA > idB) {
      float vB = elemsB[B].val_ - res.diff_mean_right_;
      res.diff_std_right_ += vB*vB; 
      B++;
    } else {
    // *A == *B
      float vA = elemsA[A].val_ - res.overlap_mean_left_; 
      res.overlap_std_left_ += vA*vA;

      float vB = elemsB[B].val_ - res.overlap_mean_right_;
      res.overlap_std_right_ += vB*vB; 

      A++; B++;
    }
  }

  while (A < elemsA.size()) {
    float vA = elemsA[A].val_ - res.diff_mean_left_; 
    res.diff_std_left_ += vA*vA; 
    A++;
  }

  while (B < elemsB.size()) {
    float vB = elemsB[B].val_ - res.diff_mean_right_;
    res.diff_std_right_ += vB*vB; 
    B++;
  }
  // 5. Normalize STDs

  if (res.overlap_qty_ > 1) {
    res.overlap_std_left_ = sqrt(res.overlap_std_left_/(res.overlap_qty_-1));
    res.overlap_std_right_ = sqrt(res.overlap_std_right_/(res.overlap_qty_-1));
  }
  if (left_diff_qty > 1) {
    res.diff_std_left_ = sqrt(res.diff_std_left_/(left_diff_qty-1));
  }
  if (right_diff_qty>1) {
    res.diff_std_right_ = sqrt(res.diff_std_right_/(right_diff_qty-1));
  }

  // 6. Normalize dot product
  if (norm_left > 0) {
    float inv = 1.0/norm_left;
    res.overlap_dotprod_norm_   *= inv;
  }
  if (norm_right > 0) {
    float inv = 1.0/norm_right;
    res.overlap_dotprod_norm_   *= inv;
  }

  return res;
}

template <typename dist_t>
size_t
SpaceSparseVectorInter<dist_t>::ComputeOverlap(const Object* obj1, 
                                               const Object* obj2,
                                               const Object* obj3) const {
  vector<SparseVectElem<dist_t>> elems1, elems2, elems3;
  UnpackSparseElements(obj1->data(), obj1->datalength(), elems1);
  UnpackSparseElements(obj2->data(), obj2->datalength(), elems2);
  UnpackSparseElements(obj3->data(), obj3->datalength(), elems3);
  vector<IdType> ids1, ids2, ids3;
  for (SparseVectElem<dist_t> e: elems1)
    ids1.push_back(e.id_);
  for (SparseVectElem<dist_t> e: elems2)
    ids2.push_back(e.id_);
  for (SparseVectElem<dist_t> e: elems3)
    ids3.push_back(e.id_);
  return IntersectSizeScalar3way(&ids1[0], ids1.size(), &ids2[0], ids2.size(), &ids3[0], ids3.size());

} 

template <typename dist_t>
size_t
SpaceSparseVectorInter<dist_t>::GetElemQty(const Object* obj) const {
  vector<SparseVectElem<dist_t>> elems;
  UnpackSparseElements(obj->data(), obj->datalength(), elems);
  return elems.size();

} 

template class SpaceSparseVectorInter<float>;

}  // namespace similarity
