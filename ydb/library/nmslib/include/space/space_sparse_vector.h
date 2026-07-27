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
#ifndef _SPACE_SPARSE_VECTOR_H_
#define _SPACE_SPARSE_VECTOR_H_

#include <string>
#include <map>
#include <stdexcept>
#include <algorithm>
#include <ostream>
#include <functional>
#include <cstdint>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "distcomp.h"
#include "read_data.h"

namespace similarity {

using std::vector;
using std::fill;

/* 
 * The maximum number of sparse elements that will be kept on the stack
 * by the function ComputeDistanceHelper. 
 *
 * TODO:@leo If there are too many threads, we might run out stack memory.
 *           but it is probably extremely unlikely with the buffer of this size.
 *
 */
#define MAX_BUFFER_QTY  8192

template <typename dist_t>
class SpaceSparseVector : public Space<dist_t> {
public:
  typedef SparseVectElem<dist_t> ElemType;

  explicit SpaceSparseVector() {}
  virtual ~SpaceSparseVector() {}

  /** Standard functions to read/write/create objects */ 
  // Create a string representation of an object.
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const;
  virtual string CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const;
  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inputFile) const;
  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector& dataset,
                                                              const string& outputFile) const;
  /*
   * Read a string representation of the next object in a file as well
   * as its label. Return false, on EOF.
   */
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const;
  /** End of standard functions to read/write/create objects */

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const;

  /* 
   * Different implementations of the sparse vector space will pack elements differently.
   * Hence, they have to provide their own procedures to create an Object as
   * well as extract elements from an Object.
   */
  virtual Object* CreateObjFromVect(IdType id, LabelType label, const vector<ElemType>& InpVect) const = 0;
  // Sparse vectors have no fixed dimensionality
  virtual size_t GetElemQty(const Object* object) const {return 0;}

protected:
  void ReadSparseVec(std::string line, size_t line_num, LabelType& label, vector<ElemType>& v) const;
  virtual void CreateVectFromObj(const Object* obj, vector<ElemType>& v) const  = 0;
  DISABLE_COPY_AND_ASSIGN(SpaceSparseVector);
};

template <typename dist_t>
class SpaceSparseVectorSimpleStorage : public SpaceSparseVector<dist_t> {
public:
  typedef SparseVectElem<dist_t> ElemType;

  virtual ~SpaceSparseVectorSimpleStorage() {}
  explicit SpaceSparseVectorSimpleStorage() {}

  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                 size_t nElem) const override {
    static std::hash<size_t>   indexHash;
    fill(pVect, pVect + nElem, static_cast<dist_t>(0));
    const ElemType* beg = reinterpret_cast<const ElemType*>(obj->data());
    const ElemType* const end =
        reinterpret_cast<const ElemType*>(obj->data() + obj->datalength());
    for (;beg < end;++beg) {
      size_t idx = indexHash(beg->id_) % nElem;
      pVect[idx] += beg->val_;
    }
  }

  virtual Object* CreateObjFromVect(IdType id, LabelType label, const vector<ElemType>& InpVect) const override {
    return new Object(id, label, InpVect.size() * sizeof(ElemType), &InpVect[0]);
  };

protected:
  DISABLE_COPY_AND_ASSIGN(SpaceSparseVectorSimpleStorage);

  virtual void CreateVectFromObj(const Object* obj, vector<ElemType>& v) const override {
    const ElemType* beg = reinterpret_cast<const ElemType*>(obj->data());
    const ElemType* const end =
        reinterpret_cast<const ElemType*>(obj->data() + obj->datalength());
    size_t qty = end - beg;
    v.resize(qty);
    for (size_t i = 0; i < qty; ++i) v[i] = beg[i];
  }

  /* 
   * This helper function converts a dense vector to a sparse one and then calls a generic distance function.
   * It can be used only with simple-storage sparse vector spaces, not with
   * children of SpaceSparseVectorInter.
   */
  template <typename DistObjType> 
  static dist_t ComputeDistanceHelper(const Object* obj1, const Object* obj2,  
                               const DistObjType distObj, dist_t missingValue = 0) {
    dist_t *mem1 = NULL, *mem2 = NULL;
    dist_t buf1[MAX_BUFFER_QTY], buf2[MAX_BUFFER_QTY];
    dist_t *vect1 = buf1;
    dist_t *vect2 = buf2;

    dist_t res = 0;

    const ElemType* beg1 = reinterpret_cast<const ElemType*>(obj1->data());
    const ElemType* beg2 = reinterpret_cast<const ElemType*>(obj2->data());
    const ElemType* end1 = reinterpret_cast<const ElemType*>(obj1->data() + obj1->datalength());
    const ElemType* end2 = reinterpret_cast<const ElemType*>(obj2->data() + obj2->datalength());
    const size_t qty1 = obj1->datalength() / sizeof(ElemType);
    const size_t qty2 = obj2->datalength() / sizeof(ElemType);
    const size_t maxQty = qty1 + qty2;

    /* 
     * We hope not to allocate memory frequently, b/c memory allocation can be expensive
     * TODO:@leo check how expensive are memory allocations, perhaps, this fear is not substantiated
     */      
    try {
      if (maxQty > MAX_BUFFER_QTY) {
        vect1 = mem1 = new dist_t[maxQty];
        vect2 = mem2 = new dist_t[maxQty];
      }

      size_t qty = 0;
   
      const ElemType* it1 = beg1, *it2 = beg2; 

      while (it1 < end1 && it2 < end2) {
        if (it1->id_ == it2->id_) {
          vect1[qty] = it1->val_;
          vect2[qty] = it2->val_;
          ++it1;
          ++it2;
          ++qty; 
        } else if (it1->id_ < it2->id_) {
          vect1[qty] = it1->val_;
          vect2[qty] = missingValue;
          ++qty;
          ++it1;
        } else {
          vect1[qty] = missingValue;
          vect2[qty] = it2->val_;
          ++qty;
          ++it2;
        }
      }
      
      while (it1 < end1) {
        vect1[qty] = it1->val_;
        vect2[qty] = missingValue;
        ++qty;
        ++it1;
      }
      
      while (it2 < end2) {
        vect1[qty] = missingValue;
        vect2[qty] = it2->val_;
        ++qty;
        ++it2;
      }

      if (qty > maxQty) {
        LOG(LIB_ERROR) << qty1;
        LOG(LIB_ERROR) << qty2;
        LOG(LIB_ERROR) << qty;
      }
      CHECK(qty <= maxQty);

      res = distObj(vect1, vect2, qty);

    } catch (...) {
      delete [] mem1;
      delete [] mem2;
    }

    delete [] mem1;
    delete [] mem2;

    return res;
  }
};

}  // namespace similarity

#endif
