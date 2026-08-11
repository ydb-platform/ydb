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
#ifndef _SPACE_SPARSE_JACCARD_H_
#define _SPACE_SPARSE_JACCARD_H_

#include <string>
#include <limits>
#include <map>
#include <stdexcept>
#include <sstream>

#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_vector.h"
#include "distcomp.h"

#define SPACE_SPARSE_JACCARD              "jaccard_sparse"

namespace similarity {


/*
 * This is a Jaccard distance sparse space.
 */

template <typename dist_t>
class SpaceSparseJaccard : public Space<dist_t> {
 public:
  explicit SpaceSparseJaccard() {}
  virtual ~SpaceSparseJaccard() {}

  /* 
   * Space name: It will be used in result files.
   * Consider, including all the parameters where you
   * print the space name:
   */
  virtual std::string StrDesc() const {
    return SPACE_SPARSE_JACCARD;
  }

  /** Standard functions to read/write/create objects */ 
  /*
   * Create an object from a (possibly binary) string.
   * If the input state pointer isn't null, we check
   * if the new vector is consistent with previously read vectors.
   * For example, when we start reading vectors,
   * we don't know the number of elements. When, we see the first
   * vector, we memorize dimensionality. If a subsequently read
   * vector has a different dimensionality, an exception will be thrown.
   */
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const;
  // Create a string representation of an object
  // The string representation may include external ID.
  virtual string CreateStrFromObj(const Object* pObj, const string& externId) const;
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

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const;

  /*
   * CreateDenseVectFromObj and GetElemQty() are only needed, if
   * one wants to use methods with random projections.
   */
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                   size_t nElem) const {
    throw runtime_error("Cannot create vector for the space: " + StrDesc());
  }
  virtual size_t GetElemQty(const Object* object) const {
    return object->datalength()/ sizeof(IdType); // We expect division by 2^n to be implemented efficiently by the compiler
  }
  unsigned ComputeOverlap(const Object* pObj1, const Object* pObj2) const {
    const IdType* p1 = reinterpret_cast<const IdType*>(pObj1->data());
    const IdType* p2 = reinterpret_cast<const IdType*>(pObj2->data());
    return IntersectSizeScalarFast(p1, GetElemQty(pObj1), p2, GetElemQty(pObj2));
  }
  unsigned ComputeOverlap(const Object* pObj1, const Object* pObj2, const Object* pObj3) const {
    const IdType* p1 = reinterpret_cast<const IdType*>(pObj1->data());
    const IdType* p2 = reinterpret_cast<const IdType*>(pObj2->data());
    const IdType* p3 = reinterpret_cast<const IdType*>(pObj3->data());
    return IntersectSizeScalar3way(p1, GetElemQty(pObj1), p2, GetElemQty(pObj2), p3, GetElemQty(pObj3));
  }
 protected:
 /*
  * This function should always be protected.
  * Only children and friends of the Space class
  * should be able to access it.
  */
  virtual dist_t HiddenDistance(const Object* pObj1, const Object* pObj2) const {
    const IdType* p1 = reinterpret_cast<const IdType*>(pObj1->data());
    const IdType* p2 = reinterpret_cast<const IdType*>(pObj2->data());
    return JaccardSparse(p1, GetElemQty(pObj1), p2, GetElemQty(pObj2));
  }
  Object* CreateObjFromIds(IdType id, LabelType label, const vector<IdType>& InpVect) const;
 private:
  /*
   * One should forbid making copies of the Space object
   */
  DISABLE_COPY_AND_ASSIGN(SpaceSparseJaccard);
};


}  // namespace similarity

#endif 
