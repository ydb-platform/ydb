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
#ifndef _SPACE_VECTOR_H_
#define _SPACE_VECTOR_H_

#include <string>
#include <map>
#include <stdexcept>
#include <sstream>
#include <memory>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"

namespace similarity {

using std::string;
using std::unique_ptr;

template <typename dist_t>
class VectorSpace : public Space<dist_t> {
 public:
  explicit VectorSpace() {}
  virtual ~VectorSpace() {}

  /** Standard functions to read/write/create objects */ 
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                                DataFileInputState* pInpState) const;
    // Create a string representation of an object.
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

  virtual Object* CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const;
  virtual size_t GetElemQty(const Object* object) const = 0;
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                 size_t nElem) const = 0;

  static void ReadVec(std::string line, LabelType& label, std::vector<dist_t>& v);

protected:
  DISABLE_COPY_AND_ASSIGN(VectorSpace);

  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const = 0;

  void CreateVectFromObjSimpleStorage(const char *pFuncName,
                                 const Object* obj, dist_t* pDstVect,
                                 size_t nElem) const {
    const dist_t* pSrcVec = reinterpret_cast<const dist_t*>(obj->data());
    const size_t len = GetElemQty(obj);
    if (nElem > len) {
      std::stringstream err;
      err << pFuncName << " The number of requested elements "
          << nElem << " is larger than the actual number of elements " << len;
      throw runtime_error(err.str());
    }
    for (size_t i = 0; i < nElem; ++i) pDstVect[i]=pSrcVec[i];
  }
};

template <typename dist_t>
class VectorSpaceSimpleStorage : public VectorSpace<dist_t> {
 public:
  virtual ~VectorSpaceSimpleStorage() {}
  explicit VectorSpaceSimpleStorage() {}
  virtual size_t GetElemQty(const Object* object) const {
    // We expect division by 2^n to be implemented efficiently by the compiler
    return object->datalength()/ sizeof(dist_t);
  }
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pDstVect,
                                 size_t nElem) const {
    VectorSpace<dist_t>::
                CreateVectFromObjSimpleStorage(__func__, obj, pDstVect, nElem);
  }
private:
  DISABLE_COPY_AND_ASSIGN(VectorSpaceSimpleStorage);
};

}  // namespace similarity

#endif
