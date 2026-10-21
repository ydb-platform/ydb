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
#ifndef _SPACE_BIT_VECTOR_H_
#define _SPACE_BIT_VECTOR_H_

#include <string>
#include <map>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "distcomp.h"
#include "read_data.h"
#include "permutation_utils.h"
#include "logging.h"
#include "experimentconf.h"


// Defines an abstract base class for BitVector spaces

namespace similarity {

template <typename dist_t, typename dist_uint_t>
class SpaceBitVector : public Space<dist_t> {
 public:
  explicit SpaceBitVector() {}
  virtual ~SpaceBitVector() {}

  /** Standard functions to read/write/create objects */ 
  // Create an object from string representation.
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpStateBase) const {
    DataFileInputStateVec*  pInpState = NULL;
    if (pInpStateBase != NULL) {
      pInpState = dynamic_cast<DataFileInputStateVec*>(pInpStateBase);
      if (NULL == pInpState) {
        PREPARE_RUNTIME_ERR(err) << "Bug: unexpected pointer type";
        THROW_RUNTIME_ERR(err);
      }
    }
    vector<dist_uint_t>  vec;
    ReadBitMaskVect(s, label, vec);
    if (pInpState != NULL) {
      size_t elemQty = vec[vec.size() - 1];
      if (pInpState->dim_ == 0) pInpState->dim_ = elemQty;
      else if (elemQty != pInpState->dim_) {
        PREPARE_RUNTIME_ERR(err) << "The # of bit-vector elements (" << elemQty << ")" <<
                        " doesn't match the # of elements in previous lines. (" << pInpState->dim_ << " )";
        THROW_RUNTIME_ERR(err);
      }
    }
    return unique_ptr<Object>(CreateObjFromVectInternal(id, label, vec));
  }

  // Create a string representation of an object.
  virtual string CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
    stringstream out;
    const dist_uint_t* p = reinterpret_cast<const dist_uint_t*>(pObj->data());
    const size_t length = pObj->datalength() / sizeof(dist_uint_t)
                          - 1; // the last integer is an original number of elements
    const size_t elemQty = p[length]; // last elem

    for (size_t i = 0; i < elemQty; ++i) {
      if (i) out << " ";
      out << ((p[i/32] >> (i & 31)) & 1);
    }

    return out.str();
  }

  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inpFileName) const {
    return unique_ptr<DataFileInputState>(new DataFileInputStateVec(inpFileName));
  }

  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector& dataset,
                                                              const string& outFileName) const {
    return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
  }

  /*
   * Read a string representation of the next object in a file as well
   * as its label. Return false, on EOF.
   */
  virtual bool ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
    externId.clear();
    DataFileInputStateOneFile* pInpState = dynamic_cast<DataFileInputStateOneFile*>(&inpStateBase);
    CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
    if (!pInpState->inp_file_) return false;
    if (!getline(pInpState->inp_file_, strObj)) return false;
    pInpState->line_num_++;
    return true;
  }
  /** End of standard functions to read/write/create objects */

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const {
    const dist_uint_t* p1 = reinterpret_cast<const dist_uint_t*>(obj1.data());
    const dist_uint_t* p2 = reinterpret_cast<const dist_uint_t*>(obj2.data());
    const size_t len1 = obj1.datalength() / sizeof(dist_uint_t)
                          - 1; // the last integer is an original number of elements
    const size_t len2 = obj2.datalength() / sizeof(dist_uint_t)
                          - 1; // the last integer is an original number of elements
    if (len1 != len2) {
      PREPARE_RUNTIME_ERR(err) << "Bug: comparing vectors of different lengths: " << len1 << " and " << len2;
      THROW_RUNTIME_ERR(err);
    }
    for (size_t i = 0; i < len1; ++i) {
      dist_uint_t v1 =  ((p1[i/32] >> (i & 31)) & 1);
      dist_uint_t v2 =  ((p2[i/32] >> (i & 31)) & 1);
      if (v1 != v2) return false;
    }

    return true;
  }

  virtual std::string StrDesc() const { return "Vector (bit-storage) space"; }
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                      size_t nElem) const {
    throw runtime_error("Cannot create a dense vector for the space: " + StrDesc());
  }
  virtual size_t GetElemQty(const Object* object) const {return 0;}
  virtual Object* CreateObjFromVect(IdType id, LabelType label, std::vector<dist_uint_t>& InpVect) const {
    InpVect.push_back(InpVect.size());
    return CreateObjFromVectInternal(id, label, InpVect);
  }
 protected:
  virtual Object* CreateObjFromVectInternal(IdType id, LabelType label, const std::vector<dist_uint_t>& InpVect) const {
    return new Object(id, label, InpVect.size() * sizeof(dist_uint_t), &InpVect[0]);
  }

  Object* CreateObjFromBitMaskVect(IdType id, LabelType label, const std::vector<dist_uint_t>& bitMaskVect) const {
    return new Object(id, label, bitMaskVect.size() * sizeof(dist_uint_t), &bitMaskVect[0]);
  }

//  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
  void ReadBitMaskVect(std::string line, LabelType& label, std::vector<dist_uint_t>& binVect) const {
    binVect.clear();

    label = Object::extractLabel(line);

    std::stringstream str(line);

    str.exceptions(std::ios::badbit);


    ReplaceSomePunct(line);

    vector<PivotIdType> v;

  #if 0
    try {
      unsigned val;

      while (str >> val) {
        if (val != 0 && val != 1) {
          throw runtime_error("Only zeros and ones are allowed");
        }
        v.push_back(val);
      }
    } catch (const std::exception &e) {
      LOG(LIB_ERROR) << "Exception: " << e.what();
      PREPARE_RUNTIME_ERR(err) << "Failed to parse the line: '" << line << "'";
      LOG(LIB_ERROR) << err.stream().str();
      THROW_RUNTIME_ERR(err);
    }
  #else
    if (!ReadVecDataEfficiently(line, v)) {
      PREPARE_RUNTIME_ERR(err) << "Failed to parse the line: '" << line << "'";
      LOG(LIB_ERROR) << err.stream().str();
      THROW_RUNTIME_ERR(err);
    }
    for (auto val : v) {
      if (val != 0 && val != 1) {
        PREPARE_RUNTIME_ERR(err) << "Only zeros and ones are allowed, offending line: '" << line << "'";
        LOG(LIB_ERROR) << err.stream().str();
        THROW_RUNTIME_ERR(err);
      }
    }
  #endif
    Binarize(v, 1, binVect);      // Create the binary vector
    binVect.push_back(v.size());   // Put the number of elements in the end
  }

  DISABLE_COPY_AND_ASSIGN(SpaceBitVector);
};

}  // namespace similarity

#endif
