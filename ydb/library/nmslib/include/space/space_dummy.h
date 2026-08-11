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
#ifndef _SPACE_DUMMY_H_
#define _SPACE_DUMMY_H_

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

#define SPACE_DUMMY              "dummy"

namespace similarity {


/*
 * This is a dummy, i.e., zero-functionality space,
 * which can be used as a template to create fully functional
 * space.
 */

template <typename dist_t>
class SpaceDummy : public Space<dist_t> {
 public:
  // A constructor can have arbitrary params
  explicit SpaceDummy(int param1, int param2) : param1_(param1), param2_(param2) {
    LOG(LIB_INFO) << "Created " << StrDesc();
  }
  virtual ~SpaceDummy() {}

  /* 
   * Space name: It will be used in result files.
   * Consider, including all the parameters where you
   * print the space name:
   */
  virtual std::string StrDesc() const {
    stringstream  str;
    str << "DummySpace param1=" << param1_ << " param2=" << param2_;
    return str.str();
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
   * Write a string representation of the next object to a file. We totally delegate
   * this to a Space object, because it may package the string representation, by
   * e.g., in the form of an XML fragment.
   */
  virtual void WriteNextObj(const Object& obj, const string& externId, DataFileOutputState &) const;
  /** End of standard functions to read/write/create objects */


  /*
 * if the space stores some useful information in the input state,
 * it needs to override ReadObjectVectorFromBinData, and WriteObjectVectorBinData.
 * PS: Here an assumption is that data is empty
 */
  virtual unique_ptr<DataFileInputState> ReadObjectVectorFromBinData(ObjectVector& dataset,
                                                                     vector<string>& vExternIds,
                                                                     const std::string& inputFile,
                                                                     const IdTypeUnsign maxQty=MAX_DATASET_QTY) const {
    return Space<dist_t>::ReadObjectVectorFromBinData(dataset, vExternIds, inputFile, maxQty);
  }

  virtual void WriteObjectVectorBinData(const ObjectVector& dataset,
                                        const vector<string>& vExternIds,
                                        const std::string& outputFile,
                                        const IdTypeUnsign MaxNumObjects = MAX_DATASET_QTY) const {
    Space<dist_t>::WriteObjectVectorBinData(dataset, vExternIds, outputFile, MaxNumObjects);
  }

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const {
    return true; // In an actual, non-dummy class, you should return the result
                 // of an actual comparison. Here 'true' is used only to make it compile.
  }

  /*
   * CreateDenseVectFromObj and GetElemQty() are only needed, if
   * one wants to use methods with random projections.
   */
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                   size_t nElem) const {
    throw runtime_error("Cannot create vector for the space: " + StrDesc());
  }
  virtual size_t GetElemQty(const Object* object) const {return 0;}
 protected:
 /*
  * This function should always be protected.
  * Only children and friends of the Space class
  * should be able to access it.
  */
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
 private:
 
  /* 
   * We typically use a trailing underscore to distinguish between
   * class-member variables from non-member variables.
   */
  int param1_;
  int param2_;
  /*
   * One should forbid making copies of the Space object
   */
  DISABLE_COPY_AND_ASSIGN(SpaceDummy);
};


}  // namespace similarity

#endif 
