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
#ifndef _SPACE_STRING_H_
#define _SPACE_STRING_H_

#include <string>
#include <map>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"

namespace similarity {

using std::string;

/*
 * TODO: @leo/@bileg Currently we support only char, but need to support char32_t and UTF-8 as well. 
 */
template <typename dist_t>
class StringSpace : public Space<dist_t> {
 public:
  explicit StringSpace() {}
  virtual ~StringSpace() {}

  /** Standard functions to read/write/create objects */ 
  // Create an object from string representation.
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const {
    // TODO double-check that sizeof(char) should always be 1 as guaranteed by the C++ standard
    // then sizeof can be removed
    return unique_ptr<Object>(new Object(id, label, s.size() * sizeof(char), s.c_str()));
  }
  // Create a string representation of an object.
  virtual string CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const;
  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inputFile) const ;
  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector& dataset,
                                                              const string& outputFile) const ;
  // Read a string representation of the next object in a file
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const;
  // Write a string representation of the next object to a file
  virtual void WriteNextObj(const Object& obj, const string& externId, DataFileOutputState &) const;
  /** End of standard functions to read/write/create objects */ 

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const;

  virtual string StrDesc() const = 0;
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                 size_t nElem) const {
    throw runtime_error("Cannot create vector for the space: " + StrDesc());
  }
  virtual size_t GetElemQty(const Object* object) const {return 0;}
 protected:
  DISABLE_COPY_AND_ASSIGN(StringSpace);

  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const = 0;
  void ReadStr(std::string line, LabelType& label, std::string& str, size_t* pLineNum) const;
};

}  // namespace similarity

#endif
