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
#ifndef _SPACE_SQFD_H_
#define _SPACE_SQFD_H_

// Don't use on windows
#if !defined(_MSC_VER)

#include <string>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "distcomp.h"

#define SPACE_SQFD_HEURISTIC_FUNC "sqfd_heuristic_func"
#define SPACE_SQFD_MINUS_FUNC     "sqfd_minus_func"
#define SPACE_SQFD_GAUSSIAN_FUNC  "sqfd_gaussian_func"

#define FAKE_FILE_NAME            "fake_file"

namespace similarity {

template <typename dist_t>
class SqfdFunction {
 public:
  virtual ~SqfdFunction() {}
  virtual dist_t f(const dist_t* p1, const dist_t* p2, const int sz) const = 0;
  virtual std::string StrDesc() const = 0;
  virtual SqfdFunction<dist_t>* Clone() const = 0;
};

template <typename dist_t>
class SqfdMinusFunction : public SqfdFunction<dist_t> {
 public:
  dist_t f(const dist_t* p1, const dist_t* p2, const int sz) const {
    return -L2NormSIMD(p1, p2, sz);
  }
  std::string StrDesc() const {
    return "minus function";
  }
  virtual SqfdFunction<dist_t>* Clone() const {
    return new SqfdMinusFunction<dist_t>(); // No parameters!!!!
  }
};

template <typename dist_t>
class SqfdHeuristicFunction : public SqfdFunction<dist_t> {
 public:
  SqfdHeuristicFunction(float alpha) : alpha_(alpha) {}
  dist_t f(const dist_t* p1, const dist_t* p2, const int sz) const {
    return 1.0 / (alpha_ + L2NormSIMD(p1, p2, sz));
  }
  std::string StrDesc() const {
    std::stringstream stream;
    stream << "heuristic function alpha=" << alpha_;
    return stream.str();
  }
  virtual SqfdFunction<dist_t>* Clone() const {
    return new SqfdHeuristicFunction<dist_t>(alpha_);
  }
 private:
  float alpha_;
};

template <typename dist_t>
class SqfdGaussianFunction : public SqfdFunction<dist_t> {
 public:
  SqfdGaussianFunction(float alpha) : alpha_(alpha) {}
  dist_t f(const dist_t* p1, const dist_t* p2, const int sz) const {
    const dist_t d = L2NormSIMD(p1, p2, sz);
    return exp(-alpha_ * d * d);
  }
  std::string StrDesc() const {
    std::stringstream stream;
    stream << "gaussian function alpha=" << alpha_;
    return stream.str();
  }
  virtual SqfdFunction<dist_t>* Clone() const {
    return new SqfdGaussianFunction<dist_t>(alpha_);
  }
 private:
  float alpha_;
};

template <typename dist_t>
class SpaceSqfd : public Space<dist_t> {
 public:
  SpaceSqfd(SqfdFunction<dist_t>* func);
  virtual ~SpaceSqfd();

  std::string StrDesc() const;

  /** Standard functions to read/write/create objects */ 
  /*
   * Create an object from string representation.
   * If the input state pointer isn't null, we check
   * if the new vector is consistent with previous output.
   * If now previous output was seen, the state vector may be
   * updated. For example, when we start reading vectors,
   * we don't know the number of elements. When, we see the first
   * vector, we memorize dimensionality. If a subsequently read
   * vector has a different dimensionality, an exception will be thrown.
   */
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const;
  // Create a string representation of an object.
  // It doesn't recreate the file name, b/c we don't memorize it.
  // Instead it prints a fake entry.
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
   * CreateDenseVectFromObj and GetElemQty() are only needed, if
   * one wants to use methods with random projections.
   */
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                   size_t nElem) const {
    throw runtime_error("Cannot create vector for the space: " + StrDesc());
  }
  virtual size_t GetElemQty(const Object* object) const {return 0;}
 protected:
  DISABLE_COPY_AND_ASSIGN(SpaceSqfd);

  dist_t HiddenDistance(
      const Object* obj1,
      const Object* obj2) const;
 private:
  SqfdFunction<dist_t>* func_;
};

}  // namespace similarity

#endif

#endif

