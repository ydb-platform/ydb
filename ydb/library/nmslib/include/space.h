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
#ifndef SPACE_H
#define SPACE_H

#include <string>
#include <map>
#include <stdexcept>
#include <iostream>
#include <cmath>
#include <memory>
#include <limits>
#include <vector>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "logging.h"
#include "permutation_type.h"

#define LABEL_PREFIX "label:"

#define DIST_TYPE_INT      "int"
#define DIST_TYPE_FLOAT    "float"

namespace similarity {

using std::map;
using std::string;
using std::ifstream;
using std::ofstream;
using std::pair;
using std::make_pair;
using std::runtime_error;
using std::hash;

template <typename dist_t>
class ExperimentConfig;

template <typename dist_t>
const char* DistTypeName();

template <> inline const char* DistTypeName<float>() { return "FLOAT"; }
template <> inline const char* DistTypeName<int>() { return "INT"; }
template <typename dist_t> inline const char* DistTypeName() { return typeid(dist_t).name(); }

template <typename dist_t>
class Query;

template <typename dist_t>
class KNNQuery;

template <typename dist_t>
class RangeQuery;

template <typename dist_t>
class Experiments;

struct DataFileInputState {
  virtual void Close() {};
  virtual ~DataFileInputState(){};
};

struct DataFileInputStateOneFile : public DataFileInputState {
  virtual void Close() { inp_file_.close(); }
  DataFileInputStateOneFile(const string& inpFileName) :
                                          inp_file_(inpFileName.c_str()), line_num_(0) {

    if (!inp_file_) {
      PREPARE_RUNTIME_ERR(err) << "Cannot open file: " << inpFileName << " for reading";
      THROW_RUNTIME_ERR(err);
    }

    inp_file_.exceptions(ios::badbit);
  }
  virtual ~DataFileInputStateOneFile(){};
  ifstream        inp_file_;
  size_t          line_num_;
};

struct DataFileInputStateVec : public DataFileInputStateOneFile {
  DataFileInputStateVec(const string& inpFileName) : DataFileInputStateOneFile(inpFileName), dim_(0) { } 
  unsigned        dim_;
};

struct DataFileOutputState {
  DataFileOutputState(const string& outputFile) : out_file_(outputFile.c_str()) {
    out_file_.exceptions(std::ios::badbit | std::ios::failbit);
  }
  virtual void Close() { out_file_.close(); }
  virtual ~DataFileOutputState(){};
  ofstream out_file_;
};

template <typename dist_t>
class PivotIndex {
public:
  virtual ~PivotIndex();
  virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<dist_t>& vResDist) const = 0;
  virtual void ComputePivotDistancesQueryTime(const Query<dist_t>* pQuery, vector<dist_t>& vResDist) const = 0;
};

template <typename dist_t> class Space;
template <typename dist_t> class Query;

template <typename dist_t>
class DummyPivotIndex : public PivotIndex<dist_t> {
  const Space<dist_t>&  space_;
  ObjectVector          pivots_;
public:
  virtual ~DummyPivotIndex();
  DummyPivotIndex(const Space<dist_t>& space, const ObjectVector pivots) : space_(space), pivots_(pivots) {}
  virtual void ComputePivotDistancesIndexTime(const Object* pObj, vector<dist_t>& vResDist) const override;
  virtual void ComputePivotDistancesQueryTime(const Query<dist_t>* pQuery, vector<dist_t>& vResDist) const override;
};

template <typename dist_t>
class Space {
 public:
  explicit Space() {}
  virtual ~Space() {}
  // This function is public and it is not supposed to be used in the query-mode
  dist_t IndexTimeDistance(const Object* obj1, const Object* obj2) const {
    if (!bIndexPhase) {
      throw runtime_error(string("The public function ") + __func__ + 
                                 " function is accessible only during the indexing phase!");
    }
    return HiddenDistance(obj1, obj2);
  }

  virtual dist_t ProxyDistance(const Object* obj1, const Object* obj2) const {
    throw runtime_error("Not supported!");
  }

  virtual string StrDesc() const = 0;

  /*
   * This virtual function can be overridden to create an index that
   * efficiently computes distance to all pivots. Contract: this
   * function may assume that objects (instances of Object) in the 
   * array passed as this function argument exist at a later time,
   * i.e., when we call functions ComputePivotDistancesIndexTime and
   * ComputePivotDistancesQueryTime.
   */
  virtual PivotIndex<dist_t>* CreatePivotIndex(const ObjectVector& pivots,
                                               size_t hashTrickDim = 0) const {
    return new DummyPivotIndex<dist_t>(*this, pivots);
  }

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
                                              DataFileInputState* pInpState) const = 0;
  // Create a string representation of an object.
  virtual string CreateStrFromObj(const Object* pObj, const string& externId) const = 0;
  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inputFile) const = 0;
  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector& dataset, 
                                                              const string& outputFile) const = 0;
  /*
   * Read a string representation of the next object in a file as well
   * as its label. Return false, on EOF.
   */
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const = 0;
  /* 
   * Write a string representation of the next object to a file. We totally delegate
   * this to a Space object, because it may package the string representation, by
   * e.g., in the form of an XML fragment.
   */
  virtual void WriteNextObj(const Object& obj, const string& externId, DataFileOutputState &outState) const {
    outState.out_file_ << CreateStrFromObj(&obj, externId) << endl;
  }
  /*
   * This function allows setting space parameters (and creating additional parameter-dependent data structures)
   * based on the content of the input file.
   */
  virtual void UpdateParamsFromFile(DataFileInputState& inpState) {}
  /** End of standard functions to read/write/create objects as well as update state parameters */ 

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const = 0;

  unique_ptr<DataFileInputState>  ReadDataset(ObjectVector& dataset,
                   vector<string>& vExternIds,
                   const string& inputFile,
                   const IdTypeUnsign MaxNumObjects = MAX_DATASET_QTY) const;
  void WriteDataset(const ObjectVector& dataset,
                   const vector<string>& vExternIds,
                   const string& outputFile,
                   const IdTypeUnsign MaxNumObjects = MAX_DATASET_QTY) const;

  /*
   * if the space stores some useful information in the input state,
   * it needs to override ReadObjectVectorFromBinData, and WriteObjectVectorBinData.
   * PS: Here an assumption is that data is empty
   */
  virtual unique_ptr<DataFileInputState> ReadObjectVectorFromBinData(ObjectVector& dataset,
                                                                     vector<string>& vExternIds,
                                                                     const std::string& inputFile,
                                                                     const IdTypeUnsign maxQty=MAX_DATASET_QTY) const;

  virtual void WriteObjectVectorBinData(const ObjectVector& dataset,
                                        const vector<string>& vExternIds,
                                        const std::string& outputFile,
                                        const IdTypeUnsign MaxNumObjects = MAX_DATASET_QTY) const;

  /*
   * For some real-valued or integer-valued *DENSE* vector spaces this function
   * returns the number of vector elements. For all other spaces, it returns
   * 0.
   *
   * TODO: @leo This seems to be related to issue #7
   * https://github.com/nmslib/nmslib/issues/7
   *
   * With a proper hierarchy of Object classes, getDimension() would
   * be a function of an object, not of a space. At some point Object
   * must become smarter. Now it's a dumb container, while all the heavy
   * lifting is done by a Space object.
   *
   */
  virtual size_t GetElemQty(const Object* obj) const = 0;
  /*
   * For some dense vector spaces this function extracts the first nElem
   * elements from the object. If nElem > getElemQty(), an exception
   * will be thrown. For sparse vector spaces, the algorithm may "hash"
   * several elements together by summing up their values.
   *
   * Non-vector spaces don't have to support this function, they may
   * throw an exception.
   */
  virtual void CreateDenseVectFromObj(const Object* obj, dist_t* pVect,
                                 size_t nElem) const = 0;
 protected:
  void SetIndexPhase() const { bIndexPhase = true; }
  void SetQueryPhase() const { bIndexPhase = false; }

  friend class Query<dist_t>;
  friend class RangeQuery<dist_t>;
  friend class KNNQuery<dist_t>;
  friend class Experiments<dist_t>;
  /* 
   * This function is private, but it will be accessible by the friend class Query
   * IndexTimeDistance access can be disable/enabled only by function friends 
   */
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const = 0;
 private:
  bool mutable bIndexPhase = true;
  
  DISABLE_COPY_AND_ASSIGN(Space);
};

}  // namespace similarity

#endif
