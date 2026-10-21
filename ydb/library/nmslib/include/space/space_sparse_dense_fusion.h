/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/searchivarius/NonMetricSpaceLib
 *
 * Copyright (c) 2013-2019
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef _SPACE_SPARSE_DENSE_FUSION_H
#define _SPACE_SPARSE_DENSE_FUSION_H

#include <space.h>
#include <object.h>

#include <memory>

#include <space/space_sparse_bin_common.h>
#include <space/space_sparse_vector_inter.h>

#define SPACE_SPARSE_DENSE_FUSION   "sparse_dense_fusion"


namespace similarity {

class SpaceSparseDenseFusion : public Space<float> {

public:
  explicit SpaceSparseDenseFusion(const string& weightFileName);

private:

  virtual ~SpaceSparseDenseFusion() {}

  /** Standard functions to read/write/create objects */
  // Create an object from string representation.
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string &s,
                                              DataFileInputState *pInpState) const override;

  // Create a string representation of an object.
  virtual string CreateStrFromObj(const Object *pObj, const string &externId) const override {
    throw runtime_error("Not implemented!");
  }

  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string &inputFile) const override;

  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector &dataset,
                                                              const string &outputFile) const override {
    throw runtime_error("Not implemented!");
  }

  void UpdateParamsFromFile(DataFileInputState& inpStateBase) override {
    DataFileInputStateSparseDenseFusion& inpState = dynamic_cast<DataFileInputStateSparseDenseFusion&>(inpStateBase);
    vCompDesc_ = inpState.vCompDesc_;
  }

  // Read a string representation of the next object in a file
  virtual bool ReadNextObjStr(DataFileInputState &, string &strObj, LabelType &label, string &externId) const override;

  // Write a string representation of the next object to a file
  virtual void WriteNextObj(const Object &obj, const string &externId, DataFileOutputState &) const override {
    throw runtime_error("Not implemented!");
  }
  /** End of standard functions to read/write/create objects */

  /*
   * Used only for testing/debugging: compares objects approximately. Floating point numbers
   * should be nearly equal. Integers and strings should coincide exactly.
   */
  virtual bool ApproxEqual(const Object &obj1, const Object &obj2) const override {
    throw runtime_error("Not supported!");
  }

  virtual string StrDesc() const override { return SPACE_SPARSE_DENSE_FUSION; }

  virtual void CreateDenseVectFromObj(const Object *obj, float *pVect, size_t nElem) const override {
    throw runtime_error("Cannot create vector for the space: " + StrDesc());
  }

  virtual size_t GetElemQty(const Object *object) const override { return 0; }

  virtual float ProxyDistance(const Object* obj1, const Object* obj2) const override;

protected:
  DISABLE_COPY_AND_ASSIGN(SpaceSparseDenseFusion);

  virtual float HiddenDistance(const Object *obj1, const Object *obj2) const override;

  float compDistance(const Object *obj1, const Object *obj2, bool isIndex) const;

  vector<CompDesc>  vCompDesc_;

  string            weightFileName_;
  vector<float>     vHeaderIndexWeights_;
  vector<float>     vHeaderQueryWeights_;


};



}


#endif

