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
#ifndef _DUMMY_METHOD_H_
#define _DUMMY_METHOD_H_

#include <string>
#include <sstream>

#include "index.h"

#define METH_DUMMY             "dummy"

namespace similarity {

using std::string;

/*
 * This is a dummy, i.e., zero-functionality search method,
 * which can be used as a template to create fully functional
 * search method.
 */

template <typename dist_t>
class DummyMethod : public Index<dist_t> {
 public:
  /*
   * The constructor here space and data-objects' references,
   * which are guaranteed to be be valid during testing.
   * So, we can memorize them safely.
   */
  DummyMethod(Space<dist_t>& space, 
              const ObjectVector& data) : Index<dist_t>(data), space_(space) {}

  /*
   * This function is supposed to create a search index (or call a 
   * function to create it)!
   */
  void CreateIndex(const AnyParams& IndexParams) override {
    AnyParamManager  pmgr(IndexParams);
    pmgr.GetParamOptional("doSeqSearch",  
                          bDoSeqSearch_, 
      // One should always specify the default value of an optional parameter!
                          false
                          );
    // Check if a user specified extra parameters, which can be also misspelled variants of existing ones
    pmgr.CheckUnused();
    // Always call ResetQueryTimeParams() to set query-time parameters to their default values
    this->ResetQueryTimeParams();
  }

  /*
   * One can implement functions for index serialization and reading.
   * However, this is not required.
   */
  // SaveIndex is not necessarily implemented
  virtual void SaveIndex(const string& location) override {
    throw runtime_error("SaveIndex is not implemented for method: " + StrDesc());
  }
  // LoadIndex is not necessarily implemented
  virtual void LoadIndex(const string& location) override {
    throw runtime_error("LoadIndex is not implemented for method: " + StrDesc());
  }

  void SetQueryTimeParams(const AnyParams& QueryTimeParams) override;

  ~DummyMethod(){};

  /* 
   * Just the name of the method, consider printing crucial parameter values
   */
  const std::string StrDesc() const override { 
    stringstream str;
    str << "Dummy method: " << (bDoSeqSearch_ ? " does seq. search " : " does nothing (really dummy)"); 
    return str.str();
  }

  /* 
   *  One needs to implement two search functions.
   */
  void Search(RangeQuery<dist_t>* query, IdType) const override;
  void Search(KNNQuery<dist_t>* query, IdType) const override;

  /*
   * In rare cases, mostly when we wrap up 3rd party methods,
   * we simply duplicate the data set. This function
   * let the experimentation code know this, so it could
   * adjust the memory consumption of the index.
   *
   * Note, however, that this method doesn't create any data duplicates.
   */
  virtual bool DuplicateData() const override { return false; }

 private:
  bool                    data_duplicate_;
  Space<dist_t>&          space_;
  bool                    bDoSeqSearch_;
  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(DummyMethod);
};

}   // namespace similarity

#endif     // _DUMMY_METHOD_H_
