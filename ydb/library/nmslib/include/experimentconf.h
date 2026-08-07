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
#ifndef _EXPERIMENT_CONFIG_H_
#define _EXPERIMENT_CONFIG_H_

#include <string.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"

namespace similarity {

using std::string;
using std::stringstream;
using std::vector;
using std::unordered_map;

template <typename dist_t>
class ExperimentConfig {
public:
  ExperimentConfig(Space<dist_t>& space,
                   const string& datafile,
                   const string& queryfile,
                   unsigned TestSetQty, // The # of times the datafile is randomly divided into the query and the test set
                   unsigned MaxNumData,
                   unsigned MaxNumQueryToRun,
                   const typename std::vector<unsigned>& knn,
                   float eps,
                   const typename std::vector<dist_t>& range)
      : space_(space),
        datafile_(datafile),
        queryfile_(queryfile),
        pExternalData_(NULL),
        pExternalQuery_(NULL),
        noQueryData_(queryfile.empty()),
        testSetToRunQty_(TestSetQty),
        testSetQty_(TestSetQty),
        maxNumData_(MaxNumData),
        maxNumQuery_(MaxNumQueryToRun),
        maxNumQueryToRun_(MaxNumQueryToRun),
        range_(range),
        knn_(knn),
        eps_(eps),
        dataSetWasRead_(false) {
    if (noQueryData_) {
      if (!testSetToRunQty_) {
        throw runtime_error(
            "Bad configuration. One should either specify a query file/data, "
            " or the number of test sets obtained by bootstrapping "
            "(random division into query and data files).");
      }
    }
  }

  ExperimentConfig(Space<dist_t>& space,
                   const ObjectVector& externalData,
                   const ObjectVector& externalQuery,
                   unsigned TestSetQty, // The # of times the datafile is randomly divided into the query and the test set
                   IdTypeUnsign MaxNumData,
                   IdTypeUnsign MaxNumQueryToRun,
                   const typename std::vector<unsigned>& knn,
                   float eps,
                   const typename std::vector<dist_t>& range)
      : space_(space),
        datafile_(""),
        queryfile_(""),
        pExternalData_(&externalData),
        pExternalQuery_(&externalQuery),
        noQueryData_(externalQuery.empty()),
        testSetToRunQty_(TestSetQty),
        testSetQty_(TestSetQty),
        maxNumData_(MaxNumData),
        maxNumQuery_(MaxNumQueryToRun),
        maxNumQueryToRun_(MaxNumQueryToRun),
        range_(range),
        knn_(knn),
        eps_(eps),
        dataSetWasRead_(false) {
    if (noQueryData_) {
      if (!testSetToRunQty_) {
        throw runtime_error(
            "Bad configuration. One should either specify a query file/data, "
            " or the number of test sets obtained by bootstrapping "
            "(random division into query and data files).");
      }
    }
  }

  ~ExperimentConfig() {
    for (auto it = origData_.begin(); it != origData_.end(); ++it) {
      delete *it;
    }
    for (auto it = origQuery_.begin(); it != origQuery_.end(); ++it) {
      delete *it;
    }
  }

  void PrintInfo() const;
  void SelectTestSet(int SetNum);
  int GetTestSetToRunQty() const {
    if (!noQueryData_) return 1;
    return testSetToRunQty_;
  }
  int GetTestSetTotalQty() const {
    if (!noQueryData_) return 1;
    return testSetQty_;
  }
  size_t GetOrigDataQty() const { return origData_.size(); }
  const Space<dist_t>&  GetSpace() const { return space_; }
  Space<dist_t>&  GetSpace() { return space_; }
  const ObjectVector& GetDataObjects() const { return dataobjects_; }
  const ObjectVector& GetQueryObjects() const { return queryobjects_; }
  const typename std::vector<unsigned>& GetKNN() const { return knn_; }
  float GetEPS() const { return eps_; }
  const typename std::vector<dist_t>& GetRange() const { return range_; }
  int   GetQueryToRunQty() const {
    return noQueryData_ ? maxNumQueryToRun_ :
                          static_cast<unsigned>(origQuery_.size());
  }
  int   GetTotalQueryQty() const {
    return noQueryData_ ? maxNumQuery_:
                          static_cast<unsigned>(origQuery_.size());
  }

  /*
   * Read/Write : save/retrieve some of the config information.
   */
  void Write(ostream& controlStream, ostream& binaryStream);
  /* 
   * If this function is called (i.e., the cache is read), it should be read
   * before the dataset is read, because data/query splits are stored in cache.
   * TODO: this is an evil form of delayed (and ordered) initialization,
   *       we'll need to get rid of this some day. Instead, we should do
   *       all the work in the constructor.
   *
   *       For now, as a protection from changing the sequence of calls,
   *       we have a special flag (dataSetWasRead_). However, this is not
   *       an ideal solution, in particular, b/c the bug will be noticed
   *       only at run-time.
   *       
   */
  void Read(istream& controlStream, istream& binaryStream, size_t& cacheDataSetQty);

  void ReadDataset();
private:
  Space<dist_t>&    space_;
  ObjectVector      dataobjects_;
  ObjectVector      queryobjects_;
  ObjectVector      origData_;
  ObjectVector      origQuery_;
  vector<int>       origDataAssignment_;  // >=0 denotes an index of the test set, -1 denotes data points
  unordered_map<size_t, size_t> cachedDataAssignment_;
  string            datafile_;
  string            queryfile_;
  const ObjectVector* pExternalData_;  
  const ObjectVector* pExternalQuery_;  
  bool              noQueryData_;
  unsigned          testSetToRunQty_;
  unsigned          testSetQty_;

  IdTypeUnsign      maxNumData_;
  IdTypeUnsign      maxNumQuery_;
  IdTypeUnsign      maxNumQueryToRun_;


  vector<dist_t>    range_;  // range search parameter
  vector<unsigned>  knn_;    // knn search parameters
  float             eps_;    // knn search parameter

  bool              dataSetWasRead_;

  void CopyExternal(const ObjectVector& src, ObjectVector& dst, size_t maxQty);
};

}


#endif
