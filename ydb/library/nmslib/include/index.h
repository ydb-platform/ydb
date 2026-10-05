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
#ifndef _INDEX_STRUCTURE_H_
#define _INDEX_STRUCTURE_H_

#include <stdio.h>
#include <string>
#include <vector>

#include "params.h"
#include "object.h"

namespace similarity {

using std::string;
using std::vector;

/*
 * This error message is used to report the situation
 * when a previously used index is used with a different
 * data set
 */
const string DATA_MUTATION_ERROR_MSG =
    "A previously saved index is apparently used with a different data set, a different data set split, and/or a different gold standard file!";
const string METHOD_DESC="MethodDesc";
const string LINE_QTY="LineQty";

template <typename dist_t>
class RangeQuery;

template <typename dist_t>
class KNNQuery;

/*
 * Abstract class for all index structures
 */
template <typename dist_t>
class Index {
public:
  Index(const ObjectVector& data) : data_(data) {}

  // Create an index using given parameters
  virtual void CreateIndex(const AnyParams& indexParams) = 0;
  // SaveIndex is not necessarily implemented
  virtual void SaveIndex(const string& location) {
    throw runtime_error("SaveIndex is not implemented for method: " + StrDesc());
  }
  // LoadIndex is not necessarily implemented
  virtual void LoadIndex(const string& location) {
    throw runtime_error("LoadIndex is not implemented for method: " + StrDesc());
  }
  virtual ~Index() {}
  /*
   * There are two type of search methods: a range search and a k-Nearest Neighbor search.
   * In both cases, you can specify an optional starting data point/object. Some methods, e.g.,
   * based on proximity graphs, can use this information to make the search faster and/or more 
   * accurate. 
   */
  virtual void Search(RangeQuery<dist_t>* query, IdType startObj = -1) const = 0;
  virtual void Search(KNNQuery<dist_t>* query, IdType startObj = -1) const = 0;
  // Get the description of the method
  virtual const string StrDesc() const = 0;
  // Set query-time parameters
  virtual void SetQueryTimeParams(const AnyParams& params) = 0;
  // Reset query-time parameters so that they have default values
  virtual void ResetQueryTimeParams() { SetQueryTimeParams(getEmptyParams()); }
  /*
   * In rare cases, mostly when we wrap up 3rd party methods,
   * we simply duplicate the data set. This function
   * let the experimentation code know this, so it could
   * adjust the memory consumption of the index.
   */
  virtual bool DuplicateData() const { return false; }

  virtual void AddBatch(const ObjectVector& batchData, bool printProgress, bool checkIDs = false/* this is a debug flag only, turning it on may affect performance */) {
    throw runtime_error("AddBatch is not implemented!");
  }

  virtual void DeleteBatch(const ObjectVector& batchData, int delStrategy /* this flag is method-specific */,
                           bool checkIDs = false/* this is a debug flag only, turning it on may affect performance */) {
    throw runtime_error("DeleteBatch is not implemented!");
  }

  virtual void DeleteBatch(const vector<IdType>& batchData, int delStrategy /* this flag is method-specific */,
                           bool checkIDs = false/* this is a debug flag only, turning it on may affect performance */) {
    throw runtime_error("DeleteBatch is not implemented!");
  }

  virtual size_t GetSize() const { return data_.size(); }
protected:
  const ObjectVector& data_;

private:
  template <typename QueryType>
  void GenericSearch(QueryType* query, IdType) const;

};

}  // namespace similarity

#endif     // _METRIC_INDEX_STRUCTURE_H_
