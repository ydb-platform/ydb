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
#ifndef _TEST_DATASET_H_
#define _TEST_DATASET_H_

#include "object.h"
#include "space.h"

#include <string>

#ifdef NMSLIB_ARCADIA_TEST
#include <library/cpp/testing/common/env.h>
#endif

namespace similarity {

using std::string;

// Upstream reads sample data from "../sample_data/" relative to the current
// working directory. Under the Arcadia (ya make) test runner the working
// directory is a writable sandbox, so instead we resolve the committed data
// (exposed via DATA()) through the Arcadia source root. Standalone builds keep
// the original relative prefix.
inline string GetSampleDataPrefix() {
#ifdef NMSLIB_ARCADIA_TEST
  return string(ArcadiaSourceRoot().c_str()) +
         PATH_SEPARATOR + string("ydb") +
         PATH_SEPARATOR + string("library") +
         PATH_SEPARATOR + string("nmslib") +
         PATH_SEPARATOR + string("sample_data") + PATH_SEPARATOR;
#else
  return string("..") + PATH_SEPARATOR + string("sample_data") + PATH_SEPARATOR;
#endif
}

const string sampleDataPrefix = GetSampleDataPrefix();

// ObjectVector holds raw Object* pointers whose ownership is not managed by the
// vector itself. Tests that build an ObjectVector directly (e.g. via ReadDataset)
// must free the objects explicitly, otherwise LeakSanitizer flags them.
inline void DeleteObjects(ObjectVector& dataSet) {
  for (const Object* pObj : dataSet) {
    delete pObj;
  }
  dataSet.clear();
}

// Scope guard that releases the objects held by an ObjectVector when it goes out
// of scope, regardless of which return path is taken.
class ObjectVectorGuard {
 public:
  explicit ObjectVectorGuard(ObjectVector& dataSet) : dataSet_(dataSet) {}
  ~ObjectVectorGuard() { DeleteObjects(dataSet_); }

  ObjectVectorGuard(const ObjectVectorGuard&) = delete;
  ObjectVectorGuard& operator=(const ObjectVectorGuard&) = delete;

 private:
  ObjectVector& dataSet_;
};

class TestDataset {
 public:
  virtual ~TestDataset() {
    DeleteObjects(dataobjects_);
  }

  const ObjectVector& GetDataObjects() const { return dataobjects_; }

 protected:
  ObjectVector dataobjects_;
};

}  // namespace similarity

#endif      //  _TEST_DATASET_H_
