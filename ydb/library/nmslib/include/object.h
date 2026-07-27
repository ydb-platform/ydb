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
#ifndef _OBJECT_H_
#define _OBJECT_H_

#include <cstring>
#include <cctype>
#include <string>
#include <sstream>
#include <vector>
#include <list>
#include <utility>
#include <limits>
#include <algorithm>
#include <cstdint>

#include "global.h"
#include "utils.h"
#include "idtype.h"
#include "logging.h"

namespace similarity {

using std::string;
using std::stringstream;
using std::numeric_limits;

/* 
 * Structure of object: | 4-byte id | 4-byte label | 8-byte datasize | data ........ |
 * We need data to be aligned on 8-byte boundaries.
 * 
 * TODO 1) this all apparenlty hinges on the assumption that malloc() gives addresses
 *      that are 8-bye aligned. So, this is related to issue #9
 *      2) even though GCC doesn't complain, using a single char buffer may break aliasing rules
 *
 * See also http://searchivarius.org/blog/what-you-must-know-about-alignment-21st-century
 */

class Object {
 public:
  /*
   * Memory ownership handling of the Object class is not good, but we keep it this way for historical/compatibility reasons
   * Currrently, when the second constructor is called, new memory is always allocated.
   * However, this constructor is nornally called to created an object that reuses someone else's memory.
   * In NMSLIB 1.8.1 we make it possible to take ownership of the memory provided.
   */
  explicit Object(char* buffer, bool memory_allocated=false) : buffer_(buffer), memory_allocated_(memory_allocated) {}

  Object(IdType id, LabelType label, size_t datalength, const void* data) {
    buffer_ = new char[ID_SIZE + LABEL_SIZE + DATALENGTH_SIZE + datalength];
    CHECK(buffer_ != NULL);
    memory_allocated_ = true;
    char* ptr = buffer_;
    memcpy(ptr, &id, ID_SIZE);
    ptr += ID_SIZE;
    memcpy(ptr, &label, LABEL_SIZE); 
    ptr += LABEL_SIZE;
    memcpy(ptr, &datalength, DATALENGTH_SIZE);
    ptr += DATALENGTH_SIZE;
    if (data != NULL) {
      memcpy(ptr, data, datalength);
    } else {
      memset(ptr, 0, datalength);
    }
  }

  ~Object() {
    if (memory_allocated_) {
      delete[] buffer_;
    }
  }

  static Object* CreateNewEmptyObject(size_t datalength) {
    // the caller is responsible for releasing the pointer
    Object* empty_object = new Object(-1, -1, datalength, NULL);
    CHECK(empty_object != NULL);
    return empty_object;
  }

  Object* Clone() const {
    Object* clone = new Object(id(), label(), datalength(), data());
    return clone;
  }

  inline IdType    id()         const { return *(reinterpret_cast<IdType*>(buffer_)); }
  inline LabelType label()      const { return *(reinterpret_cast<LabelType*>(buffer_ + ID_SIZE)); }
  inline size_t datalength()    const { return *(reinterpret_cast<size_t*>(buffer_ + LABEL_SIZE + ID_SIZE));}
  inline const char* data() const { return buffer_ + ID_SIZE + LABEL_SIZE+ DATALENGTH_SIZE; }
  inline char* data()             { return buffer_ + ID_SIZE + LABEL_SIZE+ DATALENGTH_SIZE; }

  inline const char* buffer()  const { return buffer_; }
  inline size_t bufferlength() const { return ID_SIZE + LABEL_SIZE+ DATALENGTH_SIZE + datalength(); }

  void Print() const {
    LOG(LIB_INFO) << "id = " << id()
        << "\tlabel = " << label()
        << "\tdatalength = " << datalength()
        << "\tbuffer = " << buffer()
        << "\tdata = " << data();
  }

  /*
   * Extracts label from the beginning of the input string and modifies the string by removing label information.
   * If no label is present, the input string remains unchanged.
   */
  static LabelType extractLabel(string& fileLine) {
    static string labelPrefix = LABEL_PREFIX; // thread-safe in C++11

    LabelType res = EMPTY_LABEL;
    if (fileLine.size() > labelPrefix.size() + 1 &&
        fileLine.substr(0, labelPrefix.size()) == labelPrefix) {
      int p = -1;

      for (size_t i = labelPrefix.size(); i < fileLine.size(); ++i) {
        if (isspace(fileLine[i])) {
          p = i; 
          break;
        }
      }
      if (p >= 0) {
        size_t j = p;
        // j is the first non-white space char
        while(j < fileLine.size() && isspace(fileLine[j])) ++j;

        stringstream numstr(fileLine.substr(labelPrefix.size(), p - labelPrefix.size()));

        if (!(numstr >> res) || !numstr.eof()) {
          PREPARE_RUNTIME_ERR(err) << "Cannot extract label from the file line: '" << fileLine << "'";
          THROW_RUNTIME_ERR(err);
        }

        fileLine = fileLine.substr(j);
       
      } else {
        PREPARE_RUNTIME_ERR(err) << "No space is found after the label definition in the file line: '" << fileLine << "'";
        THROW_RUNTIME_ERR(err);
      }
    }
    return res;
  }
  /*
   * Adds a label to the beginning of the string.
   */
  static void addLabel(string& fileLine, LabelType label) {
    stringstream str;
    str << LABEL_PREFIX << label << " ";
    fileLine.insert(0, str.str()); 
  }
 private:
  char* buffer_;
  bool  memory_allocated_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(Object);
};


/* 
 * Clearing memory: we will use some smart pointer here (somewhen).
 *                  can't use standard shared_ptr, b/c they have
 *                  performance issues.
 * see, e.g.: http://nerds-central.blogspot.com/2012/03/sharedptr-performance-issues-and.html 
 */
typedef std::vector<const Object*> ObjectVector;

inline size_t DataSpaceUsed(const ObjectVector &vect) {
  size_t res = 0;
  for (const auto elem: vect) res += elem->datalength();
  return res;
}

inline size_t TotalSpaceUsed(const ObjectVector &vect) {
  size_t res = 0;
  for (const auto elem: vect) res += elem->bufferlength();
  return res;
}

/* 
 * The caller is repsonsible for deleting:
 *  1) bucket
 *  2) Object pointers stored in the bucket
 */
inline void CreateCacheOptimizedBucket(const ObjectVector& data, 
                                       char*& CacheOptimizedBucket, 
                                       ObjectVector*& bucket) {
  if (data.empty()) {
    /* TODO @leo Normally this wouldn't happen
     *           However, some methods, e.g., list of clusters
     *           with KLDiv may produce empty clusters.
     */
    LOG(LIB_WARNING) << "Empty bucket!"; 
  }
  CacheOptimizedBucket = new char [TotalSpaceUsed(data)];
  char *p = CacheOptimizedBucket;
  bucket = new ObjectVector(data.size());

  for(size_t i = 0; i < data.size(); ++i) {
    memcpy(p, data[i]->buffer(), data[i]->bufferlength());
    (*bucket)[i] = new Object(const_cast<char*>(p));
    p += data[i]->bufferlength();
  }
}

inline void ClearBucket(char* CacheOptimizedBucket,
                        ObjectVector* bucket) {
  if (CacheOptimizedBucket) {
    for(auto i:(*bucket)) {
      delete i;
    }
  }
  delete [] CacheOptimizedBucket;
  delete bucket;
}

typedef std::list<const Object*> ObjectList;

template<typename dist_t>
using DistObjectPair = std::pair<dist_t, const Object*>;

template <typename dist_t>
using DistObjectPairVector = std::vector<DistObjectPair<dist_t>>;


template <typename dist_t>
struct DistObjectPairAscComparator {
  bool operator()(const DistObjectPair<dist_t>& x,
                  const DistObjectPair<dist_t>& y) const {
    return x.first < y.first;
  }
};

template <typename dist_t>
struct DistObjectPairDescComparator {
  bool operator()(const DistObjectPair<dist_t>& x,
                  const DistObjectPair<dist_t>& y) const {
    return x.first > y.first;
  }
};

struct ObjectIdAscComparator {
  inline bool operator()(const Object* x, const Object* y) const {
    return x->id() < y->id();
  }
};

/*
 * We do not support very large data sets.
 */
inline void CheckDataSize(const ObjectVector &data) {
  if (data.size() > MAX_DATASET_QTY) {
    PREPARE_RUNTIME_ERR(err) << "Bug: the number of data elements (" << data.size() << ") is too big, "
                             << "bigger than " << MAX_DATASET_QTY;
  }
}

/*
 * Creates a recoding array to efficiently map object IDs to their
 * positions in the data vector. The array-based mapping is
 * quite space-efficient, because the largest object ID is rouhgly
 * equal to the number of data vector elements. The array based mapping
 * also permits extremely fast lookups.
 */
inline void CreateObjIdToPosMapper(const ObjectVector& data, std::vector<IdType>& mapper) {
  CheckDataSize(data);
  IdType maxId = -1;
  for (const Object* pObj : data)  {
    CHECK_MSG(pObj->id() >= 0, "Bug: encountered negative object ID");
    maxId = std::max(maxId, pObj->id());
  }
  mapper.resize(maxId + 1);
  std::fill(mapper.begin(), mapper.end(), -1);
  for (IdTypeUnsign i = 0; i < data.size(); ++i) {
    CHECK(data[i]->id() >= 0);
    CHECK(data[i]->id() < mapper.size());
    mapper[data[i]->id()] = i;
  }
}

inline IdType ConvertId(IdType srcId,  const std::vector<IdType>& mapper) {
  CHECK_MSG(mapper.size() > 0, "Only non-empty datasets are supported!");
  CHECK_MSG(srcId >= 0, "Invalid negative source ID");
  CHECK_MSG(srcId < mapper.size(), "Invalid source ID: " + ConvertToString(srcId) + " max allowed: " + ConvertToString(mapper.size() - 1));
  return mapper[srcId];
}



}   // namespace similarity

#endif    // _OBJECT_H_

