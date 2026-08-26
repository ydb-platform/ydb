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
#ifndef READ_DATA_H
#define READ_DATA_H

#include <cmath>
#include <string>
#include <limits>
#include <iomanip>
#include <sstream>
#include <cstdio>
#include <vector>

#include "logging.h"

namespace similarity {

using std::string;
using std::vector;
using std::stringstream;

template <typename dist_t>
struct SparseVectElem {  
  uint32_t  id_;
  dist_t    val_;
  SparseVectElem(uint32_t id = 0, dist_t val = 0) : id_(id), val_(val) {}
  bool operator<(const SparseVectElem<dist_t>& that) const {
    return id_ < that.id_;
  }
  bool operator==(const SparseVectElem<dist_t>& that) const {
    return id_ == that.id_ && val_ == that.val_;
  }
  bool operator!=(const SparseVectElem<dist_t>& that) const {
    return !operator==(that);
  }
  static size_t dataSize() {
    return sizeof(id_) + sizeof(val_);
  }
};

template <typename dist_t>
inline ostream& operator<<(ostream& out, SparseVectElem<dist_t> e) {
  return out << "[" << e.id_ << ": " << e.val_ << "]";
}

inline int strtoi_wrapper(const char* ptr, char** endPtr) {
  errno = 0;
  long val = strtol(ptr, endPtr, 10);
  if (errno == ERANGE){
    return 0;
  }
  if (val < INT_MIN || val > INT_MAX) {
    *endPtr = const_cast<char*>(ptr);
    errno = ERANGE;
    return 0;
  }
  return static_cast<int>(val);
}

template <typename T>
inline bool ReadVecDataViaStream(string line, vector<T>& res) {
  try {
    res.clear();
    ReplaceSomePunct(line); 

    stringstream str(line);
    str.exceptions(ios::badbit);

    T val;
    while (str >> val) {
      res.push_back(val);
    }
  } catch (const exception& e) {
    LOG(LIB_ERROR) << "Exception: " << e.what();
    return false;
  }
  return true;
}

template <typename T>
inline bool ReadVecDataEfficiently(string line, vector<T>& res);

template <>
inline bool ReadVecDataEfficiently<float>(string line, vector<float>& res) {
  ReplaceSomePunct(line); 
  const char *ptr = line.c_str();
  char *endPtr = nullptr;

  res.clear();
  errno = 0;

  for (float val = strtof(ptr, &endPtr);
    ptr != endPtr;
    val = strtof(ptr, &endPtr)) {
    ptr = endPtr;
    if (errno == ERANGE){
      errno = 0;
      return false;
    }
    res.push_back(val);
  }

  if (errno == ERANGE){
    errno = 0;
    return false;
  }

  return true;
}

template <>
inline bool ReadVecDataEfficiently<double>(string line, vector<double>& res) {
  ReplaceSomePunct(line); 
  const char *ptr = line.c_str();
  char *endPtr = nullptr;

  res.clear();
  errno = 0;

  for (double val = strtod(ptr, &endPtr);
    ptr != endPtr;
    val = strtod(ptr, &endPtr)) {
    ptr = endPtr;
    if (errno == ERANGE){
      errno = 0;
      return false;
    }
    res.push_back(val);
  }

  if (errno == ERANGE){
    errno = 0;
    return false;
  }

  return true;
}


template <>
inline bool ReadVecDataEfficiently<int>(string line, vector<int>& res) {
  ReplaceSomePunct(line); 
  const char *ptr = line.c_str();
  char *endPtr = nullptr;

  res.clear();
  errno = 0;

  for (int val = strtoi_wrapper(ptr, &endPtr);
    ptr != endPtr;
    val = strtoi_wrapper(ptr, &endPtr)) {
    ptr = endPtr;
    if (errno == ERANGE){
      errno = 0;
      return false;
    }
    res.push_back(val);
  }

  if (errno == ERANGE){
    errno = 0;
    return false;
  }

  return true;
}


template <typename T>
inline bool ReadSparseVecDataViaStream(string line, vector<SparseVectElem<T>>& res) {
  try {
    ReplaceSomePunct(line); 
    std::stringstream str(line);
    str.exceptions(std::ios::badbit);

    res.clear();

    uint32_t id;
    T        val;

    while (str >> id && str >> val) {
      res.push_back(SparseVectElem<T>(id, val));
    }
  } catch (const exception& e) {
    LOG(LIB_ERROR) << "Exception: " << e.what();
    return false;
  }

  return true;
}

template <typename T>
inline bool ReadSparseVecDataEfficiently(string line, vector<SparseVectElem<T>>& res);

template <>
inline bool ReadSparseVecDataEfficiently<float>(string line, vector<SparseVectElem<float>>& res) {
  ReplaceSomePunct(line); 
  const char *ptr = line.c_str();
  char *endPtr = nullptr;

  float val; IdType id;

  res.clear();
  errno = 0;

  while (true) {
    if (endPtr != nullptr) ptr = endPtr;
    id = strtoi_wrapper(ptr, &endPtr);
    if (errno == ERANGE){
      errno = 0;
      return false;
    }
    if (ptr == endPtr) break;

    ptr = endPtr;
    val = strtof(ptr, &endPtr);
    if (errno == ERANGE) {
      errno = 0;
      return false;
    }
    if (ptr == endPtr) return false;

    res.push_back(SparseVectElem<float>(id, val));
  };

  return true;
}

template <>
inline bool ReadSparseVecDataEfficiently<double>(string line, vector<SparseVectElem<double>>& res) {
  ReplaceSomePunct(line); 
  const char *ptr = line.c_str();
  char *endPtr = nullptr;

  double val; IdType id;

  res.clear();
  errno = 0;

  while (true) {
    if (endPtr != nullptr) ptr = endPtr;
    id = strtoi_wrapper(ptr, &endPtr);
    if (errno == ERANGE){
      errno = 0;
      return false;
    }
    if (ptr == endPtr) break;

    ptr = endPtr;
    val = strtod(ptr, &endPtr);
    if (errno == ERANGE) {
      errno = 0;
      return false;
    }
    if (ptr == endPtr) return false;

    res.push_back(SparseVectElem<double>(id, val));
  };

  return true;
}

}

#endif
