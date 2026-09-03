/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/searchivarius/NonMetricSpaceLib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */

#ifndef NONMETRICSPACELIB_SPACE_SPARSE_BIN_COMMON_H
#define NONMETRICSPACELIB_SPACE_SPARSE_BIN_COMMON_H

#include <space/space_sparse_vector_inter.h>

namespace similarity {

struct DataFileInputStateBinSparseVec : public DataFileInputStateOneFile {

  DataFileInputStateBinSparseVec(const string &inpFileName) : DataFileInputStateOneFile(inpFileName) {
    readBinaryPOD(inp_file_, qty_);
    LOG(LIB_INFO) << "Preparing to read sparse vectors from the binary file: " << inpFileName
                  << " header claims to have: " << qty_ << " vectors";
  }

  uint32_t qty_;
  uint32_t readQty_ = 0;
};

struct CompDesc {
  CompDesc(bool isSparse, size_t dim,
           float indexWeight, float queryWeight) :
    isSparse_(isSparse), dim_(dim) {
    indexWeight_ = indexWeight;
    queryWeight_ = queryWeight;
  }

  bool isSparse_;
  size_t dim_;
  float indexWeight_;
  float queryWeight_;
};


struct DataFileInputStateSparseDenseFusion : public DataFileInputState {
  virtual void Close() { inp_file_.close(); }

  DataFileInputStateSparseDenseFusion(const string& inpFileName) :
    inp_file_(inpFileName.c_str(), ios::binary), readQty_(0) {

    if (!inp_file_) {
      PREPARE_RUNTIME_ERR(err) << "Cannot open file: " << inpFileName << " for reading";
      THROW_RUNTIME_ERR(err);
    }

    inp_file_.exceptions(ios::badbit);
  }

  virtual ~DataFileInputStateSparseDenseFusion(){};

  ifstream          inp_file_;
  size_t            qty_ = 0; // Total number of entries
  size_t            readQty_ = 0; // Number of read entries
  vector<CompDesc>  vCompDesc_;

  friend class SpaceSparseDenseFusion;
};


template<class InputState>
inline void readBinaryStringId(InputState& state, string& externId) {
  string s;
  uint32_t idSize;

  readBinaryPOD(state.inp_file_, idSize);

  vector<char> data(idSize);

  state.inp_file_.read(&data[0], idSize);
  externId.assign(&data[0], data.size());
}

template<class InputState>
inline void readNextBinSparseVect(InputState& state, string& strObj)  {

  uint32_t qty;
  readBinaryPOD(state.inp_file_, qty);
  size_t elemSize = SparseVectElem<float>::dataSize();
  // Data has extra space for the number of sparse vector elements
  vector<char> data(elemSize * qty  + 4);
  char *p = &data[0];

  writeBinaryPOD(p, qty);

  state.inp_file_.read(p + 4, qty * elemSize);

  strObj.assign(p, data.size());
}


inline void readNextBinDenseVect(DataFileInputStateSparseDenseFusion &state, string& strObj, unsigned dim)  {
  uint32_t qty;
  readBinaryPOD(state.inp_file_, qty);
  if (qty != dim) {
    PREPARE_RUNTIME_ERR(err) << "Mismatch between dimension in the header (" << dim <<
                             ") and the actual dimensionality of the current entry (" << qty << ")";
    THROW_RUNTIME_ERR(err);

  }
  size_t vec_size = dim * sizeof(float);
  vector<char> data(4 + vec_size);
  char * const pStart = &data[0];
  writeBinaryPOD(pStart, qty);

  state.inp_file_.read(pStart + 4, vec_size);

  strObj.assign(pStart, data.size());
}

/*
 * Extract/parses a binary dense vector stored in the string
 * strObj starting from the position start.
 *
 * @param strObj a string that stores a vector + possibly something else
 * @param vDense a vector to store the result
 * @param start where to start extraction
 * @param dim dimensionality of the vector.
 */
inline void parseDenseBinVect(const string& strObj, vector<float>& vDense, unsigned& start, unsigned dim)  {
  const char* p = strObj.data() + start;
  size_t expectSize = 4 + dim * 4;

  CHECK_MSG(strObj.size() >= expectSize + start,
            string("The received string object is too small! ") +
            " Start: " + ConvertToString(start) +
            " Str obj size: " + ConvertToString(strObj.size()) +
            " # dim: " + ConvertToString(dim) +
            " expected size: " + ConvertToString(expectSize));

  uint32_t qty;

  readBinaryPOD(p, qty);
  p += 4;

  CHECK_MSG(qty == dim, "Unexpected # of vector elements: " + ConvertToString(qty) + " expected: " + ConvertToString(dim));

  vDense.resize(dim);

  for (uint_fast32_t i = 0; i < dim; ++i) {
    readBinaryPOD(p, vDense[i]);
    p += 4;
  }

  start += expectSize;

}

/**
 * Extract/parses a binary sparse vector stored in the string
 * strObj starting from the position start.
 *
 * @param strObj a string that stores a vector + possibly something else
 * @param v an output vector id-value pairs
 * @param start where to start extraction
 * @param sortDimId true if we want to re-sort results by the ids
 */
inline void parseSparseBinVect(const string &strObj,
                        vector<SparseVectElem<float>> &v,
                        unsigned &start,
                        bool sortDimId = true)  {
  uint32_t qty;

  CHECK(strObj.size() >= sizeof(qty));

  const char* p = strObj.data() + start;

  readBinaryPOD(p, qty);
  v.resize(qty);
  size_t elemSize = SparseVectElem<float>::dataSize();
  size_t expectSize = sizeof(qty) + elemSize * qty;
  CHECK_MSG(strObj.size() >= expectSize + start,
            string("The received string object is too little! ") +
            " Start: " + ConvertToString(start) +
            " Str obj size: " + ConvertToString(strObj.size()) +
            " # of vect elems: " + ConvertToString(qty) +
            " expected size: " + ConvertToString(expectSize));

  start += expectSize;
  p += sizeof(qty);
  for (uint_fast32_t i = 0; i < qty; ++i) {
    readBinaryPOD(p, v[i].id_);
    p += sizeof(v[i].id_);

    readBinaryPOD(p, v[i].val_);
    p += sizeof(v[i].val_);
  }
  if (sortDimId) {
    sort(v.begin(), v.end());
  }
  for (uint_fast32_t i = 1; i < qty; ++i) {
    CHECK_MSG(v[i].id_ > v[i-1].id_, "Ids in the input file are either unsorted or have duplicates!")
  }
}

inline size_t getPad4(size_t len)  {
  size_t rem = len & 3;
  if (rem) {
    return 4 - rem;
  }
  return 0;
}


}

#endif //NONMETRICSPACELIB_SPACE_SPARSE_BIN_COMMON_H
