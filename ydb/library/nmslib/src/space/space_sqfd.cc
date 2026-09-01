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
#if !defined(_MSC_VER)

#include <cmath>
#include <fstream>
#include <string>
#include <sstream>
#include <iomanip>
#include <limits>
#include <algorithm>
#include <Eigen/Dense>

#include "object.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"
#include "space/space_sqfd.h"

namespace similarity {

using Eigen::MatrixXd;
using Eigen::VectorXd;

using namespace std;

template <typename dist_t>
SpaceSqfd<dist_t>::SpaceSqfd(SqfdFunction<dist_t>* func)
    : func_(func) {
}

template <typename dist_t>
SpaceSqfd<dist_t>::~SpaceSqfd() {
  delete func_;
}

struct DataFileInputStateSQFD : public DataFileInputStateOneFile {
  DataFileInputStateSQFD(const string& inpFileName) : 
                              DataFileInputStateOneFile(inpFileName), 
                              num_clusters_(0), 
                              feature_dimension_(0),
                              num_rand_pixels_(0) { 
    string line;
    if (!getline(inp_file_, line))
      throw runtime_error("Expecting a non-empty first line in '" + inpFileName + "'");
    line_num_++;

    stringstream ss(line);
    ss.exceptions(std::ios::badbit);
    ss >> num_clusters_ >> feature_dimension_ >> num_rand_pixels_;

    if (!getline(inp_file_, line) || !line.empty())
      throw runtime_error("Expecting an empty second line in '" + inpFileName + "'");

    line_num_++;
  }
  
  uint32_t        num_clusters_;
  uint32_t        feature_dimension_;
  uint32_t        num_rand_pixels_;
};

/** Standard functions to read/write/create objects */ 

template <typename dist_t>
unique_ptr<DataFileInputState> SpaceSqfd<dist_t>::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateSQFD(inpFileName));
}

template <typename dist_t>
unique_ptr<DataFileOutputState> SpaceSqfd<dist_t>::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                       const string& outFileName) const {
  unique_ptr<DataFileOutputState> outState(new DataFileOutputState(outFileName));
  uint32_t num_clusters = 0, feature_dimension = 0, num_rand_pixels = 0;
  if (!dataset.empty()) {
    const uint32_t* h = reinterpret_cast<const uint32_t*>(dataset[0]->data());
    num_clusters = h[0]; 
    feature_dimension = h[1];
  }
  outState->out_file_ << num_clusters << " " << feature_dimension << " " << num_rand_pixels << endl << endl; 
  return outState;
}

template <typename dist_t>
bool SpaceSqfd<dist_t>::ApproxEqual(const Object& obj1, const Object& obj2) const {
  if (obj1.datalength() < 8) {
    PREPARE_RUNTIME_ERR(err) << "Bug: object size " << obj1.datalength() << " is smaller than 8 bytes!";
    THROW_RUNTIME_ERR(err);
  }
  if (obj2.datalength() < 8) {
    PREPARE_RUNTIME_ERR(err) << "Bug: object size " << obj2.datalength() << " is smaller than 8 bytes!";
    THROW_RUNTIME_ERR(err);
  }
  const uint32_t* h1 = reinterpret_cast<const uint32_t*>(obj1.data());
  const uint32_t* h2 = reinterpret_cast<const uint32_t*>(obj2.data());
  const uint32_t num_clusters1 = h1[0], feature_dimension1 = h1[1];
  const uint32_t num_clusters2 = h2[0], feature_dimension2 = h2[1];
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1.data() + 2*sizeof(uint32_t));
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2.data() + 2*sizeof(uint32_t));
  if (feature_dimension1 != feature_dimension2) {
    PREPARE_RUNTIME_ERR(err) << "Bug: different feature dimensions: " 
               << feature_dimension1 << " vs " << feature_dimension2;
    THROW_RUNTIME_ERR(err);
  }
  if (num_clusters1 != num_clusters2) return false;

  for (size_t i = 0; i < num_clusters1; ++i) {
    const dist_t* p1 = x + i * (feature_dimension1 + 1);
    const dist_t* p2 = y + i * (feature_dimension1 + 1);
    for (size_t k = 0; k < feature_dimension1; ++k) {
      if (!similarity::ApproxEqual(p1[k], p2[k])) return false;
    }
  }

  return true;
}

template <typename dist_t>
string SpaceSqfd<dist_t>::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  if (pObj->datalength() < 8) {
    PREPARE_RUNTIME_ERR(err) << "Bug: object size " << pObj->datalength() << " is smaller than 8 bytes!";
    THROW_RUNTIME_ERR(err);
  }
  const uint32_t* h = reinterpret_cast<const uint32_t*>(pObj->data());
  const uint32_t num_clusters = h[0], feature_dimension = h[1];
  const dist_t*   pElems = reinterpret_cast<const dist_t*>(pObj->data() + 2*sizeof(uint32_t));
  stringstream out;
  size_t pos = 0;
  out << FAKE_FILE_NAME << endl;
  for (uint32_t i = 0; i < num_clusters; ++i) {
    for (uint32_t j = 0; j < feature_dimension + 1; ++j) {
      if (j) out << " ";
      // Clear all previous flags & set to the maximum precision available
      out.unsetf(ios_base::floatfield);
      out << setprecision(numeric_limits<dist_t>::max_digits10) << pElems[pos++];
    }
    out << endl;
  }
  return out.str();
}

template <typename dist_t>
unique_ptr<Object> SpaceSqfd<dist_t>::CreateObjFromStr(IdType id, LabelType label, const string& s,
                                                       DataFileInputState* pInpStateBase) const {
  DataFileInputStateSQFD*  pInpState = NULL;
  if (pInpStateBase != NULL) {
    pInpState = dynamic_cast<DataFileInputStateSQFD*>(pInpStateBase);
    if (NULL == pInpState) {
      PREPARE_RUNTIME_ERR(err) << "Bug: unexpected pointer type";
      THROW_RUNTIME_ERR(err);
    }
  }
  stringstream stream1(s);
  string line;

  int prevQty = -1;

  vector<float> obj;
  if (pInpState != NULL) 
    obj.reserve(pInpState->num_clusters_ * pInpState->feature_dimension_);

  size_t currLine = 0;

  if (pInpState != NULL) {
    currLine = pInpState->line_num_;
    if (!currLine) {
      throw  runtime_error("Bug: got a zero line number while expecting a positive one!");
    }
    currLine--; // One is for the empty line that is not present in the input string
    // line_num_ points to the last read line, we need to identify the number of the first line
    for (char c: s) 
    if (c =='\n') {
      if (!currLine) {
        throw  runtime_error("Bug: got a zero line number while expecting a positive one!");
      }
      --currLine;
    }
  }

  if (!getline(stream1, line)) {
    PREPARE_RUNTIME_ERR(err) << "Expecting a non-empty line # " << currLine;
    THROW_RUNTIME_ERR(err);
  }
  ++currLine;

  while (getline(stream1, line)) {
    ++currLine;

    if (line.length() == 0) break; // Empty line
    stringstream stream2(line);
    stream2.exceptions(ios::badbit);

    int qty = 0;
    dist_t v;

    while (stream2 >> v) {
      obj.push_back(v);
      qty++;
    }

    if (prevQty == -1) prevQty = qty;
    else if (qty != prevQty) {
      PREPARE_RUNTIME_ERR(err) << "The number of elements " << qty << " in line " << currLine << 
                                  " doesn't match the number of elements " << prevQty << " in previous lines " << 
                                  " offending line: '" << line << "'" << endl <<
                                  " offending block:" << endl << s;
      THROW_RUNTIME_ERR(err);
    }
    // +1 is because one element is the feature weight
    if (pInpState != NULL && qty != pInpState->feature_dimension_ + 1) {
      PREPARE_RUNTIME_ERR(err) << "The number of elements in line " << currLine << 
                         " doesn't match the number of elements in the file header'" <<
                         " expected: " << (pInpState->feature_dimension_ + 1) << " but got: " << qty;
      THROW_RUNTIME_ERR(err);
    }
  }

  const uint32_t feature_weight = prevQty;
  const uint32_t num_clusters = obj.size() / prevQty;
  const int object_size =
        2 * sizeof(uint32_t) + // num_clusters & feature_dimension
        num_clusters * feature_weight * sizeof(dist_t);

  vector<char> buf(object_size);
  uint32_t* h = reinterpret_cast<uint32_t*>(&buf[0]);
  h[0] = num_clusters;
  h[1] = feature_weight - 1;

  dist_t* pVect = reinterpret_cast<dist_t*>(h+2);
  copy(obj.begin(), obj.end(), pVect);

  return unique_ptr<Object>(new Object(id, label, object_size, &buf[0]));
}

template <typename dist_t>
bool SpaceSqfd<dist_t>::ReadNextObjStr(DataFileInputState &inpState, string& strObj, LabelType& label, string& externId) const {
  externId.clear();
  DataFileInputStateSQFD*  pInpState = NULL;
  pInpState = dynamic_cast<DataFileInputStateSQFD*>(&inpState);
  if (NULL == pInpState) {
    PREPARE_RUNTIME_ERR(err) << "Bug: unexpected reference type";
    THROW_RUNTIME_ERR(err);
  }

  stringstream stream1;
  string line;

  size_t currLine = pInpState->line_num_;

  if (!getline(pInpState->inp_file_, line)) {
    return false;
  }
  ++currLine;

  stream1 << line << endl;

  while (getline(pInpState->inp_file_, line)) {
    ++currLine;

    if (line.length() == 0) break; // Empty line, don't append it to the object representation!
    stream1 << line << endl;
  }
  pInpState->line_num_ = currLine;
  strObj = stream1.str();
  return true;
}

template <typename dist_t>
dist_t SpaceSqfd<dist_t>::HiddenDistance(
    const Object* obj1, const Object* obj2) const {
  if (obj1->datalength() < 8) {
    PREPARE_RUNTIME_ERR(err) << "Bug: object size " << obj1->datalength() << " is smaller than 8 bytes!";
    THROW_RUNTIME_ERR(err);
  }
  if (obj2->datalength() < 8) {
    PREPARE_RUNTIME_ERR(err) << "Bug: object size " << obj2->datalength() << " is smaller than 8 bytes!";
    THROW_RUNTIME_ERR(err);
  }
  const uint32_t* h1 = reinterpret_cast<const uint32_t*>(obj1->data());
  const uint32_t* h2 = reinterpret_cast<const uint32_t*>(obj2->data());
  const uint32_t num_clusters1 = h1[0], feature_dimension1 = h1[1];
  const uint32_t num_clusters2 = h2[0], feature_dimension2 = h2[1];
  const dist_t* x = reinterpret_cast<const dist_t*>(
      obj1->data() + 2*sizeof(uint32_t));
  const dist_t* y = reinterpret_cast<const dist_t*>(
      obj2->data() + 2*sizeof(uint32_t));
  if (feature_dimension1 != feature_dimension2) {
    PREPARE_RUNTIME_ERR(err) << "Bug: different feature dimensions: " 
               << feature_dimension1 << " vs " << feature_dimension2;
    THROW_RUNTIME_ERR(err);
  }
  const uint32_t sz = num_clusters1 + num_clusters2;
  VectorXd W(sz);
  size_t pos = feature_dimension1;
  for (uint32_t i = 0; i < num_clusters1; ++i) {
    W(i) = x[pos];
    pos += feature_dimension1 + 1;
  }
  pos = feature_dimension2;
  for (uint32_t i = 0; i < num_clusters2; ++i) {
    W(num_clusters1 + i) = -y[pos];
    pos += feature_dimension2 + 1;
  }
  MatrixXd A(sz,sz);
  for (uint32_t i = 0; i < sz; ++i) {
    for (uint32_t j = i; j < sz; ++j) {
      const dist_t* p1 = i < num_clusters1
          ? x + i * (feature_dimension1 + 1)
          : y + (i - num_clusters1) * (feature_dimension1 + 1);
      const dist_t* p2 = j < num_clusters1
          ? x + j * (feature_dimension1 + 1)
          : y + (j - num_clusters1) * (feature_dimension1 + 1);
      A(i,j) = A(j,i) = func_->f(p1, p2, feature_dimension1);
    }
  }
  auto res = W.transpose() * A * W;
  return sqrt(res(0,0));
}

template <typename dist_t>
string SpaceSqfd<dist_t>::StrDesc() const {
  stringstream stream;
  stream << "SpaceSqfd: " << func_->StrDesc();
  return stream.str();
}

template class SpaceSqfd<float>;
}  // namespace similarity

#endif
