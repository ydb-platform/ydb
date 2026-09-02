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
#include <cmath>
#include <fstream>
#include <sstream>
#include <string>
#include <sstream>
#include <memory>
#include <iomanip>
#include <limits>
#include <cstdio>
#include <cstdint>

#include "object.h"
#include "utils.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"
#include "space/space_vector.h"
#include "read_data.h"

namespace similarity {

using namespace std;

/** Standard functions to read/write/create objects */ 

template <typename dist_t>
unique_ptr<DataFileInputState> VectorSpace<dist_t>::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateVec(inpFileName));
}

template <typename dist_t>
unique_ptr<DataFileOutputState> VectorSpace<dist_t>::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                         const string& outFileName) const {
  return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
}

template <typename dist_t>
unique_ptr<Object> 
VectorSpace<dist_t>::CreateObjFromStr(IdType id, LabelType label, const string& s,
                                            DataFileInputState* pInpStateBase) const {
  DataFileInputStateVec*  pInpState = NULL;
  if (pInpStateBase != NULL) {
    pInpState = dynamic_cast<DataFileInputStateVec*>(pInpStateBase);
    if (NULL == pInpState) {
      PREPARE_RUNTIME_ERR(err) << "Bug: unexpected pointer type";
      THROW_RUNTIME_ERR(err);
    }
  }
  vector<dist_t>  vec;
  ReadVec(s, label, vec);
  if (pInpState != NULL) {
    if (pInpState->dim_ == 0) pInpState->dim_ = vec.size();
    else if (vec.size() != pInpState->dim_) {
      stringstream lineStr;
      if (pInpStateBase != NULL) lineStr <<  " line:" << pInpState->line_num_ << " ";
      PREPARE_RUNTIME_ERR(err) << "The # of vector elements (" << vec.size() << ")" << lineStr.str() << 
                      " doesn't match the # of elements in previous lines. (" << pInpState->dim_ << " )";
      THROW_RUNTIME_ERR(err);
    }
  }
  return unique_ptr<Object>(CreateObjFromVect(id, label, vec));
}

template <typename dist_t>
bool VectorSpace<dist_t>::ApproxEqual(const Object& obj1, const Object& obj2) const {
  const dist_t* p1 = reinterpret_cast<const dist_t*>(obj1.data());
  const dist_t* p2 = reinterpret_cast<const dist_t*>(obj2.data());
  const size_t len1 = GetElemQty(&obj1);
  const size_t len2 = GetElemQty(&obj2);
  if (len1 != len2) {
    PREPARE_RUNTIME_ERR(err) << "Bug: comparing vectors of different lengths: " << len1 << " and " << len2;
    THROW_RUNTIME_ERR(err);
  }
  for (size_t i = 0; i < len1; ++i) 
  // We have to specify the namespace here, otherwise a compiler
  // thinks that it should use the equally named member function
  if (!similarity::ApproxEqual(p1[i], p2[i])) return false;
  return true;
}

template <typename dist_t>
string VectorSpace<dist_t>::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  stringstream out;
  const dist_t* p = reinterpret_cast<const dist_t*>(pObj->data());
  const size_t length = GetElemQty(pObj);
  for (size_t i = 0; i < length; ++i) {
    if (i) out << " ";
    // Clear all previous flags & set to the maximum precision available
    out.unsetf(ios_base::floatfield);
    out << setprecision(numeric_limits<dist_t>::max_digits10) << noshowpoint << p[i];
  }

  return out.str();
}

template <typename dist_t>
bool VectorSpace<dist_t>::ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
  externId.clear();
  DataFileInputStateOneFile* pInpState = dynamic_cast<DataFileInputStateOneFile*>(&inpStateBase);
  CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
  if (!pInpState->inp_file_) return false;
  if (!getline(pInpState->inp_file_, strObj)) return false;
  pInpState->line_num_++;
  return true;
}

/** End of standard functions to read/write/create objects */ 

template <typename dist_t>
void VectorSpace<dist_t>::ReadVec(string line, LabelType& label, vector<dist_t>& v)
{
  v.clear();

  label = Object::extractLabel(line);

#if 0
  if (!ReadVecDataViaStream(line, v)) {
#else
  if (!ReadVecDataEfficiently(line, v)) {
#endif
    PREPARE_RUNTIME_ERR(err) << "Failed to parse the line: '" << line << "'";
    LOG(LIB_ERROR) << err.stream().str();
    THROW_RUNTIME_ERR(err);
  }
}

template <typename dist_t>
Object* VectorSpace<dist_t>::CreateObjFromVect(IdType id, LabelType label, const vector<dist_t>& InpVect) const {
  return new Object(id, label, InpVect.size() * sizeof(dist_t), &InpVect[0]);
};

template class VectorSpace<PivotIdType>;
template class VectorSpace<float>;

}  // namespace similarity
