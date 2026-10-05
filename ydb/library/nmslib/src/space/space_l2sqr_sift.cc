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
#include "space/space_l2sqr_sift.h"
#include "read_data.h"

namespace similarity {

using namespace std;

/** Standard functions to read/write/create objects */ 

unique_ptr<DataFileInputState> SpaceL2SqrSift::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateVec(inpFileName));
}

unique_ptr<DataFileOutputState> SpaceL2SqrSift::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                         const string& outFileName) const {
  return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
}

unique_ptr<Object> 
SpaceL2SqrSift::CreateObjFromStr(IdType id, LabelType label, const string& s,
                                            DataFileInputState* pInpStateBase) const {
  DataFileInputStateVec*  pInpState = NULL;
  if (pInpStateBase != NULL) {
    pInpState = dynamic_cast<DataFileInputStateVec*>(pInpStateBase);
    if (NULL == pInpState) {
      PREPARE_RUNTIME_ERR(err) << "Bug: unexpected pointer type";
      THROW_RUNTIME_ERR(err);
    }
  }
  vector<uint8_t>  vec;
  ReadUint8Vec(s, label, vec);
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
  return unique_ptr<Object>(CreateObjFromUint8Vect(id, label, vec));
}

string SpaceL2SqrSift::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  stringstream out;
  const uint8_t* p = reinterpret_cast<const uint8_t*>(pObj->data());
  for (size_t i = 0; i < SIFT_DIM; ++i) {
    if (i) out << " ";
    // Clear all previous flags & set to the maximum precision available
    out << p[i];
  }

  return out.str();
}

bool SpaceL2SqrSift::ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
  externId.clear();
  DataFileInputStateOneFile* pInpState = dynamic_cast<DataFileInputStateOneFile*>(&inpStateBase);
  CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
  if (!pInpState->inp_file_) return false;
  if (!getline(pInpState->inp_file_, strObj)) return false;
  pInpState->line_num_++;
  return true;
}

/** End of standard functions to read/write/create objects */ 

void SpaceL2SqrSift::ReadUint8Vec(string line, LabelType& label, vector<uint8_t>& v)
{
  v.clear();

  label = Object::extractLabel(line);

  vector<float> vtmp;

  if (!ReadVecDataEfficiently(line, vtmp)) {
    PREPARE_RUNTIME_ERR(err) << "Failed to parse the line: '" << line << "'";
    LOG(LIB_ERROR) << err.stream().str();
    THROW_RUNTIME_ERR(err);
  }
  if (vtmp.size() != SIFT_DIM) {
    PREPARE_RUNTIME_ERR(err) << "Wrong number of vector elements " 
                             << "(expected " << SIFT_DIM << " but got " << vtmp.size() << ")"
                             << " in line: '" << line << "'";
    LOG(LIB_ERROR) << err.stream().str();
    THROW_RUNTIME_ERR(err);
  }
  v.resize(SIFT_DIM);
  for (unsigned i = 0; i < SIFT_DIM; ++i) {
    float fval = vtmp[i];
    if (fval < 0 || fval > numeric_limits<uint8_t>::max()) {
      PREPARE_RUNTIME_ERR(err) << "Out-of range integer values (for SIFT) in the line: '" << line << "'";
      LOG(LIB_ERROR) << err.stream().str();
      THROW_RUNTIME_ERR(err);
    }
    v[i] = static_cast<uint8_t>(fval);
    if (fabs(v[i] - fval) > numeric_limits<float>::min()) {
      PREPARE_RUNTIME_ERR(err) << "Non-integer values (for SIFT) in the line: '" << line << "'";
      LOG(LIB_ERROR) << err.stream().str();
      THROW_RUNTIME_ERR(err);
    }
  }
}

Object* 
SpaceL2SqrSift::CreateObjFromUint8Vect(IdType id, LabelType label, const vector<uint8_t>& InpVect) const {
  CHECK_MSG(InpVect.size() == SIFT_DIM, 
           "Bug or internal error, SIFT vectors dim " + ConvertToString(InpVect.size()) + 
           " isn't == " + ConvertToString(SIFT_DIM));
  DistTypeSIFT sum = 0;
  // We precompute and memorize the sum
  for (DistTypeSIFT e : InpVect) 
    sum += e * e;
  unique_ptr<Object> res(new Object(id, label, SIFT_DIM + sizeof(DistTypeSIFT), 
                                               &InpVect[0]));

  *reinterpret_cast<DistTypeSIFT*>(res->data() + SIFT_DIM) = sum;
  return res.release();
}

void 
SpaceL2SqrSift::CreateDenseVectFromObj(const Object* obj, DistTypeSIFT* pVect, size_t nElem) const {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(obj->data());
  for (unsigned i = 0; i < min<size_t>(nElem, SIFT_DIM); ++i) {
    pVect[i] = p[i];
  }
}

// This approximate comparison is actually an exact one
bool SpaceL2SqrSift::ApproxEqual(const Object& obj1, const Object& obj2) const {
  return HiddenDistance(&obj1, &obj2) == 0;
}

}  // namespace similarity
