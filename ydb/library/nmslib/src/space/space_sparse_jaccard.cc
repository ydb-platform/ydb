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

#include "object.h"
#include "utils.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"
#include "read_data.h"
#include "space/space_sparse_jaccard.h"

namespace similarity {

using namespace std;

static void ReadIdList(string line, LabelType& label, vector<IdType>& v)
{
  v.clear();

  label = Object::extractLabel(line);

#if 0
  ReplaceSomePunct(line); 
  stringstream str(line);

  str.exceptions(ios::badbit);

  IdType id;


  try {
    while (str >> id) {
      v.push_back(id);
    }
  } catch (const exception &e) {
    LOG(LIB_ERROR) << "Exception: " << e.what();
    LOG(LIB_FATAL) << "Failed to parse the line: '" << line << "'";
  }
#else
  if (!ReadVecDataEfficiently(line, v)) {
    PREPARE_RUNTIME_ERR(err) << "Failed to parse the line: '" << line << "'";
    LOG(LIB_ERROR) << err.stream().str();
    THROW_RUNTIME_ERR(err);
  }
#endif

  sort(v.begin(), v.end());
}

/** Standard functions to read/write/create objects */ 

template <typename dist_t>
unique_ptr<DataFileInputState> SpaceSparseJaccard<dist_t>::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateVec(inpFileName));
}

template <typename dist_t>
unique_ptr<DataFileOutputState> SpaceSparseJaccard<dist_t>::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                         const string& outFileName) const {
  return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
}

template <typename dist_t>
unique_ptr<Object> 
SpaceSparseJaccard<dist_t>::CreateObjFromStr(IdType id, LabelType label, const string& s,
                                            DataFileInputState* pInpStateBase) const {
  DataFileInputStateVec*  pInpState = NULL;
  if (pInpStateBase != NULL) {
    pInpState = dynamic_cast<DataFileInputStateVec*>(pInpStateBase);
    if (NULL == pInpState) {
      PREPARE_RUNTIME_ERR(err) << "Bug: unexpected pointer type";
      THROW_RUNTIME_ERR(err);
    }
  }
  vector<IdType>  ids;
  ReadIdList(s, label, ids);
  return unique_ptr<Object>(CreateObjFromIds(id, label, ids));
}

template <typename dist_t>
bool SpaceSparseJaccard<dist_t>::ApproxEqual(const Object& obj1, const Object& obj2) const {
  const IdType* p1 = reinterpret_cast<const IdType*>(obj1.data());
  const IdType* p2 = reinterpret_cast<const IdType*>(obj2.data());
  const size_t len1 = GetElemQty(&obj1);
  const size_t len2 = GetElemQty(&obj2);
  if (len1 != len2) {
    return false;
  }
  for (size_t i = 0; i < len1; ++i) {
    if (p1[i] != p2[i]) return false;
  }
  return true;
}

template <typename dist_t>
string SpaceSparseJaccard<dist_t>::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  stringstream out;
  const IdType* p = reinterpret_cast<const IdType*>(pObj->data());
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
bool SpaceSparseJaccard<dist_t>::ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
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
Object* SpaceSparseJaccard<dist_t>::CreateObjFromIds(IdType id, LabelType label, const vector<IdType>& InpVect) const {
  return new Object(id, label, InpVect.size() * sizeof(IdType), &InpVect[0]);
};

template class SpaceSparseJaccard<float>;

}  // namespace similarity
