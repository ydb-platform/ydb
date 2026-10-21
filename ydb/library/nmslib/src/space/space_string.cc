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
#include <string>
#include <sstream>

#include "object.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"
#include "space/space_string.h"

namespace similarity {

using namespace std;

template <typename dist_t>
void StringSpace<dist_t>::ReadStr(string line, LabelType& label, string& str, size_t* pLineNum) const
{
  label = Object::extractLabel(line);

  if (EMPTY_LABEL == label) {
    stringstream lineStr;
    if (pLineNum != NULL) lineStr << " in line " << *pLineNum;
    throw runtime_error("Missing label" + lineStr.str());
  }

  str = line;
}

/** Standard functions to read/write/create objects */ 

template <typename dist_t>
unique_ptr<DataFileInputState> StringSpace<dist_t>::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateVec(inpFileName));
}

template <typename dist_t>
unique_ptr<DataFileOutputState> StringSpace<dist_t>::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                         const string& outFileName) const {
  return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
}

template <typename dist_t>
bool StringSpace<dist_t>::ApproxEqual(const Object& obj1, const Object& obj2) const {
  return CreateStrFromObj(&obj1, "") == CreateStrFromObj(&obj2, "");
}

template <typename dist_t>
string StringSpace<dist_t>::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  const char* p = reinterpret_cast<const char*>(pObj->data());
  // TODO double-check that sizeof(char) should always be 1 as guaranteed by the C++ standard
  // then sizeof can be removed
  const size_t length = pObj->datalength() / sizeof(char);
  return string(p, length);
}

template <typename dist_t>
bool StringSpace<dist_t>::ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
  externId.clear();
  DataFileInputStateOneFile* pInpState = dynamic_cast<DataFileInputStateOneFile*>(&inpStateBase);
  CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
  if (!pInpState->inp_file_) return false;
  string line;
  if (!getline(pInpState->inp_file_, line)) return false;
  pInpState->line_num_++;
  ReadStr(line, label, strObj, &pInpState->line_num_);
  return true;
}

template <typename dist_t>
void StringSpace<dist_t>::WriteNextObj(const Object& obj, const string& externId, DataFileOutputState &outState) const {
  string s = CreateStrFromObj(&obj, externId);
  outState.out_file_ << LABEL_PREFIX << obj.label() << " " << s << endl;
}
/** End of standard functions to read/write/create objects */ 


template class StringSpace<int>;
// TODO: do we really need floating-point distances for string spaces? 
template class StringSpace<float>;

}  // namespace similarity
