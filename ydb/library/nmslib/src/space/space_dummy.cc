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

#include "space/space_dummy.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
dist_t SpaceDummy<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  LOG(LIB_INFO) << "Calculating the distance between objects: " << obj1->id() << " and " << obj2->id();
  CHECK(obj1->datalength() > 0);
  CHECK(obj1->datalength() == obj2->datalength());
  /* 
   * Data in the object is accessed using functions:
   * obj1->datalength()
   * obj1->data();
   * obj2->datalength()
   * obj2->data();
   */
  return static_cast<dist_t>(0);

} 

/** Sample standard functions to read/write/create objects */ 

template <typename dist_t>
unique_ptr<DataFileInputState> SpaceDummy<dist_t>::OpenReadFileHeader(const string& inpFileName) const {
  return unique_ptr<DataFileInputState>(new DataFileInputStateOneFile(inpFileName));
}

template <typename dist_t>
unique_ptr<DataFileOutputState> SpaceDummy<dist_t>::OpenWriteFileHeader(const ObjectVector& dataset,
                                                                        const string& outFileName) const {
  return unique_ptr<DataFileOutputState>(new DataFileOutputState(outFileName));
}

template <typename dist_t>
unique_ptr<Object> 
SpaceDummy<dist_t>::CreateObjFromStr(IdType id, LabelType label, const string& s,
                                            DataFileInputState* pInpStateBase) const {
  // Object that stores the string
  return unique_ptr<Object>(new Object(id, label, s.size(), s.data())); 
}

template <typename dist_t>
string SpaceDummy<dist_t>::CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const {
  // Return a string that is a copy of the Object buffer
  return string(pObj->data(), pObj->datalength());
}

template <typename dist_t>
bool SpaceDummy<dist_t>::ReadNextObjStr(DataFileInputState &inpStateBase, string& strObj, LabelType& label, string& externId) const {
  externId.clear();
  DataFileInputStateOneFile* pInpState = dynamic_cast<DataFileInputStateOneFile*>(&inpStateBase);
  CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
  if (!pInpState->inp_file_) return false;
  // Read the next line
  if (!getline(pInpState->inp_file_, strObj)) return false;
  // Increase the line number counter
  pInpState->line_num_++;
  return true;
}

template <typename dist_t>
void SpaceDummy<dist_t>::WriteNextObj(const Object& obj, const string& externId, DataFileOutputState &outState) const {
  string s = CreateStrFromObj(&obj, externId);
  outState.out_file_ << s;
}
/** End of standard functions to read/write/create objects */ 

 

template class SpaceDummy<int>;
template class SpaceDummy<float>;

}  // namespace similarity
