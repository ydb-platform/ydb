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
#include <space.h>
#include <query.h>

namespace similarity {

using std::vector;
using std::string;

template <typename dist_t>
unique_ptr<DataFileInputState> Space<dist_t>::ReadDataset(ObjectVector& dataset,
                           vector<string>& vExternIds,
                           const string& inputFile,
                           const IdTypeUnsign MaxNumObjects) const {
  CHECK_MSG(MaxNumObjects >=0, "Bug: MaxNumObjects should be >= 0");
  unique_ptr<DataFileInputState> inpState(OpenReadFileHeader(inputFile));
  string line;
  LabelType label;
  string externId;
  for (size_t id = 0; id < MaxNumObjects || !MaxNumObjects; ++id) {
    if (!ReadNextObjStr(*inpState, line, label, externId)) break;
    dataset.push_back(CreateObjFromStr(id, label, line, inpState.get()).release());
    vExternIds.push_back(externId);
  }
  inpState->Close();
  return inpState;
}

template <typename dist_t>
void Space<dist_t>::WriteDataset(const ObjectVector& dataset,
                           const vector<string>& vExternIds,
                           const string& outputFile,
                           const IdTypeUnsign MaxNumObjects) const {
  CHECK_MSG(MaxNumObjects >=0, "Bug: MaxNumObjects should be >= 0");
  if (dataset.size() != vExternIds.size()) {
    PREPARE_RUNTIME_ERR(err) << "Bug, dataset.size() != vExternIds.size(): " << dataset.size() << " != " << vExternIds.size();
    THROW_RUNTIME_ERR(err);
  }
  unique_ptr<DataFileOutputState> outState(OpenWriteFileHeader(dataset, outputFile));
  for (size_t i = 0; i < MaxNumObjects && i < dataset.size(); ++i) {
    WriteNextObj(*dataset[i], vExternIds[i], *outState);
  }
  outState->Close();
}

// Doesn't support external IDs, just makes them all empty
template <typename dist_t>
unique_ptr<DataFileInputState>
Space<dist_t>::ReadObjectVectorFromBinData(ObjectVector& data,
                                           vector<string>& vExternIds,
                                           const std::string& fileName,
                                           const IdTypeUnsign maxQty) const {
  CHECK_MSG(data.empty(), "this function expects data to be empty on call");
  size_t qty;
  size_t objSize;
  std::ifstream input(fileName, std::ios::binary);
  CHECK_MSG(input, "Cannot open file '" + fileName + "' for reading");
  input.exceptions(std::ios::badbit | std::ios::failbit);
  vExternIds.clear();

  readBinaryPOD(input, qty);

  for (unsigned i = 0; i < std::min(qty, size_t(maxQty)); ++i) {
    readBinaryPOD(input, objSize);
    unique_ptr<char []> buf(new char[objSize]);
    input.read(&buf[0], objSize);
    // true guarantees that the Object will take ownership of memory
    // less than ideal, but ok for now
    data.push_back(new Object(buf.release(), true));
  }
  
  return unique_ptr<DataFileInputState>(new DataFileInputState());
}

// Doesn't support external IDs
template <typename dist_t>
void Space<dist_t>::WriteObjectVectorBinData(const ObjectVector& data,
                                             const vector<string>& vExternIds,
                                             const std::string& fileName,
                                             const IdTypeUnsign maxQty) const {
  std::ofstream output(fileName, std::ios::binary);
  CHECK_MSG(output, "Cannot open file '" + fileName + "' for writing");
  output.exceptions(std::ios::badbit | std::ios::failbit);

  writeBinaryPOD(output, size_t(data.size()));
  for (unsigned i = 0; i < std::min(data.size(), size_t(maxQty)); ++i) {
    const Object* o = data[i];
    writeBinaryPOD(output, o->bufferlength());
    output.write(o->buffer(), o->bufferlength());
  }
  output.close();
}


template class Space<int>;
template class Space<float>;

template <class dist_t>
 void DummyPivotIndex<dist_t>::ComputePivotDistancesIndexTime(const Object* pObj, vector<dist_t>& vResDist) const  {
  vResDist.resize(pivots_.size());
// Pivot is a left argument
  for (size_t i = 0; i < pivots_.size(); ++i) vResDist[i] = space_.IndexTimeDistance(pivots_[i], pObj);
}

template <class dist_t>
void DummyPivotIndex<dist_t>::ComputePivotDistancesQueryTime(const Query<dist_t>* pQuery, vector<dist_t>& vResDist) const  {
  vResDist.resize(pivots_.size());
  for (size_t i = 0; i < pivots_.size(); ++i) vResDist[i] = pQuery->DistanceObjLeft(pivots_[i]);
}

template <class dist_t>
DummyPivotIndex<dist_t>::~DummyPivotIndex() {}

template <class dist_t>
PivotIndex<dist_t>::~PivotIndex() {}

template class PivotIndex<int>;
template class PivotIndex<float>;
template class DummyPivotIndex<int>;
template class DummyPivotIndex<float>;

}
