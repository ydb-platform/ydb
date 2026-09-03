

#include <string>
#include <algorithm>
#include <utils.h>

#include <read_data.h>
#include <space/space_sparse_dense_fusion.h>


using namespace std;

namespace similarity {


/*
 * Structure of the data file (all data entries occupy 4 bytes except the external ID:
 *
 * A separate text file:
 *
 * queryWeights: <vector of query weights>
 * indexWeights: <vector of index weights>
 *
 * The number of both index and query weights should be == to the number of
 * components (see below).
 *
 * Preamble/header:
 *
 * <# of entries>
 * <# of vectors per entry>
 *
 * Description of vectors, one description per vector in the entry:
 *
 * <sparsity flag: 1 for sparse vectors>
 * <dimensionality: used only for dense vectors>
 *
 * Entries:
 *
 * External ID:
 * <length of the id> <string id>
 *
 * A number of sparse and/or dense vectors.
 *
 * Sparse vector:
 *
 * <number of elements> (  <id> <value ) *
 *
 * Dense vector (# of dimensions is given in the header:
 *
 * <dimension values>
 *
 */



const string &queryWeightsPref = "queryWeights:";
const string &indexWeightsPref = "indexWeights:";

SpaceSparseDenseFusion::SpaceSparseDenseFusion(const string& weightFileName) : weightFileName_(weightFileName) {
  ifstream inp(weightFileName);

  if (!inp) {
    PREPARE_RUNTIME_ERR(err) << "Cannot open file: '" << weightFileName << "' for reading";
    THROW_RUNTIME_ERR(err);
  }

  string line;

  for (unsigned int i = 0; i < 2; ++i) {

    if (!getline(inp, line)) {
      PREPARE_RUNTIME_ERR(err) << "Error reading line #" <<(i+1) << " from the file: '" << weightFileName << "'";
      THROW_RUNTIME_ERR(err);
    }

    if (StartsWith(line, queryWeightsPref)) {
      string weightLine = line.substr(queryWeightsPref.size());
      if (!ReadVecDataEfficiently(weightLine, vHeaderQueryWeights_)) {
        PREPARE_RUNTIME_ERR(err) << "Error parsing a vector in line #" <<(i+1) << " of the file " << weightFileName;
        THROW_RUNTIME_ERR(err);
      }
    } else if (StartsWith(line, indexWeightsPref)) {
      string weightLine = line.substr(indexWeightsPref.size());
      if (!ReadVecDataEfficiently(weightLine, vHeaderIndexWeights_)) {
        PREPARE_RUNTIME_ERR(err) << "Error parsing a vector in line #" <<(i+1) << " of the file " << weightFileName;;
        THROW_RUNTIME_ERR(err);
      }
    } else {
      PREPARE_RUNTIME_ERR(err) << "Invalid format in line #" <<(i+1)
                               << "expecting one of the prefixes: " << indexWeightsPref << "," << queryWeightsPref;
      THROW_RUNTIME_ERR(err);
    }
  }

  if (vHeaderIndexWeights_.size() == 0 ||
      vHeaderQueryWeights_.size() == 0 ||
      vHeaderIndexWeights_.size() != vHeaderQueryWeights_.size()) {
    PREPARE_RUNTIME_ERR(err) << "Zero or non-matching # of weights in the file '" << weightFileName << "' "
                             << "# of index weights: " << vHeaderIndexWeights_.size()
                             << "# of query weights: " << vHeaderQueryWeights_.size();
  }

}

unique_ptr<DataFileInputState> similarity::SpaceSparseDenseFusion::OpenReadFileHeader(const string &inputFile) const {
  auto state = unique_ptr<DataFileInputStateSparseDenseFusion>(new DataFileInputStateSparseDenseFusion(inputFile));

  auto &vCompDesc = state->vCompDesc_;

  uint32_t qty, compQty;

  readBinaryPOD(state->inp_file_, qty);
  readBinaryPOD(state->inp_file_, compQty);
  state->qty_= qty;

  LOG(LIB_INFO) << "Expecting " << state->qty_ << " entries each of which has " << compQty << " sparse or dense vectors.";

  for (unsigned i = 0; i < compQty; ++i) {
    // To keep a 4-byte alignment, we use only 32-bit variables
    uint32_t isSparseFlag;
    uint32_t dim;
    float indexWeight, queryWeight;

    readBinaryPOD(state->inp_file_, isSparseFlag);
    readBinaryPOD(state->inp_file_, dim);
    CHECK_MSG(i < vHeaderIndexWeights_.size(),
              "Too few index weights in the weight file: " + weightFileName_ +
              ", detected while reading component # " + ConvertToString(i + 1));
    indexWeight = vHeaderIndexWeights_[i];
    CHECK_MSG(i < vHeaderQueryWeights_.size(),
              "Too few query weights in the weight file: " + weightFileName_ +
              ", detected while reading component # " + ConvertToString(i + 1));
    queryWeight = vHeaderQueryWeights_[i];

    vCompDesc.push_back(CompDesc(isSparseFlag != 0, dim, indexWeight, queryWeight));
    auto &e = vCompDesc.back();

    LOG(LIB_INFO) << "Component: " << i << " is sparse?: " << e.isSparse_ << " dim: " << e.dim_ <<
                  " indexWeight: " << e.indexWeight_ << " queryWeight: " << e.queryWeight_;
  }

  return unique_ptr<DataFileInputState>(state.release());
}

/*
 * Structure of the file:
 */
bool
SpaceSparseDenseFusion::ReadNextObjStr(DataFileInputState &stateBase, string &strObj, LabelType &label,
                                       string &externId) const {
  DataFileInputStateSparseDenseFusion *pInpState = dynamic_cast<DataFileInputStateSparseDenseFusion *>(&stateBase);
  CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");

  if (pInpState->readQty_ >= pInpState->qty_)
    return false;

  strObj.clear();

  readBinaryStringId(*pInpState, externId);

  string s;

  for (const auto e : pInpState->vCompDesc_) {
    if (e.isSparse_) {
      readNextBinSparseVect(*pInpState, s);
    } else {
      readNextBinDenseVect(*pInpState, s, e.dim_);
    }
    strObj.append(s);
  }

  pInpState->readQty_++;

  return true;
}


unique_ptr<Object>
SpaceSparseDenseFusion::CreateObjFromStr(IdType id, LabelType label, const string &objStr,
                                           DataFileInputState *pInpStateBase) const {

  const vector<CompDesc>* pCompDesc = nullptr;
  /*
   * After we read the data and call CreateObjFromStr from e.g., query server,
   * pInpStateBase will be null.
   */
  if (pInpStateBase) {
    DataFileInputStateSparseDenseFusion *pInpState = dynamic_cast<DataFileInputStateSparseDenseFusion *>(pInpStateBase);
    CHECK_MSG(pInpState != NULL, "Bug: unexpected pointer type");
    pCompDesc = &pInpState->vCompDesc_;
  } else {
    pCompDesc = &vCompDesc_;
  }

  vector<float> vDataBuff; // The buffer stores 4-byte floats and 4-byte unsigned ints
  vector<float> vDense;
  vector<SparseVectElem<float>> vSparse;

  unsigned extractStart = 0;

  vector<char> buf;

  for (const auto e : *pCompDesc) {
    if (e.isSparse_) {
      parseSparseBinVect(objStr, vSparse, extractStart); // modifies extractStart

      char *pData = NULL;
      size_t dataLen = 0;

      PackSparseElements(vSparse, pData, dataLen);
      unique_ptr<char[]> data(pData); // data needs to be deleted when out of scope

      size_t oldSize = buf.size();
      CHECK((oldSize & 3) == 0);
      size_t padSize = getPad4(dataLen); // To align on 4-byte boundary

      size_t currElemBuffSize = dataLen + 4 + padSize;

      buf.resize(oldSize + currElemBuffSize);
      CHECK((buf.size() & 3) == 0);

      CHECK_MSG(dataLen <= numeric_limits<uint32_t>::max(),
                "The size of the data is huge: " + ConvertToString(dataLen) + " this is likely an bug!");

      char *pCurrBuffStart = &buf[oldSize];
      writeBinaryPOD(pCurrBuffStart, (uint32_t) dataLen);
      memcpy(pCurrBuffStart + 4, pData, dataLen);
      memset(pCurrBuffStart + dataLen, 0, padSize); // zero padded area

    } else {

      parseDenseBinVect(objStr, vDense, extractStart, e.dim_);  // modifies extractStart
      size_t oldSize = buf.size();
      CHECK_MSG((oldSize & 3) == 0, "old buffer size: " + ConvertToString(oldSize));

      size_t vectSize = e.dim_ * sizeof(float);
      buf.resize(oldSize + vectSize);
      CHECK((buf.size() & 3) == 0);

      char *pCurrBuffStart = &buf[oldSize];
      memcpy(pCurrBuffStart, &vDense[0], vectSize);

    }
  }

  return unique_ptr<Object>(new Object(id, label, buf.size(), &buf[0]));
}

float SpaceSparseDenseFusion::compDistance(const Object* obj1, const Object* obj2, bool isQueryTime) const {
  float res = 0;

  unsigned start1 = 0, start2 = 0;

  const char* const pBeg1 = obj1->data();
  const char* const pBeg2 = obj2->data();

  uint32_t dataLen1, dataLen2 = 0;

  for (const auto e : vCompDesc_) {

    float weight = (isQueryTime ? e.queryWeight_ : e.indexWeight_);
    float val = 0;

    size_t pad1 = 0, pad2 = 0;

    if (e.isSparse_) {
      readBinaryPOD(pBeg1 + start1, dataLen1);
      start1 += 4;
      readBinaryPOD(pBeg2 + start2, dataLen2);
      start2 += 4;

      pad1 = getPad4(dataLen1);
      pad2 = getPad4(dataLen2);

    } else {
      dataLen1 = 4 * e.dim_;
      dataLen2 = 4 * e.dim_;
    }

    CHECK_MSG(start1 + dataLen1 + pad1 <= obj1->datalength(),
              " start1="   + ConvertToString(start1) +
              " pad1="  + ConvertToString(pad1) +
              " dataLen1=" + ConvertToString(dataLen1)+
              " obj1->datalength():" + ConvertToString(obj1->datalength())
    );
    CHECK_MSG(start2 + dataLen2 <= obj2->datalength(),
              " start2=" + ConvertToString(start2) +
              " pad2=" + ConvertToString(pad2) +
              " dataLen2=" + ConvertToString(dataLen2) +
              " obj2->datalength():" + ConvertToString(obj2->datalength())
    );

    if (weight > numeric_limits<float>::min()) {
      if (e.isSparse_) {
        val = SparseScalarProductFast(pBeg1 + start1, dataLen1,
                                      pBeg2 + start2, dataLen2);
      } else {
        val = ScalarProductSIMD(reinterpret_cast<const float*>(pBeg1 + start1),
                                reinterpret_cast<const float*>(pBeg2 + start2), e.dim_);
      }
    }

    res += val * weight;

    start1 += dataLen1 + pad1;
    start2 += dataLen2 + pad2;
  }

  CHECK_MSG(start1 == obj1->datalength(),
            "start1="+ConvertToString(start1) + " datalength()=" + ConvertToString(obj1->datalength()));
  CHECK_MSG(start2 == obj2->datalength(),
            "start1="+ConvertToString(start2) + " datalength()=" + ConvertToString(obj2->datalength()));

  return -res;
}

float SpaceSparseDenseFusion::ProxyDistance(const Object* obj1, const Object* obj2) const {
 return compDistance(obj1, obj2, false /* false for index-time distance */);
}

float SpaceSparseDenseFusion::HiddenDistance(const Object* obj1, const Object* obj2) const {
  return compDistance(obj1, obj2, true /* true for query-time distance */);
}



}