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
#ifndef _SPACE_SPARSE_SCALAR_BIN_FAST_H_
#define _SPACE_SPARSE_SCALAR_BIN_FAST_H_

#include <string>
#include <limits>
#include <map>
#include <stdexcept>
#include <algorithm>

#include <string.h>
#include "global.h"
#include "object.h"
#include "query.h"
#include "utils.h"
#include "space.h"

#include "space_sparse_scalar_fast.h"
#include "distcomp.h"

#define SPACE_SPARSE_COSINE_SIMILARITY_BIN_FAST       "cosinesimil_sparse_bin_fast"
#define SPACE_SPARSE_NEGATIVE_SCALAR_PROD_BIN_FAST    "negdotprod_sparse_bin_fast"

namespace similarity {


class SpaceSparseCosineSimilarityBinFast : public SpaceSparseCosineSimilarityFast {
public:
  explicit SpaceSparseCosineSimilarityBinFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_COSINE_SIMILARITY_BIN_FAST;
  }
  unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inpFileName) const override;
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const override;
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const override;

  DISABLE_COPY_AND_ASSIGN(SpaceSparseCosineSimilarityBinFast);
};

class SpaceSparseNegativeScalarProductBinFast : public SpaceSparseNegativeScalarProductFast {
public:
  explicit SpaceSparseNegativeScalarProductBinFast(){}
  virtual std::string StrDesc() const override {
    return SPACE_SPARSE_NEGATIVE_SCALAR_PROD_BIN_FAST;
  }
  unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inpFileName) const override;
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const override;
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                              DataFileInputState* pInpState) const override;

  DISABLE_COPY_AND_ASSIGN(SpaceSparseNegativeScalarProductBinFast);
};

}  // namespace similarity

#endif 
