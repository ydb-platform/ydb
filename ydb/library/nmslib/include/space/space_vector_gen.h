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
#ifndef _SPACE_VECTOR_GEN_H_
#define _SPACE_VECTOR_GEN_H_

#include <string>
#include <map>
#include <stdexcept>

#include <space/space_vector.h>

namespace similarity {

/*
 * A generic vector space: can be used with
 * an arbitrary distance function.
 */ 
template <typename dist_t, typename DistObjType>
class VectorSpaceGen : public VectorSpaceSimpleStorage<dist_t> {
 public:
  virtual ~VectorSpaceGen() {}
  explicit VectorSpaceGen() {}

  virtual void CreateDataset(ObjectVector& dataset, 
                            const vector<vector<dist_t>>& sourceData) {
    fillDataSet(dataset, sourceData, NULL);
  }

  virtual void CreateDataset(ObjectVector& dataset, 
                            const vector<vector<dist_t>>& sourceData,
                            const vector<LabelType>& labels) {
    fillDataSet(dataset, sourceData, &labels);
  }
  virtual std::string StrDesc() const {
    return "custom space";
  }
  Object* CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
    return VectorSpace<dist_t>::CreateObjFromVect(id, label, InpVect);
  };
 protected:
  DISABLE_COPY_AND_ASSIGN(VectorSpaceGen);

  DistObjType      distObj_;

  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const {
    CHECK(obj1->datalength() > 0);
    CHECK(obj1->datalength() == obj2->datalength());
    const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
    const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
    const size_t length = obj1->datalength() / sizeof(dist_t);

    return distObj_(x, y, length);
  }
  
  void fillDataSet(ObjectVector& dataset, 
                   const vector<vector<dist_t>>& sourceData,
                   const vector<LabelType>* pLabels) {
    size_t dim = 0;

    for (size_t index = 0; index < sourceData.size(); ++index) {
      const vector<dist_t>& oneElem = sourceData[index];

      size_t currDim = oneElem.size();

      if (!dim) dim = currDim;
      else {
        if (dim != currDim) {
          PREPARE_RUNTIME_ERR(err) << "The # of vector elements (" << currDim << ")" <<
                      " doesn't match the # of elements in previous lines. (" << dim << " )" <<
                      "Found mismatch, index : " << (index + 1);
          THROW_RUNTIME_ERR(err);
        }
      }
      LabelType label = pLabels ? (*pLabels)[index] :  -1 ;
      dataset.push_back(CreateObjFromVect(index, label, oneElem));
    }
  }

};

}  // namespace similarity



#endif
