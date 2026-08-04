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
#ifndef _SPACE_L2_SQR_SIFT_H_
#define _SPACE_L2_SQR_SIFT_H_

#include <string>
#include <map>
#include <stdexcept>
#include <sstream>
#include <memory>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "distcomp.h"

#define SPACE_L2SQR_SIFT    "l2sqr_sift"

namespace similarity {

using std::string;
using std::unique_ptr;

class SpaceL2SqrSift : public Space<DistTypeSIFT> {
 public:
  explicit SpaceL2SqrSift() {}
  virtual ~SpaceL2SqrSift() {}

  /** Standard functions to read/write/create objects */ 
  virtual unique_ptr<Object> CreateObjFromStr(IdType id, LabelType label, const string& s,
                                                DataFileInputState* pInpState) const override;
  // Create a string representation of an object.
  virtual string CreateStrFromObj(const Object* pObj, const string& externId /* ignored */) const override;
  // Open a file for reading, fetch a header (if there is any) and memorize an input state
  virtual unique_ptr<DataFileInputState> OpenReadFileHeader(const string& inputFile) const override;
  // Open a file for writing, write a header (if there is any) and memorize an output state
  virtual unique_ptr<DataFileOutputState> OpenWriteFileHeader(const ObjectVector& dataset,
                                                                const string& outputFile) const override;
  /*
   * Read a string representation of the next object in a file as well
   * as its label. Return false, on EOF.
   */
  virtual bool ReadNextObjStr(DataFileInputState &, string& strObj, LabelType& label, string& externId) const override;
  /** End of standard functions to read/write/create objects */ 

  virtual Object* CreateObjFromUint8Vect(IdType id, LabelType label, const std::vector<uint8_t>& InpVect) const;
  virtual size_t GetElemQty(const Object* object) const override { return SIFT_DIM; }

  virtual string StrDesc() const override { return SPACE_L2SQR_SIFT; }

  virtual bool ApproxEqual(const Object& obj1, const Object& obj2) const override;

  virtual void CreateDenseVectFromObj(const Object* obj, DistTypeSIFT* pVect, size_t nElem) const override;

  static void ReadUint8Vec(std::string line, LabelType& label, std::vector<uint8_t>& v);

protected:
  DISABLE_COPY_AND_ASSIGN(SpaceL2SqrSift);

  virtual DistTypeSIFT HiddenDistance(const Object* obj1, const Object* obj2) const override {
    const uint8_t* pVect1 = reinterpret_cast<const uint8_t*>(obj1->data());
    const uint8_t* pVect2 = reinterpret_cast<const uint8_t*>(obj2->data());

    return l2SqrSIFTPrecompAVX(pVect1, pVect2);
  }
};
 
}  // namespace similarity

#endif
