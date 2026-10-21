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
#ifndef _SPACE_SCALAR_H_
#define _SPACE_SCALAR_H_

#include <string>
#include <limits>
#include <map>
#include <stdexcept>

#include <string.h>
#include "global.h"
#include "object.h"
#include "utils.h"
#include "space.h"
#include "space_vector.h"
#include "distcomp.h"


#define SPACE_COSINE_SIMILARITY  "cosinesimil"
#define SPACE_ANGULAR_DISTANCE   "angulardist"
#define SPACE_NEGATIVE_SCALAR    "negdotprod"

namespace similarity {

template <typename dist_t>
class SpaceCosineSimilarity : public VectorSpaceSimpleStorage<dist_t> {
public:
  SpaceCosineSimilarity() {}
  virtual std::string StrDesc() const {
    return "CosineSimilarity";
  }
protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
  DISABLE_COPY_AND_ASSIGN(SpaceCosineSimilarity);
};

template <typename dist_t>
class SpaceAngularDistance : public VectorSpaceSimpleStorage<dist_t> {
public:
  SpaceAngularDistance() {}
  virtual std::string StrDesc() const {
    return "AngularDistance";
  }
  virtual size_t GetElemQty(const Object* object) const {
    return object->datalength()/ sizeof(dist_t);
  }
  virtual void createVectFromObj(const Object* obj, dist_t* pDstVect,
                                 size_t nElem) const {
    return VectorSpace<dist_t>::
                CreateVectFromObjSimpleStorage(__func__, obj, pDstVect, nElem);
  }

protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
  DISABLE_COPY_AND_ASSIGN(SpaceAngularDistance);
};

template <typename dist_t>
class SpaceNegativeScalarProduct : public VectorSpaceSimpleStorage<dist_t> {
public:
  SpaceNegativeScalarProduct() {}
  virtual std::string StrDesc() const {
    return SPACE_NEGATIVE_SCALAR;
  }
  virtual size_t GetElemQty(const Object* object) const {
    return object->datalength()/ sizeof(dist_t);
  }
  virtual void createVectFromObj(const Object* obj, dist_t* pDstVect,
                                 size_t nElem) const {
    return VectorSpace<dist_t>::
                CreateVectFromObjSimpleStorage(__func__, obj, pDstVect, nElem);
  }

protected:
  virtual dist_t HiddenDistance(const Object* obj1, const Object* obj2) const;
  DISABLE_COPY_AND_ASSIGN(SpaceNegativeScalarProduct);
};


}  // namespace similarity

#endif 
