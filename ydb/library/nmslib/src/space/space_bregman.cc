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

#include "space/space_bregman.h"
#include "logging.h"
#include "distcomp.h"
#include "experimentconf.h"

namespace similarity {

template <typename dist_t>
Object* BregmanDiv<dist_t>::Mean(const ObjectVector& data) const {
  CHECK(!data.empty());

  // the caller is responsible for releasing the pointer
  Object* mean = Object::CreateNewEmptyObject(data[0]->datalength());

  const size_t length = GetElemQty(data[0]);
  dist_t* x = reinterpret_cast<dist_t*>(mean->data());

  // init
  for (size_t d = 0; d < length; ++d) {
    x[d] = 0.0;
  }

  // sum
  for (auto it = data.begin(); it != data.end(); ++it) {
    const dist_t* y = reinterpret_cast<const dist_t*>((*it)->data());
    for (size_t d = 0; d < length; ++d) {
      x[d] += y[d];
    }
  }

  // average
  for (size_t d = 0; d < length; ++d) {
    x[d] /= static_cast<double>(data.size());
  }

  return mean;
}

//=============================================================

template <typename dist_t>
dist_t KLDivAbstract<dist_t>::Function(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  dist_t result = 0.0;
  for (size_t i = 0; i < length; ++i) {
    result += x[i] * log(x[i]);
  }
  return result;
}

template <typename dist_t>
Object* KLDivAbstract<dist_t>::GradientFunction(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  // the caller is responsible for releasing the pointer
  Object* result = Object::CreateNewEmptyObject(object->datalength());
  dist_t* y = reinterpret_cast<dist_t*>(result->data());
  for (size_t i = 0; i < length; ++i) {
    y[i] = log(x[i]) + 1.0;
  }
  return result;
}

template <typename dist_t>
Object* KLDivAbstract<dist_t>::InverseGradientFunction(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  // the caller is responsible for releasing the pointer
  Object* result = Object::CreateNewEmptyObject(object->datalength());
  dist_t* y = reinterpret_cast<dist_t*>(result->data());
  for (size_t i = 0; i < length; ++i) {
    y[i] = exp(x[i] - 1.0);
  }
  return result;
}

//=============================================================

template <typename dist_t>
Object* KLDivGenFast<dist_t>::Mean(const ObjectVector& data) const {
  CHECK(!data.empty());

  // the caller is responsible for releasing the pointer
  Object* mean = BregmanDiv<dist_t>::Mean(data);

  const size_t length = GetElemQty(data[0]);
  dist_t* x = reinterpret_cast<dist_t*>(mean->data());

  PrecompLogarithms(x, length);

  return mean;
}

template <typename dist_t>
Object* KLDivGenFast<dist_t>::InverseGradientFunction(const Object* object) const {
  DCHECK(object->datalength() > 0);

  Object* result = KLDivAbstract<dist_t>::InverseGradientFunction(object);
  dist_t* y = reinterpret_cast<dist_t*>(result->data());
  const size_t length = GetElemQty(object);
  PrecompLogarithms(y, length);
  return result;
}

template <typename dist_t>
dist_t KLDivGenFast<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return KLGeneralPrecompSIMD(x, y, length);
}

template <typename dist_t>
Object* KLDivGenFast<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}


//=============================================================

template <typename dist_t>
Object* ItakuraSaitoFast<dist_t>::Mean(const ObjectVector& data) const {
  CHECK(!data.empty());

  // the caller is responsible for releasing the pointer
  Object* mean = BregmanDiv<dist_t>::Mean(data);

  const size_t length = GetElemQty(data[0]);
  dist_t* x = reinterpret_cast<dist_t*>(mean->data());

  PrecompLogarithms(x, length);

  return mean;
}

template <typename dist_t>
dist_t ItakuraSaitoFast<dist_t>::Function(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  dist_t result = 0.0;
  for (size_t i = 0; i < length; ++i) {
    result += - log(x[i]);
  }
  return result;
}

template <typename dist_t>
Object* ItakuraSaitoFast<dist_t>::GradientFunction(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  // the caller is responsible for releasing the pointer
  Object* result = Object::CreateNewEmptyObject(object->datalength());
  dist_t* y = reinterpret_cast<dist_t*>(result->data());
  for (size_t i = 0; i < length; ++i) {
    y[i] = -1/x[i];
  }
  return result;
}

template <typename dist_t>
Object* ItakuraSaitoFast<dist_t>::InverseGradientFunction(const Object* object) const {
  DCHECK(object->datalength() > 0);
  const dist_t* x = reinterpret_cast<const dist_t*>(object->data());
  const size_t length = GetElemQty(object);

  // the caller is responsible for releasing the pointer
  Object* result = Object::CreateNewEmptyObject(object->datalength());
  dist_t* y = reinterpret_cast<dist_t*>(result->data());
  for (size_t i = 0; i < length; ++i) {
    y[i] = -1/x[i];
  }
  return result;
}

template <typename dist_t>
dist_t ItakuraSaitoFast<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return ItakuraSaitoPrecompSIMD(x, y, length);
}

template <typename dist_t>
Object* ItakuraSaitoFast<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}

//=============================================================

template <typename dist_t>
dist_t KLDivGenSlow<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return KLGeneralStandard(x, y, length);
}

template <typename dist_t>
Object* KLDivGenSlow<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  return new Object(id, label, InpVect.size() * sizeof(dist_t), &InpVect[0]);
}

template <typename dist_t>
dist_t KLDivGenFastRightQuery<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return KLGeneralPrecompSIMD(y, x, length);
}

template <typename dist_t>
Object* KLDivGenFastRightQuery<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}

//=============================================================

template <typename dist_t>
dist_t KLDivFast<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return KLPrecompSIMD(x, y, length);
}

template <typename dist_t>
Object* KLDivFast<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}

//=============================================================

template <typename dist_t>
dist_t KLDivFastRightQuery<dist_t>::HiddenDistance(const Object* obj1, const Object* obj2) const {
  DCHECK(obj1->datalength() > 0);
  DCHECK(obj1->datalength() == obj2->datalength());
  const dist_t* x = reinterpret_cast<const dist_t*>(obj1->data());
  const dist_t* y = reinterpret_cast<const dist_t*>(obj2->data());
   
  const size_t length = GetElemQty(obj1);

  return KLPrecompSIMD(y, x, length);
}

template <typename dist_t>
Object* KLDivFastRightQuery<dist_t>::CreateObjFromVect(IdType id, LabelType label, const std::vector<dist_t>& InpVect) const {
  std::vector<dist_t>   temp(InpVect);

  // Reserve space to store logarithms
  temp.resize(2 * InpVect.size());
  // Compute logarithms
  PrecompLogarithms(&temp[0], InpVect.size());
  return new Object(id, label, temp.size() * sizeof(dist_t), &temp[0]);
}

template class BregmanDiv<float>;
template class KLDivAbstract<float>;
template class KLDivGenSlow<float>;
template class KLDivGenFast<float>;
template class ItakuraSaitoFast<float>;
template class KLDivGenFastRightQuery<float>;
template class KLDivFast<float>;
template class KLDivFastRightQuery<float>;

}  // namespace similarity
