/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2022 at the University of Edinburgh    */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/*    Authors: Julian Hall, Ivet Galabova, Leona Gottwald and Michael    */
/*    Feldmeier                                                          */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file util/HVector.h
 * @brief Vector structure for HiGHS
 */
#ifndef UTIL_HVECTOR_H_
#define UTIL_HVECTOR_H_

#include "util/HVectorBase.h"
#include "util/HighsCDouble.h"

using HVector = HVectorBase<double>;
using HVectorQuad = HVectorBase<HighsCDouble>;
using HVector_ptr = HVector*;
using HVectorQuad_ptr = HVectorQuad*;

#endif /* UTIL_HVECTOR_H_ */
