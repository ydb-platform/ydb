/******************************************************************************
 * Author:   Laurent Kneip                                                    *
 * Contact:  kneip.laurent@gmail.com                                          *
 * License:  Copyright (c) 2013 Laurent Kneip, ANU. All rights reserved.      *
 *                                                                            *
 * Redistribution and use in source and binary forms, with or without         *
 * modification, are permitted provided that the following conditions         *
 * are met:                                                                   *
 * * Redistributions of source code must retain the above copyright           *
 *   notice, this list of conditions and the following disclaimer.            *
 * * Redistributions in binary form must reproduce the above copyright        *
 *   notice, this list of conditions and the following disclaimer in the      *
 *   documentation and/or other materials provided with the distribution.     *
 * * Neither the name of ANU nor the names of its contributors may be         *
 *   used to endorse or promote products derived from this software without   *
 *   specific prior written permission.                                       *
 *                                                                            *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"*
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  *
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE *
 * ARE DISCLAIMED. IN NO EVENT SHALL ANU OR THE CONTRIBUTORS BE LIABLE        *
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL *
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR *
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER *
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT         *
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY  *
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF     *
 * SUCH DAMAGE.                                                               *
 ******************************************************************************/

/**
 * \file OptimizationFunctor.hpp
 * \brief Generic functor base for use with the Eigen-nonlinear optimization
 *        toolbox.
 */

#ifndef OPENGV_OPTIMIZATIONFUNCTOR_HPP_
#define OPENGV_OPTIMIZATIONFUNCTOR_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>

using namespace std;
using namespace Eigen;

/**
 * \brief The namespace of this library.
 */
namespace opengv
{

/**
 * Generic functor base for use with the Eigen-nonlinear optimization
 * toolbox. Please refer to the Eigen-documentation for further information.
 */
template<typename _Scalar, int NX=Dynamic, int NY=Dynamic>
struct OptimizationFunctor
{
  /** undocumented */
  typedef _Scalar Scalar;
  /** undocumented */
  enum
  {
    InputsAtCompileTime = NX,
    ValuesAtCompileTime = NY
  };
  /** undocumented */
  typedef Matrix<Scalar,InputsAtCompileTime,1> InputType;
  /** undocumented */
  typedef Matrix<Scalar,ValuesAtCompileTime,1> ValueType;
  /** undocumented */
  typedef Matrix<Scalar,ValuesAtCompileTime,InputsAtCompileTime> JacobianType;

  /** undocumented */
  const int m_inputs;
  /** undocumented */
  const int m_values;

  /** undocumented */
  OptimizationFunctor() :
      m_inputs(InputsAtCompileTime),
      m_values(ValuesAtCompileTime) {}
  /** undocumented */
  OptimizationFunctor(int inputs, int values) :
      m_inputs(inputs),
      m_values(values) {}

  /** undocumented */
  int inputs() const
  {
    return m_inputs;
  }
  /** undocumented */
  int values() const
  {
    return m_values;
  }
};

}

#endif /* OPENGV_OPTIMIZATIONFUNCTOR_HPP_ */
