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
 * \file MACentralRelative.hpp
 * \brief Adapter-class for passing bearing-vector correspondences to the
 *        central relative-pose algorithms. Maps matlab types to opengv types.
 */

#ifndef OPENGV_RELATIVE_POSE_MACENTRALRELATIVE_HPP_
#define OPENGV_RELATIVE_POSE_MACENTRALRELATIVE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/relative_pose/RelativeAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the relative pose methods.
 */
namespace relative_pose
{

/**
 * Check the documentation of the parent-class to understand the meaning of
 * a RelativeAdapter. This child-class is for the central case and holds data
 * in form of pointers to matlab data.
 */
class MACentralRelative : public RelativeAdapterBase
{
protected:
  using RelativeAdapterBase::_t12;
  using RelativeAdapterBase::_R12;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  MACentralRelative(
      const double * bearingVectors1,
      const double * bearingVectors2,
      int numberBearingVectors1,
      int numberBearingVectors2 );

  /**
   * \brief Destructor.
   */
  virtual ~MACentralRelative();

  //Access of correspondences
  
  /** See parent-class */
  virtual bearingVector_t getBearingVector1( size_t index ) const;
  /** See parent-class */
  virtual bearingVector_t getBearingVector2( size_t index ) const;
  /** See parent-class */
  virtual double getWeight( size_t index ) const;
  /** See parent-class. Returns zero for this adapter. */
  virtual translation_t getCamOffset1( size_t index ) const;
  /** See parent-class. Returns identity for this adapter. */
  virtual rotation_t getCamRotation1( size_t index ) const;
  /** See parent-class. Returns zero for this adapter. */
  virtual translation_t getCamOffset2( size_t index ) const;
  /** See parent-class. Returns identity for this adapter. */
  virtual rotation_t getCamRotation2( size_t index ) const;
  /** See parent-class */
  virtual size_t getNumberCorrespondences() const;

protected:
  /** A pointer to the bearing-vectors in viewpoint 1 */  
  const double * _bearingVectors1;
  /** A pointer to the bearing-vectors in viewpoint 2 */
  const double * _bearingVectors2;
  /** The number of bearing-vectors in viewpoint 1 */
  int _numberBearingVectors1;
  /** The number of bearing-vectors in viewpoint 2 */
  int _numberBearingVectors2;
};

}
}

#endif /* OPENGV_RELATIVE_POSE_MACENTRALRELATIVE_HPP_ */
