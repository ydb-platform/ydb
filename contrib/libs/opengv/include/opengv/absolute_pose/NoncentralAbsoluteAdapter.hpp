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
 * \file NoncentralAbsoluteAdapter.hpp
 * \brief Adapter-class for passing bearing-vector-to-point correspondences to
 *        the non-central absolute-pose algorithms. It maps opengv
 *        types back to opengv types.
 */

#ifndef OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEADAPTER_HPP_
#define OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEADAPTER_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/absolute_pose/AbsoluteAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the absolute pose methods.
 */
namespace absolute_pose
{

/**
 * Check the documentation of the parent-class to understand the meaning of
 * an AbsoluteAdapter. This child-class is for the non-central case and holds
 * data in form of references to opengv-types.
 */
class NoncentralAbsoluteAdapter : public AbsoluteAdapterBase
{
protected:
  using AbsoluteAdapterBase::_t;
  using AbsoluteAdapterBase::_R;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /** A type defined for the camera-correspondences, see protected
   *  class-members
   */
  typedef std::vector<int> camCorrespondences_t;

  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  NoncentralAbsoluteAdapter(
      const bearingVectors_t & bearingVectors,
      const camCorrespondences_t & camCorrespondences,
      const points_t & points,
      const translations_t & camOffsets,
      const rotations_t & camRotations );
  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  NoncentralAbsoluteAdapter(
      const bearingVectors_t & bearingVectors,
      const camCorrespondences_t & camCorrespondences,
      const points_t & points,
      const translations_t & camOffsets,
      const rotations_t & camRotations,
      const rotation_t & R );
  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  NoncentralAbsoluteAdapter(
      const bearingVectors_t & bearingVectors,
      const camCorrespondences_t & camCorrespondences,
      const points_t & points,
      const translations_t & camOffsets,
      const rotations_t & camRotations,
      const translation_t & t,
      const rotation_t & R );
  /**
   * \brief Destructor.
   */
  virtual ~NoncentralAbsoluteAdapter();

  //Access of correspondences
  
  /** See parent-class */
  virtual opengv::bearingVector_t getBearingVector( size_t index ) const;
  /** See parent-class */
  virtual double getWeight( size_t index ) const;
  /** See parent-class */
  virtual opengv::translation_t getCamOffset( size_t index ) const;
  /** See parent-class */
  virtual opengv::rotation_t getCamRotation( size_t index ) const;
  /** See parent-class */
  virtual opengv::point_t getPoint( size_t index ) const;
  /** See parent-class */
  virtual size_t getNumberCorrespondences() const;

protected:
  /** Reference to the bearing-vectors expressed in the camera-frames */
  const bearingVectors_t & _bearingVectors;
  /** Reference to an array of camera-indices for the bearing vectors. Length
   *  equals to number of bearing-vectors, and elements are indices of cameras
   *  in the _camOffsets and _camRotations arrays.
   */
  const camCorrespondences_t & _camCorrespondences;
  /** Reference to the points expressed in the world-frame. */
  const points_t & _points;

  /** Reference to positions of the different cameras seen from the
   *  viewpoint.
   */
  const translations_t & _camOffsets;
  /** Reference to rotations from the different cameras back to the
   *  viewpoint.
   */
  const rotations_t & _camRotations;
};

}
};

#endif /* OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEADAPTER_HPP_ */
