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
 * \file RelativeAdapterBase.hpp
 * \brief Adapter-class for passing bearing-vector correspondences to the
 *        relative-pose algorithms.
 */

#ifndef OPENGV_RELATIVE_POSE_RELATIVEADAPTERBASE_HPP_
#define OPENGV_RELATIVE_POSE_RELATIVEADAPTERBASE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>

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
 * The RelativeAdapterBase is the base-class for the visitors to the central
 * and non-central relative pose algorithms. It provides a unified interface to
 * opengv-methods to access bearing-vector correspondences, priors or known
 * variables for the relative pose, and the multi-camera configuration in the
 * non-central case. Derived classes may hold the data in any user-specific
 * format, and adapt to opengv-types.
 */
class RelativeAdapterBase
{
public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor.
   */
  RelativeAdapterBase( ) :
      _t12(Eigen::Vector3d::Zero()),
      _R12(Eigen::Matrix3d::Identity()) {};
  /**
   * \brief Constructor.
   * \param[in] R12 A prior or known value for the rotation from viewpoint 2
   *                to viewpoint 1.
   */
  RelativeAdapterBase( const rotation_t & R12 ) :
      _t12(Eigen::Vector3d::Zero()),
      _R12(R12) {};
  /**
   * \brief Constructor.
   * \param[in] t12 A prior or known value for the position of viewpoint 2 seen
   *                from viewpoint 1.
   * \param[in] R12 A prior or known value for the rotation from viewpoint 2
   *                to viewpoint 1.
   */
  RelativeAdapterBase( const translation_t & t12, const rotation_t & R12 ) :
      _t12(t12),
      _R12(R12) {};
  /**
   * \brief Destructor.
   */
  virtual ~RelativeAdapterBase() {};

  //Access of correspondences
  
  /**
   * \brief Retrieve the bearing vector of a correspondence in viewpoint 1.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding bearing vector.
   */
  virtual opengv::bearingVector_t getBearingVector1( size_t index ) const = 0;
  /**
   * \brief Retrieve the bearing vector of a correspondence in viewpoint 2.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding bearing vector.
   */
  virtual opengv::bearingVector_t getBearingVector2( size_t index ) const = 0;
  /**
   * \brief Retrieve the weight of a correspondence. The weight is supposed to
   *        reflect the quality of a correspondence, and typically is between
   *        0 and 1.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding weight.
   */
  virtual double getWeight( size_t index ) const = 0;
  /**
   * \brief Retrieve the position of a camera of a correspondence in viewpoint
   *        1 seen from the origin of the viewpoint.
   * \param[in] index The serialized index of the correspondence.
   * \return The position of the corresponding camera seen from the viewpoint
   *         origin.
   */
  virtual opengv::translation_t getCamOffset1( size_t index ) const = 0;
  /**
   * \brief Retrieve the rotation from a camera of a correspondence in
   *        viewpoint 1 to the viewpoint origin.
   * \param[in] index The serialized index of the correspondence.
   * \return The rotation from the corresponding camera back to the viewpoint
   *         origin.
   */
  virtual opengv::rotation_t getCamRotation1( size_t index ) const = 0;
  /**
   * \brief Retrieve the position of a camera of a correspondence in viewpoint
   *        2 seen from the origin of the viewpoint.
   * \param[in] index The serialized index of the correspondence.
   * \return The position of the corresponding camera seen from the viewpoint
   *         origin.
   */
  virtual opengv::translation_t getCamOffset2( size_t index ) const = 0;
  /**
   * \brief Retrieve the rotation from a camera of a correspondence in
   *        viewpoint 2 to the viewpoint origin.
   * \param[in] index The serialized index of the correspondence.
   * \return The rotation from the corresponding camera back to the viewpoint
   *         origin.
   */
  virtual opengv::rotation_t getCamRotation2( size_t index ) const = 0;
  /**
   * \brief Retrieve the number of correspondences.
   * \return The number of correspondences.
   */
  virtual size_t getNumberCorrespondences() const = 0;

  //Access of priors or known values
  
  /**
   * \brief Retrieve the prior or known value for the relative position.
   * \return The prior or known value for the position.
   */
  opengv::translation_t gett12() const { return _t12; };
  /**
   * \brief Set the prior or known value for the relative position.
   * \param[in] t12 The prior or known value for the position.
   */
  void sett12(const opengv::translation_t & t12) { _t12 = t12; };
  /**
   * \brief Retrieve the prior or known value for the relative rotation.
   * \return The prior or known value for the rotation.
   */
  opengv::rotation_t getR12() const { return _R12; };
  /**
   * \brief Set the prior or known value for the relative rotation.
   * \param[in] R12 The prior or known value for the rotation.
   */
  void setR12(const opengv::rotation_t & R12) { _R12 = R12; };

protected:
  /** Prior or known value for the position of viewpoint 2 seen from
   *  viewpoint 1. Initialized to zero if not provided.
   */
  opengv::translation_t _t12;
  /** Prior or known value for the rotation from viewpoint 2 back to
   *  viewpoint 1. Initialized to identity if not provided.
   */
  opengv::rotation_t _R12;

};

}
}

#endif /* OPENGV_RELATIVE_POSE_RELATIVEADAPTERBASE_HPP_ */
