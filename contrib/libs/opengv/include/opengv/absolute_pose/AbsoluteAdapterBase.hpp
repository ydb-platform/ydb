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
 * \file AbsoluteAdapterBase.hpp
 * \brief Adapter-class for passing bearing-vector-to-point correspondences to
 *        the absolute-pose algorithms.
 */

#ifndef OPENGV_ABSOLUTE_POSE_ABSOLUTEADAPTERBASE_HPP_
#define OPENGV_ABSOLUTE_POSE_ABSOLUTEADAPTERBASE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>

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
 * The AbsoluteAdapterBase is the base-class for the visitors to the central
 * and non-central absolute pose algorithms. It provides a unified interface to
 * opengv-methods to access bearing-vectors, world points, priors or
 * known variables for the absolute pose, and the multi-camera configuration in
 * the non-central case. Derived classes may hold the data in any user-specific
 * format, and adapt to opengv-types.
 */
class AbsoluteAdapterBase
{
public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor.
   */
  AbsoluteAdapterBase() :
      _t(Eigen::Vector3d::Zero()),
      _R(Eigen::Matrix3d::Identity()) {};
  /**
   * \brief Constructor.
   * \param[in] R A prior or known value for the rotation from the viewpoint
   *              to the world frame.
   */
  AbsoluteAdapterBase( const opengv::rotation_t & R ) :
      _t(Eigen::Vector3d::Zero()),
      _R(R) {};
  /**
   * \brief Constructor.
   * \param[in] t A prior or known value for the position of the viewpoint seen
   *              from the world frame.
   * \param[in] R A prior or known value for the rotation from the viewpoint
   *              to the world frame.
   */
  AbsoluteAdapterBase(
      const opengv::translation_t & t,
      const opengv::rotation_t & R ) :
      _t(t),
      _R(R) {};
  /**
   * \brief Destructor.
   */
  virtual ~AbsoluteAdapterBase() {};

  //Access of correspondences
  
  /**
   * \brief Retrieve the bearing vector of a correspondence.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding bearing vector.
   */
  virtual opengv::bearingVector_t getBearingVector(size_t index ) const = 0;
  /**
   * \brief Retrieve the weight of a correspondence. The weight is supposed to
   *        reflect the quality of a correspondence, and typically is between
   *        0 and 1.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding weight.
   */
  virtual double getWeight( size_t index ) const = 0;
  /**
   * \brief Retrieve the position of a camera of a correspondence
   *        seen from the viewpoint origin.
   * \param[in] index The serialized index of the correspondence.
   * \return The position of the corresponding camera seen from the viewpoint
   *         origin.
   */
  virtual opengv::translation_t getCamOffset( size_t index ) const = 0;
  /**
   * \brief Retrieve the rotation from a camera of a correspondence to the
   *        viewpoint origin.
   * \param[in] index The serialized index of the correspondence.
   * \return The rotation from the corresponding camera back to the viewpoint
   *         origin.
   */
  virtual opengv::rotation_t getCamRotation( size_t index ) const = 0;
  /**
   * \brief Retrieve the world point of a correspondence.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding world point.
   */
  virtual opengv::point_t getPoint( size_t index ) const = 0;
  /**
   * \brief Retrieve the number of correspondences.
   * \return The number of correspondences.
   */
  virtual size_t getNumberCorrespondences() const = 0;

  //Access of priors or known values
  
  /**
   * \brief Retrieve the prior or known value for the position.
   * \return The prior or known value for the position.
   */
  opengv::translation_t gett() const { return _t; };
  /**
   * \brief Set the prior or known value for the position.
   * \param[in] t The prior or known value for the position.
   */
  void sett(const opengv::translation_t & t) { _t = t; };
  /**
   * \brief Retrieve the prior or known value for the rotation.
   * \return The prior or known value for the rotation.
   */
  opengv::rotation_t getR() const { return _R; };
  /**
   * \brief Set the prior or known value for the rotation.
   * \param[in] R The prior or known value for the rotation.
   */
  void setR(const opengv::rotation_t & R) { _R = R; };

protected:
  /** The prior or known value for the position of the viewpoint seen from the
   * world frame. Initialized to zero if not provided.
   */
  opengv::translation_t _t;
  /** The prior or known value for the rotation from the viewpoint back to the
   * world frame. Initialized to identity if not provided.
   */
  opengv::rotation_t _R;

};

}
};

#endif /* OPENGV_ABSOLUTE_POSE_ABSOLUTEADAPTERBASE_HPP_ */
