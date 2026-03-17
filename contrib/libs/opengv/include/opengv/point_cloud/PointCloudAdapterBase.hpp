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
 * \file PointCloudAdapter.hpp
 * \brief Adapter-class for passing 3D point correspondences.
 */

#ifndef OPENGV_POINTCLOUDADAPTERBASE_HPP_
#define OPENGV_POINTCLOUDADAPTERBASE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the point-cloud alignment methods.
 */
namespace point_cloud
{

/**
 * The PointCloudAdapterBase is the base-class for the visitors to the
 * point-cloud alignment methods. It provides a unified interface to
 * opengv-methods to access point correspondences and priors or known variables
 * for the alignment. Derived classes may hold the data in any user-specific
 * format, and adapt to opengv-types.
 */
class PointCloudAdapterBase
{
public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor.
   */
  PointCloudAdapterBase( ) :
      _t12(Eigen::Vector3d::Zero()),
      _R12(Eigen::Matrix3d::Identity()) {};
  /**
   * \brief Constructor.
   * \param[in] R12 A prior or known value for the rotation from frame 2 to
   *                frame 1.
   */
  PointCloudAdapterBase( const rotation_t & R12 ) :
      _t12(Eigen::Vector3d::Zero()),
      _R12(R12) {};
  /**
   * \brief Constructor.
   * \param[in] t12 A prior or known value for the position of frame 2 seen
   *                from frame 1.
   * \param[in] R12 A prior or known value for the rotation from frame 2
   *                to frame 1.
   */
  PointCloudAdapterBase( const translation_t & t12, const rotation_t & R12 ) :
      _t12(t12),
      _R12(R12) {};
  /**
   * \brief Destructor.
   */
  virtual ~PointCloudAdapterBase() {};

  //Access of correspondences
  
  /**
   * \brief Retrieve the 3D-point of a correspondence in frame 1.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding 3D-point.
   */
  virtual opengv::point_t getPoint1( size_t index ) const = 0;
  /**
   * \brief Retrieve the 3D-point of a correspondence in frame 2.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding 3D-point.
   */
  virtual opengv::point_t getPoint2( size_t index ) const = 0;
  /**
   * \brief Retrieve the number of correspondences.
   * \return The number of correspondences.
   */
  virtual size_t getNumberCorrespondences() const = 0;
  /**
   * \brief Retrieve the weight of a correspondence. The weight is supposed to
   *        reflect the quality of a correspondence, and typically is between
   *        0 and 1.
   * \param[in] index The serialized index of the correspondence.
   * \return The corresponding weight.
   */
  virtual double getWeight( size_t index ) const = 0;

  //Access of priors or known values
  
  /**
   * \brief Retrieve the prior or known value for the relative position.
   * \return The prior or known value for the position.
   */
  virtual opengv::translation_t gett12() const { return _t12; };
  /**
   * \brief Set the prior or known value for the relative position.
   * \param[in] t12 The prior or known value for the position.
   */
  virtual void sett12(const translation_t & t12) { _t12 = t12; };
  /**
   * \brief Retrieve the prior or known value for the relative rotation.
   * \return The prior or known value for the rotation.
   */
  virtual opengv::rotation_t getR12() const { return _R12; };
  /**
   * \brief Set the prior or known value for the relative rotation.
   * \param[in] R12 The prior or known value for the rotation.
   */
  virtual void setR12(const rotation_t & R12) { _R12 = R12; };

public:
  /** Prior or known value for the position of frame 2 seen from frame 1.
   *  Initialized to zero if not provided.
   */
  translation_t _t12;
  /** Prior or known value for the rotation from frame 2 to frame 1.
   *  Initialized to identity if not provided.
   */
  rotation_t _R12;

};

}
}

#endif /* OPENGV_POINTCLOUDADAPTERBASE_HPP_ */
