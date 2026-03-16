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
 * \brief Adapter-class for passing 3D point correspondences. Maps
 *        opengv types back to opengv types.
 */

#ifndef OPENGV_POINTCLOUDADAPTER_HPP_
#define OPENGV_POINTCLOUDADAPTER_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/point_cloud/PointCloudAdapterBase.hpp>

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
 * Check the documentation of the parent-class to understand the meaning of
 * a PointCloudAdapter. This child-class is used for holding
 * data in form of references to opengv-types.
 */
class PointCloudAdapter : public PointCloudAdapterBase
{
private:
  using PointCloudAdapterBase::_t12;
  using PointCloudAdapterBase::_R12;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  PointCloudAdapter(
      const points_t & points1,
      const points_t & points2 );
  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  PointCloudAdapter(
      const points_t & points1,
      const points_t & points2,
      const rotation_t & R12 );
  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  PointCloudAdapter(
      const points_t & points1,
      const points_t & points2,
      const translation_t & t12,
      const rotation_t & R12 );
  /**
   * Destructor
   */
  virtual ~PointCloudAdapter();

  //Access of correspondences
  
  /** See parent-class */
  virtual opengv::point_t getPoint1( size_t index ) const;
  /** See parent-class */
  virtual opengv::point_t getPoint2( size_t index ) const;
  /** See parent-class */
  virtual double getWeight( size_t index ) const;
  /** See parent-class */
  virtual size_t getNumberCorrespondences() const;

private:
  /** Reference to the 3D-points in frame 1 */
  const points_t & _points1;
  /** Reference to the 3D-points in frame 2 */
  const points_t & _points2;
};

}
}

#endif /* OPENGV_POINTCLOUDADAPTER_HPP_ */
