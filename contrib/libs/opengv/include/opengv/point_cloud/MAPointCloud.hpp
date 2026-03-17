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
 * \file MAPointCloud.hpp
 * \brief Adapter-class for passing 3D point correspondences. Maps
 *        matlab types to opengv types.
 */

#ifndef OPENGV_MAPOINTCLOUD_HPP_
#define OPENGV_MAPOINTCLOUD_HPP_

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
 * data in form of pointers to matlab-data.
 */
class MAPointCloud : public PointCloudAdapterBase
{
private:
  using PointCloudAdapterBase::_t12;
  using PointCloudAdapterBase::_R12;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  MAPointCloud(
      const double * points1,
      const double * points2,
      int numberPoints1,
      int numberPoints2 );

  /**
   * Destructor
   */
  virtual ~MAPointCloud();

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
  /** A pointer to the points in frame 1 */
  const double * _points1;
  /** A pointer to the points in frame 2 */
  const double * _points2;
  /** The number of points in frame 1 */
  int _numberPoints1;
  /** The number of points in frame 2 */
  int _numberPoints2;
};

}
}

#endif /* OPENGV_MAPOINTCLOUD_HPP_ */
