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


#include <opengv/relative_pose/CentralRelativeAdapter.hpp>

opengv::relative_pose::CentralRelativeAdapter::CentralRelativeAdapter(
    const bearingVectors_t & bearingVectors1,
    const bearingVectors_t & bearingVectors2 ) :
    RelativeAdapterBase(),
    _bearingVectors1(bearingVectors1),
    _bearingVectors2(bearingVectors2)
{}

opengv::relative_pose::CentralRelativeAdapter::CentralRelativeAdapter(
    const bearingVectors_t & bearingVectors1,
    const bearingVectors_t & bearingVectors2,
    const rotation_t & R12 ) :
    RelativeAdapterBase(R12),
    _bearingVectors1(bearingVectors1),
    _bearingVectors2(bearingVectors2)
{}

opengv::relative_pose::CentralRelativeAdapter::CentralRelativeAdapter(
    const bearingVectors_t & bearingVectors1,
    const bearingVectors_t & bearingVectors2,
    const translation_t & t12,
    const rotation_t & R12 ) :
    RelativeAdapterBase(t12,R12),
    _bearingVectors1(bearingVectors1),
    _bearingVectors2(bearingVectors2)
{}

opengv::relative_pose::CentralRelativeAdapter::~CentralRelativeAdapter()
{}

opengv::bearingVector_t
opengv::relative_pose::CentralRelativeAdapter::
    getBearingVector1( size_t index ) const
{
  assert(index < _bearingVectors1.size());
  return _bearingVectors1[index];
}

opengv::bearingVector_t
opengv::relative_pose::CentralRelativeAdapter::
    getBearingVector2( size_t index ) const
{
  assert(index < _bearingVectors2.size());
  return _bearingVectors2[index];
}

double
opengv::relative_pose::CentralRelativeAdapter::
    getWeight( size_t index ) const
{
  return 1.0;
}

opengv::translation_t
opengv::relative_pose::CentralRelativeAdapter::
    getCamOffset1( size_t index ) const
{
  //We could also check here for camIndex being 0, because this adapter is made
  //for a single camera only
  return Eigen::Vector3d::Zero();
}

opengv::rotation_t
opengv::relative_pose::CentralRelativeAdapter::
    getCamRotation1( size_t index ) const
{
  //We could also check here for camIndex being 0, because this adapter is made
  //for a single camera only
  return Eigen::Matrix3d::Identity();
}

opengv::translation_t
opengv::relative_pose::CentralRelativeAdapter::
    getCamOffset2( size_t index ) const
{
  //We could also check here for camIndex being 0, because this adapter is made
  //for a single camera only
  return Eigen::Vector3d::Zero();
}

opengv::rotation_t
opengv::relative_pose::CentralRelativeAdapter::
    getCamRotation2( size_t index ) const
{
  //We could also check here for camIndex being 0, because this adapter is made
  //for a single camera only
  return Eigen::Matrix3d::Identity();
}

size_t
opengv::relative_pose::CentralRelativeAdapter::
    getNumberCorrespondences() const
{
  return _bearingVectors2.size();
}
