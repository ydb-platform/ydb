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


#include <opengv/point_cloud/PointCloudAdapter.hpp>

opengv::point_cloud::PointCloudAdapter::PointCloudAdapter(
    const points_t & points1,
    const points_t & points2 ) :
    PointCloudAdapterBase(),
    _points1(points1),
    _points2(points2)
{}

opengv::point_cloud::PointCloudAdapter::PointCloudAdapter(
    const points_t & points1,
    const points_t & points2,
    const rotation_t & R12 ) :
    PointCloudAdapterBase(R12),
    _points1(points1),
    _points2(points2)
{}

opengv::point_cloud::PointCloudAdapter::PointCloudAdapter(
    const points_t & points1,
    const points_t & points2,
    const translation_t & t12,
    const rotation_t & R12 ) :
    PointCloudAdapterBase(t12,R12),
    _points1(points1),
    _points2(points2)
{}

opengv::point_cloud::PointCloudAdapter::~PointCloudAdapter()
{}

opengv::point_t
opengv::point_cloud::PointCloudAdapter::
    getPoint1( size_t index ) const
{
  assert(index < _points1.size());
  return _points1[index];
}

opengv::point_t
opengv::point_cloud::PointCloudAdapter::
    getPoint2( size_t index ) const
{
  assert(index < _points2.size());
  return _points2[index];
}

double
opengv::point_cloud::PointCloudAdapter::
    getWeight( size_t index ) const
{
  return 1.0;
}

size_t
opengv::point_cloud::PointCloudAdapter::
    getNumberCorrespondences() const
{
  return _points2.size();
}
