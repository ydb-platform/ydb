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


#include <opengv/absolute_pose/MANoncentralAbsolute.hpp>

opengv::absolute_pose::MANoncentralAbsolute::MANoncentralAbsolute(
    const double * points,
    const double * bearingVectors,
    int numberPoints,
    int numberBearingVectors ) :
    _points(points),
    _bearingVectors(bearingVectors),
    _numberPoints(numberPoints),
    _numberBearingVectors(numberBearingVectors)
{}

opengv::absolute_pose::MANoncentralAbsolute::~MANoncentralAbsolute()
{}

opengv::bearingVector_t
opengv::absolute_pose::MANoncentralAbsolute::
    getBearingVector( size_t index ) const
{
  assert(index < _numberBearingVectors);
  bearingVector_t bearingVector;
  bearingVector[0] = _bearingVectors[index * 6];
  bearingVector[1] = _bearingVectors[index * 6 + 1];
  bearingVector[2] = _bearingVectors[index * 6 + 2];
  return bearingVector;
}

double
opengv::absolute_pose::MANoncentralAbsolute::
    getWeight( size_t index ) const
{
  return 1.0;
}

opengv::point_t
opengv::absolute_pose::MANoncentralAbsolute::
    getPoint( size_t index ) const
{
  point_t point;
  assert(index < _numberPoints);
  point[0] = _points[index * 3];
  point[1] = _points[index * 3 + 1];
  point[2] = _points[index * 3 + 2];
  return point;
}

opengv::translation_t
opengv::absolute_pose::MANoncentralAbsolute::getCamOffset(
    size_t index ) const
{
  assert(index < _numberBearingVectors);
  translation_t camOffset;
  camOffset[0] = _bearingVectors[index * 6 + 3];
  camOffset[1] = _bearingVectors[index * 6 + 4];
  camOffset[2] = _bearingVectors[index * 6 + 5];
  return camOffset;
}

opengv::rotation_t
opengv::absolute_pose::MANoncentralAbsolute::getCamRotation(
    size_t index ) const
{
  return Eigen::Matrix3d::Identity();
}

size_t
opengv::absolute_pose::MANoncentralAbsolute::
    getNumberCorrespondences() const
{
  return _numberBearingVectors;
}
