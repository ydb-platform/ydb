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


#include <opengv/relative_pose/MANoncentralRelative.hpp>

opengv::relative_pose::MANoncentralRelative::MANoncentralRelative(
      const double * bearingVectors1,
      const double * bearingVectors2,
      int numberBearingVectors1,
      int numberBearingVectors2 ) :
      _bearingVectors1(bearingVectors1),
      _bearingVectors2(bearingVectors2),
      _numberBearingVectors1(numberBearingVectors1),
      _numberBearingVectors2(numberBearingVectors2)
{}

opengv::relative_pose::MANoncentralRelative::~MANoncentralRelative()
{}

opengv::bearingVector_t
opengv::relative_pose::MANoncentralRelative::
    getBearingVector1( size_t index ) const
{
  bearingVector_t bearingVector;
  assert(index < _numberBearingVectors1);
  bearingVector[0] = _bearingVectors1[index * 6];
  bearingVector[1] = _bearingVectors1[index * 6 + 1];
  bearingVector[2] = _bearingVectors1[index * 6 + 2];
  return bearingVector;
}

opengv::bearingVector_t
opengv::relative_pose::MANoncentralRelative::
    getBearingVector2( size_t index ) const
{
  assert(index < _numberBearingVectors2);
  bearingVector_t bearingVector;
  bearingVector[0] = _bearingVectors2[index * 6];
  bearingVector[1] = _bearingVectors2[index * 6 + 1];
  bearingVector[2] = _bearingVectors2[index * 6 + 2];
  return bearingVector;
}

double
opengv::relative_pose::MANoncentralRelative::
    getWeight( size_t index ) const
{
  return 1.0;
}

opengv::translation_t
opengv::relative_pose::MANoncentralRelative::
    getCamOffset1( size_t index ) const
{
  translation_t camOffset;
  assert(index < _numberBearingVectors1);
  camOffset[0] = _bearingVectors1[index * 6 + 3];
  camOffset[1] = _bearingVectors1[index * 6 + 4];
  camOffset[2] = _bearingVectors1[index * 6 + 5];
  return camOffset;
}

opengv::rotation_t
opengv::relative_pose::MANoncentralRelative::
    getCamRotation1( size_t index ) const
{
  return Eigen::Matrix3d::Identity();
}

opengv::translation_t
opengv::relative_pose::MANoncentralRelative::
    getCamOffset2( size_t index ) const
{
  assert(index < _numberBearingVectors2);
  translation_t camOffset;
  camOffset[0] = _bearingVectors2[index * 6 + 3];
  camOffset[1] = _bearingVectors2[index * 6 + 4];
  camOffset[2] = _bearingVectors2[index * 6 + 5];
  return camOffset;
}

opengv::rotation_t
opengv::relative_pose::MANoncentralRelative::
    getCamRotation2( size_t index ) const
{
  return Eigen::Matrix3d::Identity();
}

size_t
opengv::relative_pose::MANoncentralRelative::
    getNumberCorrespondences() const
{
  return _numberBearingVectors2;
}
