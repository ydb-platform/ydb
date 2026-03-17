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


#include <opengv/relative_pose/CentralRelativeMultiAdapter.hpp>

opengv::relative_pose::CentralRelativeMultiAdapter::CentralRelativeMultiAdapter(
    std::vector<std::shared_ptr<bearingVectors_t> > bearingVectors1,
    std::vector<std::shared_ptr<bearingVectors_t> > bearingVectors2 ) :
    _bearingVectors1(bearingVectors1),
    _bearingVectors2(bearingVectors2)
{
  // The following variables are needed for the serialization and
  // de-serialization of indices
  size_t singleIndexOffset = 0;
  for( size_t pairIndex = 0; pairIndex < bearingVectors2.size(); pairIndex++ )
  {
    singleIndexOffsets.push_back(singleIndexOffset);
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < bearingVectors2[pairIndex]->size();
        correspondenceIndex++ )
    {
      multiPairIndices.push_back(pairIndex);
      multiKeypointIndices.push_back(correspondenceIndex);
    }
    singleIndexOffset += bearingVectors2[pairIndex]->size();
  }
}
  
opengv::relative_pose::CentralRelativeMultiAdapter::~CentralRelativeMultiAdapter()
{}

//The multi interface!!
opengv::bearingVector_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getBearingVector1( size_t pairIndex, size_t correspondenceIndex ) const
{
  assert(pairIndex < _bearingVectors1.size());

  assert(correspondenceIndex < _bearingVectors1[pairIndex]->size());
  return (*_bearingVectors1[pairIndex])[correspondenceIndex];
}

opengv::bearingVector_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getBearingVector2( size_t pairIndex, size_t correspondenceIndex ) const
{
  assert(pairIndex < _bearingVectors2.size());

  assert(correspondenceIndex < _bearingVectors2[pairIndex]->size());
  return (*_bearingVectors2[pairIndex])[correspondenceIndex];
}

double
opengv::relative_pose::CentralRelativeMultiAdapter::
    getWeight( size_t pairIndex, size_t correspondenceIndex ) const
{
  return 1.0;
}

opengv::translation_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getCamOffset( size_t pairIndex ) const
{
  return Eigen::Vector3d::Zero();
}

opengv::rotation_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getCamRotation( size_t pairIndex ) const
{
  return Eigen::Matrix3d::Identity();
}

size_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getNumberCorrespondences(size_t pairIndex) const
{
  assert(pairIndex < _bearingVectors2.size());
  return _bearingVectors2[pairIndex]->size();
}

size_t
opengv::relative_pose::CentralRelativeMultiAdapter::
    getNumberPairs() const
{
  return _bearingVectors2.size();
}

//important conversion function for the serialization of indices
std::vector<int>
opengv::relative_pose::CentralRelativeMultiAdapter::
    convertMultiIndices( const std::vector<std::vector<int> > & multiIndices ) const
{
  std::vector<int> singleIndices;
  for(size_t pairIndex = 0; pairIndex < multiIndices.size(); pairIndex++)
  {
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < multiIndices[pairIndex].size();
        correspondenceIndex++ )
      singleIndices.push_back(convertMultiIndex(
          pairIndex, multiIndices[pairIndex][correspondenceIndex] ));
  }

  return singleIndices;
}

int
opengv::relative_pose::CentralRelativeMultiAdapter::
    convertMultiIndex( size_t pairIndex, size_t correspondenceIndex ) const
{
  return singleIndexOffsets[pairIndex]+correspondenceIndex;
}

int
opengv::relative_pose::CentralRelativeMultiAdapter::
    multiPairIndex( size_t index ) const
{
  return multiPairIndices[index];
}

int
opengv::relative_pose::CentralRelativeMultiAdapter::
    multiCorrespondenceIndex( size_t index ) const
{
  return multiKeypointIndices[index];
}
