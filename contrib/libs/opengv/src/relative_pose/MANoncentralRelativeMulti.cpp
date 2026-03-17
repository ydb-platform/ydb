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


#include <opengv/relative_pose/MANoncentralRelativeMulti.hpp>

opengv::relative_pose::MANoncentralRelativeMulti::MANoncentralRelativeMulti(
    const std::vector<double*> & bearingVectors1,
    const std::vector<double*> & bearingVectors2,
    const double * camOffsets,
    const std::vector<int> & numberBearingVectors ) :
    _bearingVectors1(bearingVectors1),
    _bearingVectors2(bearingVectors2),
    _camOffsets(camOffsets),
    _numberBearingVectors(numberBearingVectors)
{  
  // The following variables are needed for the serialization and
  // de-serialization of indices
  size_t singleIndexOffset = 0;
  for( size_t pairIndex = 0; pairIndex < _numberBearingVectors.size(); pairIndex++ )
  {
    singleIndexOffsets.push_back(singleIndexOffset);
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < (unsigned int) _numberBearingVectors[pairIndex];
        correspondenceIndex++ )
    {
      multiPairIndices.push_back(pairIndex);
      multiKeypointIndices.push_back(correspondenceIndex);
    }
    singleIndexOffset += _numberBearingVectors[pairIndex];
  }
}

opengv::relative_pose::MANoncentralRelativeMulti::~MANoncentralRelativeMulti()
{}

opengv::bearingVector_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getBearingVector1( size_t pairIndex, size_t correspondenceIndex ) const
{
  assert(pairIndex < _numberBearingVectors.size());
  assert(correspondenceIndex < _numberBearingVectors[pairIndex]);
  
  bearingVector_t bearingVector;
  bearingVector[0] = _bearingVectors1[pairIndex][correspondenceIndex * 3];
  bearingVector[1] = _bearingVectors1[pairIndex][correspondenceIndex * 3 + 1];
  bearingVector[2] = _bearingVectors1[pairIndex][correspondenceIndex * 3 + 2];
  
  return bearingVector;
}

opengv::bearingVector_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getBearingVector2( size_t pairIndex, size_t correspondenceIndex ) const
{
  assert(pairIndex < _numberBearingVectors.size());
  assert(correspondenceIndex < _numberBearingVectors[pairIndex]);
  
  bearingVector_t bearingVector;
  bearingVector[0] = _bearingVectors2[pairIndex][correspondenceIndex * 3];
  bearingVector[1] = _bearingVectors2[pairIndex][correspondenceIndex * 3 + 1];
  bearingVector[2] = _bearingVectors2[pairIndex][correspondenceIndex * 3 + 2];
  
  return bearingVector;
}

double
opengv::relative_pose::MANoncentralRelativeMulti::
    getWeight( size_t pairIndex, size_t correspondenceIndex ) const
{
  return 1.0;
}

opengv::translation_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getCamOffset( size_t pairIndex ) const
{
  assert(pairIndex < _numberBearingVectors.size());
  translation_t camOffset;
  camOffset[0] = _camOffsets[pairIndex * 3];
  camOffset[1] = _camOffsets[pairIndex * 3 + 1];
  camOffset[2] = _camOffsets[pairIndex * 3 + 2];
  return camOffset;
}

opengv::rotation_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getCamRotation( size_t pairIndex ) const
{
  return Eigen::Matrix3d::Identity();
}

size_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getNumberCorrespondences(size_t pairIndex) const
{
  assert(pairIndex < _numberBearingVectors.size());
  return _numberBearingVectors[pairIndex];
}

size_t
opengv::relative_pose::MANoncentralRelativeMulti::
    getNumberPairs() const
{
  return _numberBearingVectors.size();
}

//important conversion between the serialized and the multi interface
std::vector<int>
opengv::relative_pose::MANoncentralRelativeMulti::
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
opengv::relative_pose::MANoncentralRelativeMulti::
    convertMultiIndex( size_t pairIndex, size_t correspondenceIndex ) const
{
  return singleIndexOffsets[pairIndex]+correspondenceIndex;
}

int
opengv::relative_pose::MANoncentralRelativeMulti::
    multiPairIndex( size_t index ) const
{
  return multiPairIndices[index];
}

int
opengv::relative_pose::MANoncentralRelativeMulti::
    multiCorrespondenceIndex( size_t index ) const
{
  return multiKeypointIndices[index];
}
