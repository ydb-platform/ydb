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


#include <opengv/absolute_pose/NoncentralAbsoluteMultiAdapter.hpp>

opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::NoncentralAbsoluteMultiAdapter(
    std::vector<std::shared_ptr<bearingVectors_t> > bearingVectors,
    std::vector<std::shared_ptr<points_t> > points,
    const translations_t & camOffsets,
    const rotations_t & camRotations ) :
    _bearingVectors(bearingVectors),
    _points(points),
    _camOffsets(camOffsets),
    _camRotations(camRotations)
{
  // The following variables are needed for the serialization and
  // de-serialization of indices

  size_t singleIndexOffset = 0;
  for( size_t frameIndex = 0; frameIndex < bearingVectors.size(); frameIndex++ )
  {
    singleIndexOffsets.push_back(singleIndexOffset);
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < bearingVectors[frameIndex]->size();
        correspondenceIndex++ )
    {
      multiFrameIndices.push_back(frameIndex);
      multiKeypointIndices.push_back(correspondenceIndex);
    }
    singleIndexOffset += bearingVectors[frameIndex]->size();
  }
}

opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::~NoncentralAbsoluteMultiAdapter()
{}

opengv::point_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getPoint( size_t frameIndex, size_t correspondenceIndex ) const
{
  assert(frameIndex < _points.size());
  assert(correspondenceIndex < _points[frameIndex]->size());

  return (*_points[frameIndex])[correspondenceIndex];
}

opengv::bearingVector_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getBearingVector( size_t frameIndex, size_t correspondenceIndex ) const
{
  assert(frameIndex < _bearingVectors.size());
  assert(correspondenceIndex < _bearingVectors[frameIndex]->size());

  return (*_bearingVectors[frameIndex])[correspondenceIndex];
}

double
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getWeight( size_t frameIndex, size_t correspondenceIndex ) const
{
  return 1.0;
}

opengv::translation_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getMultiCamOffset( size_t frameIndex ) const
{
  assert(frameIndex < _camOffsets.size());
  return _camOffsets[frameIndex];
}

opengv::rotation_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getMultiCamRotation( size_t frameIndex ) const
{
  assert(frameIndex < _camRotations.size());
  return _camRotations[frameIndex];
}

size_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getNumberCorrespondences(size_t frameIndex) const
{
  assert(frameIndex < _bearingVectors.size());

  return _bearingVectors[frameIndex]->size();
}

size_t
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    getNumberFrames() const
{
  return _camOffsets.size();
}

//important conversion between the serialized and the multi interface
std::vector<int>
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    convertMultiIndices( const std::vector<std::vector<int> > & multiIndices ) const
{
  std::vector<int> singleIndices;
  for(size_t frameIndex = 0; frameIndex < multiIndices.size(); frameIndex++)
  {
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < multiIndices[frameIndex].size();
        correspondenceIndex++ )
    {
      singleIndices.push_back(convertMultiIndex(
          frameIndex, multiIndices[frameIndex][correspondenceIndex] ));
    }
  }

  return singleIndices;
}

int
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    convertMultiIndex( size_t frameIndex, size_t correspondenceIndex ) const
{
  return singleIndexOffsets[frameIndex]+correspondenceIndex;
}

int
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    multiFrameIndex( size_t index ) const
{
  return multiFrameIndices[index];
}

int
opengv::absolute_pose::NoncentralAbsoluteMultiAdapter::
    multiCorrespondenceIndex( size_t index ) const
{
  return multiKeypointIndices[index];
}
