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
 * \file NoncentralAbsoluteMultiAdapter.hpp
 * \brief Adapter-class for passing bearing-vector-to-point correspondences to
 *        the non-central absolute-pose algorithms. It maps opengv
 *        types back to opengv types. Manages multiple match-lists for each camera.
 *        This allows to draw samples homogeneously over the cameras.
 */

#ifndef OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEMULTIADAPTER_HPP_
#define OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEMULTIADAPTER_HPP_

#include <memory>
#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/absolute_pose/AbsoluteMultiAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the absolute pose methods.
 */
namespace absolute_pose
{

/**
 * Check the documentation of the parent-class to understand the meaning of
 * an AbsoluteAdapter. This child-class is for the non-central case and holds
 * data in form of references to opengv-types.
 */

class NoncentralAbsoluteMultiAdapter : public AbsoluteMultiAdapterBase
{
protected:
  using AbsoluteMultiAdapterBase::_t;
  using AbsoluteMultiAdapterBase::_R;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor. See protected class-members to understand parameters
   */
  NoncentralAbsoluteMultiAdapter(
      std::vector<std::shared_ptr<bearingVectors_t> > bearingVectors,
      std::vector<std::shared_ptr<points_t> > points,
      const translations_t & camOffsets,
      const rotations_t & camRotations );
  /**
   * \brief Destructor.
   */
  virtual ~NoncentralAbsoluteMultiAdapter();

  //camera-wise access of correspondences

  /** See parent-class */
  virtual point_t getPoint(
      size_t frameIndex, size_t correspondenceIndex ) const;
  /** See parent-class */
  virtual bearingVector_t getBearingVector(
      size_t frameIndex, size_t correspondenceIndex ) const;
  /** See parent-class */
  virtual double getWeight( size_t frameIndex, size_t correspondenceIndex ) const;
  /** See parent-class */
  virtual translation_t getMultiCamOffset( size_t frameIndex ) const;
  /** See parent-class */
  virtual rotation_t getMultiCamRotation( size_t frameIndex ) const;
  /** See parent-class */
  virtual size_t getNumberCorrespondences( size_t frameIndex ) const;
  /** See parent-class */
  virtual size_t getNumberFrames() const;

  //Conversion to and from serialized indices

  /** See parent-class */
  virtual std::vector<int> convertMultiIndices(
      const std::vector<std::vector<int> > & multiIndices ) const;
  /** See parent-class */
  virtual int convertMultiIndex(
      size_t frameIndex, size_t correspondenceIndex ) const;
  /** See parent-class */
  virtual int multiFrameIndex( size_t index ) const;
  /** See parent-class */
  virtual int multiCorrespondenceIndex( size_t index ) const;

protected:
  /** References to multiple sets of bearing-vectors (the ones from each camera).
   */
  std::vector<std::shared_ptr<bearingVectors_t> > _bearingVectors;
  /** References to multiple sets of points (the ones from each camera).
   */
  std::vector<std::shared_ptr<points_t> > _points;

  /** Reference to positions of the different cameras seen from the
   *  viewpoint.
   */
  const translations_t & _camOffsets;
  /** Reference to rotations from the different cameras back to the
   *  viewpoint.
   */
  const rotations_t & _camRotations;

  /** Initialized in constructor, used for (de)-serialiaztion of indices */
  std::vector<int> multiFrameIndices;
  /** Initialized in constructor, used for (de)-serialiaztion of indices */
  std::vector<int> multiKeypointIndices;
  /** Initialized in constructor, used for (de)-serialiaztion of indices */
  std::vector<int> singleIndexOffsets;
};

}
};

#endif /* OPENGV_ABSOLUTE_POSE_NONCENTRALABSOLUTEADAPTER_HPP_ */
