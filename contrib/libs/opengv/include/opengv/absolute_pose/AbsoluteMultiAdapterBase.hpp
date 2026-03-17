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
 * \file AbsoluteMultiAdapterBase.hpp
 * \brief Adapter-class for passing bearing-vector-to-point correspondences to
 *        the absolute-pose algorithms. Intended for absolute non-central-
 *        viewpoint problems. Access of correspondences etc. via an additional
 *        frame-index referring to the camera.
 */

#ifndef OPENGV_ABSOLUTE_POSE_ABSOLUTEMULTIADAPTERBASE_HPP_
#define OPENGV_ABSOLUTE_POSE_ABSOLUTEMULTIADAPTERBASE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/absolute_pose/AbsoluteAdapterBase.hpp>

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
 * See the documentation of AbsoluteAdapterBase to understand the meaning of
 * an AbsoluteAdapter. AbsoluteMultiAdapterBase extends the interface of
 * AbsoluteAdapterBase by an additional frame-index for referring to a
 * camera. Intended for non-central absolute viewpoint problems, allowing
 * camera-wise access of correspondences. Derived classes need to implement
 * functionalities for deriving unique serialization of multi-indices.
 */
class AbsoluteMultiAdapterBase : public AbsoluteAdapterBase
{
protected:
  using AbsoluteAdapterBase::_t;
  using AbsoluteAdapterBase::_R;
public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /**
   * \brief Constructor.
   */
  AbsoluteMultiAdapterBase() :
      AbsoluteAdapterBase() {};
  /**
   * \brief Constructor.
   * \param[in] R A prior or known value for the rotation from the viewpoint
   *              to the world frame.
   */
  AbsoluteMultiAdapterBase( const opengv::rotation_t & R ) :
      AbsoluteAdapterBase(R) {};
  /**
   * \brief Constructor.
   * \param[in] t A prior or known value for the position of the viewpoint seen
   *              from the world frame.
   * \param[in] R A prior or known value for the rotation from the viewpoint
   *              to the world frame.
   */
  AbsoluteMultiAdapterBase(
      const opengv::translation_t & t,
      const opengv::rotation_t & R ) :
      AbsoluteAdapterBase(t,R) {};
  /**
   * \brief Destructor.
   */
  virtual ~AbsoluteMultiAdapterBase() {};

  //camera-wise access of correspondences
  
  /**
   * \brief Retrieve the bearing vector of a correspondence in a certain frame.
   * \param[in] frameIndex Index of the frame.
   * \param[in] correspondenceIndex Index of the correspondence in this frame.
   * \return The corresponding bearing vector.
   */
  virtual opengv::bearingVector_t getBearingVector(
      size_t frameIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the weight of a correspondence. The weight is supposed to
   *        reflect the quality of a correspondence, and typically is between
   *        0 and 1.
   * \param[in] frameIndex Index of the frame.
   * \param[in] correspondenceIndex Index of the correspondence in this frame.
   * \return The corresponding weight.
   */
  virtual double getWeight(
      size_t frameIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the position of a camera seen from the viewpoint origin.
   * \param[in] frameIndex Index of the frame.
   * \return The position of the corresponding camera seen from the viewpoint
   *         origin.
   */
  virtual opengv::translation_t getMultiCamOffset( size_t frameIndex ) const = 0;
  /**
   * \brief Retrieve the rotation from a camera to the viewpoint frame.
   * \param[in] frameIndex Index of the frame.
   * \return The rotation from the corresponding camera back to the viewpoint
   *         origin.
   */
  virtual opengv::rotation_t getMultiCamRotation( size_t frameIndex ) const = 0;
  /**
   * \brief Retrieve the world point of a correspondence.
   * \param[in] frameIndex Index of the frame.
   * \param[in] correspondenceIndex Index of the correspondence in this frame.
   * \return The corresponding world point.
   */
  virtual opengv::point_t getPoint(
      size_t frameIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the number of correspondences for a camera.
   * \param[in] frameIndex Index of the camera.
   * \return The number of correspondences in this camera.
   */
  virtual size_t getNumberCorrespondences( size_t frameIndex ) const = 0;
  /**
   * \brief Retrieve the number of cameras.
   * \return The number of cameras.
   */
  virtual size_t getNumberFrames() const = 0;
  
  //Conversion to and from serialized indices
  
  /**
   * \brief Convert an array of (frameIndex,correspondenceIndex)-pairs into an
   *        array of serialized indices.
   * \param[in] multiIndices Array of (frameIndex,correspondenceIndex)-pairs.
   * \return Array of single serialized indices referring uniquely to
   *         (frameIndex,correspondenceIndex)-pairs.
   */
  virtual std::vector<int> convertMultiIndices(
      const std::vector<std::vector<int> > & multiIndices ) const = 0;
  /**
   * \brief Convert a (frameIndex,correspondenceIndex)-pair into a serialized
   *        index.
   * \param[in] frameIndex The index of the camera.
   * \param[in] correspondenceIndex The index of the correspondence in the camera.
   * \return Array of single serialized indices referring uniquely to
   *         (frameIndex,correspondenceIndex)-pairs.
   */
  virtual int convertMultiIndex(
      size_t frameIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Get the frame-index corresponding to a serialized index.
   * \param[in] index The serialized index.
   * \return The frame index.
   */
  virtual int multiFrameIndex( size_t index ) const = 0;
  /**
   * \brief Get the correspondence-index in a camera for a serialized index.
   * \param[in] index The serialized index.
   * \return The correspondence-index in the camera.
   */
  virtual int multiCorrespondenceIndex( size_t index ) const = 0;
  
  //the classic interface (with serialized indices, used by the opengv-methods)
  
  /** See parent-class (no need to overload) */
  virtual bearingVector_t getBearingVector( size_t index ) const
  {
    return getBearingVector(
        multiFrameIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual double getWeight( size_t index ) const
  {
    return getWeight(
        multiFrameIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual translation_t getCamOffset( size_t index ) const
  { return getMultiCamOffset( multiFrameIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual rotation_t getCamRotation( size_t index ) const
  { return getMultiCamRotation( multiFrameIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual point_t getPoint( size_t index ) const
  {
    return getPoint(
        multiFrameIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual size_t getNumberCorrespondences() const
  {
    size_t numberCorrespondences = 0;
    for(size_t i = 0; i < getNumberFrames(); i++)
      numberCorrespondences += getNumberCorrespondences(i);
    return numberCorrespondences;
  }
};

}
};

#endif /* OPENGV_ABSOLUTE_POSE_ABSOLUTEMULTIADAPTERBASE_HPP_ */
