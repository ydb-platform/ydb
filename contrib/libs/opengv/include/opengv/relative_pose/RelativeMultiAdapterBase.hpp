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
 * \file RelativeMultiAdapterBase.hpp
 * \brief Adapter-class for passing bearing-vector correspondences to the
 *        relative-pose algorithms. Intended for multi-central-viewpoint or
 *        relative non-central-viewpoint problems. Access of correspondences
 *        etc. via an additional pair-index referring to the pairs of cameras.
 */

#ifndef OPENGV_RELATIVE_POSE_RELATIVEMULTIADAPTERBASE_HPP_
#define OPENGV_RELATIVE_POSE_RELATIVEMULTIADAPTERBASE_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/relative_pose/RelativeAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the relative pose methods.
 */
namespace relative_pose
{

/**
 * See the documentation of RelativeAdapterBase to understand the meaning of
 * a RelativeAdapter. RelativeMultiAdapterBase extends the interface of
 * RelativeAdapterBase by an additional pair-index for referring to pairs
 * of cameras. Intended for special central multi-viewpoint or non-central
 * relative viewpoint problems, allowing "camera-pair"-wise grouping of
 * correspondences. Derived classes need to implement functionalities for
 * deriving unique serialization of multi-indices.
 */
class RelativeMultiAdapterBase : public RelativeAdapterBase
{
protected:
  using RelativeAdapterBase::_t12;
  using RelativeAdapterBase::_R12;

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW
  
  /** See parent-class */
  RelativeMultiAdapterBase() :
      RelativeAdapterBase() {};
  /** See parent-class */
  RelativeMultiAdapterBase( const rotation_t & R12 ) :
      RelativeAdapterBase( R12 ) {};
  /** See parent-class */
  RelativeMultiAdapterBase( const translation_t & t12, const rotation_t & R12 ) :
      RelativeAdapterBase( t12, R12 ) {};
  /** See parent-class */
  virtual ~RelativeMultiAdapterBase() {};
  
  //camera-pair-wise access of correspondences
  
  /**
   * \brief Retrieve the bearing vector of a correspondence in camera 1 of a pair.
   * \param[in] pairIndex Index of the camera-pair.
   * \param[in] correspondenceIndex Index of the correspondence in the camera-pair.
   * \return The corresponding bearing vector.
   */
  virtual bearingVector_t getBearingVector1(
      size_t pairIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the bearing vector of a correspondence in camera 2 of a pair.
   * \param[in] pairIndex Index of the camera-pair.
   * \param[in] correspondenceIndex Index of the correspondence in the camera-pair.
   * \return The corresponding bearing vector.
   */
  virtual bearingVector_t getBearingVector2(
      size_t pairIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the weight of a correspondence. The weight is supposed to
   *        reflect the quality of a correspondence, and typically is between
   *        0 and 1.
   * \param[in] pairIndex Index of the camera-pair.
   * \param[in] correspondenceIndex Index of the correspondence in the camera-pair.
   * \return The corresponding weight.
   */
  virtual double getWeight(
      size_t pairIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Retrieve the position of the cameras of a camera-pair seen from the
   *        origin of the viewpoints (assumed to be the same in both
   *        viewpoints).
   * \param[in] pairIndex Index of the camera-pair.
   * \return The position of the camera seen from the viewpoint origin.
   */
  virtual translation_t getCamOffset( size_t pairIndex ) const = 0;
  /**
   * \brief Retrieve the rotation from the cameras of a camera-pair back to the
   *        origin of the viewpoints (assumed to be the same in both
   *        viewpoints).
   * \param[in] pairIndex Index of the camera-pair.
   * \return The rotation from the camera back to the viewpoint origin.
   */
  virtual rotation_t getCamRotation( size_t pairIndex ) const = 0;
  /**
   * \brief Retrieve the number of correspondences for a camera-pair.
   * \param[in] pairIndex Index of the camera-pair.
   * \return The number of correspondences in this camera-pair.
   */
  virtual size_t getNumberCorrespondences( size_t pairIndex ) const = 0;
  /**
   * \brief Retrieve the number of camera-pairs.
   * \return The number of camera-pairs.
   */
  virtual size_t getNumberPairs() const = 0;

  //Conversion to and from serialized indices
  
  /**
   * \brief Convert an array of (pairIndex,correspondenceIndex)-pairs into an
   *        array of serialized indices.
   * \param[in] multiIndices Array of (pairIndex,correspondenceIndex)-pairs.
   * \return Array of single serialized indices referring uniquely to
   *         (pairIndex,correspondenceIndex)-pairs.
   */
  virtual std::vector<int> convertMultiIndices(
      const std::vector<std::vector<int> > & multiIndices ) const = 0;
  /**
   * \brief Convert a (pairIndex,correspondenceIndex)-pair into a serialized index.
   * \param[in] pairIndex The index of the camera-pair.
   * \param[in] correspondenceIndex The index of the keypoint in the camera-pair.
   * \return Array of single serialized indices referring uniquely to
   *         (pairIndex,correspondenceIndex)-pairs.
   */
  virtual int convertMultiIndex(
      size_t pairIndex, size_t correspondenceIndex ) const = 0;
  /**
   * \brief Get the camera-pair-index corresponding to a serialized index.
   * \param[in] index The serialized index.
   * \return The camera-pair index.
   */
  virtual int multiPairIndex( size_t index ) const = 0;
  /**
   * \brief Get the keypoint-index in a camera-pair for a serialized index.
   * \param[in] index The serialized index.
   * \return The keyopint-index in the camera-pair.
   */
  virtual int multiCorrespondenceIndex( size_t index ) const = 0;

  //the classic interface (with serialized indices, used by the opengv-methods)
  
  /** See parent-class (no need to overload) */
  virtual bearingVector_t getBearingVector1( size_t index ) const
  {
    return getBearingVector1(
        multiPairIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual bearingVector_t getBearingVector2( size_t index ) const
  {
    return getBearingVector2(
        multiPairIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual double getWeight( size_t index ) const
  {
    return getWeight(
        multiPairIndex(index), multiCorrespondenceIndex(index) );
  }
  /** See parent-class (no need to overload) */
  virtual translation_t getCamOffset1( size_t index ) const
  { return getCamOffset( multiPairIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual rotation_t getCamRotation1( size_t index ) const
  { return getCamRotation( multiPairIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual translation_t getCamOffset2( size_t index ) const
  { return getCamOffset( multiPairIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual rotation_t getCamRotation2( size_t index ) const
  { return getCamRotation( multiPairIndex(index) ); }
  /** See parent-class (no need to overload) */
  virtual size_t getNumberCorrespondences() const
  {
    size_t numberCorrespondences = 0;
    for(size_t i = 0; i < getNumberPairs(); i++)
      numberCorrespondences += getNumberCorrespondences(i);
    return numberCorrespondences;
  }
};

}
}

#endif /* OPENGV_RELATIVE_POSE_RELATIVEMULTIADAPTERBASE_HPP_ */
