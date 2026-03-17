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
 * \file RotationOnlySacProblem.hpp
 * \brief Functions for fitting a rotation-only model to a set of bearing-vector
 *        correspondences. The viewpoints are assumed to be separated by
 *        rotation only. Used within a random-sample paradigm for rejecting
 *        outlier correspondences.
 */

#ifndef OPENGV_SAC_PROBLEMS_RELATIVE_POSE_ROTATIONONLYSACPROBLEM_HPP_
#define OPENGV_SAC_PROBLEMS_RELATIVE_POSE_ROTATIONONLYSACPROBLEM_HPP_

#include <opengv/sac/SampleConsensusProblem.hpp>
#include <opengv/types.hpp>
#include <opengv/relative_pose/RelativeAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the sample consensus problems.
 */
namespace sac_problems
{
/**
 * \brief The namespace for the relative pose methods.
 */
namespace relative_pose
{

/**
 * Functions for fitting a rotation-only model to a set of bearing-vector
 * correspondences (using twopt_rotationOnly). The viewpoints are assumed to
 * be separated by rotation only. Used within a random-sample paradigm for
 * rejecting outlier correspondences.
 */
class RotationOnlySacProblem :
    public sac::SampleConsensusProblem<rotation_t>
{
public:
  /** The model we are trying to fit (rotation) */
  typedef rotation_t model_t;
  /** The type of adapter that is expected by the methods */
  typedef opengv::relative_pose::RelativeAdapterBase adapter_t;


  /**
   * \brief Constructor.
   * \param[in] adapter Visitor holding bearing vector correspondences etc.
   * \param[in] randomSeed Whether to seed the random number generator with
   *            the current time.
   */
  RotationOnlySacProblem(adapter_t & adapter, bool randomSeed = true) :
    sac::SampleConsensusProblem<model_t> (randomSeed),
    _adapter(adapter)
  {
    setUniformIndices(adapter.getNumberCorrespondences());
  };

  /**
   * \brief Constructor.
   * \param[in] adapter Visitor holding bearing vector correspondences etc.
   * \param[in] indices A vector of indices to be used from all available
   *                    correspondences.
   * \param[in] randomSeed Whether to seed the random number generator with
   *            the current time.
   */
  RotationOnlySacProblem(
      adapter_t & adapter,
      const std::vector<int> & indices, bool randomSeed = true) :
      sac::SampleConsensusProblem<model_t> (randomSeed),
      _adapter(adapter)
  {
    setIndices(indices);
  };

  /**
   * Destructor.
   */
  virtual ~RotationOnlySacProblem() {};

  /**
   * \brief See parent-class.
   */
  virtual bool computeModelCoefficients(
      const std::vector<int> & indices,
      model_t & outModel) const;

  /**
   * \brief See parent-class.
   */
  virtual void getSelectedDistancesToModel(
      const model_t & model,
      const std::vector<int> & indices,
      std::vector<double> & scores) const;

  /**
   * \brief See parent-class.
   */
  virtual void optimizeModelCoefficients(
      const std::vector<int> & inliers,
      const model_t & model,
      model_t & optimized_model);

  /**
   * \brief See parent-class.
   */
  virtual int getSampleSize() const;

protected:
  /** The adapter holding all input data. */
  adapter_t & _adapter;
};

}
}
}

#endif  //#ifndef OPENGV_SAC_PROBLEMS_RELATIVE_POSE_ROTATIONONLYSACPROBLEM_HPP_
