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
 * \file MultiCentralRelativePoseSacProblem.hpp
 * \brief Functions for fitting a multi relative-pose model to sets of
 *        bearing-vector correspondences between multiple viewpoints. Exploits
 *        mutual agreement. Used within a multi random-sample paradigm for
 *        rejecting outlier correspondences. Experimental!
 */

#ifndef OPENGV_SAC_PROBLEMS_RELATIVE_POSE_MULTICENTRALRELATIVEPOSESACPROBLEM_HPP_
#define OPENGV_SAC_PROBLEMS_RELATIVE_POSE_MULTICENTRALRELATIVEPOSESACPROBLEM_HPP_

#include <opengv/sac/MultiSampleConsensusProblem.hpp>
#include <opengv/types.hpp>
#include <opengv/relative_pose/RelativeMultiAdapterBase.hpp>

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
 * Functions for fitting a multi relative-pose model to sets of
 * bearing-vector correspondences between multiple viewpoints. Exploits mutual
 * agreement. Used within a multi random-sample paradigm for rejecting outlier
 * correspondences. Experimental! Currently using eightpt [9,10].
 */
class MultiCentralRelativePoseSacProblem :
    public sac::MultiSampleConsensusProblem< transformations_t >
{
public:
  /** The model we are trying to fit (multiple transformations) */
  typedef transformations_t model_t;
  /** The type of adapter that is expected by the methods */
  typedef opengv::relative_pose::RelativeMultiAdapterBase adapter_t;

  /**
   * \brief Constructor.
   * \param[in] adapter Visitor holding bearing vector correspondences etc.
   * \param[in] sampleSize The number of samples for each "sub"-hypothesis.
   * \param[in] randomSeed Whether to seed the random number generator with
   *            the current time.
   */
  MultiCentralRelativePoseSacProblem(adapter_t & adapter, int sampleSize,
                                     bool randomSeed = true) :
    sac::MultiSampleConsensusProblem<model_t> (randomSeed),
    _adapter(adapter),
    _sampleSize(sampleSize)
  {
    std::vector<int> numberCorrespondences;
    for(size_t i = 0; i < adapter.getNumberPairs(); i++)
      numberCorrespondences.push_back(adapter.getNumberCorrespondences(i));
    setUniformIndices(numberCorrespondences);
  };

  /**
   * \brief Constructor.
   * \param[in] adapter Visitor holding bearing vector correspondences etc.
   * \param[in] indices A vector of multi-indices to be used from all available
   *                    correspondences.
   * \param[in] sampleSize The number of samples for each "sub"-hypothesis.
   * \param[in] randomSeed Whether to seed the random number generator with
   *            the current time.
   */
  MultiCentralRelativePoseSacProblem(
      adapter_t & adapter,
      const std::vector<std::vector<int> > & indices,
      int sampleSize, bool randomSeed = true) :
      sac::MultiSampleConsensusProblem<model_t> (randomSeed),
      _adapter(adapter),
      _sampleSize(sampleSize)
  {
    setIndices(indices);
  };

  /**
   * Destructor.
   */
  virtual ~MultiCentralRelativePoseSacProblem() {};

  /**
   * \brief See parent-class.
   */
  virtual bool computeModelCoefficients(
      const std::vector< std::vector<int> > & indices,
      model_t & outModel) const;

  /**
   * \brief See parent-class.
   */
  virtual void getSelectedDistancesToModel(
      const model_t & model,
      const std::vector<std::vector<int> > & indices,
      std::vector<std::vector<double> > & scores) const;

  /**
   * \brief See parent-class.
   */
  virtual void optimizeModelCoefficients(
      const std::vector<std::vector<int> > & inliers,
      const model_t & model,
      model_t & optimized_model);

  /**
   * \brief See parent-class.
   */
  virtual std::vector<int> getSampleSizes() const;

protected:
  /** The adapter holding all input data. */
  adapter_t & _adapter;
  /** The number of samples for each "sub"-hypothesis. */
  int _sampleSize;
};

}
}
}

#endif  //#ifndef OPENGV_SAC_PROBLEMS_RELATIVE_POSE_MULTICENTRALRELATIVEPOSESACPROBLEM_HPP_
