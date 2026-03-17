/******************************************************************************
 * Authors:  Laurent Kneip & Paul Furgale                                     *
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

//Note: has been derived from ROS

/**
 * \file SampleConsensusProblem.hpp
 * \brief Basis-class for Sample-consensus problems. Contains declarations for
 *        the three basic functions of a sample-consensus problem (sample
 *        drawing, computation of a hypothesis, and verification of a
 *        hypothesis).
 */

#ifndef OPENGV_SAC_SAMPLECONSENSUSPROBLEM_HPP_
#define OPENGV_SAC_SAMPLECONSENSUSPROBLEM_HPP_

#include <stdlib.h>
#include <stdio.h>
#include <random>
#include <functional>
#include <memory>
#include <ctime>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the sample consensus methods.
 */
namespace sac
{

/**
 * Basis-class for Sample-consensus problems containing the three basic
 * functions for model-fitting.
 */
template<typename MODEL_T>
class SampleConsensusProblem
{
public:
  /** The model we are trying to fit */
  typedef MODEL_T model_t;

  /**
   * \brief Contructor.
   * \param[in] randomSeed Setting the seed of the random number generator with
   *                       something unique, namely the current time.
   */
  SampleConsensusProblem( bool randomSeed = true );
  /**
   * \brief Destructor.
   */
  virtual ~SampleConsensusProblem();

  /**
   * \brief Get samples for hypothesis generation.
   * \param[in] iterations We won't try forever to get a good sample, this
   *                       parameter keeps track of the iterations.
   * \param[out] samples The indices of the samples we attempt to use.
   */
  virtual void getSamples( int &iterations, std::vector<int> &samples );

  /**
   * \brief Check if a set of samples for model generation is degenerate
   * \param[in] sample The indices of the samples we attempt to use for
   *                   model instantiation.
   * \return Is this set of samples ok?
   */
  virtual bool isSampleGood( const std::vector<int> & sample ) const;

  /**
   * \brief Get a pointer to the vector of indices used.
   * \return A pointer to the vector of indices used.
   */
  std::shared_ptr< std::vector<int> > getIndices() const;

  /**
   * \brief Sub-function for getting samples for hypothesis generation.
   * \param[out] sample The indices of the samples we attempt to use.
   */
  void drawIndexSample( std::vector<int> & sample );

  /**
   * \brief Get the number of samples needed for a hypothesis generation.
   *        Needs implementation in the child class.
   * \return The number of samples needed for hypothesis generation.
   */
  virtual int getSampleSize() const = 0;

  /**
   * \brief Compute a model from a set of samples. Needs implementation in the
   *        child-class.
   * \param[in] indices The indices of the samples we use for the hypothesis.
   * \param[out] outModel The computed model.
   * \return Success?
   */
  virtual bool computeModelCoefficients(
      const std::vector<int> & indices,
      model_t & outModel) const = 0;

  /**
   * \brief Refine the model coefficients over a given set (inliers). Needs
   *        implementation in the child-class.
   * \param[in] inliers The indices of the inlier samples supporting the model.
   * \param[in] model_coefficients The initial guess for the model coefficients.
   * \param[out] optimized_coefficients The resultant refined coefficients.
   */
  virtual void optimizeModelCoefficients(
      const std::vector<int> & inliers,
      const model_t & model_coefficients,
      model_t & optimized_coefficients ) = 0;

  /**
   * \brief Compute the distances of all samples whith respect to given model
   *        coefficients. Needs implementation in the child-class.
   * \param[in] model The coefficients of the model hypothesis.
   * \param[in] indices The indices of the samples of which we compute distances.
   * \param[out] scores The resultant distances of the selected samples. Low
   *                    distances mean a good fit.
   */
  virtual void getSelectedDistancesToModel(
      const model_t & model,
      const std::vector<int> & indices,
      std::vector<double> & scores ) const = 0;

  /**
   * \brief Compute the distances of all samples which respect to given model
   *        coefficients.
   * \param[in] model_coefficients The coefficients of the model hypothesis.
   * \param[out] distances The resultant distances of all samples. Low distances
   *                       mean a good fit.
   */
  virtual void getDistancesToModel(
      const model_t & model_coefficients,
      std::vector<double> &distances );

  /**
   * \brief Select all the inlier samples whith respect to given model
   *        coefficients.
   * \param[in] model_coefficients The coefficients of the model hypothesis.
   * \param[in] threshold A maximum admissible distance threshold for
   *                      determining the inliers and outliers.
   * \param[out] inliers The resultant indices of inlier samples.
   */
  virtual void selectWithinDistance(
      const model_t &model_coefficients,
      const double threshold,
      std::vector<int> &inliers );

  /**
   * \brief Count all the inlier samples whith respect to given model
   *        coefficients.
   * \param[in] model_coefficients The coefficients of the model hypothesis.
   * \param[in] threshold A maximum admissible distance threshold for
   *                      determining the inliers and outliers.
   * \return The resultant number of inliers
   */
  virtual int countWithinDistance(
      const model_t &model_coefficients,
      const double threshold );

  /**
   * \brief Set the indices_ variable (see member-description).
   * \param[in] indices The indices we want to use.
   */
  void setIndices( const std::vector<int> & indices );

  /**
   * \brief Use this method if you want to use all samples.
   * \param[in] N The number of samples.
   */
  void setUniformIndices( int N );

  /**
   * \brief Get a random number.
   * \return A random number.
   */
  int rnd();



  /** The maximum number of times we try to extract a valid set of samples */
  int max_sample_checks_;

  /** The indices of the samples we are using for solving the entire
   *  problem. These are not the indices for generating a hypothesis, but
   *  all indices for model verification
   */
  std::shared_ptr< std::vector<int> > indices_;

  /** A shuffled version of the indices used for random sample drawing */
  std::vector<int> shuffled_indices_;

  /** \brief std-based random number generator algorithm. */
  std::mt19937 rng_alg_;

  /** \brief std-based random number generator distribution. */
  std::shared_ptr< std::uniform_int_distribution<> > rng_dist_;

  /** \brief std-based random number generator. */
  std::shared_ptr< std::function<int()> > rng_gen_;

};

} // namespace sac
} // namespace opengv

#include "implementation/SampleConsensusProblem.hpp"

#endif /* OPENGV_SAC_SAMPLECONSENSUSPROBLEM_HPP_ */
