/******************************************************************************
 * Authors:  Johannes Mikulasch                                               *
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

//Note: has been derived from PCL and from Ransac.hpp which has been derived from ROS

template<typename P>
opengv::sac::Lmeds<P>::Lmeds(
    int maxIterations, double threshold, double probability) :
    SampleConsensus<P>(maxIterations, threshold, probability)
{}

template<typename P>
opengv::sac::Lmeds<P>::~Lmeds(){}

template<typename PROBLEM_T>
bool
opengv::sac::Lmeds<PROBLEM_T>::computeModel(int debug_verbosity_level)
{
  typedef PROBLEM_T problem_t;
  typedef typename problem_t::model_t model_t;

  // Warn and exit if no threshold was set
  if (threshold_ == std::numeric_limits<double>::max())
  {
	  fprintf(stderr,"[sm::LeastMedianSquares::computeModel] No threshold set!\n");
	  return (false);
  }

  iterations_ = 0;
  double d_best_penalty = std::numeric_limits<double>::max();

  std::vector<int> best_model;
  std::vector<int> selection;
  model_t model_coefficients;
  std::vector<double> distances;

  int n_inliers_count = 0;

  unsigned skipped_count = 0;
  // suppress infinite loops by just allowing 10 x maximum allowed iterations for
  // invalid model parameters!
  const unsigned max_skip = max_iterations_ * 10;

  if(debug_verbosity_level > 1)
    fprintf(stdout,
        "[sm::LeastMedianSquares::computeModel] Starting Least Median of Squares\n"
        "max_iterations: %d\n"
        "max_skip: %d\n",
        max_iterations_, max_skip);

  // Iterate
  while(iterations_ < max_iterations_ && skipped_count < max_skip)
  {
    // Get X samples which satisfy the model criteria
    sac_model_->getSamples(iterations_, selection);

    if(selection.empty()) 
    {
      fprintf(stderr, "[sm::LeastMedianSquares::computeModel] No samples could be selected!\n");
      break;
    }

    if(!sac_model_->computeModelCoefficients(selection, model_coefficients))
    {
      ++ skipped_count;
      continue;
    }

    double d_cur_penalty = 0;

    // Iterate through the 3d points and calculate the distances from them to the model
    distances.clear();
    sac_model_->getDistancesToModel(model_coefficients, distances);

    // No distances? The model must not respect the user given constraints
    if (distances.empty ())
    {
        ++skipped_count;
        continue;
    }

    // Clip distances smaller than 0. Square the distances
    for (std::size_t i = 0; i < distances.size(); ++i) {
    	if (distances[i] < 0) {
    		distances[i] = 0;
    	}
    	distances[i] = distances[i] * distances[i];
    }

    std::sort (distances.begin(), distances.end());

    size_t mid = sac_model_->getIndices()->size() / 2;
    if (mid >= distances.size())
    {
        ++skipped_count;
        continue;
    }

    // Do we have a "middle" point or should we "estimate" one ?
    if (sac_model_->getIndices()->size() % 2 == 0) {
        d_cur_penalty = (distances[mid-1] + distances[mid]) / 2;
    } else {
		d_cur_penalty = distances[mid];
    }

    // Better match ?
    if(d_cur_penalty < d_best_penalty)
    {
      d_best_penalty = d_cur_penalty;

      // Save the current model/inlier/coefficients selection as being the best so far
      model_              = selection;
      model_coefficients_ = model_coefficients;
    }

    ++iterations_;

    if(debug_verbosity_level > 1)
      fprintf(stdout,
          "[sm::LeastMedianSquares::computeModel] Trial %d out of %d. Best penalty is: %f so far. Current penalty is: %f\n",
          iterations_, max_iterations_, d_best_penalty, d_cur_penalty);
  }

  if(model_.empty())
  {
    if (debug_verbosity_level > 0)
		fprintf(stdout,"[sm::LeastMedianSquares::computeModel] Unable to find a solution!\n");
	return (false);
  }

  // Classify the data points into inliers and outliers
  // Sigma = 1.4826 * (1 + 5 / (n-d)) * sqrt (M)
  // @note: See "Robust Regression Methods for Computer Vision: A Review"
  //double sigma = 1.4826 * (1 + 5 / (sac_model_->getIndices ()->size () - best_model.size ())) * sqrt (d_best_penalty);
  //double threshold = 2.5 * sigma;

  // Iterate through the 3d points and calculate the distances from them to the model again
  distances.clear();
  sac_model_->getDistancesToModel(model_coefficients_, distances);
  // No distances? The model must not respect the user given constraints
  if (distances.empty())
  {
	  fprintf(stderr,"[sm::LeastMedianSquares::computeModel] The model found failed to verify against the given constraints!\n");
	  return (false);
  }

  std::vector<int> &indices = *sac_model_->getIndices();

  if (distances.size () != indices.size ())
  {
	  fprintf(stderr,"[sm::LeastMedianSquares::computeModel] Estimated distances (%zu) differs than the normal of indices (%zu).\n", distances.size (), indices.size ());
	  return false;
  }

  inliers_.resize(distances.size());
  // Get the inliers for the best model found
  n_inliers_count = 0;
  for (size_t i = 0; i < distances.size(); ++i)
	  if (distances[i] <= threshold_)
		  inliers_[n_inliers_count++] = indices[i];

  // Resize the inliers vector
  inliers_.resize(n_inliers_count);

  if (debug_verbosity_level > 0)
	  fprintf(stdout,"[sm::LeastMedianSquares::computeModel] Model: %zu size, %d inliers.\n", model_.size (), n_inliers_count);

  return (true);
}
