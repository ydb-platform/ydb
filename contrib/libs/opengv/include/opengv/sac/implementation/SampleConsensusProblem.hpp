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
#include <functional>

#include <ctime>

template<typename M>
opengv::sac::SampleConsensusProblem<M>::SampleConsensusProblem(
    bool randomSeed) :
    max_sample_checks_(10)
{
  rng_dist_.reset(new std::uniform_int_distribution<>( 0, std::numeric_limits<int>::max() ));
  // Create a random number generator object
  if(randomSeed)
    rng_alg_.seed(static_cast<unsigned>(time(0)) + static_cast<unsigned>(clock()) );
  else
    rng_alg_.seed(12345u);

  rng_gen_.reset(new std::function<int()>(std::bind(*rng_dist_, rng_alg_)));
}

template<typename M>
opengv::sac::SampleConsensusProblem<M>::~SampleConsensusProblem()
{}

template<typename M>
bool opengv::sac::SampleConsensusProblem<M>::isSampleGood(
    const std::vector<int> & sample) const
{
  // Default implementation
  return true;
}

template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::drawIndexSample(
    std::vector<int> & sample)
{
  size_t sample_size = sample.size();
  size_t index_size = shuffled_indices_.size();
  for( unsigned int i = 0; i < sample_size; ++i )
  {
    // The 1/(RAND_MAX+1.0) trick is when the random numbers are not uniformly
    // distributed and for small modulo elements, that does not matter
    // (and nowadays, random number generators are good)
    //std::swap (shuffled_indices_[i], shuffled_indices_[i + (rand () % (index_size - i))]);
    std::swap(
        shuffled_indices_[i],
        shuffled_indices_[i + (rnd() % (index_size - i))] );
  }
  std::copy(
      shuffled_indices_.begin(),
      shuffled_indices_.begin() + sample_size,
      sample.begin() );
}


template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::getSamples(
    int &iterations, std::vector<int> &samples)
{
  // We're assuming that indices_ have already been set in the constructor
  if (indices_->size() < (size_t)getSampleSize())
  {
    fprintf( stderr,
        "[sm::SampleConsensusModel::getSamples] Can not select %zu unique points out of %zu!\n",
        (size_t) getSampleSize(), indices_->size() );
    // one of these will make it stop :)
    samples.clear();
    iterations = std::numeric_limits<int>::max();
    return;
  }

  // Get a second point which is different than the first
  samples.resize( getSampleSize() );

  for( int iter = 0; iter < max_sample_checks_; ++iter )
  {
    drawIndexSample(samples);

    // If it's a good sample, stop here
    if(isSampleGood(samples))
      return;
  }
  fprintf( stdout,
      "[sm::SampleConsensusModel::getSamples] WARNING: Could not select %d sample points in %d iterations!\n",
      getSampleSize(), max_sample_checks_ );
  samples.clear();

}

template<typename M>
std::shared_ptr< std::vector<int> >
opengv::sac::SampleConsensusProblem<M>::getIndices() const
{
  return indices_;
}

template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::getDistancesToModel(
    const model_t & model_coefficients, std::vector<double> & distances )
{
  getSelectedDistancesToModel( model_coefficients, *indices_, distances );
}

template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::setUniformIndices(int N)
{
  indices_.reset( new std::vector<int>() );
  indices_->resize(N);
  for( int i = 0; i < N; ++i )
    (*indices_)[i] = i;
  shuffled_indices_ = *indices_;
}

template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::setIndices(
    const std::vector<int> & indices )
{
  indices_.reset( new std::vector<int>(indices) );
  shuffled_indices_ = *indices_;
}


template<typename M>
int
opengv::sac::SampleConsensusProblem<M>::rnd()
{
  return ((*rng_gen_)());
}


template<typename M>
void
opengv::sac::SampleConsensusProblem<M>::selectWithinDistance(
    const model_t & model_coefficients,
    const double threshold,
    std::vector<int> &inliers )
{
  std::vector<double> dist;
  dist.reserve(indices_->size());
  getDistancesToModel( model_coefficients, dist );

  inliers.clear();
  inliers.reserve(indices_->size());
  for( size_t i = 0; i < dist.size(); ++i )
  {
    if( dist[i] < threshold )
      inliers.push_back( (*indices_)[i] );
  }
}

template<typename M>
int
opengv::sac::SampleConsensusProblem<M>::countWithinDistance(
    const model_t & model_coefficients, const double threshold)
{
  std::vector<double> dist;
  dist.reserve(indices_->size());
  getDistancesToModel( model_coefficients, dist );

  int count = 0;
  for( size_t i = 0; i < dist.size(); ++i )
  {
    if( dist[i] < threshold )
      ++count;
  }

  return count;
}
