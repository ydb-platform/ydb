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

#include <ctime>

template<typename M>
opengv::sac::MultiSampleConsensusProblem<M>::MultiSampleConsensusProblem(
    bool randomSeed) :
    max_sample_checks_(10)
{
  rng_dist_.reset(new std::uniform_int_distribution<>( 0, std::numeric_limits<int>::max () ));
  // Create a random number generator object
  if (randomSeed)
    rng_alg_.seed(static_cast<unsigned>(time(0)) + static_cast<unsigned>(clock()) );
  else
    rng_alg_.seed(12345u);

  rng_gen_.reset(new std::function<int()>(std::bind(*rng_dist_, rng_alg_)));
}

template<typename M>
opengv::sac::MultiSampleConsensusProblem<M>::~MultiSampleConsensusProblem()
{}

template<typename M>
bool
opengv::sac::MultiSampleConsensusProblem<M>::isSampleGood(
    const std::vector< std::vector<int> > & sample) const
{
  // Default implementation
  return true;
}

template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::drawIndexSample(
    std::vector< std::vector<int> > & sample)
{
  for( size_t subIter = 0; subIter < sample.size(); subIter++ )
  {
    size_t sample_size = sample[subIter].size();
    size_t index_size = shuffled_indices_[subIter].size();
    for( unsigned int i = 0; i < sample_size; ++i )
    {
      // The 1/(RAND_MAX+1.0) trick is when the random numbers are not uniformly
      // distributed and for small modulo elements, that does not matter
      // (and nowadays, random number generators are good)
      //std::swap (shuffled_indices_[i], shuffled_indices_[i + (rand () % (index_size - i))]);
      std::swap(
          shuffled_indices_[subIter][i],
          shuffled_indices_[subIter][i + (rnd() % (index_size - i))] );
    }
    std::copy(
        shuffled_indices_[subIter].begin (),
        shuffled_indices_[subIter].begin () + sample_size,
        sample[subIter].begin () );
  }
}

template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::getSamples(
    int &iterations, std::vector< std::vector<int> > &samples )
{
  std::vector<int> sampleSizes = getSampleSizes();
  samples.resize( sampleSizes.size() );

  for( size_t subIter = 0; subIter < samples.size(); subIter++ )
  {
    // We're assuming that indices_ have already been set in the constructor
    if( (*indices_)[subIter].size() < (size_t) sampleSizes[subIter] )
    {
      fprintf( stderr,
          "[sm::SampleConsensusModel::getSamples] Can not select %d unique points out of %zu!\n",
          sampleSizes[subIter], (*indices_)[subIter].size() );
      // one of these will make it stop :)
      samples.clear();
      iterations = std::numeric_limits<int>::max();
      return;
    }

    // Get a second point which is different than the first
    samples[subIter].resize( sampleSizes[subIter] );
  }

  for( int iter = 0; iter < max_sample_checks_; ++iter )
  {
    drawIndexSample(samples);

    // If it's a good sample, stop here
    if (isSampleGood (samples))
      return;
  }
  size_t multiSampleSize = 0;
  for( size_t multiIter = 0; multiIter < samples.size(); multiIter++ )
    multiSampleSize += samples[multiIter].size();

  fprintf( stdout,
      "[sm::SampleConsensusModel::getSamples] WARNING: Could not select %zu sample points in %d iterations!\n",
      multiSampleSize, max_sample_checks_ );
  samples.clear();
}

template<typename M>
std::shared_ptr< std::vector< std::vector<int> > >
opengv::sac::MultiSampleConsensusProblem<M>::getIndices() const
{
  return indices_;
}

template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::getDistancesToModel(
    const model_t & model_coefficients,
    std::vector<std::vector<double> > & distances )
{
  getSelectedDistancesToModel( model_coefficients, *indices_, distances );
}

template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::setUniformIndices(
    std::vector<int> N)
{
  indices_.reset( new std::vector< std::vector<int> >() );
  indices_->resize(N.size());
  for( size_t j = 0; j < N.size(); j++ )
  {
    (*indices_)[j].resize(N[j]);
    for( int i = 0; i < N[j]; ++i )
      (*indices_)[j][i] = i;
  }
  shuffled_indices_ = *indices_;
}

template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::setIndices(
    const std::vector< std::vector<int> > & indices )
{
    indices_.reset( new std::vector<std::vector<int> >(indices) );
    shuffled_indices_ = *indices_;
}


template<typename M>
int
opengv::sac::MultiSampleConsensusProblem<M>::rnd()
{
  return ((*rng_gen_) ());
}


template<typename M>
void
opengv::sac::MultiSampleConsensusProblem<M>::selectWithinDistance(
    const model_t & model_coefficients,
    const double threshold,
    std::vector<std::vector<int> > &inliers )
{
  std::vector<std::vector<double> > dist;
  dist.resize(indices_->size());
  inliers.clear();
  inliers.resize(indices_->size());

  for( size_t j = 0; j < indices_->size(); j++ )
    dist[j].reserve((*indices_)[j].size());

  getDistancesToModel( model_coefficients, dist );

  for( size_t j = 0; j < indices_->size(); j++ )
  {
    inliers[j].clear();
    inliers[j].reserve((*indices_)[j].size());
    for(size_t i = 0; i < dist[j].size(); ++i)
    {
      if( dist[j][i] < threshold )
        inliers[j].push_back( (*indices_)[j][i] );
    }
  }
}

template<typename M>
int
opengv::sac::MultiSampleConsensusProblem<M>::countWithinDistance(
    const model_t & model_coefficients, const double threshold )
{
  std::vector< std::vector<double> > dist;
  dist.resize(indices_->size());

  for( size_t j = 0; j < indices_->size(); j++ )
    dist[j].reserve((*indices_)[j].size());

  getDistancesToModel( model_coefficients, dist );

  int count = 0;
  for( size_t j = 0; j < indices_->size(); j++ )
  {
    for( size_t i = 0; i < dist[j].size(); ++i )
    {
      if( dist[j][i] < threshold )
        ++count;
    }
  }

  return count;
}
