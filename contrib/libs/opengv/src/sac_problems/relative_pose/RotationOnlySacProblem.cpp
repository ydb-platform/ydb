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


#include <opengv/sac_problems/relative_pose/RotationOnlySacProblem.hpp>
#include <opengv/relative_pose/methods.hpp>
#include <opengv/triangulation/methods.hpp>
#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

bool
opengv::sac_problems::
    relative_pose::RotationOnlySacProblem::computeModelCoefficients(
    const std::vector<int> &indices,
    model_t & outModel) const
{
  outModel = opengv::relative_pose::twopt_rotationOnly(_adapter,indices);

  return true;
}

void
opengv::sac_problems::
    relative_pose::RotationOnlySacProblem::getSelectedDistancesToModel(
    const model_t & model,
    const std::vector<int> & indices,
    std::vector<double> & scores) const
{
  for( size_t i = 0; i < indices.size(); i++ )
  {
    bearingVector_t f1 = _adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 = _adapter.getBearingVector2(indices[i]);

    //unrotate bearing-vector f2
    bearingVector_t f2_unrotated = model * f2;

    //bearing-vector based outlier criterium (select threshold accordingly):
    //1-(f1'*f2) = 1-cos(alpha) \in [0:2]
    double error = 1.0 - (f1.transpose() * f2_unrotated);
    scores.push_back(error);
  }
}

void
opengv::sac_problems::
    relative_pose::RotationOnlySacProblem::optimizeModelCoefficients(
    const std::vector<int> & inliers,
    const model_t & model,
    model_t & optimized_model)
{
  optimized_model = opengv::relative_pose::rotationOnly(_adapter,inliers);
}

int
opengv::sac_problems::
    relative_pose::RotationOnlySacProblem::getSampleSize() const
{
  int sampleSize = 2;
  return sampleSize;
}
