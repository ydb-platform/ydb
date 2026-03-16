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


#include <opengv/sac_problems/absolute_pose/MultiNoncentralAbsolutePoseSacProblem.hpp>
#include <opengv/absolute_pose/methods.hpp>

bool
opengv::sac_problems::
    absolute_pose::MultiNoncentralAbsolutePoseSacProblem::computeModelCoefficients(
      const std::vector< std::vector<int> > & indices,
      model_t & outModel) const
{
  transformations_t solutions;
  std::vector<int> stratifiedIndices = _adapter.convertMultiIndices(indices);

  if(!_asCentral)
  {
    solutions = opengv::absolute_pose::gp3p(
        _adapter,stratifiedIndices);
  }
  else
  {
    solutions = opengv::absolute_pose::p3p_kneip(
        _adapter,stratifiedIndices);

    //transform solution into body frame (case of single shifted cam)
    translation_t t_bc = _adapter.getCamOffset(0);
    rotation_t R_bc = _adapter.getCamRotation(0);

    for(size_t i = 0; i < solutions.size(); i++)
    {
      translation_t translation = solutions[i].col(3);
      rotation_t rotation = solutions[i].block<3,3>(0,0);
      solutions[i].col(3) = translation - rotation * R_bc.transpose() * t_bc;
      solutions[i].block<3,3>(0,0) = rotation * R_bc.transpose();
    }
  }
  
  if( solutions.size() == 1 )
  {
    outModel = solutions[0];
    return true;
  }
  
  //now compute reprojection error of fourth point, in order to find the right one
  double minScore = 1000000.0;
  int minIndex = -1;
  for(size_t i = 0; i < solutions.size(); i++)
  {
    //compute inverse transformation
    model_t inverseSolution;
    inverseSolution.block<3,3>(0,0) = solutions[i].block<3,3>(0,0).transpose();
    inverseSolution.col(3) = -inverseSolution.block<3,3>(0,0)*solutions[i].col(3);

    //get the fourth point in homogeneous form
    Eigen::Matrix<double,4,1> p_hom;
    p_hom.block<3,1>(0,0) = _adapter.getPoint(stratifiedIndices[3]);
    p_hom[3] = 1.0;

    //compute the reprojection (this is working for both central and
    //non-central case)
    point_t bodyReprojection = inverseSolution * p_hom;
    point_t reprojection =
        _adapter.getCamRotation(stratifiedIndices[3]).transpose() *
        (bodyReprojection - _adapter.getCamOffset(stratifiedIndices[3]));
    reprojection = reprojection / reprojection.norm();

    //compute the score
    double score =
        1.0 - (reprojection.transpose() * _adapter.getBearingVector(stratifiedIndices[3]));

    //check for best solution
    if( score < minScore )
    {
      minScore = score;
      minIndex = i;
    }
  }

  if(minIndex == -1)
    return false;
  outModel = solutions[minIndex];

  return true;
}

void
opengv::sac_problems::
    absolute_pose::MultiNoncentralAbsolutePoseSacProblem::getSelectedDistancesToModel(
    const model_t & model,
    const std::vector<std::vector<int> > & indices,
    std::vector<std::vector<double> > & scores) const
{
  //compute the reprojection error of all points

  //compute inverse transformation
  model_t inverseSolution;
  inverseSolution.block<3,3>(0,0) = model.block<3,3>(0,0).transpose();
  inverseSolution.col(3) = -inverseSolution.block<3,3>(0,0)*model.col(3);

  Eigen::Matrix<double,4,1> p_hom;
  p_hom[3] = 1.0;

  for(size_t f = 0; f < indices.size(); f++ )
  {
    for(size_t i = 0; i < indices[f].size(); i++)
    {
      //get point in homogeneous form
      p_hom.block<3,1>(0,0) = _adapter.getPoint(f,indices[f][i]);

      //compute the reprojection (this is working for both central and
      //non-central case)
      point_t bodyReprojection = inverseSolution * p_hom;
      point_t reprojection =
          _adapter.getMultiCamRotation(f).transpose() *
          (bodyReprojection - _adapter.getMultiCamOffset(f));
      reprojection = reprojection / reprojection.norm();

      //compute the score
      scores[f].push_back(
          1.0 - (reprojection.transpose() * _adapter.getBearingVector(f,indices[f][i])));
    }
  }
}

void
opengv::sac_problems::
    absolute_pose::MultiNoncentralAbsolutePoseSacProblem::optimizeModelCoefficients(
    const std::vector< std::vector<int> > & inliers,
    const model_t & model,
    model_t & optimized_model)
{
  _adapter.sett(model.col(3));
  _adapter.setR(model.block<3,3>(0,0));
  optimized_model = opengv::absolute_pose::optimize_nonlinear(_adapter,_adapter.convertMultiIndices(inliers));
}

std::vector<int>
opengv::sac_problems::
    absolute_pose::MultiNoncentralAbsolutePoseSacProblem::getSampleSizes() const
{
  std::vector<int> sampleSizes;
  for( size_t i = 0; i < _adapter.getNumberFrames(); i++ )
    sampleSizes.push_back(0);

  int sampleSize = 4;

  //set it to a random cam index where to start
  size_t binIndex = floor(((double) _adapter.getNumberFrames()) *
      ((double) rand()) / ((double) RAND_MAX));
  for( int i = 0; i < sampleSize; i++ )
  {
    sampleSizes[binIndex]++;
    binIndex++;
    if(binIndex >= sampleSizes.size())
      binIndex = 0;
  }

  return sampleSizes;
}
