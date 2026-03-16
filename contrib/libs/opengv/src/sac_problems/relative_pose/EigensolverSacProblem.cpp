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


#include <opengv/sac_problems/relative_pose/EigensolverSacProblem.hpp>

#include <opengv/relative_pose/methods.hpp>
#include <opengv/math/cayley.hpp>
#include <opengv/triangulation/methods.hpp>

bool
opengv::sac_problems::
    relative_pose::EigensolverSacProblem::computeModelCoefficients(
    const std::vector<int> &indices,
    model_t & outModel) const
{
  double maxVariation = 0.1; //****** 0.01 ******//

  //randomize the starting point a bit
  rotation_t rotation = _adapter.getR12();
  cayley_t cayley = math::rot2cayley(rotation);
  for( size_t i = 0; i < 3; i++ )
    cayley[i] = cayley[i] + (((double) rand())/ ((double) RAND_MAX)-0.5)*2.0*maxVariation;
  outModel.rotation = math::cayley2rot(cayley);

  opengv::relative_pose::eigensolver(_adapter,indices,outModel);

  return true;
}

void
opengv::sac_problems::
    relative_pose::EigensolverSacProblem::getSelectedDistancesToModel(
    const model_t & model,
    const std::vector<int> & indices,
    std::vector<double> & scores) const
{
  /*double referenceNorm = model.translation.norm();
  double pureRotation_threshold = 0.0001;

  double tanAlpha_threshold = tan(0.002);
  double norm_threshold = 0.001;//0.000625

  for(size_t i = 0; i < indices.size(); i++)
  {
    bearingVector_t f1 = _adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 = _adapter.getBearingVector2(indices[i]);
    Eigen::Vector3d n = f1.cross(model.rotation*f2);

    if( referenceNorm > pureRotation_threshold )
    {
      double np_norm = n.transpose() * model.eigenvectors.col(0);
      Eigen::Vector3d np = np_norm * model.eigenvectors.col(0);
      Eigen::Vector3d no = n - np;
      double maxDistance = norm_threshold + tanAlpha_threshold * no.norm();
      scores.push_back(np.norm()/maxDistance);
    }
    else
      scores.push_back(n.norm()/norm_threshold);
  }*/

  translation_t tempTranslation = _adapter.gett12();
  rotation_t tempRotation = _adapter.getR12();

  translation_t translation = model.translation;
  rotation_t rotation = model.rotation;
  _adapter.sett12(translation);
  _adapter.setR12(rotation);

  transformation_t inverseSolution;
  inverseSolution.block<3,3>(0,0) = rotation.transpose();
  inverseSolution.col(3) = -inverseSolution.block<3,3>(0,0)*translation;

  Eigen::Matrix<double,4,1> p_hom;
  p_hom[3] = 1.0;

  for( size_t i = 0; i < indices.size(); i++ )
  {
    p_hom.block<3,1>(0,0) =
        opengv::triangulation::triangulate2(_adapter,indices[i]);
    bearingVector_t reprojection1 = p_hom.block<3,1>(0,0);
    bearingVector_t reprojection2 = inverseSolution * p_hom;
    reprojection1 = reprojection1 / reprojection1.norm();
    reprojection2 = reprojection2 / reprojection2.norm();
    bearingVector_t f1 = _adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 = _adapter.getBearingVector2(indices[i]);

    //bearing-vector based outlier criterium (select threshold accordingly):
    //1-(f1'*f2) = 1-cos(alpha) \in [0:2]
    double reprojError1 = 1.0 - (f1.transpose() * reprojection1);
    double reprojError2 = 1.0 - (f2.transpose() * reprojection2);
    scores.push_back(reprojError1 + reprojError2);
  }

  _adapter.sett12(tempTranslation);
  _adapter.setR12(tempRotation);
}

void
opengv::sac_problems::
    relative_pose::EigensolverSacProblem::optimizeModelCoefficients(
    const std::vector<int> & inliers,
    const model_t & model,
    model_t & optimized_model)
{
  double maxVariation = 0.0;
  size_t maxIterations = 1;

  bool firstIteration = true;

  for(size_t i = 0; i < maxIterations; i++)
  {
    model_t temp_optimized_model;

    //randomize the starting point a bit
    rotation_t rotation = model.rotation;
    cayley_t cayley = math::rot2cayley(rotation);
    for( size_t i = 0; i < 3; i++ )
      cayley[i] = cayley[i] + (((double) rand())/ ((double) RAND_MAX)-0.5)*2.0*maxVariation;
    temp_optimized_model.rotation = math::cayley2rot(cayley);
    opengv::relative_pose::eigensolver(_adapter,inliers,temp_optimized_model,true);

    if(firstIteration)
    {
      optimized_model = temp_optimized_model;
      firstIteration = false;
    }
    else
    {
      if(temp_optimized_model.eigenvalues[0] < optimized_model.eigenvalues[0])
        optimized_model = temp_optimized_model;
    }
  }
}

int
opengv::sac_problems::
    relative_pose::EigensolverSacProblem::getSampleSize() const
{
  return _sampleSize;
}
