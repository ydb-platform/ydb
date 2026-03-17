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


#include <opengv/sac_problems/relative_pose/MultiCentralRelativePoseSacProblem.hpp>

#include <opengv/relative_pose/methods.hpp>
#include <opengv/triangulation/methods.hpp>
#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

bool
opengv::sac_problems::
    relative_pose::MultiCentralRelativePoseSacProblem::computeModelCoefficients(
    const std::vector<std::vector<int> > &indices,
    model_t & outModel) const
{
  std::vector<std::shared_ptr<translations_t> > multiTranslations;
  std::vector<std::shared_ptr<rotations_t> > multiRotations;

  for(size_t pairIndex = 0; pairIndex < _adapter.getNumberPairs(); pairIndex++)
  {
    std::vector<int> serializedIndices;
    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < indices[pairIndex].size();
        correspondenceIndex++ )
      serializedIndices.push_back(_adapter.convertMultiIndex(
          pairIndex, indices[pairIndex][correspondenceIndex] ));
    
    essential_t essentialMatrix =
        opengv::relative_pose::eightpt(_adapter,serializedIndices);

    //Decompose the essential matrix into rotations and translations
    std::shared_ptr<translations_t> translations(new translations_t());
    std::shared_ptr<rotations_t> rotations(new rotations_t());

    Eigen::Matrix3d W = Eigen::Matrix3d::Zero();
    W(0,1) = -1;
    W(1,0) = 1;
    W(2,2) = 1;

    Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
        essentialMatrix,
        Eigen::ComputeFullV | Eigen::ComputeFullU );
    Eigen::VectorXd singularValues = SVD.singularValues();

    // check for bad essential matrix
    if( singularValues[2] > 0.001 ) {};
    // continue; //singularity constraints not applied -> removed because too harsh
    if( singularValues[1] < 0.75 * singularValues[0] ) {};
    // continue; //bad essential matrix -> removed because too harsh

    // maintain scale
    double scale = singularValues[0];

    // get possible rotation and translation vectors
    rotation_t Ra = SVD.matrixU() * W * SVD.matrixV().transpose();
    rotation_t Rb = SVD.matrixU() * W.transpose() * SVD.matrixV().transpose();
    translation_t t = scale*SVD.matrixU().col(2);

    // change sign if det = -1
    if( Ra.determinant() < 0 ) Ra = -Ra;
    if( Rb.determinant() < 0 ) Rb = -Rb;

    //Store the decomposition, and already convert to our convention
    translations->push_back(t);
    translations->push_back(-t); //this is not needed actually!
    rotations->push_back(Ra);
    rotations->push_back(Rb);

    multiTranslations.push_back(translations);
    multiRotations.push_back(rotations);
  }


  for(size_t pairIndex = 0; pairIndex < _adapter.getNumberPairs(); pairIndex++)
  {
    //For each pair, find the right rotation and translation by our critera
    //
    //
    //fill outModel with that (it is a vector of transformations. A
    //transformation is 3x4 with R from frame 2 to 1, and position of 2 in 1)
  }

  return true;
}

void
opengv::sac_problems::
    relative_pose::MultiCentralRelativePoseSacProblem::getSelectedDistancesToModel(
    const model_t & model,
    const std::vector<std::vector<int> > & indices,
    std::vector<std::vector<double> > & scores) const
{
  Eigen::Matrix<double,4,1> p_hom;
  p_hom[3] = 1.0;

  for( size_t pairIndex = 0; pairIndex < indices.size(); pairIndex++ )
  {
    translation_t translation = model[pairIndex].col(3);
    rotation_t rotation = model[pairIndex].block<3,3>(0,0);

    for(
        size_t correspondenceIndex = 0;
        correspondenceIndex < indices[pairIndex].size();
        correspondenceIndex++ )
    {
      _adapter.sett12(translation);
      _adapter.setR12(rotation);

      transformation_t inverseSolution;
      inverseSolution.block<3,3>(0,0) = rotation.transpose();
      inverseSolution.col(3) =
          -inverseSolution.block<3,3>(0,0)*translation;

      p_hom.block<3,1>(0,0) =
          opengv::triangulation::triangulate2(
              _adapter,
              _adapter.convertMultiIndex(
              pairIndex, indices[pairIndex][correspondenceIndex] ));

      bearingVector_t reprojection1 = p_hom.block<3,1>(0,0);
      bearingVector_t reprojection2 = inverseSolution * p_hom;
      reprojection1 = reprojection1 / reprojection1.norm();
      reprojection2 = reprojection2 / reprojection2.norm();
      bearingVector_t f1 = _adapter.getBearingVector1(pairIndex,correspondenceIndex);
      bearingVector_t f2 = _adapter.getBearingVector2(pairIndex,correspondenceIndex);

      //bearing-vector based outlier criterium (select threshold accordingly):
      //1-(f1'*f2) = 1-cos(alpha) \in [0:2]
      double reprojError1 = 1.0 - (f1.transpose() * reprojection1);
      double reprojError2 = 1.0 - (f2.transpose() * reprojection2);
      scores[pairIndex].push_back(reprojError1 + reprojError2);
    }
  }
}

void
opengv::sac_problems::
    relative_pose::MultiCentralRelativePoseSacProblem::optimizeModelCoefficients(
    const std::vector<std::vector<int> > & inliers,
    const model_t & model,
    model_t & optimized_model)
{
  optimized_model = model; //todo: include non-linear optimization of model
}

std::vector<int>
opengv::sac_problems::
    relative_pose::MultiCentralRelativePoseSacProblem::getSampleSizes() const
{
  std::vector<int> sampleSizes;
  for(size_t pairIndex = 0; pairIndex < _adapter.getNumberPairs(); pairIndex++)
    sampleSizes.push_back(_sampleSize);

  return sampleSizes;
}
