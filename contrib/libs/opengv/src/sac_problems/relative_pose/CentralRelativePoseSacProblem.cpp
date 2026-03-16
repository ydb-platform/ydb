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


#include <opengv/sac_problems/relative_pose/CentralRelativePoseSacProblem.hpp>
#include <opengv/relative_pose/methods.hpp>
#include <opengv/triangulation/methods.hpp>
#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

bool
opengv::sac_problems::
    relative_pose::CentralRelativePoseSacProblem::computeModelCoefficients(
    const std::vector<int> &indices,
    model_t & outModel) const
{
  essentials_t essentialMatrices;

  switch(_algorithm)
  {
  case NISTER:
  {
    std::vector<int> subIndices1;
    for(size_t i = 0; i < 5; i++) subIndices1.push_back(indices[i]);
    essentialMatrices =
        opengv::relative_pose::fivept_nister(_adapter,subIndices1);
    break;
  }
  case STEWENIUS:
  {
    std::vector<int> subIndices2;
    for(size_t i = 0; i < 5; i++) subIndices2.push_back(indices[i]);
    complexEssentials_t complexEssentialMatrices =
        opengv::relative_pose::fivept_stewenius(_adapter,subIndices2);
    // convert from complexEssential to essential
    for(size_t i = 0; i < complexEssentialMatrices.size(); i++)
    {
      essential_t essentialMatrix;
      for(size_t r = 0; r < 3; r++)
      {
        for(size_t c = 0; c < 3; c++)
          essentialMatrix(r,c) = complexEssentialMatrices.at(i)(r,c).real();
      }
      essentialMatrices.push_back(essentialMatrix);
    }
    break;
  }
  case SEVENPT:
  {
    std::vector<int> subIndices3;
    for(size_t i = 0; i < 7; i++) subIndices3.push_back(indices[i]);
    essentialMatrices =
        opengv::relative_pose::sevenpt(_adapter,subIndices3);
    break;
  }
  case EIGHTPT:
  {
    std::vector<int> subIndices4;
    for(size_t i = 0; i < 8; i++) subIndices4.push_back(indices[i]);
    essential_t essentialMatrix =
        opengv::relative_pose::eightpt(_adapter,subIndices4);
    essentialMatrices.push_back(essentialMatrix);
    break;
  }
  }

  //now decompose each essential matrix into transformations and find the
  //right one
  Eigen::Matrix3d W = Eigen::Matrix3d::Zero();
  W(0,1) = -1;
  W(1,0) = 1;
  W(2,2) = 1;

  double bestQuality = 1000000.0;
  int bestQualityIndex = -1;
  int bestQualitySubindex = -1;

  for(size_t i = 0; i < essentialMatrices.size(); i++)
  {
    // decompose
    Eigen::MatrixXd tempEssential = essentialMatrices[i];
    Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
        tempEssential,
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
    translation_t ta = scale*SVD.matrixU().col(2);
    translation_t tb = -ta;

    // change sign if det = -1
    if( Ra.determinant() < 0 ) Ra = -Ra;
    if( Rb.determinant() < 0 ) Rb = -Rb;

    //derive transformations
    transformation_t transformation;
    transformations_t transformations;
    transformation.col(3) = ta;
    transformation.block<3,3>(0,0) = Ra;
    transformations.push_back(transformation);
    transformation.col(3) = ta;
    transformation.block<3,3>(0,0) = Rb;
    transformations.push_back(transformation);
    transformation.col(3) = tb;
    transformation.block<3,3>(0,0) = Ra;
    transformations.push_back(transformation);
    transformation.col(3) = tb;
    transformation.block<3,3>(0,0) = Rb;
    transformations.push_back(transformation);
    
    // derive inverse transformations
    transformations_t inverseTransformations;
    for(size_t j = 0; j < 4; j++)
    {
      transformation_t inverseTransformation;
      inverseTransformation.block<3,3>(0,0) =
          transformations[j].block<3,3>(0,0).transpose();
      inverseTransformation.col(3) =
          -inverseTransformation.block<3,3>(0,0)*transformations[j].col(3);
      inverseTransformations.push_back(inverseTransformation);
    }

    // collect qualities for each of the four solutions solution
    Eigen::Matrix<double,4,1> p_hom;
    p_hom[3] = 1.0;

    for(size_t j = 0; j<4; j++)
    {
      // prepare variables for triangulation and reprojection
      _adapter.sett12(transformations[j].col(3));
      _adapter.setR12(transformations[j].block<3,3>(0,0));

      // go through all features and compute quality of reprojection
      double quality = 0.0;

      for( int k = 0; k < getSampleSize(); k++ )
      {
        p_hom.block<3,1>(0,0) =
            opengv::triangulation::triangulate2(_adapter,indices[k]);
        bearingVector_t reprojection1 = p_hom.block<3,1>(0,0);
        bearingVector_t reprojection2 = inverseTransformations[j] * p_hom;
        reprojection1 = reprojection1 / reprojection1.norm();
        reprojection2 = reprojection2 / reprojection2.norm();
        bearingVector_t f1 = _adapter.getBearingVector1(indices[k]);
        bearingVector_t f2 = _adapter.getBearingVector2(indices[k]);

        // bearing-vector based outlier criterium (select threshold accordingly):
        // 1-(f1'*f2) = 1-cos(alpha) \in [0:2]
        double reprojError1 = 1.0 - (f1.transpose() * reprojection1);
        double reprojError2 = 1.0 - (f2.transpose() * reprojection2);
        quality += reprojError1 + reprojError2;
      }

      // is quality better? (lower)
      if( quality < bestQuality )
      {
        bestQuality = quality;
        bestQualityIndex = i;
        bestQualitySubindex = j;
      }
    }
  }

  if( bestQualityIndex == -1 )
    return false; // no solution found
  else
  {
    // rederive the best solution
    // decompose
    Eigen::MatrixXd tempEssential = essentialMatrices[bestQualityIndex];
    Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
        tempEssential,
        Eigen::ComputeFullV | Eigen::ComputeFullU );
    const Eigen::VectorXd singularValues = SVD.singularValues();

    // maintain scale
    const double scale = singularValues[0];

    // get possible rotation and translation vectors
    translation_t translation;
    rotation_t rotation;

    switch(bestQualitySubindex)
    {
    case 0:
      translation = scale*SVD.matrixU().col(2);
      rotation = SVD.matrixU() * W * SVD.matrixV().transpose();
      break;
    case 1:
      translation = scale*SVD.matrixU().col(2);
      rotation = SVD.matrixU() * W.transpose() * SVD.matrixV().transpose();
      break;
    case 2:
      translation = -scale*SVD.matrixU().col(2);
      rotation = SVD.matrixU() * W * SVD.matrixV().transpose();
      break;
    case 3:
      translation = -scale*SVD.matrixU().col(2);
      rotation = SVD.matrixU() * W.transpose() * SVD.matrixV().transpose();
      break;
    default:
      return false;
    }

    // change sign if det = -1
    if( rotation.determinant() < 0 ) rotation = -rotation;

    // output final selection
    outModel.block<3,3>(0,0) = rotation;
    outModel.col(3) = translation;
  }

  return true;
}

void
opengv::sac_problems::
    relative_pose::CentralRelativePoseSacProblem::getSelectedDistancesToModel(
    const model_t & model,
    const std::vector<int> & indices,
    std::vector<double> & scores) const
{
  translation_t translation = model.col(3);
  rotation_t rotation = model.block<3,3>(0,0);
  _adapter.sett12(translation);
  _adapter.setR12(rotation);

  model_t inverseSolution;
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
}

void
opengv::sac_problems::
    relative_pose::CentralRelativePoseSacProblem::optimizeModelCoefficients(
    const std::vector<int> & inliers,
    const model_t & model,
    model_t & optimized_model)
{
  translation_t translation = model.col(3);
  rotation_t rotation = model.block<3,3>(0,0);
  _adapter.sett12(translation);
  _adapter.setR12(rotation);
  optimized_model =
      opengv::relative_pose::optimize_nonlinear(_adapter,inliers);
}

int
opengv::sac_problems::
    relative_pose::CentralRelativePoseSacProblem::getSampleSize() const
{
  int sampleSize = 0;

  switch(_algorithm)
  {
  case NISTER:
    //5 for minimal solver and additional 3 for decomposition and disambiguation
    sampleSize = 5 + 3;
    break;
  case STEWENIUS:
    //5 for minimal solver and additional 3 for decomposition and disambiguation
    sampleSize = 5 + 3;
    break;
  case SEVENPT:
    //7 for minimal solver and additional 2 for decomposition and disambiguation
    sampleSize = 7 + 2;
    break;
  case EIGHTPT:
    //8 for minimal solver and additional 1 for decomposition
    sampleSize = 8 + 1;
    break;
  }

  return sampleSize;
}
