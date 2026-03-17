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


#include <opengv/relative_pose/methods.hpp>
#include <opengv/Indices.hpp>

#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

#include <opengv/OptimizationFunctor.hpp>
#include <opengv/math/arun.hpp>
#include <opengv/math/cayley.hpp>
#include <opengv/relative_pose/modules/main.hpp>
#include <opengv/triangulation/methods.hpp>

#include <iostream>

opengv::translation_t
opengv::relative_pose::twopt(
    const RelativeAdapterBase & adapter,
    bool unrotate,
    const std::vector<int> & indices )
{
  assert(indices.size()>1);
  return twopt( adapter, unrotate, indices[0], indices[1] );
};

opengv::translation_t
opengv::relative_pose::twopt(
    const RelativeAdapterBase & adapter,
    bool unrotate,
    size_t index0,
    size_t index1 )
{
  bearingVector_t f1 = adapter.getBearingVector1(index0);
  bearingVector_t f1prime = adapter.getBearingVector2(index0);
  bearingVector_t f2 = adapter.getBearingVector1(index1);
  bearingVector_t f2prime = adapter.getBearingVector2(index1);

  if(unrotate)
  {
    rotation_t R12 = adapter.getR12();
    f1prime = R12 * f1prime;
    f2prime = R12 * f2prime;
  }

  Eigen::Vector3d normal1 = f1.cross(f1prime);
  Eigen::Vector3d normal2 = f2.cross(f2prime);

  translation_t translation = normal1.cross(normal2);
  translation = translation/translation.norm();

  Eigen::Vector3d opticalFlow = f1 - f1prime;
  if( opticalFlow.dot(translation) < 0 )
    translation = -translation;

  return translation;
};

opengv::rotation_t
opengv::relative_pose::twopt_rotationOnly(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  assert(indices.size() > 1);
  return twopt_rotationOnly( adapter, indices[0], indices[1] );
};

opengv::rotation_t
opengv::relative_pose::twopt_rotationOnly(
    const RelativeAdapterBase & adapter,
    size_t index0,
    size_t index1)
{
  Eigen::Vector3d pointsCenter1 =
      adapter.getBearingVector1(index0) + adapter.getBearingVector1(index1);
  Eigen::Vector3d pointsCenter2 =
      adapter.getBearingVector2(index0) + adapter.getBearingVector2(index1);
  pointsCenter1 = pointsCenter1/3.0;
  pointsCenter2 = pointsCenter2/3.0;

  Eigen::MatrixXd Hcross(3,3);
  Hcross = Eigen::Matrix3d::Zero();

  Eigen::Vector3d f = adapter.getBearingVector1(index0) - pointsCenter1;
  Eigen::Vector3d fprime = adapter.getBearingVector2(index0) - pointsCenter2;
  Hcross += fprime * f.transpose();
  f = adapter.getBearingVector1(index1) - pointsCenter1;
  fprime = adapter.getBearingVector2(index1) - pointsCenter2;
  Hcross += fprime * f.transpose();

  return math::arun(Hcross);
};

namespace opengv
{
namespace relative_pose
{

rotation_t rotationOnly(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 2);

  Eigen::Vector3d pointsCenter1 = Eigen::Vector3d::Zero();
  Eigen::Vector3d pointsCenter2 = Eigen::Vector3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    pointsCenter1 += adapter.getBearingVector1(indices[i]);
    pointsCenter2 += adapter.getBearingVector2(indices[i]);
  }

  pointsCenter1 = pointsCenter1 / numberCorrespondences;
  pointsCenter2 = pointsCenter2 / numberCorrespondences;

  Eigen::MatrixXd Hcross(3,3);
  Hcross = Eigen::Matrix3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    Eigen::Vector3d f = adapter.getBearingVector1(indices[i]) - pointsCenter1;
    Eigen::Vector3d fprime =
        adapter.getBearingVector2(indices[i]) - pointsCenter2;
    Hcross += fprime * f.transpose();
  }

  return math::arun(Hcross);
};

}
}

opengv::rotation_t
opengv::relative_pose::rotationOnly( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return rotationOnly(adapter,idx);
};

opengv::rotation_t
opengv::relative_pose::rotationOnly(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return rotationOnly(adapter,idx);
};

namespace opengv
{
namespace relative_pose
{

complexEssentials_t fivept_stewenius(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 4);

  Eigen::MatrixXd Q(numberCorrespondences,9);
  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    //bearingVector_t f = adapter.getBearingVector1(indices[i]);
    //bearingVector_t fprime = adapter.getBearingVector2(indices[i]);
    //Stewenius' algorithm is computing the inverse transformation, so we simply
    //invert the input here
    bearingVector_t f = adapter.getBearingVector2(indices[i]);
    bearingVector_t fprime = adapter.getBearingVector1(indices[i]);
    Eigen::Matrix<double,1,9> row;
    row <<  f[0]*fprime[0], f[1]*fprime[0], f[2]*fprime[0],
        f[0]*fprime[1], f[1]*fprime[1], f[2]*fprime[1],
        f[0]*fprime[2], f[1]*fprime[2], f[2]*fprime[2];
    Q.row(i) = row;
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > SVD(Q, Eigen::ComputeFullV );
  Eigen::Matrix<double,9,4> EE = SVD.matrixV().block(0,5,9,4);
  complexEssentials_t complexEssentials;
  modules::fivept_stewenius_main(EE,complexEssentials);
  return complexEssentials;
};

}
}

opengv::complexEssentials_t
opengv::relative_pose::fivept_stewenius( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return fivept_stewenius(adapter,idx);
};

opengv::complexEssentials_t
opengv::relative_pose::fivept_stewenius(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return fivept_stewenius(adapter,idx);
};

namespace opengv
{
namespace relative_pose
{

essentials_t fivept_nister(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 4);

  Eigen::MatrixXd Q(numberCorrespondences,9);
  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    //bearingVector_t f = adapter.getBearingVector1(indices[i]);
    //bearingVector_t fprime = adapter.getBearingVector2(indices[i]);
    //Nister's algorithm is computing the inverse transformation, so we simply
    //invert the input here
    bearingVector_t f = adapter.getBearingVector2(indices[i]);
    bearingVector_t fprime = adapter.getBearingVector1(indices[i]);
    Eigen::Matrix<double,1,9> row;
    row <<  f[0]*fprime[0], f[1]*fprime[0], f[2]*fprime[0],
        f[0]*fprime[1], f[1]*fprime[1], f[2]*fprime[1],
        f[0]*fprime[2], f[1]*fprime[2], f[2]*fprime[2];
    Q.row(i) = row;
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > SVD(Q, Eigen::ComputeFullV );
  Eigen::Matrix<double,9,4> EE = SVD.matrixV().block(0,5,9,4);
  essentials_t essentials;
  modules::fivept_nister_main(EE,essentials);

  return essentials;
};

}
}

opengv::essentials_t
opengv::relative_pose::fivept_nister( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return fivept_nister(adapter,idx);
};

opengv::essentials_t
opengv::relative_pose::fivept_nister(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return fivept_nister(adapter,idx);
};

opengv::rotations_t
opengv::relative_pose::fivept_kneip(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences == 5);

  Eigen::Matrix<double,3,5> f1;
  Eigen::Matrix<double,3,5> f2;

  for(size_t i = 0; i < numberCorrespondences; i++)
  {
    f1.col(i) = adapter.getBearingVector1(indices[i]);
    f2.col(i) = adapter.getBearingVector2(indices[i]);
  }

  rotations_t rotations;
  modules::fivept_kneip_main( f1, f2, rotations );
  return rotations;
}

namespace opengv
{
namespace relative_pose
{

essentials_t sevenpt(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 6);

  Eigen::MatrixXd A(numberCorrespondences,9);

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    //bearingVector_t f1 = adapter.getBearingVector1(indices[i]);
    //bearingVector_t f2 = adapter.getBearingVector2(indices[i]);
    //The seven-point is computing the inverse transformation, which is why we
    //invert the input
    bearingVector_t f1 = adapter.getBearingVector2(indices[i]);
    bearingVector_t f2 = adapter.getBearingVector1(indices[i]);

    A.block<1,3>(i,0) = f2[0] * f1.transpose();
    A.block<1,3>(i,3) = f2[1] * f1.transpose();
    A.block<1,3>(i,6) = f2[2] * f1.transpose();
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
      A,
      Eigen::ComputeFullU | Eigen::ComputeFullV );

  Eigen::Matrix<double,9,1> f1 = SVD.matrixV().col(8);
  Eigen::Matrix<double,9,1> f2 = SVD.matrixV().col(7);

  Eigen::MatrixXd F1_temp(3,3);
  F1_temp.col(0) = f1.block<3,1>(0,0);
  F1_temp.col(1) = f1.block<3,1>(3,0);
  F1_temp.col(2) = f1.block<3,1>(6,0);
  essential_t F1 = F1_temp.transpose();

  Eigen::MatrixXd F2_temp(3,3);
  F2_temp.col(0) = f2.block<3,1>(0,0);
  F2_temp.col(1) = f2.block<3,1>(3,0);
  F2_temp.col(2) = f2.block<3,1>(6,0);
  essential_t F2 = F2_temp.transpose();

  double eps = 0.00000001;
  essentials_t essentials;
  
  if( fabs(F1.determinant()) < eps || numberCorrespondences > 7 )
  {
    essentials.push_back(F1);
  }
  else
  {
    essential_t M = F2.inverse() * F1;
    Eigen::EigenSolver< essential_t > Eig(M,true);
    Eigen::Matrix< std::complex<double>,3,1 > D = Eig.eigenvalues();

    double val1 = fabs(D(0,0).imag());
    double val2 = fabs(D(1,0).imag());
    double val3 = fabs(D(2,0).imag());

    if( val1 < eps && val2 < eps && val3 < eps )
    {
      essentials.push_back( F1 - D(0,0).real() * F2 );
      essentials.push_back( F1 - D(1,0).real() * F2 );
      essentials.push_back( F1 - D(2,0).real() * F2 );
    }
    else
    {
      double min = val1;
      int minIndex = 0;
      if( val2 < min )
      {
        min = val2;
        minIndex = 1;
      }
      if( val3 < min )
        minIndex = 2;
      
      essentials.push_back( F1 - D(minIndex,0).real() * F2 );
    }
  }

  return essentials;
}

}
}

opengv::essentials_t
opengv::relative_pose::sevenpt( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return sevenpt(adapter,idx);
}

opengv::essentials_t
opengv::relative_pose::sevenpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return sevenpt(adapter,idx);
}

namespace opengv
{
namespace relative_pose
{

essential_t eightpt(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 7);

  Eigen::MatrixXd A(numberCorrespondences,9);

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    //bearingVector_t f1 = adapter.getBearingVector1(indices[i]);
    //bearingVector_t f2 = adapter.getBearingVector2(indices[i]);
    //The eight-point essentially computes the inverse transformation, which is
    //why we invert the input here
    bearingVector_t f1 = adapter.getBearingVector2(indices[i]);
    bearingVector_t f2 = adapter.getBearingVector1(indices[i]);

    A.block<1,3>(i,0) = f2[0] * f1.transpose();
    A.block<1,3>(i,3) = f2[1] * f1.transpose();
    A.block<1,3>(i,6) = f2[2] * f1.transpose();
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
      A,
      Eigen::ComputeFullU | Eigen::ComputeFullV );
  Eigen::Matrix<double,9,1> f = SVD.matrixV().col(8);

  Eigen::MatrixXd F_temp(3,3);
  F_temp.col(0) = f.block<3,1>(0,0);
  F_temp.col(1) = f.block<3,1>(3,0);
  F_temp.col(2) = f.block<3,1>(6,0);
  essential_t F = F_temp.transpose();

  Eigen::JacobiSVD< Eigen::MatrixXd > SVD2(
      F,
      Eigen::ComputeFullU | Eigen::ComputeFullV );
  Eigen::Matrix3d S = Eigen::Matrix3d::Zero();
  S(0,0) = SVD2.singularValues()[0];
  S(1,1) = SVD2.singularValues()[1];

  Eigen::Matrix3d U = SVD2.matrixU();
  Eigen::Matrix3d Vtr = SVD2.matrixV().transpose();

  essential_t essential = U * S * Vtr;
  return essential;
}

}
}

opengv::essential_t
opengv::relative_pose::eightpt( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return eightpt(adapter,idx);
}

opengv::essential_t
opengv::relative_pose::eightpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return eightpt(adapter,idx);
}

namespace opengv
{
namespace relative_pose
{

rotation_t eigensolver(
    const RelativeAdapterBase & adapter,
    const Indices & indices,
    eigensolverOutput_t & output,
    bool useWeights )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 4);

  Eigen::Matrix3d xxF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d yyF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d zzF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d xyF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d yzF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d zxF = Eigen::Matrix3d::Zero();

  //compute the norm of all the scores
  double norm = 0.0;
  for(size_t i=0; i < numberCorrespondences; i++)
    norm += pow(adapter.getWeight(indices[i]),2);
  norm = sqrt(norm);

  //Fill summation terms
  for(size_t i=0; i < numberCorrespondences; i++)
  {
    bearingVector_t f1 = adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 = adapter.getBearingVector2(indices[i]);
    Eigen::Matrix3d F = f2*f2.transpose();
    
    double weight = 1.0;
    if( useWeights )
      weight = adapter.getWeight(indices[i])/norm;

    xxF = xxF + weight*f1[0]*f1[0]*F;
    yyF = yyF + weight*f1[1]*f1[1]*F;
    zzF = zzF + weight*f1[2]*f1[2]*F;
    xyF = xyF + weight*f1[0]*f1[1]*F;
    yzF = yzF + weight*f1[1]*f1[2]*F;
    zxF = zxF + weight*f1[2]*f1[0]*F;
  }

  //Do minimization
  modules::eigensolver_main(xxF,yyF,zzF,xyF,yzF,zxF,output);

  //Correct the translation
  bearingVector_t f1 = adapter.getBearingVector1(indices[0]);
  bearingVector_t f2 = adapter.getBearingVector2(indices[0]);
  f2 = output.rotation * f2;
  Eigen::Vector3d opticalFlow = f1 - f2;
  if( opticalFlow.dot(output.translation) < 0.0 )
    output.translation = -output.translation;

  return output.rotation;
}

}
}

opengv::rotation_t
opengv::relative_pose::eigensolver(
    const RelativeAdapterBase & adapter,
    eigensolverOutput_t & output,
    bool useWeights )
{
  Indices idx(adapter.getNumberCorrespondences());
  return eigensolver(adapter,idx,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::eigensolver(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    eigensolverOutput_t & output,
    bool useWeights )
{
  Indices idx(indices);
  return eigensolver(adapter,idx,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::eigensolver(
    const RelativeAdapterBase & adapter,
    bool useWeights )
{
  eigensolverOutput_t output;
  output.rotation = adapter.getR12();
  return eigensolver(adapter,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::eigensolver(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    bool useWeights )
{
  eigensolverOutput_t output;
  output.rotation = adapter.getR12();
  return eigensolver(adapter,indices,output,useWeights);
}

namespace opengv
{
namespace relative_pose
{

rotations_t sixpt(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences == 6);

  Eigen::Matrix<double,6,6> L1;
  Eigen::Matrix<double,6,6> L2;

  for(size_t i = 0; i < numberCorrespondences; i++)
  {
    bearingVector_t f1 =
        adapter.getCamRotation1(indices[i]) * adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 =
        adapter.getCamRotation2(indices[i]) * adapter.getBearingVector2(indices[i]);
        
    L1.block<3,1>(0,i) = f1;
    L2.block<3,1>(0,i) = f2;
    
    L1.block<3,1>(3,i) = f1.cross(adapter.getCamOffset1(indices[i]));
    L2.block<3,1>(3,i) = f2.cross(adapter.getCamOffset2(indices[i]));
  }

  rotations_t solutions;
  modules::sixpt_main( L1, L2, solutions );  
  return solutions;
}

}
}

opengv::rotations_t
opengv::relative_pose::sixpt(
    const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return sixpt(adapter,idx);
}

opengv::rotations_t
opengv::relative_pose::sixpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return sixpt(adapter,idx);
}

namespace opengv
{
namespace relative_pose
{

rotation_t ge(
    const RelativeAdapterBase & adapter,
    const Indices & indices,
    geOutput_t & output,
    bool useWeights )
{ 
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 5);

  Eigen::Matrix3d xxF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d yyF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d zzF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d xyF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d yzF = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d zxF = Eigen::Matrix3d::Zero();
  
  Eigen::Matrix<double,3,9> x1P = Eigen::Matrix<double,3,9>::Zero();
  Eigen::Matrix<double,3,9> y1P = Eigen::Matrix<double,3,9>::Zero();
  Eigen::Matrix<double,3,9> z1P = Eigen::Matrix<double,3,9>::Zero();
  Eigen::Matrix<double,3,9> x2P = Eigen::Matrix<double,3,9>::Zero();
  Eigen::Matrix<double,3,9> y2P = Eigen::Matrix<double,3,9>::Zero();
  Eigen::Matrix<double,3,9> z2P = Eigen::Matrix<double,3,9>::Zero();
  
  Eigen::Matrix<double,9,9> m11P = Eigen::Matrix<double,9,9>::Zero();
  Eigen::Matrix<double,9,9> m12P = Eigen::Matrix<double,9,9>::Zero();
  Eigen::Matrix<double,9,9> m22P = Eigen::Matrix<double,9,9>::Zero();

  //compute the norm of all the scores
  double norm = 0.0;
  for(size_t i=0; i < numberCorrespondences; i++)
    norm += pow(adapter.getWeight(indices[i]),2);
  norm = sqrt(norm);

  //Fill summation terms
  for(size_t i=0; i < numberCorrespondences; i++)
  {
    //get the weight of this feature
    double weight = 1.0;
    if( useWeights )
      weight = adapter.getWeight(indices[i])/norm;
    
    //unrotate the bearing vectors
    bearingVector_t f1 = adapter.getCamRotation1(indices[i]) *
        adapter.getBearingVector1(indices[i]);
    bearingVector_t f2 = adapter.getCamRotation2(indices[i]) *
        adapter.getBearingVector2(indices[i]);
    
    //compute the standard summation terms
    Eigen::Matrix3d F = f2*f2.transpose();

    xxF = xxF + weight*f1[0]*f1[0]*F;
    yyF = yyF + weight*f1[1]*f1[1]*F;
    zzF = zzF + weight*f1[2]*f1[2]*F;
    xyF = xyF + weight*f1[0]*f1[1]*F;
    yzF = yzF + weight*f1[1]*f1[2]*F;
    zxF = zxF + weight*f1[2]*f1[0]*F;
    
    //now compute the "cross"-summation terms    
    Eigen::Vector3d t1 = adapter.getCamOffset1(indices[i]);
    Eigen::Vector3d t2 = adapter.getCamOffset2(indices[i]);
    
    Eigen::Matrix<double,1,9> f2_19;
    double temp = f1[1]*t1[2]-f1[2]*t1[1];
    f2_19(0,0) = f2[0] * temp;
    f2_19(0,1) = f2[1] * temp;
    f2_19(0,2) = f2[2] * temp;
    temp = f1[2]*t1[0]-f1[0]*t1[2];
    f2_19(0,3) = f2[0] * temp;
    f2_19(0,4) = f2[1] * temp;
    f2_19(0,5) = f2[2] * temp;
    temp = f1[0]*t1[1]-f1[1]*t1[0];
    f2_19(0,6) = f2[0] * temp;
    f2_19(0,7) = f2[1] * temp;
    f2_19(0,8) = f2[2] * temp;
    
    Eigen::Matrix<double,1,9> f1_19;
    temp = f2[1]*t2[2]-f2[2]*t2[1];
    f1_19(0,0) = f1[0] * temp;
    f1_19(0,1) = f1[1] * temp;
    f1_19(0,2) = f1[2] * temp;
    temp = f2[2]*t2[0]-f2[0]*t2[2];
    f1_19(0,3) = f1[0] * temp;
    f1_19(0,4) = f1[1] * temp;
    f1_19(0,5) = f1[2] * temp;
    temp = f2[0]*t2[1]-f2[1]*t2[0];
    f1_19(0,6) = f1[0] * temp;
    f1_19(0,7) = f1[1] * temp;
    f1_19(0,8) = f1[2] * temp;
    
    if( useWeights )
    {
      x1P = x1P + ( (weight * f1[0]) * f2 ) * f1_19;
      y1P = y1P + ( (weight * f1[1]) * f2 ) * f1_19;
      z1P = z1P + ( (weight * f1[2]) * f2 ) * f1_19;
      
      x2P = x2P + ( (weight * f1[0]) * f2 ) * f2_19;
      y2P = y2P + ( (weight * f1[1]) * f2 ) * f2_19;
      z2P = z2P + ( (weight * f1[2]) * f2 ) * f2_19;
      
      m11P = m11P - ( weight * f1_19.transpose() ) * f1_19;
      m22P = m22P - ( weight * f2_19.transpose() ) * f2_19;
      m12P = m12P - ( weight * f2_19.transpose() ) * f1_19;
    }
    else
    {
      x1P = x1P + ( f1[0] * f2 ) * f1_19;
      y1P = y1P + ( f1[1] * f2 ) * f1_19;
      z1P = z1P + ( f1[2] * f2 ) * f1_19;
      
      x2P = x2P + ( f1[0]) * f2 * f2_19;
      y2P = y2P + ( f1[1]) * f2 * f2_19;
      z2P = z2P + ( f1[2]) * f2 * f2_19;
      
      m11P = m11P - f1_19.transpose() * f1_19;
      m22P = m22P - f2_19.transpose() * f2_19;
      m12P = m12P - f2_19.transpose() * f1_19;
    }
  }

  Eigen::Vector3d pointsCenter1 = Eigen::Vector3d::Zero();
  Eigen::Vector3d pointsCenter2 = Eigen::Vector3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    pointsCenter1 += adapter.getCamRotation1(indices[i]) *
        adapter.getBearingVector1(indices[i]);
    pointsCenter2 += adapter.getCamRotation2(indices[i]) *
        adapter.getBearingVector2(indices[i]);
  }

  pointsCenter1 = pointsCenter1 / numberCorrespondences;
  pointsCenter2 = pointsCenter2 / numberCorrespondences;

  Eigen::MatrixXd Hcross(3,3);
  Hcross = Eigen::Matrix3d::Zero();

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    Eigen::Vector3d f =      adapter.getCamRotation1(indices[i]) *
        adapter.getBearingVector1(indices[i]) - pointsCenter1;
    Eigen::Vector3d fprime = adapter.getCamRotation2(indices[i]) *
        adapter.getBearingVector2(indices[i]) - pointsCenter2;
    Hcross += fprime * f.transpose();
  }

  rotation_t startingRotation = math::arun(Hcross);

  //Do minimization
  modules::ge_main2(
      xxF, yyF, zzF, xyF, yzF, zxF,
      x1P, y1P, z1P, x2P, y2P, z2P,
      m11P, m12P, m22P, math::rot2cayley(startingRotation), output);

  return output.rotation;
}

}
}

opengv::rotation_t
opengv::relative_pose::ge(
    const RelativeAdapterBase & adapter,
    geOutput_t & output,
    bool useWeights )
{
  Indices idx(adapter.getNumberCorrespondences());
  return ge(adapter,idx,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::ge(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    geOutput_t & output,
    bool useWeights )
{
  Indices idx(indices);
  return ge(adapter,idx,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::ge( const RelativeAdapterBase & adapter, bool useWeights )
{
  geOutput_t output;
  //output.rotation = adapter.getR12(); //finding starting value using arun
  return ge(adapter,output,useWeights);
}

opengv::rotation_t
opengv::relative_pose::ge(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    bool useWeights )
{
  geOutput_t output;
  //output.rotation = adapter.getR12(); //finding starting value using arun
  return ge(adapter,indices,output,useWeights);
}

namespace opengv
{
namespace relative_pose
{

transformation_t seventeenpt(
    const RelativeAdapterBase & adapter,
    const Indices & indices )
{
  size_t numberCorrespondences = indices.size();
  assert(numberCorrespondences > 16);

  Eigen::MatrixXd AE(numberCorrespondences,9);
  Eigen::MatrixXd AR(numberCorrespondences,9);

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    bearingVector_t d1 = adapter.getBearingVector1(indices[i]);
    bearingVector_t d2 = adapter.getBearingVector2(indices[i]);
    translation_t v1 = adapter.getCamOffset1(indices[i]);
    translation_t v2 = adapter.getCamOffset2(indices[i]);
    rotation_t R1 = adapter.getCamRotation1(indices[i]);
    rotation_t R2 = adapter.getCamRotation2(indices[i]);

    //unrotate the bearing-vectors to express everything in the body frame
    d1 = R1*d1;
    d2 = R2*d2;

    //generate the Plücker line coordinates
    Eigen::Matrix<double,6,1> l1;
    l1.block<3,1>(0,0) = d1;
    l1.block<3,1>(3,0) = v1.cross(d1);
    Eigen::Matrix<double,6,1> l2;
    l2.block<3,1>(0,0) = d2;
    l2.block<3,1>(3,0) = v2.cross(d2);

    //fill line of matrix A
    AE(i,0) = l2[0]*l1[0];
    AE(i,1) = l2[0]*l1[1];
    AE(i,2) = l2[0]*l1[2];
    AE(i,3) = l2[1]*l1[0];
    AE(i,4) = l2[1]*l1[1];
    AE(i,5) = l2[1]*l1[2];
    AE(i,6) = l2[2]*l1[0];
    AE(i,7) = l2[2]*l1[1];
    AE(i,8) = l2[2]*l1[2];

    AR(i,0) = l2[0]*l1[3]+l2[3]*l1[0];
    AR(i,1) = l2[0]*l1[4]+l2[3]*l1[1];
    AR(i,2) = l2[0]*l1[5]+l2[3]*l1[2];
    AR(i,3) = l2[1]*l1[3]+l2[4]*l1[0];
    AR(i,4) = l2[1]*l1[4]+l2[4]*l1[1];
    AR(i,5) = l2[1]*l1[5]+l2[4]*l1[2];
    AR(i,6) = l2[2]*l1[3]+l2[5]*l1[0];
    AR(i,7) = l2[2]*l1[4]+l2[5]*l1[1];
    AR(i,8) = l2[2]*l1[5]+l2[5]*l1[2];
  }

  Eigen::JacobiSVD< Eigen::MatrixXd > SVDARP(
      AR,
      Eigen::ComputeThinU | Eigen::ComputeThinV );

  Eigen::VectorXd sigma_ = SVDARP.singularValues();
  double pinvtoler_ =
      sigma_(0)*numberCorrespondences*NumTraits<double>::epsilon();
  Eigen::MatrixXd SigmaInverse_(9,9);
  SigmaInverse_ = Eigen::MatrixXd::Zero(9,9);
  for ( size_t i=0; i < 9; ++i)
  {
    double temp = sigma_(i);
    if( temp > pinvtoler_ )
      SigmaInverse_(i,i) = 1.0/temp;
  }
  
  Eigen::MatrixXd ARP(9,numberCorrespondences);
  ARP = SVDARP.matrixV()*SigmaInverse_*SVDARP.matrixU().transpose();

  Eigen::MatrixXd B(numberCorrespondences,numberCorrespondences);
  B = -Eigen::MatrixXd::Identity(numberCorrespondences,numberCorrespondences);
  B = B + AR*ARP;

  Eigen::MatrixXd C = B*AE;
  
  Eigen::JacobiSVD< Eigen::MatrixXd > SVDE(
      C,
      Eigen::ComputeThinU | Eigen::ComputeThinV );
  Eigen::Matrix<double,9,1> e = SVDE.matrixV().col(8);

  Eigen::MatrixXd E_temp(3,3);
  E_temp.col(0) = e.block<3,1>(0,0);
  E_temp.col(1) = e.block<3,1>(3,0);
  E_temp.col(2) = e.block<3,1>(6,0);
  essential_t E = E_temp.transpose();

  Eigen::JacobiSVD< Eigen::MatrixXd > SVDR(
      E,
      Eigen::ComputeFullV | Eigen::ComputeFullU );

  Eigen::Matrix3d W = Eigen::Matrix3d::Zero();
  W(0,1) = -1.0;
  W(1,0) = 1.0;
  W(2,2) = 1.0;

  // get possible rotation and translation vectors
  rotation_t Ra = SVDR.matrixU() * W * SVDR.matrixV().transpose();
  rotation_t Rb = SVDR.matrixU() * W.transpose() * SVDR.matrixV().transpose();

  // change sign if det = -1
  if( Ra.determinant() < 0 ) Ra = -Ra;
  if( Rb.determinant() < 0 ) Rb = -Rb;

  Ra.transposeInPlace();
  Rb.transposeInPlace();

  Eigen::MatrixXd A_tra(numberCorrespondences,3);
  Eigen::MatrixXd A_trb(numberCorrespondences,3);
  Eigen::VectorXd b_tra(numberCorrespondences);
  Eigen::VectorXd b_trb(numberCorrespondences);

  for( size_t i = 0; i < numberCorrespondences; i++ )
  {
    bearingVector_t d1 = adapter.getBearingVector1(indices[i]);
    bearingVector_t d2 = adapter.getBearingVector2(indices[i]);
    translation_t v1 = adapter.getCamOffset1(indices[i]);
    translation_t v2 = adapter.getCamOffset2(indices[i]);
    rotation_t R1 = adapter.getCamRotation1(indices[i]);
    rotation_t R2 = adapter.getCamRotation2(indices[i]);

    //unrotate the bearing-vectors to express everything in the body frame
    d1 = R1*d1;
    d2 = R2*d2;

    A_tra(i,0) = d1[2]*d2[0]*Ra(1,0)+d1[2]*d2[1]*Ra(1,1)+d1[2]*d2[2]*Ra(1,2)
                -d1[1]*d2[0]*Ra(2,0)-d1[1]*d2[1]*Ra(2,1)-d1[1]*d2[2]*Ra(2,2);
    A_tra(i,1) = d1[0]*d2[0]*Ra(2,0)+d1[0]*d2[1]*Ra(2,1)+d1[0]*d2[2]*Ra(2,2)
                -d1[2]*d2[0]*Ra(0,0)-d1[2]*d2[1]*Ra(0,1)-d1[2]*d2[2]*Ra(0,2);
    A_tra(i,2) = d1[1]*d2[0]*Ra(0,0)+d1[1]*d2[1]*Ra(0,1)+d1[1]*d2[2]*Ra(0,2)
                -d1[0]*d2[0]*Ra(1,0)-d1[0]*d2[1]*Ra(1,1)-d1[0]*d2[2]*Ra(1,2);

    A_trb(i,0) = d1[2]*d2[0]*Rb(1,0)+d1[2]*d2[1]*Rb(1,1)+d1[2]*d2[2]*Rb(1,2)
                -d1[1]*d2[0]*Rb(2,0)-d1[1]*d2[1]*Rb(2,1)-d1[1]*d2[2]*Rb(2,2);
    A_trb(i,1) = d1[0]*d2[0]*Rb(2,0)+d1[0]*d2[1]*Rb(2,1)+d1[0]*d2[2]*Rb(2,2)
                -d1[2]*d2[0]*Rb(0,0)-d1[2]*d2[1]*Rb(0,1)-d1[2]*d2[2]*Rb(0,2);
    A_trb(i,2) = d1[1]*d2[0]*Rb(0,0)+d1[1]*d2[1]*Rb(0,1)+d1[1]*d2[2]*Rb(0,2)
                -d1[0]*d2[0]*Rb(1,0)-d1[0]*d2[1]*Rb(1,1)-d1[0]*d2[2]*Rb(1,2);

    Eigen::Vector3d temp1 = v1.cross(d1);
    Eigen::Vector3d temp2 = v2.cross(d2);
    b_tra(i) = -d1.dot(Ra*temp2) -temp1.dot(Ra*d2);
    b_trb(i) = -d1.dot(Rb*temp2) -temp1.dot(Rb*d2);
  }
  
  Eigen::JacobiSVD< Eigen::MatrixXd > SVDa(
      A_tra,
      Eigen::ComputeThinU | Eigen::ComputeThinV );

  Eigen::VectorXd sigma = SVDa.singularValues();
  double pinvtoler =
      numberCorrespondences*sigma(0)*NumTraits<double>::epsilon();
  Eigen::MatrixXd SigmaInverse(3,3);
  SigmaInverse = Eigen::MatrixXd::Zero(3,3);
  for ( size_t i=0; i < 3; ++i)
  {
    double temp = sigma(i);
    if( temp > pinvtoler )
      SigmaInverse(i,i) = 1.0/temp;
  }

  Eigen::MatrixXd PI(3,numberCorrespondences);
  PI = SVDa.matrixV()*SigmaInverse*SVDa.matrixU().transpose();
  Eigen::Vector3d ta = PI*b_tra;

  Eigen::JacobiSVD< Eigen::MatrixXd > SVDb(
      A_trb,
      Eigen::ComputeThinU | Eigen::ComputeThinV );

  sigma = SVDb.singularValues();
  pinvtoler = numberCorrespondences*sigma(0)*NumTraits<double>::epsilon();
  SigmaInverse = Eigen::MatrixXd::Zero(3,3);
  for ( size_t i=0; i < 3; ++i)
  {
    double temp = sigma(i);
    if( temp > pinvtoler )
      SigmaInverse(i,i) = 1.0/temp;
  }

  PI = SVDb.matrixV()*SigmaInverse*SVDb.matrixU().transpose();
  Eigen::Vector3d tb = PI*b_trb;

  Eigen::VectorXd fita = A_tra * ta - b_tra;
  Eigen::VectorXd fitb = A_trb * tb - b_trb;

  transformation_t transformation;
  if( fita.norm() < fitb.norm() )
  {
    transformation.block<3,3>(0,0) = Ra;
    transformation.col(3) = ta;
  }
  else
  {
    transformation.block<3,3>(0,0) = Rb;
    transformation.col(3) = tb;
  }

  return transformation;
}

}
}

opengv::transformation_t
opengv::relative_pose::seventeenpt( const RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return seventeenpt(adapter,idx);
}

opengv::transformation_t
opengv::relative_pose::seventeenpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return seventeenpt(adapter,idx);
}

namespace opengv
{
namespace relative_pose
{

struct OptimizeNonlinearFunctor1 : OptimizationFunctor<double>
{
  RelativeAdapterBase & _adapter;
  const Indices & _indices;

  OptimizeNonlinearFunctor1(
      RelativeAdapterBase & adapter,
      const Indices & indices ) :
      OptimizationFunctor<double>(6,indices.size()),
      _adapter(adapter),
      _indices(indices) {}

  int operator()(const VectorXd &x, VectorXd &fvec) const
  {
    assert( x.size() == 6 );
    assert( (unsigned int) fvec.size() == _indices.size());

    //compute the current position
    translation_t translation = x.block<3,1>(0,0);
    cayley_t cayley = x.block<3,1>(3,0);
    rotation_t rotation = math::cayley2rot(cayley);

    Eigen::Matrix<double,4,1> p_hom;
    p_hom[3] = 1.0;

    for( size_t i = 0; i < _indices.size(); i++ )
    {
      translation_t cam1Offset = _adapter.getCamOffset1(_indices[i]);
      rotation_t cam1Rotation = _adapter.getCamRotation1(_indices[i]);
      translation_t cam2Offset = _adapter.getCamOffset2(_indices[i]);
      rotation_t cam2Rotation = _adapter.getCamRotation2(_indices[i]);

      translation_t directTranslation =
          cam1Rotation.transpose() *
          ((translation - cam1Offset) + rotation * cam2Offset);
      rotation_t directRotation =
          cam1Rotation.transpose() * rotation * cam2Rotation;

      _adapter.sett12(directTranslation);
      _adapter.setR12(directRotation);

      transformation_t inverseSolution;
      inverseSolution.block<3,3>(0,0) = directRotation.transpose();
      inverseSolution.col(3) =
          -inverseSolution.block<3,3>(0,0)*directTranslation;

      p_hom.block<3,1>(0,0) =
          opengv::triangulation::triangulate2(_adapter,_indices[i]);
      bearingVector_t reprojection1 = p_hom.block<3,1>(0,0);
      bearingVector_t reprojection2 = inverseSolution * p_hom;
      reprojection1 = reprojection1 / reprojection1.norm();
      reprojection2 = reprojection2 / reprojection2.norm();
      bearingVector_t f1 = _adapter.getBearingVector1(_indices[i]);
      bearingVector_t f2 = _adapter.getBearingVector2(_indices[i]);

      //bearing-vector based outlier criterium (select threshold accordingly):
      //1-(f1'*f2) = 1-cos(alpha) \in [0:2]
      double reprojError1 = 1.0 - (f1.transpose() * reprojection1);
      double reprojError2 = 1.0 - (f2.transpose() * reprojection2);
      double factor = 1.0;
      fvec[i] = factor*(reprojError1 + reprojError2);
    }

    return 0;
  }
};

transformation_t optimize_nonlinear(
    RelativeAdapterBase & adapter,
    const Indices & indices )
{
  const int n=6;
  VectorXd x(n);

  x.block<3,1>(0,0) = adapter.gett12();
  x.block<3,1>(3,0) = math::rot2cayley(adapter.getR12());

  OptimizeNonlinearFunctor1 functor( adapter, indices );
  NumericalDiff<OptimizeNonlinearFunctor1> numDiff(functor);
  LevenbergMarquardt< NumericalDiff<OptimizeNonlinearFunctor1> >
      lm(numDiff);

  lm.resetParameters();
  lm.parameters.ftol = 1.E1*NumTraits<double>::epsilon();
  lm.parameters.xtol = 1.E1*NumTraits<double>::epsilon();
  lm.parameters.maxfev = 1000;
  lm.minimize(x);

  transformation_t transformation;
  transformation.col(3) = x.block<3,1>(0,0);
  transformation.block<3,3>(0,0) = math::cayley2rot(x.block<3,1>(3,0));
  return transformation;
}

}
}

opengv::transformation_t
opengv::relative_pose::optimize_nonlinear( RelativeAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return optimize_nonlinear(adapter,idx);
}

opengv::transformation_t
opengv::relative_pose::optimize_nonlinear(
    RelativeAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return optimize_nonlinear(adapter,idx);
}
