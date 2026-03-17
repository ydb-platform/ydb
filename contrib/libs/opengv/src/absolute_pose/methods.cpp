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


#include <opengv/absolute_pose/methods.hpp>
#include <opengv/Indices.hpp>

#include <Eigen/NonLinearOptimization>
#include <Eigen/NumericalDiff>

#include <opengv/absolute_pose/modules/main.hpp>
#include <opengv/absolute_pose/modules/Epnp.hpp>
#include <opengv/OptimizationFunctor.hpp>
#include <opengv/math/cayley.hpp>
#include <opengv/math/quaternion.hpp>
#include <opengv/math/roots.hpp>

#include <iostream>

opengv::translation_t
opengv::absolute_pose::p2p(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  assert(indices.size()>1);
  return p2p( adapter, indices[0], indices[1] );
}

opengv::translation_t
opengv::absolute_pose::p2p(
    const AbsoluteAdapterBase & adapter,
    size_t index0,
    size_t index1)
{
  Eigen::Vector3d e1 = adapter.getBearingVector(index0);
  Eigen::Vector3d e3 = adapter.getBearingVector(index1);
  e3 = e1.cross(e3);
  e3 = e3/e3.norm();
  Eigen::Vector3d e2 = e3.cross(e1);

  rotation_t T;
  T.row(0) = e1.transpose();
  T.row(1) = e2.transpose();
  T.row(2) = e3.transpose();

  Eigen::Vector3d n1 = adapter.getPoint(index1) - adapter.getPoint(index0);
  n1 = n1/n1.norm();
  Eigen::Vector3d n3;
  if( (fabs(n1[0]) > fabs(n1[1])) && (fabs(n1[0]) > fabs(n1[2])) )
  {
    n3[1] = 1.0;
    n3[2] = 0.0;
    n3[0] = -n1[1]/n1[0];
  }
  else
  {
    if( (fabs(n1[1]) > fabs(n1[0])) && (fabs(n1[1]) > fabs(n1[2])) )
    {
      n3[2] = 1.0;
      n3[0] = 0.0;
      n3[1] = -n1[2]/n1[1];
    }
    else
    {
      n3[0] = 1.0;
      n3[1] = 0.0;
      n3[2] = -n1[0]/n1[2];
    }
  }
  n3 = n3 / n3.norm();
  Eigen::Vector3d n2 = n3.cross(n1);

  rotation_t N;
  N.row(0) = n1.transpose();
  N.row(1) = n2.transpose();
  N.row(2) = n3.transpose();

  Eigen::Matrix3d Q = T * adapter.getR().transpose() * N.transpose();
  Eigen::Vector3d temp1 = adapter.getPoint(index1) - adapter.getPoint(index0);
  double d_12 = temp1.norm();

  Eigen::Vector3d temp2 = adapter.getBearingVector(index1);
  double cos_beta = e1.dot(temp2);
  double b = 1/( 1 - pow( cos_beta, 2 ) ) - 1;

  if( cos_beta < 0 )
    b = -sqrt(b);
  else
    b = sqrt(b);

  double temp3 = d_12 * ( Q(1,0) * b - Q(0,0) );

  translation_t solution = -temp3 * Q.row(0).transpose();
  solution = adapter.getPoint(index0) + N.transpose()*solution;

  if(
    solution(0,0) != solution(0,0) ||
    solution(1,0) != solution(1,0) ||
    solution(2,0) != solution(2,0) )
    solution = Eigen::Vector3d::Zero();

  return solution;
}

opengv::transformations_t
opengv::absolute_pose::p3p_kneip(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  assert(indices.size()>2);
  return p3p_kneip( adapter, indices[0], indices[1], indices[2] );
}

opengv::transformations_t
opengv::absolute_pose::p3p_kneip(
    const AbsoluteAdapterBase & adapter,
    size_t index0,
    size_t index1,
    size_t index2)
{
  bearingVectors_t f;
  f.push_back(adapter.getBearingVector(index0));
  f.push_back(adapter.getBearingVector(index1));
  f.push_back(adapter.getBearingVector(index2));
  points_t p;
  p.push_back(adapter.getPoint(index0));
  p.push_back(adapter.getPoint(index1));
  p.push_back(adapter.getPoint(index2));
  transformations_t solutions;
  modules::p3p_kneip_main( f, p, solutions );
  return solutions;
}

opengv::transformations_t
opengv::absolute_pose::p3p_gao(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  assert(indices.size()>2);
  return p3p_gao( adapter, indices[0], indices[1], indices[2] );
}

opengv::transformations_t
opengv::absolute_pose::p3p_gao(
    const AbsoluteAdapterBase & adapter,
    size_t index0,
    size_t index1,
    size_t index2)
{
  bearingVectors_t f;
  f.push_back(adapter.getBearingVector(index0));
  f.push_back(adapter.getBearingVector(index1));
  f.push_back(adapter.getBearingVector(index2));
  points_t p;
  p.push_back(adapter.getPoint(index0));
  p.push_back(adapter.getPoint(index1));
  p.push_back(adapter.getPoint(index2));
  transformations_t solutions;
  modules::p3p_gao_main( f, p, solutions );
  return solutions;
}

opengv::transformations_t
opengv::absolute_pose::gp3p(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  assert(indices.size()>2);

  Eigen::Matrix3d f;
  Eigen::Matrix3d v;
  Eigen::Matrix3d p;

  for(size_t i = 0; i < 3; i++)
  {
    f.col(i) = adapter.getBearingVector(indices[i]);
    rotation_t R = adapter.getCamRotation(indices[i]);
    
    //unrotate the bearingVectors already so the camera rotation doesn't appear
    //in the problem
    f.col(i) = R * f.col(i);
    v.col(i) = adapter.getCamOffset(indices[i]);
    p.col(i) = adapter.getPoint(indices[i]);
  }

  transformations_t solutions;
  modules::gp3p_main(f,v,p,solutions);

  return solutions;
}

opengv::transformations_t
opengv::absolute_pose::gp3p(
    const AbsoluteAdapterBase & adapter,
    size_t index0,
    size_t index1,
    size_t index2)
{
  std::vector<int> indices;
  indices.push_back(index0);
  indices.push_back(index1);
  indices.push_back(index2);

  return gp3p(adapter,indices);
}

namespace opengv
{
namespace absolute_pose
{

transformation_t epnp(
    const AbsoluteAdapterBase & adapter,
    const Indices & indices )
{
  //starting from 4 points, we have a unique solution
  assert(indices.size() > 5);

  modules::Epnp PnP;
  PnP.set_maximum_number_of_correspondences(indices.size());
  PnP.reset_correspondences();

  for( size_t i = 0; i < indices.size(); i++ )
  {
    point_t p = adapter.getPoint(indices[i]);
    bearingVector_t f = adapter.getBearingVector(indices[i]);
    PnP.add_correspondence(p[0], p[1], p[2], f[0], f[1], f[2]);
  }

  double R_epnp[3][3], t_epnp[3];
  PnP.compute_pose(R_epnp, t_epnp);

  rotation_t rotation;
  translation_t translation;

  for(int r = 0; r < 3; r++)
  {
    for(int c = 0; c < 3; c++)
      rotation(r,c) = R_epnp[r][c];
  }

  translation[0] = t_epnp[0];
  translation[1] = t_epnp[1];
  translation[2] = t_epnp[2];

  //take inverse transformation
  rotation.transposeInPlace();
  translation = -rotation * translation;

  transformation_t transformation;
  transformation.col(3) = translation;
  transformation.block<3,3>(0,0) = rotation;
  return transformation;
}

}
}

opengv::transformation_t
opengv::absolute_pose::epnp( const AbsoluteAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return epnp(adapter,idx);
}

opengv::transformation_t
opengv::absolute_pose::epnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return epnp(adapter,idx);
}

namespace opengv
{
namespace absolute_pose
{

transformation_t gpnp(
    const AbsoluteAdapterBase & adapter,
    const Indices & indices )
{
  assert( indices.size() > 5 );

  //compute the centroid
  point_t c0 = Eigen::Vector3d::Zero();
  for( size_t i = 0; i < indices.size(); i++ )
    c0 = c0 + adapter.getPoint(indices[i]);
  c0 = c0 / indices.size();

  //compute the point-cloud
  Eigen::MatrixXd p(3,indices.size());
  for( size_t i = 0; i < indices.size(); i++ )
    p.col(i) = adapter.getPoint(indices[i]) - c0;

  //compute the moment
  Eigen::JacobiSVD< Eigen::MatrixXd > SVD(
      p,
      Eigen::ComputeThinU | Eigen::ComputeThinV );

  //define the control points
  points_t c;
  c.push_back(c0);
  //c.push_back(c0 + SVD.singularValues()[0] * SVD.matrixU().col(0));
  //c.push_back(c0 + SVD.singularValues()[1] * SVD.matrixU().col(1));
  //c.push_back(c0 + SVD.singularValues()[2] * SVD.matrixU().col(2));
  c.push_back(c0 + 15.0 * SVD.matrixU().col(0));
  c.push_back(c0 + 15.0 * SVD.matrixU().col(1));
  c.push_back(c0 + 15.0 * SVD.matrixU().col(2));

  //derive the barycentric frame
  Eigen::Vector3d e1 = c[1]-c0;
  double e1dote1 = e1.dot(e1);
  Eigen::Vector3d e2 = c[2]-c0;
  double e2dote2 = e2.dot(e2);
  Eigen::Vector3d e3 = c[3]-c0;
  double e3dote3 = e3.dot(e3);

  //derive the weighting factors
  Eigen::MatrixXd weights(4,indices.size());
  for( size_t i = 0; i < indices.size(); i++ )
  {
    Eigen::Vector3d temp = p.col(i);
    weights(1,i) = temp.dot(e1)/e1dote1;
    weights(2,i) = temp.dot(e2)/e2dote2;
    weights(3,i) = temp.dot(e3)/e3dote3;
    weights(0,i) = 1.0-(weights(1,i)+weights(2,i)+weights(3,i));
  }

  //setup matrix A and vector b
  Eigen::MatrixXd A = Eigen::MatrixXd::Zero(2*indices.size(),12);
  Eigen::MatrixXd b = Eigen::MatrixXd::Zero(2*indices.size(),1);
  for( size_t i = 0; i < indices.size(); i++ )
  {
    translation_t camOffset = adapter.getCamOffset(indices[i]);
    rotation_t camRotation = adapter.getCamRotation(indices[i]);
    //respect the rotation
    bearingVector_t f = camRotation * adapter.getBearingVector(indices[i]);

    A(2*i,0)  =  weights(0,i)*f[2];
    A(2*i,2)  = -weights(0,i)*f[0];
    A(2*i,3)  =  weights(1,i)*f[2];
    A(2*i,5)  = -weights(1,i)*f[0];
    A(2*i,6)  =  weights(2,i)*f[2];
    A(2*i,8)  = -weights(2,i)*f[0];
    A(2*i,9)  =  weights(3,i)*f[2];
    A(2*i,11) = -weights(3,i)*f[0];

    A(2*i+1,1)  =  weights(0,i)*f[2];
    A(2*i+1,2)  = -weights(0,i)*f[1];
    A(2*i+1,4)  =  weights(1,i)*f[2];
    A(2*i+1,5)  = -weights(1,i)*f[1];
    A(2*i+1,7)  =  weights(2,i)*f[2];
    A(2*i+1,8)  = -weights(2,i)*f[1];
    A(2*i+1,10) =  weights(3,i)*f[2];
    A(2*i+1,11) = -weights(3,i)*f[1];

    b(2*i,0)   = f[2]*camOffset[0]-f[0]*camOffset[2];
    b(2*i+1,0) = f[2]*camOffset[1]-f[1]*camOffset[2];
  }

  //computing the SVD
  Eigen::JacobiSVD< Eigen::MatrixXd > SVD2(
      A,
      Eigen::ComputeThinV | Eigen::ComputeThinU );

  //computing the pseudoinverse
  Eigen::MatrixXd invD = Eigen::MatrixXd::Zero(12,12);
  Eigen::MatrixXd D = SVD2.singularValues();
  for( size_t i = 0; i < 12; i++ )
  {
    if( D(i,0) > 1.e-6 )
      invD(i,i) = 1.0/D(i,0);
    else
      invD(i,i) = 0.0;
  }

  //Extract the nullsapce vectors;
  Eigen::MatrixXd V = SVD2.matrixV();

  //computing the nullspace intercept
  Eigen::MatrixXd pinvA = V * invD * SVD2.matrixU().transpose();

  //compute the intercept
  Eigen::Matrix<double,12,1> a = pinvA * b;

  //compute the solution
  transformation_t transformation;
  modules::gpnp_main( a, V, c, transformation );
  return transformation;
}

}
}

opengv::transformation_t
opengv::absolute_pose::gpnp( const AbsoluteAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return gpnp(adapter,idx);
}

opengv::transformation_t
opengv::absolute_pose::gpnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return gpnp(adapter,idx);
}

namespace opengv
{
namespace absolute_pose
{

void fill3x10( const Eigen::Vector3d & x, Eigen::Matrix<double,3,10> & Phi )
{
  double x1 = x[0];
  double x2 = x[1];
  double x3 = x[2];
  
  Phi << x1,  x1, -x1, -x1,     0.0,  2.0*x3, -2.0*x2, 2.0*x2, 2.0*x3,    0.0,
         x2, -x2,  x2, -x2, -2.0*x3,     0.0,  2.0*x1, 2.0*x1,    0.0, 2.0*x3,
         x3, -x3, -x3,  x3,  2.0*x2, -2.0*x1,     0.0,    0.0, 2.0*x1, 2.0*x2;
}

void f(
    const Eigen::Matrix<double,10,10> & M,
    const Eigen::Matrix<double,1,10> & C,
    double gamma,
    Eigen::Vector3d & f )
{
  f[0] = (2*M(0,4)+2*C(0,4));
  f[1] = (2*M(0,5)+2*C(0,5));
  f[2] = (2*M(0,6)+2*C(0,6));
}

void Jac(
    const Eigen::Matrix<double,10,10> & M,
    const Eigen::Matrix<double,1,10> & C,
    double gamma,
    Eigen::Matrix3d & Jac )
{
  Jac(0,0) = (2*M(4,4)+4*M(0,1)-4*M(0,0)+4*C(0,1)-4*C(0,0));
  Jac(0,1) = (2*M(5,4)+2*M(0,7)+2*C(0,7));
  Jac(0,2) = (2*M(6,4)+2*M(0,8)+2*C(0,8));
  Jac(1,0) = (2*M(4,5)+2*M(0,7)+2*C(0,7));
  Jac(1,1) = (2*M(5,5)+4*M(0,2)-4*M(0,0)+4*C(0,2)-4*C(0,0));
  Jac(1,2) = (2*M(6,5)+2*M(0,9)+2*C(0,9));
  Jac(2,0) = (2*M(4,6)+2*M(0,8)+2*C(0,8));
  Jac(2,1) = (2*M(5,6)+2*M(0,9)+2*C(0,9));
  Jac(2,2) = (2*M(6,6)+4*M(0,3)-4*M(0,0)+4*C(0,3)-4*C(0,0));
}

transformations_t upnp(
    const AbsoluteAdapterBase & adapter,
    const Indices & indices )
{
  assert( indices.size() > 2 );
    
  Eigen::Matrix<double,3,3> F = Eigen::Matrix3d::Zero();
  for( int i = 0; i < (int) indices.size(); i++ )
  {
    Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[i]) * adapter.getBearingVector(indices[i]);
    F += f * f.transpose();
  }
  
  Eigen::Matrix<double,3,3> H_inv = (indices.size() * Eigen::Matrix<double,3,3>::Identity()) - F;
  Eigen::Matrix<double,3,3> H = H_inv.inverse();
  
  Eigen::Matrix<double,3,10> I = Eigen::Matrix<double,3,10>::Zero();
  Eigen::Matrix<double,3,1> J = Eigen::Matrix<double,3,1>::Zero();
  Eigen::Matrix<double,3,10> Phi;
  
  for( int i = 0; i < (int) indices.size(); i++ )
  {
    Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[i]) * adapter.getBearingVector(indices[i]);
    Eigen::Matrix<double,3,3> Vk = H * ( f * f.transpose() - Eigen::Matrix<double,3,3>::Identity() );
    Eigen::Matrix<double,3,1> p = adapter.getPoint(indices[i]);
    Eigen::Matrix<double,3,1> v = adapter.getCamOffset(indices[i]);
    
    fill3x10(p,Phi);
    I += Vk * Phi;
    J += Vk * v;
  }
  
  Eigen::Matrix<double,10,10> M = Eigen::Matrix<double,10,10>::Zero();
  Eigen::Matrix<double,1,10>  C = Eigen::Matrix<double,1,10>::Zero();
  double gamma = 0.0;
  
  for(int i = 0; i < (int) indices.size(); i++ )
  {    
    Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[i]) * adapter.getBearingVector(indices[i]);
    Eigen::Matrix<double,3,1> v = adapter.getCamOffset(indices[i]);
    Eigen::Matrix<double,3,1> p = adapter.getPoint(indices[i]);
    
    fill3x10(p,Phi);
    Eigen::Matrix<double,3,3> temp = f*f.transpose() - Eigen::Matrix<double,3,3>::Identity();
    Eigen::Matrix<double,3,10> Ai =  temp * (Phi + I);
    Eigen::Matrix<double,3, 1> bi = -temp * (  v + J);
    
    M     += (Ai.transpose() * Ai);
    C     += (bi.transpose() * Ai);
    gamma += (bi.transpose() * bi);
  }
  
  //now do the main computation
  std::vector<std::pair<double,Eigen::Vector4d>,Eigen::aligned_allocator< std::pair<double,Eigen::Vector4d> > > quaternions1;
  if( indices.size() > 4 )
    modules::upnp_main_sym( M, C, gamma, quaternions1 );
  else
    modules::upnp_main( M, C, gamma, quaternions1 );
  
  //prepare the output vector
  transformations_t transformations;
  
  //Round 1: chirality check
  std::vector<std::pair<double,Eigen::Vector4d>,Eigen::aligned_allocator< std::pair<double,Eigen::Vector4d> > > quaternions2;
  for( size_t i = 0; i < quaternions1.size(); i++ )
  {
    rotation_t Rinv = math::quaternion2rot(quaternions1[i].second);
    
    Eigen::Matrix<double,10,1> s;
    modules::upnp_fill_s( quaternions1[i].second, s );
    translation_t tinv = I*s - J;
    
    if( transformations.size() == 0 )
    {
      transformation_t newTransformation;
      newTransformation.block<3,3>(0,0) = Rinv.transpose();
      newTransformation.block<3,1>(0,3) = -newTransformation.block<3,3>(0,0) * tinv;
      transformations.push_back(newTransformation);
    }
    
    int count_negative = 0;
    
    for( int j = 0; j < (int) indices.size(); j++ )
    {
      Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[j]) * adapter.getBearingVector(indices[j]);
      Eigen::Matrix<double,3,1> p = adapter.getPoint(indices[j]);
      Eigen::Matrix<double,3,1> v = adapter.getCamOffset(indices[j]);
      
      Eigen::Vector3d p_est = Rinv*p + tinv - v;
      
      if( p_est.transpose()*f < 0.0 )
        count_negative++;
    }
    
    if( count_negative < floor(0.2 * indices.size() + 0.5) )
      quaternions2.push_back(quaternions1[i]);
  }
  
  if( quaternions2.size() == 0 )
    return transformations;
  else
    transformations.clear();
  
  //Round 2: Second order optimality (plus polishing)
  Eigen::Matrix<double,3,10> I_cay;
  Eigen::Matrix<double,10,10> M_cay;
  Eigen::Matrix<double,1,10>  C_cay;
  double gamma_cay;
  
  for( size_t q = 0; q < quaternions2.size(); q++ )
  {    
    I_cay = Eigen::Matrix<double,3,10>::Zero();
    rotation_t Rinv = math::quaternion2rot(quaternions2[q].second);
    
    for( int i = 0; i < (int) indices.size(); i++ )
    {
      Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[i]) * adapter.getBearingVector(indices[i]);
      Eigen::Matrix<double,3,3> Vk = H * ( f * f.transpose() - Eigen::Matrix<double,3,3>::Identity() );
      Eigen::Matrix<double,3,1> p = Rinv * adapter.getPoint(indices[i]);
      
      fill3x10(p,Phi);
      I_cay += Vk * Phi;
    }
    
    M_cay = Eigen::Matrix<double,10,10>::Zero();
    C_cay = Eigen::Matrix<double,1,10>::Zero();
    gamma_cay = 0.0;
    
    for(int i = 0; i < (int) indices.size(); i++ )
    {    
      Eigen::Matrix<double,3,1> f = adapter.getCamRotation(indices[i]) * adapter.getBearingVector(indices[i]);
      Eigen::Matrix<double,3,1> v = adapter.getCamOffset(indices[i]);
      Eigen::Matrix<double,3,1> p = Rinv * adapter.getPoint(indices[i]);
      
      fill3x10(p,Phi);
      Eigen::Matrix<double,3,3> temp = f*f.transpose() - Eigen::Matrix<double,3,3>::Identity();
      Eigen::Matrix<double,3,10> Ai =  temp * (Phi + I_cay);
      Eigen::Matrix<double,3,1> bi = -temp * (  v + J);
      
      M_cay     += (Ai.transpose() * Ai);
      C_cay     += (bi.transpose() * Ai);
      gamma_cay += (bi.transpose() * bi);
    }
    
    //now analyze the eigenvalues of the "Hessian"
    Eigen::Vector3d val;
    Eigen::Matrix3d Jacobian;
    f( M_cay, C_cay, gamma_cay, val );
    Jac( M_cay, C_cay, gamma_cay, Jacobian );
    std::vector<double> characteristicPolynomial;
    characteristicPolynomial.push_back(-1.0);
    characteristicPolynomial.push_back(Jacobian(2,2)+Jacobian(1,1)+Jacobian(0,0));
    characteristicPolynomial.push_back(-Jacobian(2,2)*Jacobian(1,1)-Jacobian(2,2)*Jacobian(0,0)-Jacobian(1,1)*Jacobian(0,0)+pow(Jacobian(1,2),2)+pow(Jacobian(0,2),2)+pow(Jacobian(0,1),2));
    characteristicPolynomial.push_back(Jacobian(2,2)*Jacobian(1,1)*Jacobian(0,0)+2*Jacobian(1,2)*Jacobian(0,2)*Jacobian(0,1)-Jacobian(2,2)*pow(Jacobian(0,1),2)-pow(Jacobian(1,2),2)*Jacobian(0,0)-Jacobian(1,1)*pow(Jacobian(0,2),2));
    
    
    //const std::vector<double> roots = opengv::math::o3_roots( characteristicPolynomial );
    //
    //bool allPositive = true;
    //for( size_t i = 0; i < roots.size(); i++ )
    //{
    //  if( roots[i] < 0.0 )
    //  {
    //    allPositive = false;
    //    break;
    //  }
    //}
    //
    // use all results for the moment
    //
    //if( allPositive )
    {
      //perform the polishing step
      Eigen::Vector3d cay = - Jacobian.inverse() * val;
      rotation_t Rinv2 = math::cayley2rot(cay) * Rinv;
      quaternion_t q = math::rot2quaternion(Rinv2);
      
      Eigen::Matrix<double,10,1> s;
      modules::upnp_fill_s(q,s);
      translation_t tinv = I*s - J;
      
      transformation_t newTransformation;
      newTransformation.block<3,3>(0,0) = Rinv2.transpose();
      newTransformation.block<3,1>(0,3) = -newTransformation.block<3,3>(0,0) * tinv;
      transformations.push_back(newTransformation);
    }
  }
  
  //if there are no results, simply add the one with lowest score
  if( transformations.size() == 0 )
  {
    Eigen::Vector4d q = quaternions2[0].second;
    Eigen::Matrix<double,10,1> s;
    modules::upnp_fill_s(q,s);
    translation_t tinv = I*s - J;
    rotation_t Rinv = math::quaternion2rot(q);
    
    transformation_t newTransformation;
    newTransformation.block<3,3>(0,0) = Rinv.transpose();
    newTransformation.block<3,1>(0,3) = -newTransformation.block<3,3>(0,0) * tinv;
    transformations.push_back(newTransformation);
  }
  
  return transformations;
}

}
}

opengv::transformations_t
opengv::absolute_pose::upnp( const AbsoluteAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return upnp(adapter,idx);
}

opengv::transformations_t
opengv::absolute_pose::upnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return upnp(adapter,idx);
}

namespace opengv
{
namespace absolute_pose
{

struct OptimizeNonlinearFunctor1 : OptimizationFunctor<double>
{
  const AbsoluteAdapterBase & _adapter;
  const Indices & _indices;

  OptimizeNonlinearFunctor1(
      const AbsoluteAdapterBase & adapter,
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

    //compute inverse transformation
    transformation_t inverseSolution;
    inverseSolution.block<3,3>(0,0) = rotation.transpose();
    inverseSolution.col(3) = -inverseSolution.block<3,3>(0,0)*translation;

    Eigen::Matrix<double,4,1> p_hom;
    p_hom[3] = 1.0;

    for(size_t i = 0; i < _indices.size(); i++)
    {
      //get point in homogeneous form
      p_hom.block<3,1>(0,0) = _adapter.getPoint(_indices[i]);

      //compute the reprojection (this is working for both central and
      //non-central case)
      point_t bodyReprojection = inverseSolution * p_hom;
      point_t reprojection = _adapter.getCamRotation(_indices[i]).transpose() *
          (bodyReprojection - _adapter.getCamOffset(_indices[i]));
      reprojection = reprojection / reprojection.norm();

      //compute the score
      double factor = 1.0;
      fvec[i] = factor *
          (1.0 -
          (reprojection.transpose() * _adapter.getBearingVector(_indices[i])));
    }

    return 0;
  }
};

transformation_t optimize_nonlinear(
    const AbsoluteAdapterBase & adapter,
    const Indices & indices )
{
  const int n=6;
  VectorXd x(n);

  x.block<3,1>(0,0) = adapter.gett();
  x.block<3,1>(3,0) = math::rot2cayley(adapter.getR());

  OptimizeNonlinearFunctor1 functor( adapter, indices );
  NumericalDiff<OptimizeNonlinearFunctor1> numDiff(functor);
  LevenbergMarquardt< NumericalDiff<OptimizeNonlinearFunctor1> > lm(numDiff);

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
opengv::absolute_pose::optimize_nonlinear( const AbsoluteAdapterBase & adapter )
{
  Indices idx(adapter.getNumberCorrespondences());
  return optimize_nonlinear(adapter,idx);
}

opengv::transformation_t
opengv::absolute_pose::optimize_nonlinear(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices )
{
  Indices idx(indices);
  return optimize_nonlinear(adapter,idx);
}
