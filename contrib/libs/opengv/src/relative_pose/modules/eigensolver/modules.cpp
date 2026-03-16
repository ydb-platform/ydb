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


#include <opengv/relative_pose/modules/eigensolver/modules.hpp>
#include <opengv/math/cayley.hpp>
#include <iostream>

double
opengv::relative_pose::modules::eigensolver::getSmallestEV(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix3d & M )
{
  M = composeM(xxF,yyF,zzF,xyF,yzF,zxF,cayley);

  //Retrieve the smallest Eigenvalue by the following closed form solution
  double b = -M(0,0)-M(1,1)-M(2,2);
  double c =
      -pow(M(0,2),2)-pow(M(1,2),2)-pow(M(0,1),2)+
      M(0,0)*M(1,1)+M(0,0)*M(2,2)+M(1,1)*M(2,2);
  double d =
      M(1,1)*pow(M(0,2),2)+M(0,0)*pow(M(1,2),2)+M(2,2)*pow(M(0,1),2)-
      M(0,0)*M(1,1)*M(2,2)-2*M(0,1)*M(1,2)*M(0,2);

  double s = 2*pow(b,3)-9*b*c+27*d;
  double t = 4*pow((pow(b,2)-3*c),3);

  double alpha = acos(s/sqrt(t));
  double beta = alpha/3;
  double y = cos(beta);

  double r = 0.5*sqrt(t);
  double w = pow(r,(1.0/3.0));

  double k = w*y;
  double smallestEV = (-b-2*k)/3;
  return smallestEV;
}

double
opengv::relative_pose::modules::eigensolver::
    getSmallestEVwithJacobian(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix<double,1,3> & jacobian)
{
  Eigen::Matrix3d M_jac1 = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d M_jac2 = Eigen::Matrix3d::Zero();
  Eigen::Matrix3d M_jac3 = Eigen::Matrix3d::Zero();

  Eigen::Matrix3d M = composeMwithJacobians(
      xxF,yyF,zzF,xyF,yzF,zxF,cayley,M_jac1,M_jac2,M_jac3);

  //Retrieve the smallest Eigenvalue by the following closed form solution.
  //Plus Jacobian.
  double b = -M(0,0)-M(1,1)-M(2,2);
  double b_jac1 = -M_jac1(0,0)-M_jac1(1,1)-M_jac1(2,2);
  double b_jac2 = -M_jac2(0,0)-M_jac2(1,1)-M_jac2(2,2);
  double b_jac3 = -M_jac3(0,0)-M_jac3(1,1)-M_jac3(2,2);
  double c =
      -pow(M(0,2),2)-pow(M(1,2),2)-pow(M(0,1),2)+
      M(0,0)*M(1,1)+M(0,0)*M(2,2)+M(1,1)*M(2,2);
  double c_jac1 =
      -2.0*M(0,2)*M_jac1(0,2)-2.0*M(1,2)*M_jac1(1,2)-2.0*M(0,1)*M_jac1(0,1)
      +M_jac1(0,0)*M(1,1)+M(0,0)*M_jac1(1,1)+M_jac1(0,0)*M(2,2)
      +M(0,0)*M_jac1(2,2)+M_jac1(1,1)*M(2,2)+M(1,1)*M_jac1(2,2);
  double c_jac2 =
      -2.0*M(0,2)*M_jac2(0,2)-2.0*M(1,2)*M_jac2(1,2)-2.0*M(0,1)*M_jac2(0,1)
      +M_jac2(0,0)*M(1,1)+M(0,0)*M_jac2(1,1)+M_jac2(0,0)*M(2,2)
      +M(0,0)*M_jac2(2,2)+M_jac2(1,1)*M(2,2)+M(1,1)*M_jac2(2,2);
  double c_jac3 =
      -2.0*M(0,2)*M_jac3(0,2)-2.0*M(1,2)*M_jac3(1,2)-2.0*M(0,1)*M_jac3(0,1)
      +M_jac3(0,0)*M(1,1)+M(0,0)*M_jac3(1,1)+M_jac3(0,0)*M(2,2)
      +M(0,0)*M_jac3(2,2)+M_jac3(1,1)*M(2,2)+M(1,1)*M_jac3(2,2);
  double d =
      M(1,1)*pow(M(0,2),2)+M(0,0)*pow(M(1,2),2)+M(2,2)*pow(M(0,1),2)-
      M(0,0)*M(1,1)*M(2,2)-2*M(0,1)*M(1,2)*M(0,2);
  double d_jac1 =
      M_jac1(1,1)*pow(M(0,2),2)+M(1,1)*2*M(0,2)*M_jac1(0,2)
      +M_jac1(0,0)*pow(M(1,2),2)+M(0,0)*2.0*M(1,2)*M_jac1(1,2)
      +M_jac1(2,2)*pow(M(0,1),2)+M(2,2)*2.0*M(0,1)*M_jac1(0,1)
      -M_jac1(0,0)*M(1,1)*M(2,2)-M(0,0)*M_jac1(1,1)*M(2,2)
      -M(0,0)*M(1,1)*M_jac1(2,2)-2.0*(M_jac1(0,1)*M(1,2)*M(0,2)
      +M(0,1)*M_jac1(1,2)*M(0,2)+M(0,1)*M(1,2)*M_jac1(0,2));
  double d_jac2 =
      M_jac2(1,1)*pow(M(0,2),2)+M(1,1)*2*M(0,2)*M_jac2(0,2)
      +M_jac2(0,0)*pow(M(1,2),2)+M(0,0)*2.0*M(1,2)*M_jac2(1,2)
      +M_jac2(2,2)*pow(M(0,1),2)+M(2,2)*2.0*M(0,1)*M_jac2(0,1)
      -M_jac2(0,0)*M(1,1)*M(2,2)-M(0,0)*M_jac2(1,1)*M(2,2)
      -M(0,0)*M(1,1)*M_jac2(2,2)-2.0*(M_jac2(0,1)*M(1,2)*M(0,2)
      +M(0,1)*M_jac2(1,2)*M(0,2)+M(0,1)*M(1,2)*M_jac2(0,2));
  double d_jac3 =
      M_jac3(1,1)*pow(M(0,2),2)+M(1,1)*2*M(0,2)*M_jac3(0,2)
      +M_jac3(0,0)*pow(M(1,2),2)+M(0,0)*2.0*M(1,2)*M_jac3(1,2)
      +M_jac3(2,2)*pow(M(0,1),2)+M(2,2)*2.0*M(0,1)*M_jac3(0,1)
      -M_jac3(0,0)*M(1,1)*M(2,2)-M(0,0)*M_jac3(1,1)*M(2,2)
      -M(0,0)*M(1,1)*M_jac3(2,2)-2.0*(M_jac3(0,1)*M(1,2)*M(0,2)
      +M(0,1)*M_jac3(1,2)*M(0,2)+M(0,1)*M(1,2)*M_jac3(0,2));

  double s = 2*pow(b,3)-9*b*c+27*d;
  double t = 4*pow((pow(b,2)-3*c),3);
  double s_jac1 = 2.0*3.0*pow(b,2)*b_jac1-9.0*b_jac1*c-9.0*b*c_jac1+27.0*d_jac1;
  double s_jac2 = 2.0*3.0*pow(b,2)*b_jac2-9.0*b_jac2*c-9.0*b*c_jac2+27.0*d_jac2;
  double s_jac3 = 2.0*3.0*pow(b,2)*b_jac3-9.0*b_jac3*c-9.0*b*c_jac3+27.0*d_jac3;
  double t_jac1 = 4.0*3.0*pow((pow(b,2)-3.0*c),2)*(2.0*b*b_jac1-3.0*c_jac1);
  double t_jac2 = 4.0*3.0*pow((pow(b,2)-3.0*c),2)*(2.0*b*b_jac2-3.0*c_jac2);
  double t_jac3 = 4.0*3.0*pow((pow(b,2)-3.0*c),2)*(2.0*b*b_jac3-3.0*c_jac3);

  double alpha = acos(s/sqrt(t));
  double alpha_jac1 =
      -1.0/sqrt(1.0-(pow(s,2)/t)) *
      (s_jac1*sqrt(t)-s*0.5*pow(t,-0.5)*t_jac1)/t;
  double alpha_jac2 =
      -1.0/sqrt(1.0-(pow(s,2)/t)) *
      (s_jac2*sqrt(t)-s*0.5*pow(t,-0.5)*t_jac2)/t;
  double alpha_jac3 =
      -1.0/sqrt(1.0-(pow(s,2)/t)) *
      (s_jac3*sqrt(t)-s*0.5*pow(t,-0.5)*t_jac3)/t;
  double beta = alpha/3;
  double beta_jac1 = alpha_jac1/3.0;
  double beta_jac2 = alpha_jac2/3.0;
  double beta_jac3 = alpha_jac3/3.0;
  double y = cos(beta);
  double y_jac1 = -sin(beta)*beta_jac1;
  double y_jac2 = -sin(beta)*beta_jac2;
  double y_jac3 = -sin(beta)*beta_jac3;

  double r = 0.5*sqrt(t);
  double r_jac1 = 0.25*pow(t,-0.5)*t_jac1;
  double r_jac2 = 0.25*pow(t,-0.5)*t_jac2;
  double r_jac3 = 0.25*pow(t,-0.5)*t_jac3;
  double w = pow(r,(1.0/3.0));
  double w_jac1 = (1.0/3.0)*pow(r,-2.0/3.0)*r_jac1;
  double w_jac2 = (1.0/3.0)*pow(r,-2.0/3.0)*r_jac2;
  double w_jac3 = (1.0/3.0)*pow(r,-2.0/3.0)*r_jac3;

  double k = w*y;
  double k_jac1 = w_jac1*y+w*y_jac1;
  double k_jac2 = w_jac2*y+w*y_jac2;
  double k_jac3 = w_jac3*y+w*y_jac3;
  double smallestEV = (-b-2*k)/3;
  double smallestEV_jac1 = (-b_jac1-2.0*k_jac1)/3.0;
  double smallestEV_jac2 = (-b_jac2-2.0*k_jac2)/3.0;
  double smallestEV_jac3 = (-b_jac3-2.0*k_jac3)/3.0;

  jacobian(0,0) = smallestEV_jac1;
  jacobian(0,1) = smallestEV_jac2;
  jacobian(0,2) = smallestEV_jac3;
  return smallestEV;
}

Eigen::Matrix3d
opengv::relative_pose::modules::eigensolver::composeM(
  const Eigen::Matrix3d & xxF,
  const Eigen::Matrix3d & yyF,
  const Eigen::Matrix3d & zzF,
  const Eigen::Matrix3d & xyF,
  const Eigen::Matrix3d & yzF,
  const Eigen::Matrix3d & zxF,
  const cayley_t & cayley)
{
  Eigen::Matrix3d M;
  rotation_t R = math::cayley2rot_reduced(cayley);

  //Fill the matrix M using the precomputed summation terms
  double temp;
  temp =      R.row(2)*yyF*R.row(2).transpose();
  M(0,0)  = temp;
  temp = -2.0*R.row(2)*yzF*R.row(1).transpose();
  M(0,0) += temp;
  temp =      R.row(1)*zzF*R.row(1).transpose();
  M(0,0) += temp;

  temp =      R.row(2)*yzF*R.row(0).transpose();
  M(0,1)  = temp;
  temp = -1.0*R.row(2)*xyF*R.row(2).transpose();
  M(0,1) += temp;
  temp = -1.0*R.row(1)*zzF*R.row(0).transpose();
  M(0,1) += temp;
  temp =      R.row(1)*zxF*R.row(2).transpose();
  M(0,1) += temp;

  temp =      R.row(2)*xyF*R.row(1).transpose();
  M(0,2)  = temp;
  temp = -1.0*R.row(2)*yyF*R.row(0).transpose();
  M(0,2) += temp;
  temp = -1.0*R.row(1)*zxF*R.row(1).transpose();
  M(0,2) += temp;
  temp =      R.row(1)*yzF*R.row(0).transpose();
  M(0,2) += temp;

  temp =      R.row(0)*zzF*R.row(0).transpose();
  M(1,1)  = temp;
  temp = -2.0*R.row(0)*zxF*R.row(2).transpose();
  M(1,1) += temp;
  temp =      R.row(2)*xxF*R.row(2).transpose();
  M(1,1) += temp;

  temp =      R.row(0)*zxF*R.row(1).transpose();
  M(1,2)  = temp;
  temp = -1.0*R.row(0)*yzF*R.row(0).transpose();
  M(1,2) += temp;
  temp = -1.0*R.row(2)*xxF*R.row(1).transpose();
  M(1,2) += temp;
  temp =      R.row(2)*xyF*R.row(0).transpose();
  M(1,2) += temp;

  temp =      R.row(1)*xxF*R.row(1).transpose();
  M(2,2)  = temp;
  temp = -2.0*R.row(0)*xyF*R.row(1).transpose();
  M(2,2) += temp;
  temp =      R.row(0)*yyF*R.row(0).transpose();
  M(2,2) += temp;

  M(1,0) = M(0,1);
  M(2,0) = M(0,2);
  M(2,1) = M(1,2);

  return M;
}

Eigen::Matrix3d
opengv::relative_pose::modules::eigensolver::composeMwithJacobians(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix3d & M_jac1,
    Eigen::Matrix3d & M_jac2,
    Eigen::Matrix3d & M_jac3 )
{
  rotation_t R = math::cayley2rot_reduced(cayley);
  rotation_t R_jac1;
  rotation_t R_jac2;
  rotation_t R_jac3;

  R_jac1(0,0) = 2*cayley[0];
  R_jac1(0,1) = 2*cayley[1];
  R_jac1(0,2) = 2*cayley[2];
  R_jac1(1,0) = 2*cayley[1];
  R_jac1(1,1) = -2*cayley[0];
  R_jac1(1,2) = -2;
  R_jac1(2,0) = 2*cayley[2];
  R_jac1(2,1) = 2;
  R_jac1(2,2) = -2*cayley[0];
  R_jac2(0,0) = -2*cayley[1];
  R_jac2(0,1) = 2*cayley[0];
  R_jac2(0,2) = 2;
  R_jac2(1,0) = 2*cayley[0];
  R_jac2(1,1) = 2*cayley[1];
  R_jac2(1,2) = 2*cayley[2];
  R_jac2(2,0) = -2;
  R_jac2(2,1) = 2*cayley[2];
  R_jac2(2,2) = -2*cayley[1];
  R_jac3(0,0) = -2*cayley[2];
  R_jac3(0,1) = -2;
  R_jac3(0,2) = 2*cayley[0];
  R_jac3(1,0) = 2;
  R_jac3(1,1) = -2*cayley[2];
  R_jac3(1,2) = 2*cayley[1];
  R_jac3(2,0) = 2*cayley[0];
  R_jac3(2,1) = 2*cayley[1];
  R_jac3(2,2) = 2*cayley[2];

  //Fill the matrix M using the precomputed summation terms. Plus Jacobian.
  Eigen::Matrix3d M;
  double temp;
  temp =      R.row(2)*yyF*R.row(2).transpose();
  M(0,0)  = temp;
  temp = -2.0*R.row(2)*yzF*R.row(1).transpose();
  M(0,0) += temp;
  temp =      R.row(1)*zzF*R.row(1).transpose();
  M(0,0) += temp;
  temp = 2.0*R_jac1.row(2)*yyF*R.row(2).transpose();
  M_jac1(0,0)  = temp;
  temp = -2.0*R_jac1.row(2)*yzF*R.row(1).transpose();
  M_jac1(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac1.row(1).transpose();
  M_jac1(0,0) += temp;
  temp = 2.0*R_jac1.row(1)*zzF*R.row(1).transpose();
  M_jac1(0,0) += temp;
  temp = 2.0*R_jac2.row(2)*yyF*R.row(2).transpose();
  M_jac2(0,0)  = temp;
  temp = -2.0*R_jac2.row(2)*yzF*R.row(1).transpose();
  M_jac2(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac2.row(1).transpose();
  M_jac2(0,0) += temp;
  temp = 2.0*R_jac2.row(1)*zzF*R.row(1).transpose();
  M_jac2(0,0) += temp;
  temp = 2.0*R_jac3.row(2)*yyF*R.row(2).transpose();
  M_jac3(0,0)  = temp;
  temp = -2.0*R_jac3.row(2)*yzF*R.row(1).transpose();
  M_jac3(0,0) += temp;
  temp = -2.0*R.row(2)*yzF*R_jac3.row(1).transpose();
  M_jac3(0,0) += temp;
  temp = 2.0*R_jac3.row(1)*zzF*R.row(1).transpose();
  M_jac3(0,0) += temp;

  temp =      R.row(2)*yzF*R.row(0).transpose();
  M(0,1)  = temp;
  temp = -1.0*R.row(2)*xyF*R.row(2).transpose();
  M(0,1) += temp;
  temp = -1.0*R.row(1)*zzF*R.row(0).transpose();
  M(0,1) += temp;
  temp =      R.row(1)*zxF*R.row(2).transpose();
  M(0,1) += temp;
  temp = R_jac1.row(2)*yzF*R.row(0).transpose();
  M_jac1(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac1.row(0).transpose();
  M_jac1(0,1) += temp;
  temp = -2.0*R_jac1.row(2)*xyF*R.row(2).transpose();
  M_jac1(0,1) += temp;
  temp = -R_jac1.row(1)*zzF*R.row(0).transpose();
  M_jac1(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac1.row(0).transpose();
  M_jac1(0,1) += temp;
  temp = R_jac1.row(1)*zxF*R.row(2).transpose();
  M_jac1(0,1) += temp;
  temp = R.row(1)*zxF*R_jac1.row(2).transpose();
  M_jac1(0,1) += temp;
  temp = R_jac2.row(2)*yzF*R.row(0).transpose();
  M_jac2(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac2.row(0).transpose();
  M_jac2(0,1) += temp;
  temp = -2.0*R_jac2.row(2)*xyF*R.row(2).transpose();
  M_jac2(0,1) += temp;
  temp = -R_jac2.row(1)*zzF*R.row(0).transpose();
  M_jac2(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac2.row(0).transpose();
  M_jac2(0,1) += temp;
  temp = R_jac2.row(1)*zxF*R.row(2).transpose();
  M_jac2(0,1) += temp;
  temp = R.row(1)*zxF*R_jac2.row(2).transpose();
  M_jac2(0,1) += temp;
  temp = R_jac3.row(2)*yzF*R.row(0).transpose();
  M_jac3(0,1)  = temp;
  temp = R.row(2)*yzF*R_jac3.row(0).transpose();
  M_jac3(0,1) += temp;
  temp = -2.0*R_jac3.row(2)*xyF*R.row(2).transpose();
  M_jac3(0,1) += temp;
  temp = -R_jac3.row(1)*zzF*R.row(0).transpose();
  M_jac3(0,1) += temp;
  temp = -R.row(1)*zzF*R_jac3.row(0).transpose();
  M_jac3(0,1) += temp;
  temp = R_jac3.row(1)*zxF*R.row(2).transpose();
  M_jac3(0,1) += temp;
  temp = R.row(1)*zxF*R_jac3.row(2).transpose();
  M_jac3(0,1) += temp;

  temp =      R.row(2)*xyF*R.row(1).transpose();
  M(0,2)  = temp;
  temp = -1.0*R.row(2)*yyF*R.row(0).transpose();
  M(0,2) += temp;
  temp = -1.0*R.row(1)*zxF*R.row(1).transpose();
  M(0,2) += temp;
  temp =      R.row(1)*yzF*R.row(0).transpose();
  M(0,2) += temp;
  temp = R_jac1.row(2)*xyF*R.row(1).transpose();
  M_jac1(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac1.row(1).transpose();
  M_jac1(0,2) += temp;
  temp = -R_jac1.row(2)*yyF*R.row(0).transpose();
  M_jac1(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac1.row(0).transpose();
  M_jac1(0,2) += temp;
  temp = -2.0*R_jac1.row(1)*zxF*R.row(1).transpose();
  M_jac1(0,2) += temp;
  temp = R_jac1.row(1)*yzF*R.row(0).transpose();
  M_jac1(0,2) += temp;
  temp = R.row(1)*yzF*R_jac1.row(0).transpose();
  M_jac1(0,2) += temp;
  temp = R_jac2.row(2)*xyF*R.row(1).transpose();
  M_jac2(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac2.row(1).transpose();
  M_jac2(0,2) += temp;
  temp = -R_jac2.row(2)*yyF*R.row(0).transpose();
  M_jac2(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac2.row(0).transpose();
  M_jac2(0,2) += temp;
  temp = -2.0*R_jac2.row(1)*zxF*R.row(1).transpose();
  M_jac2(0,2) += temp;
  temp = R_jac2.row(1)*yzF*R.row(0).transpose();
  M_jac2(0,2) += temp;
  temp = R.row(1)*yzF*R_jac2.row(0).transpose();
  M_jac2(0,2) += temp;
  temp = R_jac3.row(2)*xyF*R.row(1).transpose();
  M_jac3(0,2)  = temp;
  temp = R.row(2)*xyF*R_jac3.row(1).transpose();
  M_jac3(0,2) += temp;
  temp = -R_jac3.row(2)*yyF*R.row(0).transpose();
  M_jac3(0,2) += temp;
  temp = -R.row(2)*yyF*R_jac3.row(0).transpose();
  M_jac3(0,2) += temp;
  temp = -2.0*R_jac3.row(1)*zxF*R.row(1).transpose();
  M_jac3(0,2) += temp;
  temp = R_jac3.row(1)*yzF*R.row(0).transpose();
  M_jac3(0,2) += temp;
  temp = R.row(1)*yzF*R_jac3.row(0).transpose();
  M_jac3(0,2) += temp;

  temp =      R.row(0)*zzF*R.row(0).transpose();
  M(1,1)  = temp;
  temp = -2.0*R.row(0)*zxF*R.row(2).transpose();
  M(1,1) += temp;
  temp =      R.row(2)*xxF*R.row(2).transpose();
  M(1,1) += temp;
  temp = 2.0*R_jac1.row(0)*zzF*R.row(0).transpose();
  M_jac1(1,1)  = temp;
  temp = -2.0*R_jac1.row(0)*zxF*R.row(2).transpose();
  M_jac1(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac1.row(2).transpose();
  M_jac1(1,1) += temp;
  temp = 2.0*R_jac1.row(2)*xxF*R.row(2).transpose();
  M_jac1(1,1) += temp;
  temp = 2.0*R_jac2.row(0)*zzF*R.row(0).transpose();
  M_jac2(1,1)  = temp;
  temp = -2.0*R_jac2.row(0)*zxF*R.row(2).transpose();
  M_jac2(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac2.row(2).transpose();
  M_jac2(1,1) += temp;
  temp = 2.0*R_jac2.row(2)*xxF*R.row(2).transpose();
  M_jac2(1,1) += temp;
  temp = 2.0*R_jac3.row(0)*zzF*R.row(0).transpose();
  M_jac3(1,1)  = temp;
  temp = -2.0*R_jac3.row(0)*zxF*R.row(2).transpose();
  M_jac3(1,1) += temp;
  temp = -2.0*R.row(0)*zxF*R_jac3.row(2).transpose();
  M_jac3(1,1) += temp;
  temp = 2.0*R_jac3.row(2)*xxF*R.row(2).transpose();
  M_jac3(1,1) += temp;

  temp =      R.row(0)*zxF*R.row(1).transpose();
  M(1,2)  = temp;
  temp = -1.0*R.row(0)*yzF*R.row(0).transpose();
  M(1,2) += temp;
  temp = -1.0*R.row(2)*xxF*R.row(1).transpose();
  M(1,2) += temp;
  temp =      R.row(2)*xyF*R.row(0).transpose();
  M(1,2) += temp;
  temp = R_jac1.row(0)*zxF*R.row(1).transpose();
  M_jac1(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac1.row(1).transpose();
  M_jac1(1,2) += temp;
  temp = -2.0*R_jac1.row(0)*yzF*R.row(0).transpose();
  M_jac1(1,2) += temp;
  temp = -R_jac1.row(2)*xxF*R.row(1).transpose();
  M_jac1(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac1.row(1).transpose();
  M_jac1(1,2) += temp;
  temp = R_jac1.row(2)*xyF*R.row(0).transpose();
  M_jac1(1,2) += temp;
  temp = R.row(2)*xyF*R_jac1.row(0).transpose();
  M_jac1(1,2) += temp;
  temp = R_jac2.row(0)*zxF*R.row(1).transpose();
  M_jac2(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac2.row(1).transpose();
  M_jac2(1,2) += temp;
  temp = -2.0*R_jac2.row(0)*yzF*R.row(0).transpose();
  M_jac2(1,2) += temp;
  temp = -R_jac2.row(2)*xxF*R.row(1).transpose();
  M_jac2(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac2.row(1).transpose();
  M_jac2(1,2) += temp;
  temp = R_jac2.row(2)*xyF*R.row(0).transpose();
  M_jac2(1,2) += temp;
  temp = R.row(2)*xyF*R_jac2.row(0).transpose();
  M_jac2(1,2) += temp;
  temp = R_jac3.row(0)*zxF*R.row(1).transpose();
  M_jac3(1,2)  = temp;
  temp = R.row(0)*zxF*R_jac3.row(1).transpose();
  M_jac3(1,2) += temp;
  temp = -2.0*R_jac3.row(0)*yzF*R.row(0).transpose();
  M_jac3(1,2) += temp;
  temp = -R_jac3.row(2)*xxF*R.row(1).transpose();
  M_jac3(1,2) += temp;
  temp = -R.row(2)*xxF*R_jac3.row(1).transpose();
  M_jac3(1,2) += temp;
  temp = R_jac3.row(2)*xyF*R.row(0).transpose();
  M_jac3(1,2) += temp;
  temp = R.row(2)*xyF*R_jac3.row(0).transpose();
  M_jac3(1,2) += temp;

  temp =      R.row(1)*xxF*R.row(1).transpose();
  M(2,2)  = temp;
  temp = -2.0*R.row(0)*xyF*R.row(1).transpose();
  M(2,2) += temp;
  temp =      R.row(0)*yyF*R.row(0).transpose();
  M(2,2) += temp;
  temp = 2.0*R_jac1.row(1)*xxF*R.row(1).transpose();
  M_jac1(2,2)  = temp;
  temp = -2.0*R_jac1.row(0)*xyF*R.row(1).transpose();
  M_jac1(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac1.row(1).transpose();
  M_jac1(2,2) += temp;
  temp = 2.0*R_jac1.row(0)*yyF*R.row(0).transpose();
  M_jac1(2,2) += temp;
  temp = 2.0*R_jac2.row(1)*xxF*R.row(1).transpose();
  M_jac2(2,2)  = temp;
  temp = -2.0*R_jac2.row(0)*xyF*R.row(1).transpose();
  M_jac2(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac2.row(1).transpose();
  M_jac2(2,2) += temp;
  temp = 2.0*R_jac2.row(0)*yyF*R.row(0).transpose();
  M_jac2(2,2) += temp;
  temp = 2.0*R_jac3.row(1)*xxF*R.row(1).transpose();
  M_jac3(2,2)  = temp;
  temp = -2.0*R_jac3.row(0)*xyF*R.row(1).transpose();
  M_jac3(2,2) += temp;
  temp = -2.0*R.row(0)*xyF*R_jac3.row(1).transpose();
  M_jac3(2,2) += temp;
  temp = 2.0*R_jac3.row(0)*yyF*R.row(0).transpose();
  M_jac3(2,2) += temp;

  M(1,0) = M(0,1);
  M(2,0) = M(0,2);
  M(2,1) = M(1,2);
  M_jac1(1,0) = M_jac1(0,1);
  M_jac1(2,0) = M_jac1(0,2);
  M_jac1(2,1) = M_jac1(1,2);
  M_jac2(1,0) = M_jac2(0,1);
  M_jac2(2,0) = M_jac2(0,2);
  M_jac2(2,1) = M_jac2(1,2);
  M_jac3(1,0) = M_jac3(0,1);
  M_jac3(2,0) = M_jac3(0,2);
  M_jac3(2,1) = M_jac3(1,2);

  return M;
}
