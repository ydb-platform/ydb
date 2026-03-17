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


#include <opengv/triangulation/methods.hpp>
#include <Eigen/Eigenvalues>

opengv::point_t
opengv::triangulation::triangulate(
    const relative_pose::RelativeAdapterBase & adapter,
    size_t index )
{
  translation_t t12 = adapter.gett12();
  rotation_t R12 = adapter.getR12();
  Eigen::Matrix<double,3,4> P1 = Eigen::Matrix<double,3,4>::Zero();
  P1.block<3,3>(0,0) = Eigen::Matrix3d::Identity();
  Eigen::Matrix<double,3,4> P2 = Eigen::Matrix<double,3,4>::Zero();
  P2.block<3,3>(0,0) = R12.transpose();
  P2.block<3,1>(0,3) = -R12.transpose()*t12;
  bearingVector_t f1 = adapter.getBearingVector1(index);
  bearingVector_t f2 = adapter.getBearingVector2(index);

  Eigen::MatrixXd A(4,4);
  A.row(0) = f1[0] * P1.row(2) - f1[2] * P1.row(0);
  A.row(1) = f1[1] * P1.row(2) - f1[2] * P1.row(1);
  A.row(2) = f2[0] * P2.row(2) - f2[2] * P2.row(0);
  A.row(3) = f2[1] * P2.row(2) - f2[2] * P2.row(1);

  Eigen::JacobiSVD< Eigen::MatrixXd > mySVD(A, Eigen::ComputeFullV );
  point_t worldPoint;
  worldPoint[0] = mySVD.matrixV()(0,3);
  worldPoint[1] = mySVD.matrixV()(1,3);
  worldPoint[2] = mySVD.matrixV()(2,3);
  worldPoint = worldPoint / mySVD.matrixV()(3,3);

  return worldPoint;
};

opengv::point_t
opengv::triangulation::triangulate2(
    const relative_pose::RelativeAdapterBase & adapter,
    size_t index )
{
  translation_t t12 = adapter.gett12();
  rotation_t R12 = adapter.getR12();
  bearingVector_t f1 = adapter.getBearingVector1(index);
  bearingVector_t f2 = adapter.getBearingVector2(index);

  bearingVector_t f2_unrotated = R12 * f2;
  Eigen::Vector2d b;
  b[0] = t12.dot(f1);
  b[1] = t12.dot(f2_unrotated);
  Eigen::Matrix2d A;
  A(0,0) = f1.dot(f1);
  A(1,0) = f1.dot(f2_unrotated);
  A(0,1) = -A(1,0);
  A(1,1) = -f2_unrotated.dot(f2_unrotated);
  Eigen::Vector2d lambda = A.inverse() * b;
  Eigen::Vector3d xm = lambda[0] * f1;
  Eigen::Vector3d xn = t12 + lambda[1] * f2_unrotated;
  point_t point = ( xm + xn )/2;
  return point;
};
