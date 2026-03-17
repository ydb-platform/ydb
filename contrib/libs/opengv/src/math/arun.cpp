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


#include <opengv/math/arun.hpp>

opengv::rotation_t
opengv::math::arun( const Eigen::MatrixXd & Hcross )
{
  //decompose matrix H to obtain rotation
  Eigen::JacobiSVD< Eigen::MatrixXd > SVDcross(
      Hcross,
      Eigen::ComputeFullU | Eigen::ComputeFullV );

  Eigen::Matrix3d V = SVDcross.matrixV();
  Eigen::Matrix3d U = SVDcross.matrixU();
  rotation_t R = V * U.transpose();

  //modify the result in case the rotation has determinant=-1
  if( R.determinant() < 0 )
  {
    Eigen::Matrix3d V_prime;
    V_prime.col(0) = V.col(0);
    V_prime.col(1) = V.col(1);
    V_prime.col(2) = -V.col(2);
    R = V_prime * U.transpose();
  }

  return R;
}

opengv::transformation_t
opengv::math::arun_complete(
    const points_t & p1,
    const points_t & p2 )
{
  assert(p1.size() == p2.size());

  //derive the centroid of the two point-clouds
  point_t pointsCenter1 = Eigen::Vector3d::Zero();
  point_t pointsCenter2 = Eigen::Vector3d::Zero();

  for( size_t i = 0; i < p1.size(); i++ )
  {
    pointsCenter1 += p1[i];
    pointsCenter2 += p2[i];
  }

  pointsCenter1 = pointsCenter1 / p1.size();
  pointsCenter2 = pointsCenter2 / p2.size();

  //compute the matrix H = sum(f'*f^{T})
  Eigen::MatrixXd Hcross(3,3);
  Hcross = Eigen::Matrix3d::Zero();

  for( size_t i = 0; i < p1.size(); i++ )
  {
    Eigen::Vector3d f = p1[i] - pointsCenter1;
    Eigen::Vector3d fprime = p2[i] - pointsCenter2;
    Hcross += fprime * f.transpose();
  }

  //decompose this matrix (SVD) to obtain rotation
  rotation_t rotation = arun(Hcross);
  translation_t translation = pointsCenter1 - rotation*pointsCenter2;
  transformation_t solution;
  solution.block<3,3>(0,0) = rotation;
  solution.col(3) = translation;

  return solution;
}
