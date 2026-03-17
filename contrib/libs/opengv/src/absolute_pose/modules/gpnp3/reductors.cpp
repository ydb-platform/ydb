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


#include <opengv/absolute_pose/modules/gpnp3/modules.hpp>


void
opengv::absolute_pose::modules::gpnp3::groebnerRow4_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(4,9);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(4,10);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(4,11);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(4,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(4,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(4,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(4,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(4,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(4,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow5_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,10) / groebnerMatrix(5,10);
  groebnerMatrix(targetRow,10) = 0.0;
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(5,11);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(5,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(5,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(5,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(5,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(5,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow5_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,4) / groebnerMatrix(5,10);
  groebnerMatrix(targetRow,4) = 0.0;
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(5,11);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(5,12);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(5,13);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(5,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(5,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(5,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow6_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,5) / groebnerMatrix(6,11);
  groebnerMatrix(targetRow,5) = 0.0;
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(6,12);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(6,13);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(6,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(6,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(6,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow6_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(6,11);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(6,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(6,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(6,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(6,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(6,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow4_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,3) / groebnerMatrix(4,9);
  groebnerMatrix(targetRow,3) = 0.0;
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(4,10);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(4,11);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(4,12);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(4,13);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(4,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(4,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(4,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(4,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow7_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,6) / groebnerMatrix(7,6);
  groebnerMatrix(targetRow,6) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(7,7);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(7,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(7,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(7,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(7,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(7,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(7,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow3_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,8) / groebnerMatrix(3,8);
  groebnerMatrix(targetRow,8) = 0.0;
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(3,9);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(3,10);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(3,11);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(3,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(3,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(3,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(3,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(3,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(3,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow5_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(5,10);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(5,11);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(5,12);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(5,13);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(5,14);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(5,15);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(5,16);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(5,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow3_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,2) / groebnerMatrix(3,8);
  groebnerMatrix(targetRow,2) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(3,9);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(3,10);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(3,11);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(3,12);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(3,13);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(3,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(3,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(3,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(3,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow8_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(8,7);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(8,12);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(8,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(8,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(8,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(8,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(8,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow4_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,0) / groebnerMatrix(4,9);
  groebnerMatrix(targetRow,0) = 0.0;
  groebnerMatrix(targetRow,1) -= factor * groebnerMatrix(4,10);
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(4,11);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(4,12);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(4,13);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(4,14);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(4,15);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(4,16);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(4,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow9_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,6) / groebnerMatrix(9,12);
  groebnerMatrix(targetRow,6) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(9,13);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(9,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(9,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(9,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(9,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow9_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(9,12);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(9,13);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(9,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(9,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(9,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(9,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow10_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,13) / groebnerMatrix(10,13);
  groebnerMatrix(targetRow,13) = 0.0;
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(10,14);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(10,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(10,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(10,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow10_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(10,13);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(10,14);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(10,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(10,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(10,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow11_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(11,14);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(11,15);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(11,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(11,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow11_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,14) / groebnerMatrix(11,14);
  groebnerMatrix(targetRow,14) = 0.0;
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(11,15);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(11,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(11,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow11_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(11,14);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(11,15);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(11,16);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(11,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow12_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,10) / groebnerMatrix(12,15);
  groebnerMatrix(targetRow,10) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(12,16);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(12,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow12_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(12,15);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(12,16);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(12,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow12_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,15) / groebnerMatrix(12,15);
  groebnerMatrix(targetRow,15) = 0.0;
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(12,16);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(12,17);
}

void
opengv::absolute_pose::modules::gpnp3::groebnerRow13_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,16) / groebnerMatrix(13,16);
  groebnerMatrix(targetRow,16) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(13,17);
}

