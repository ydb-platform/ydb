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


#include <opengv/absolute_pose/modules/gpnp4/modules.hpp>


void
opengv::absolute_pose::modules::gpnp4::groebnerRow5_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,23) / groebnerMatrix(5,23);
  groebnerMatrix(targetRow,23) = 0.0;
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(5,24);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(5,25);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(5,26);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(5,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(5,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(5,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(5,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(5,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(5,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(5,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(5,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(5,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(5,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow6_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,24) / groebnerMatrix(6,24);
  groebnerMatrix(targetRow,24) = 0.0;
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(6,25);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(6,26);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(6,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(6,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(6,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(6,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(6,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(6,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(6,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(6,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(6,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(6,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow7_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,25) / groebnerMatrix(7,25);
  groebnerMatrix(targetRow,25) = 0.0;
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(7,26);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(7,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(7,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(7,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(7,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(7,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(7,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(7,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(7,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(7,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(7,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow7_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(7,25);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(7,26);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(7,27);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(7,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(7,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(7,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(7,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(7,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(7,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(7,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(7,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(7,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow8_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,10) / groebnerMatrix(8,26);
  groebnerMatrix(targetRow,10) = 0.0;
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(8,27);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(8,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(8,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(8,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(8,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(8,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(8,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(8,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(8,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(8,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow5_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,13) / groebnerMatrix(5,23);
  groebnerMatrix(targetRow,13) = 0.0;
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(5,24);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(5,25);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,26);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(5,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(5,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(5,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(5,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(5,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(5,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(5,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(5,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(5,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(5,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow6_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,14) / groebnerMatrix(6,24);
  groebnerMatrix(targetRow,14) = 0.0;
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(6,25);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,26);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(6,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(6,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(6,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(6,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(6,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(6,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(6,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(6,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(6,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(6,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow7_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,15) / groebnerMatrix(7,25);
  groebnerMatrix(targetRow,15) = 0.0;
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(7,26);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(7,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(7,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(7,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(7,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(7,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(7,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(7,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(7,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(7,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(7,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow8_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,16) / groebnerMatrix(8,26);
  groebnerMatrix(targetRow,16) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(8,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(8,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(8,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(8,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(8,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(8,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(8,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(8,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(8,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(8,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow8_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,26) / groebnerMatrix(8,26);
  groebnerMatrix(targetRow,26) = 0.0;
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(8,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(8,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(8,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(8,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(8,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(8,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(8,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(8,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(8,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(8,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow6_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,8) / groebnerMatrix(6,24);
  groebnerMatrix(targetRow,8) = 0.0;
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(6,25);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(6,26);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(6,27);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(6,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(6,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(6,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(6,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(6,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(6,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(6,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(6,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow9_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(9,11);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(9,17);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(9,18);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(9,19);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(9,20);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(9,21);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(9,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(9,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(9,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(9,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(9,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(9,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(9,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(9,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(9,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(9,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow4_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(4,22);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(4,23);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(4,24);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(4,25);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(4,26);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(4,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(4,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(4,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(4,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(4,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(4,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(4,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(4,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(4,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(4,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow10_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(10,17);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(10,18);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(10,19);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(10,20);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(10,21);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(10,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(10,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(10,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(10,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(10,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(10,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(10,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(10,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(10,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(10,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow4_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,22) / groebnerMatrix(4,22);
  groebnerMatrix(targetRow,22) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(4,23);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(4,24);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(4,25);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(4,26);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(4,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(4,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(4,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(4,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(4,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(4,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(4,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(4,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(4,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(4,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow5_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(5,23);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(5,24);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(5,25);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(5,26);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(5,27);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(5,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(5,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(5,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(5,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(5,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(5,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(5,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(5,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow11_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(11,18);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(11,19);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(11,20);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(11,21);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(11,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(11,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(11,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(11,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(11,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(11,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(11,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(11,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(11,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(11,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow6_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,5) / groebnerMatrix(6,24);
  groebnerMatrix(targetRow,5) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(6,25);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(6,26);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(6,27);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(6,28);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(6,29);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,30);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(6,31);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(6,32);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(6,33);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(6,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(6,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(6,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow4_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,6) / groebnerMatrix(4,22);
  groebnerMatrix(targetRow,6) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(4,23);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(4,24);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(4,25);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(4,26);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(4,27);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(4,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(4,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(4,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(4,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(4,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(4,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(4,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(4,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(4,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow12_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(12,19);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(12,20);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(12,21);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(12,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(12,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(12,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(12,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(12,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(12,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(12,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(12,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(12,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(12,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow5_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,4) / groebnerMatrix(5,23);
  groebnerMatrix(targetRow,4) = 0.0;
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(5,24);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(5,25);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(5,26);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(5,27);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(5,28);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(5,29);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,30);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(5,31);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(5,32);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(5,33);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(5,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(5,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(5,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow13_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,20) / groebnerMatrix(13,20);
  groebnerMatrix(targetRow,20) = 0.0;
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(13,21);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(13,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(13,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(13,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(13,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(13,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(13,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(13,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(13,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(13,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(13,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow14_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,3) / groebnerMatrix(14,21);
  groebnerMatrix(targetRow,3) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(14,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(14,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(14,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(14,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(14,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(14,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(14,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(14,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(14,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(14,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow14_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(14,21);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(14,27);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(14,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(14,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(14,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(14,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(14,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(14,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(14,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(14,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(14,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow13_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,2) / groebnerMatrix(13,20);
  groebnerMatrix(targetRow,2) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(13,21);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(13,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(13,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(13,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(13,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(13,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(13,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(13,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(13,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(13,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(13,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow15_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(15,27);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(15,28);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(15,29);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(15,30);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(15,31);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(15,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(15,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(15,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(15,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(15,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow15_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(15,27);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(15,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(15,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(15,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(15,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(15,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(15,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(15,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(15,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(15,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow15_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,27) / groebnerMatrix(15,27);
  groebnerMatrix(targetRow,27) = 0.0;
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(15,28);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(15,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(15,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(15,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(15,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(15,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(15,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(15,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(15,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow12_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(12,19);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,2) -= factor * groebnerMatrix(12,20);
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(12,21);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(12,27);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(12,28);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(12,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(12,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(12,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(12,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(12,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(12,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(12,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(12,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow16_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(16,28);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(16,29);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(16,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(16,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(16,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(16,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(16,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(16,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(16,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow16_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,28) / groebnerMatrix(16,28);
  groebnerMatrix(targetRow,28) = 0.0;
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(16,29);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(16,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(16,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(16,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(16,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(16,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(16,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(16,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow14_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,0) / groebnerMatrix(14,21);
  groebnerMatrix(targetRow,0) = 0.0;
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(14,27);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(14,28);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(14,29);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(14,30);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(14,31);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(14,32);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(14,33);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(14,34);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(14,35);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(14,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow14_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(14,21);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(14,27);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(14,28);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(14,29);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(14,30);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(14,31);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(14,32);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(14,33);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(14,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(14,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(14,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow17_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(17,29);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(17,30);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(17,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(17,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(17,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(17,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(17,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(17,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow17_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,29) / groebnerMatrix(17,29);
  groebnerMatrix(targetRow,29) = 0.0;
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(17,30);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(17,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(17,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(17,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(17,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(17,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(17,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow18_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,30) / groebnerMatrix(18,30);
  groebnerMatrix(targetRow,30) = 0.0;
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(18,31);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(18,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(18,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(18,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(18,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(18,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow18_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,20) / groebnerMatrix(18,30);
  groebnerMatrix(targetRow,20) = 0.0;
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(18,31);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(18,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(18,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(18,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(18,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(18,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow19_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(19,31);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(19,32);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(19,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(19,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(19,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(19,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow19_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,31) / groebnerMatrix(19,31);
  groebnerMatrix(targetRow,31) = 0.0;
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(19,32);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(19,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(19,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(19,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(19,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,28) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,28) = 0.0;
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,32) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,32) = 0.0;
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow19_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(19,31);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(19,32);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(19,33);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(19,34);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(19,35);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(19,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow19_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(19,31);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(19,32);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(19,33);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(19,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(19,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(19,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,22) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,22) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,23) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,23) = 0.0;
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow21_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,24) / groebnerMatrix(21,33);
  groebnerMatrix(targetRow,24) = 0.0;
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(21,34);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(21,35);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(21,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,25) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,25) = 0.0;
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow21_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,26) / groebnerMatrix(21,33);
  groebnerMatrix(targetRow,26) = 0.0;
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(21,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(21,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(21,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow21_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,29) / groebnerMatrix(21,33);
  groebnerMatrix(targetRow,29) = 0.0;
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(21,34);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(21,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(21,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow21_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,33) / groebnerMatrix(21,33);
  groebnerMatrix(targetRow,33) = 0.0;
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(21,34);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(21,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(21,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow20_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,15) / groebnerMatrix(20,32);
  groebnerMatrix(targetRow,15) = 0.0;
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(20,33);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(20,34);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(20,35);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(20,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow21_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,16) / groebnerMatrix(21,33);
  groebnerMatrix(targetRow,16) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(21,34);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(21,35);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(21,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow22_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(22,34);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(22,35);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(22,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow19_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,20) / groebnerMatrix(19,31);
  groebnerMatrix(targetRow,20) = 0.0;
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(19,32);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(19,33);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(19,34);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(19,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(19,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow22_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,27) / groebnerMatrix(22,34);
  groebnerMatrix(targetRow,27) = 0.0;
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(22,35);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(22,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow22_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,30) / groebnerMatrix(22,34);
  groebnerMatrix(targetRow,30) = 0.0;
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(22,35);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(22,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow22_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,34) / groebnerMatrix(22,34);
  groebnerMatrix(targetRow,34) = 0.0;
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(22,35);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(22,36);
}

void
opengv::absolute_pose::modules::gpnp4::groebnerRow23_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,35) / groebnerMatrix(23,35);
  groebnerMatrix(targetRow,35) = 0.0;
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(23,36);
}

