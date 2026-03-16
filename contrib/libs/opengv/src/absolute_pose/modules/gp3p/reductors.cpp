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


#include <opengv/absolute_pose/modules/gp3p/modules.hpp>


void
opengv::absolute_pose::modules::gp3p::groebnerRow9_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(9,21);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(9,33);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(9,53);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(9,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(9,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(9,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(9,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(9,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow10_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,20) / groebnerMatrix(10,66);
  groebnerMatrix(targetRow,20) = 0.0;
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(10,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(10,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(10,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(10,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow10_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,66) / groebnerMatrix(10,66);
  groebnerMatrix(targetRow,66) = 0.0;
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(10,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(10,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(10,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(10,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow11_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,29) / groebnerMatrix(11,29);
  groebnerMatrix(targetRow,29) = 0.0;
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(11,32);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(11,43);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(11,47);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(11,52);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(11,64);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(11,68);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(11,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(11,71);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(11,73);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(11,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(11,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(11,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(11,79);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(11,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(11,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(11,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(11,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow10_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(10,66);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(10,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(10,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(10,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(10,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,29) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,29) = 0.0;
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,30) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,30) = 0.0;
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,70) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,70) = 0.0;
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow14_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,33) / groebnerMatrix(14,33);
  groebnerMatrix(targetRow,33) = 0.0;
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(14,44);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(14,48);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(14,53);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(14,65);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(14,69);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(14,71);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(14,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(14,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(14,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(14,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(14,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(14,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(14,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(14,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(14,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,35) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,35) = 0.0;
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow10_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,45) / groebnerMatrix(10,66);
  groebnerMatrix(targetRow,45) = 0.0;
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(10,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(10,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(10,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(10,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,49) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,49) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,50) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,50) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,71) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,71) = 0.0;
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,33) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,33) = 0.0;
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow17_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,44) / groebnerMatrix(17,44);
  groebnerMatrix(targetRow,44) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(17,48);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(17,53);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(17,65);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(17,69);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(17,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(17,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(17,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(17,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(17,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(17,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(17,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(17,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(17,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,34) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,34) = 0.0;
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow16_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(16,54);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(16,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(16,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(16,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(16,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,28) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,28) = 0.0;
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,31) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,31) = 0.0;
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow19_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(19,55);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(19,56);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(19,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(19,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(19,77);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(19,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(19,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(19,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(19,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow19_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(19,55);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(19,56);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(19,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(19,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(19,77);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(19,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(19,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(19,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(19,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow19_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,10) / groebnerMatrix(19,55);
  groebnerMatrix(targetRow,10) = 0.0;
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(19,56);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(19,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(19,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(19,77);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(19,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(19,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(19,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(19,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow18_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,43) / groebnerMatrix(18,43);
  groebnerMatrix(targetRow,43) = 0.0;
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(18,47);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(18,52);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(18,64);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(18,79);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(18,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(18,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(18,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(18,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow19_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(19,55);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(19,56);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(19,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(19,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(19,77);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(19,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(19,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(19,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(19,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow20_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(20,42);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(20,46);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(20,51);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(20,63);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(20,67);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(20,72);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(20,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(20,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(20,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(20,78);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(20,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(20,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(20,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(20,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_100100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,5) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,5) = 0.0;
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow16_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,8) / groebnerMatrix(16,54);
  groebnerMatrix(targetRow,8) = 0.0;
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(16,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(16,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(16,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(16,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow21_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,14) / groebnerMatrix(21,14);
  groebnerMatrix(targetRow,14) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(21,48);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(21,53);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(21,56);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(21,65);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(21,69);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(21,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(21,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(21,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(21,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(21,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(21,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(21,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(21,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(21,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_100010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,4) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,4) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow16_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(16,54);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(16,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(16,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(16,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(16,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow22_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,13) / groebnerMatrix(22,13);
  groebnerMatrix(targetRow,13) = 0.0;
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(22,47);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(22,52);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(22,56);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(22,64);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(22,68);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(22,73);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(22,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(22,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(22,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(22,79);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(22,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(22,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(22,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(22,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,32) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,32) = 0.0;
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow15_100001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,3) / groebnerMatrix(15,71);
  groebnerMatrix(targetRow,3) = 0.0;
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(15,81);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(15,82);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(15,83);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(15,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow16_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,6) / groebnerMatrix(16,54);
  groebnerMatrix(targetRow,6) = 0.0;
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(16,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(16,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(16,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(16,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow23_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(23,12);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(23,46);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(23,51);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(23,56);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(23,63);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(23,67);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(23,72);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(23,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(23,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(23,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(23,78);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(23,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(23,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(23,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(23,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_100100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,2) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,2) = 0.0;
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow24_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,48) / groebnerMatrix(24,48);
  groebnerMatrix(targetRow,48) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(24,53);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(24,56);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(24,65);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(24,69);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(24,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(24,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(24,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(24,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(24,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(24,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(24,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(24,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(24,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_100010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow25_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,47) / groebnerMatrix(25,47);
  groebnerMatrix(targetRow,47) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(25,52);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(25,56);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(25,64);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(25,68);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(25,73);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(25,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(25,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(25,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(25,79);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(25,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(25,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(25,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(25,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow12_100001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,0) / groebnerMatrix(12,70);
  groebnerMatrix(targetRow,0) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(12,81);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(12,82);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(12,83);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(12,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow10_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(10,66);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,28) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(10,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(10,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(10,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(10,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow26_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,46) / groebnerMatrix(26,46);
  groebnerMatrix(targetRow,46) = 0.0;
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(26,51);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(26,56);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(26,63);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(26,67);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(26,72);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(26,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(26,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(26,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(26,78);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(26,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(26,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(26,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(26,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow28_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(28,52);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(28,56);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(28,64);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(28,68);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(28,73);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(28,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(28,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(28,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(28,79);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(28,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(28,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(28,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(28,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow27_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,53) / groebnerMatrix(27,53);
  groebnerMatrix(targetRow,53) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(27,56);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(27,65);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(27,69);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(27,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(27,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(27,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(27,77);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(27,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(27,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(27,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(27,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(27,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow29_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(29,51);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(29,56);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(29,63);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(29,67);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(29,72);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(29,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(29,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(29,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(29,78);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(29,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(29,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(29,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(29,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow31_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(31,63);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(31,65);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(31,67);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(31,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(31,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(31,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(31,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(31,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow30_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,43) / groebnerMatrix(30,64);
  groebnerMatrix(targetRow,43) = 0.0;
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(30,65);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(30,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(30,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(30,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(30,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(30,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(30,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow31_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,63) / groebnerMatrix(31,63);
  groebnerMatrix(targetRow,63) = 0.0;
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(31,65);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(31,67);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(31,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(31,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(31,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(31,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(31,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow30_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,64) / groebnerMatrix(30,64);
  groebnerMatrix(targetRow,64) = 0.0;
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(30,65);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(30,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(30,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(30,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(30,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(30,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(30,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow32_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,65) / groebnerMatrix(32,65);
  groebnerMatrix(targetRow,65) = 0.0;
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(32,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(32,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(32,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(32,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(32,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(32,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(32,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(32,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow32_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,44) / groebnerMatrix(32,65);
  groebnerMatrix(targetRow,44) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(32,67);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(32,68);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(32,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(32,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(32,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(32,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(32,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(32,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow33_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,46) / groebnerMatrix(33,67);
  groebnerMatrix(targetRow,46) = 0.0;
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(33,68);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(33,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(33,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(33,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(33,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(33,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(33,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(33,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow33_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,67) / groebnerMatrix(33,67);
  groebnerMatrix(targetRow,67) = 0.0;
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(33,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(33,69);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(33,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(33,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(33,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(33,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(33,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(33,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow34_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,47) / groebnerMatrix(34,68);
  groebnerMatrix(targetRow,47) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(34,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(34,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(34,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(34,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(34,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(34,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(34,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow34_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,68) / groebnerMatrix(34,68);
  groebnerMatrix(targetRow,68) = 0.0;
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(34,69);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(34,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(34,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(34,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(34,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(34,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(34,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow35_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,48) / groebnerMatrix(35,69);
  groebnerMatrix(targetRow,48) = 0.0;
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(35,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(35,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(35,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(35,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(35,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(35,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow35_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,69) / groebnerMatrix(35,69);
  groebnerMatrix(targetRow,69) = 0.0;
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(35,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(35,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(35,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(35,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(35,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(35,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow37_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(37,72);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(37,73);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(37,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(37,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(37,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(37,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(37,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(37,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow36_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,56) / groebnerMatrix(36,56);
  groebnerMatrix(targetRow,56) = 0.0;
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(36,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(36,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(36,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(36,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(36,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(36,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(36,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow37_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,72) / groebnerMatrix(37,72);
  groebnerMatrix(targetRow,72) = 0.0;
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(37,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(37,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(37,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(37,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(37,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(37,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(37,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow38_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(38,73);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(38,74);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(38,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(38,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(38,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(38,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(38,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(38,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow38_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,73) / groebnerMatrix(38,73);
  groebnerMatrix(targetRow,73) = 0.0;
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(38,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(38,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(38,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(38,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(38,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(38,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(38,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow39_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,53) / groebnerMatrix(39,74);
  groebnerMatrix(targetRow,53) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(39,75);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(39,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(39,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(39,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(39,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(39,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(39,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow39_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,74) / groebnerMatrix(39,74);
  groebnerMatrix(targetRow,74) = 0.0;
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(39,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(39,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(39,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(39,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(39,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(39,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(39,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow40_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(40,75);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(40,76);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(40,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(40,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(40,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(40,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(40,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(40,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow40_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,75) / groebnerMatrix(40,75);
  groebnerMatrix(targetRow,75) = 0.0;
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(40,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(40,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(40,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(40,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(40,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(40,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(40,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow32_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(32,65);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(32,67);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(32,68);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(32,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(32,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(32,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(32,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(32,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(32,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow33_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,23) / groebnerMatrix(33,67);
  groebnerMatrix(targetRow,23) = 0.0;
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(33,68);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(33,69);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(33,79);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(33,80);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(33,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(33,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(33,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(33,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow34_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,24) / groebnerMatrix(34,68);
  groebnerMatrix(targetRow,24) = 0.0;
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(34,69);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(34,79);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(34,80);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(34,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(34,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(34,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(34,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow33_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,25) / groebnerMatrix(33,67);
  groebnerMatrix(targetRow,25) = 0.0;
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(33,68);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(33,69);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(33,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(33,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(33,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(33,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(33,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(33,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow34_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,26) / groebnerMatrix(34,68);
  groebnerMatrix(targetRow,26) = 0.0;
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(34,69);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(34,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(34,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(34,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(34,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(34,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(34,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow35_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,27) / groebnerMatrix(35,69);
  groebnerMatrix(targetRow,27) = 0.0;
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(35,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(35,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(35,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(35,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(35,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(35,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow37_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,37) / groebnerMatrix(37,72);
  groebnerMatrix(targetRow,37) = 0.0;
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(37,73);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(37,79);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(37,80);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(37,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(37,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(37,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(37,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow38_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,38) / groebnerMatrix(38,73);
  groebnerMatrix(targetRow,38) = 0.0;
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(38,74);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(38,79);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(38,80);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(38,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(38,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(38,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(38,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow37_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,39) / groebnerMatrix(37,72);
  groebnerMatrix(targetRow,39) = 0.0;
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(37,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(37,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(37,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(37,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(37,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(37,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(37,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow38_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,40) / groebnerMatrix(38,73);
  groebnerMatrix(targetRow,40) = 0.0;
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(38,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(38,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(38,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(38,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(38,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(38,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(38,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow39_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,41) / groebnerMatrix(39,74);
  groebnerMatrix(targetRow,41) = 0.0;
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(39,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(39,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(39,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(39,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(39,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(39,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(39,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow41_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(41,76);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(41,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(41,79);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(41,80);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(41,81);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(41,82);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(41,83);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(41,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow41_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,76) / groebnerMatrix(41,76);
  groebnerMatrix(targetRow,76) = 0.0;
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(41,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(41,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(41,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(41,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(41,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(41,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(41,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow33_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,22) / groebnerMatrix(33,67);
  groebnerMatrix(targetRow,22) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(33,68);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(33,69);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(33,79);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(33,80);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(33,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(33,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(33,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(33,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow37_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,36) / groebnerMatrix(37,72);
  groebnerMatrix(targetRow,36) = 0.0;
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(37,73);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(37,79);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(37,80);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(37,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(37,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(37,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(37,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow42_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,58) / groebnerMatrix(42,58);
  groebnerMatrix(targetRow,58) = 0.0;
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(42,59);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(42,60);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(42,61);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(42,62);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(42,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(42,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(42,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(42,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(42,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(42,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(42,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(42,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow31_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,16) / groebnerMatrix(31,63);
  groebnerMatrix(targetRow,16) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(31,65);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(31,67);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(31,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(31,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(31,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(31,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(31,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow30_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(30,64);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(30,65);
  groebnerMatrix(targetRow,26) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(30,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(30,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(30,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(30,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(30,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(30,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow36_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,14) / groebnerMatrix(36,56);
  groebnerMatrix(targetRow,14) = 0.0;
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(36,72);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(36,79);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(36,80);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(36,81);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(36,82);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(36,83);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(36,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow36_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,15) / groebnerMatrix(36,56);
  groebnerMatrix(targetRow,15) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(36,72);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(36,79);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(36,80);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(36,81);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(36,82);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(36,83);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(36,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow44_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,60) / groebnerMatrix(44,60);
  groebnerMatrix(targetRow,60) = 0.0;
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(44,61);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(44,62);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(44,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(44,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(44,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(44,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(44,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(44,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(44,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(44,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow36_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,13) / groebnerMatrix(36,56);
  groebnerMatrix(targetRow,13) = 0.0;
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(36,72);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(36,79);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(36,80);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(36,81);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(36,82);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(36,83);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(36,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow45_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,61) / groebnerMatrix(45,61);
  groebnerMatrix(targetRow,61) = 0.0;
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(45,62);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(45,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(45,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(45,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(45,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(45,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(45,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(45,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(45,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow36_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(36,56);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(36,72);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(36,79);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(36,80);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(36,81);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(36,82);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(36,83);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(36,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow43_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,57) / groebnerMatrix(43,57);
  groebnerMatrix(targetRow,57) = 0.0;
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(43,59);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(43,60);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(43,61);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(43,62);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(43,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(43,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(43,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(43,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(43,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(43,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(43,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(43,84);
}

void
opengv::absolute_pose::modules::gp3p::groebnerRow46_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,59) / groebnerMatrix(46,59);
  groebnerMatrix(targetRow,59) = 0.0;
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(46,62);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(46,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(46,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(46,79);
  groebnerMatrix(targetRow,80) -= factor * groebnerMatrix(46,80);
  groebnerMatrix(targetRow,81) -= factor * groebnerMatrix(46,81);
  groebnerMatrix(targetRow,82) -= factor * groebnerMatrix(46,82);
  groebnerMatrix(targetRow,83) -= factor * groebnerMatrix(46,83);
  groebnerMatrix(targetRow,84) -= factor * groebnerMatrix(46,84);
}

