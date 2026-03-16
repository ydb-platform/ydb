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
opengv::absolute_pose::modules::gp3p::sPolynomial9( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(9,21) = (groebnerMatrix(0,21)/(groebnerMatrix(0,20))-groebnerMatrix(1,21)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,33) = (groebnerMatrix(0,33)/(groebnerMatrix(0,20))-groebnerMatrix(1,33)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,53) = (groebnerMatrix(0,53)/(groebnerMatrix(0,20))-groebnerMatrix(1,53)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,66) = (groebnerMatrix(0,66)/(groebnerMatrix(0,20))-groebnerMatrix(1,66)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,70) = -groebnerMatrix(1,70)/(groebnerMatrix(1,20));
  groebnerMatrix(9,71) = (groebnerMatrix(0,71)/(groebnerMatrix(0,20))-groebnerMatrix(1,71)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,75) = groebnerMatrix(0,75)/(groebnerMatrix(0,20));
  groebnerMatrix(9,76) = (groebnerMatrix(0,76)/(groebnerMatrix(0,20))-groebnerMatrix(1,76)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,77) = (groebnerMatrix(0,77)/(groebnerMatrix(0,20))-groebnerMatrix(1,77)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,80) = (groebnerMatrix(0,80)/(groebnerMatrix(0,20))-groebnerMatrix(1,80)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,81) = (groebnerMatrix(0,81)/(groebnerMatrix(0,20))-groebnerMatrix(1,81)/(groebnerMatrix(1,20)));
  groebnerMatrix(9,82) = groebnerMatrix(0,82)/(groebnerMatrix(0,20));
  groebnerMatrix(9,83) = -groebnerMatrix(1,83)/(groebnerMatrix(1,20));
  groebnerMatrix(9,84) = (groebnerMatrix(0,84)/(groebnerMatrix(0,20))-groebnerMatrix(1,84)/(groebnerMatrix(1,20)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial10( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(10,21) = (groebnerMatrix(1,21)/(groebnerMatrix(1,20))-groebnerMatrix(2,21)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,33) = (groebnerMatrix(1,33)/(groebnerMatrix(1,20))-groebnerMatrix(2,33)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,53) = (groebnerMatrix(1,53)/(groebnerMatrix(1,20))-groebnerMatrix(2,53)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,66) = (groebnerMatrix(1,66)/(groebnerMatrix(1,20))-groebnerMatrix(2,66)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,70) = (groebnerMatrix(1,70)/(groebnerMatrix(1,20))-groebnerMatrix(2,70)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,71) = (groebnerMatrix(1,71)/(groebnerMatrix(1,20))-groebnerMatrix(2,71)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,75) = -groebnerMatrix(2,75)/(groebnerMatrix(2,20));
  groebnerMatrix(10,76) = groebnerMatrix(1,76)/(groebnerMatrix(1,20));
  groebnerMatrix(10,77) = (groebnerMatrix(1,77)/(groebnerMatrix(1,20))-groebnerMatrix(2,77)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,80) = (groebnerMatrix(1,80)/(groebnerMatrix(1,20))-groebnerMatrix(2,80)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,81) = groebnerMatrix(1,81)/(groebnerMatrix(1,20));
  groebnerMatrix(10,82) = -groebnerMatrix(2,82)/(groebnerMatrix(2,20));
  groebnerMatrix(10,83) = (groebnerMatrix(1,83)/(groebnerMatrix(1,20))-groebnerMatrix(2,83)/(groebnerMatrix(2,20)));
  groebnerMatrix(10,84) = (groebnerMatrix(1,84)/(groebnerMatrix(1,20))-groebnerMatrix(2,84)/(groebnerMatrix(2,20)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial11( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(11,20) = (groebnerMatrix(3,20)/(groebnerMatrix(3,19))-groebnerMatrix(4,20)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,32) = (groebnerMatrix(3,32)/(groebnerMatrix(3,19))-groebnerMatrix(4,32)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,52) = (groebnerMatrix(3,52)/(groebnerMatrix(3,19))-groebnerMatrix(4,52)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,66) = (groebnerMatrix(3,66)/(groebnerMatrix(3,19))-groebnerMatrix(4,66)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,70) = -groebnerMatrix(4,70)/(groebnerMatrix(4,19));
  groebnerMatrix(11,71) = (groebnerMatrix(3,71)/(groebnerMatrix(3,19))-groebnerMatrix(4,71)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,75) = groebnerMatrix(3,75)/(groebnerMatrix(3,19));
  groebnerMatrix(11,76) = (groebnerMatrix(3,76)/(groebnerMatrix(3,19))-groebnerMatrix(4,76)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,77) = (groebnerMatrix(3,77)/(groebnerMatrix(3,19))-groebnerMatrix(4,77)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,79) = (groebnerMatrix(3,79)/(groebnerMatrix(3,19))-groebnerMatrix(4,79)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,81) = (groebnerMatrix(3,81)/(groebnerMatrix(3,19))-groebnerMatrix(4,81)/(groebnerMatrix(4,19)));
  groebnerMatrix(11,82) = groebnerMatrix(3,82)/(groebnerMatrix(3,19));
  groebnerMatrix(11,83) = -groebnerMatrix(4,83)/(groebnerMatrix(4,19));
  groebnerMatrix(11,84) = (groebnerMatrix(3,84)/(groebnerMatrix(3,19))-groebnerMatrix(4,84)/(groebnerMatrix(4,19)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial12( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(12,20) = (groebnerMatrix(4,20)/(groebnerMatrix(4,19))-groebnerMatrix(5,20)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,32) = (groebnerMatrix(4,32)/(groebnerMatrix(4,19))-groebnerMatrix(5,32)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,52) = (groebnerMatrix(4,52)/(groebnerMatrix(4,19))-groebnerMatrix(5,52)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,66) = (groebnerMatrix(4,66)/(groebnerMatrix(4,19))-groebnerMatrix(5,66)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,70) = (groebnerMatrix(4,70)/(groebnerMatrix(4,19))-groebnerMatrix(5,70)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,71) = (groebnerMatrix(4,71)/(groebnerMatrix(4,19))-groebnerMatrix(5,71)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,75) = -groebnerMatrix(5,75)/(groebnerMatrix(5,19));
  groebnerMatrix(12,76) = groebnerMatrix(4,76)/(groebnerMatrix(4,19));
  groebnerMatrix(12,77) = (groebnerMatrix(4,77)/(groebnerMatrix(4,19))-groebnerMatrix(5,77)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,79) = (groebnerMatrix(4,79)/(groebnerMatrix(4,19))-groebnerMatrix(5,79)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,81) = groebnerMatrix(4,81)/(groebnerMatrix(4,19));
  groebnerMatrix(12,82) = -groebnerMatrix(5,82)/(groebnerMatrix(5,19));
  groebnerMatrix(12,83) = (groebnerMatrix(4,83)/(groebnerMatrix(4,19))-groebnerMatrix(5,83)/(groebnerMatrix(5,19)));
  groebnerMatrix(12,84) = (groebnerMatrix(4,84)/(groebnerMatrix(4,19))-groebnerMatrix(5,84)/(groebnerMatrix(5,19)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial13( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(13,20) = groebnerMatrix(5,20)/(groebnerMatrix(5,19));
  groebnerMatrix(13,21) = -groebnerMatrix(6,21)/(groebnerMatrix(6,19));
  groebnerMatrix(13,32) = groebnerMatrix(5,32)/(groebnerMatrix(5,19));
  groebnerMatrix(13,33) = -groebnerMatrix(6,33)/(groebnerMatrix(6,19));
  groebnerMatrix(13,52) = groebnerMatrix(5,52)/(groebnerMatrix(5,19));
  groebnerMatrix(13,53) = -groebnerMatrix(6,53)/(groebnerMatrix(6,19));
  groebnerMatrix(13,66) = (groebnerMatrix(5,66)/(groebnerMatrix(5,19))-groebnerMatrix(6,66)/(groebnerMatrix(6,19)));
  groebnerMatrix(13,70) = groebnerMatrix(5,70)/(groebnerMatrix(5,19));
  groebnerMatrix(13,71) = (groebnerMatrix(5,71)/(groebnerMatrix(5,19))-groebnerMatrix(6,71)/(groebnerMatrix(6,19)));
  groebnerMatrix(13,75) = (groebnerMatrix(5,75)/(groebnerMatrix(5,19))-groebnerMatrix(6,75)/(groebnerMatrix(6,19)));
  groebnerMatrix(13,76) = -groebnerMatrix(6,76)/(groebnerMatrix(6,19));
  groebnerMatrix(13,77) = (groebnerMatrix(5,77)/(groebnerMatrix(5,19))-groebnerMatrix(6,77)/(groebnerMatrix(6,19)));
  groebnerMatrix(13,79) = groebnerMatrix(5,79)/(groebnerMatrix(5,19));
  groebnerMatrix(13,80) = -groebnerMatrix(6,80)/(groebnerMatrix(6,19));
  groebnerMatrix(13,81) = -groebnerMatrix(6,81)/(groebnerMatrix(6,19));
  groebnerMatrix(13,82) = (groebnerMatrix(5,82)/(groebnerMatrix(5,19))-groebnerMatrix(6,82)/(groebnerMatrix(6,19)));
  groebnerMatrix(13,83) = groebnerMatrix(5,83)/(groebnerMatrix(5,19));
  groebnerMatrix(13,84) = (groebnerMatrix(5,84)/(groebnerMatrix(5,19))-groebnerMatrix(6,84)/(groebnerMatrix(6,19)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial14( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(14,21) = (groebnerMatrix(6,21)/(groebnerMatrix(6,19))-groebnerMatrix(7,21)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,33) = (groebnerMatrix(6,33)/(groebnerMatrix(6,19))-groebnerMatrix(7,33)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,53) = (groebnerMatrix(6,53)/(groebnerMatrix(6,19))-groebnerMatrix(7,53)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,66) = (groebnerMatrix(6,66)/(groebnerMatrix(6,19))-groebnerMatrix(7,66)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,70) = -groebnerMatrix(7,70)/(groebnerMatrix(7,19));
  groebnerMatrix(14,71) = (groebnerMatrix(6,71)/(groebnerMatrix(6,19))-groebnerMatrix(7,71)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,75) = groebnerMatrix(6,75)/(groebnerMatrix(6,19));
  groebnerMatrix(14,76) = (groebnerMatrix(6,76)/(groebnerMatrix(6,19))-groebnerMatrix(7,76)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,77) = (groebnerMatrix(6,77)/(groebnerMatrix(6,19))-groebnerMatrix(7,77)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,80) = (groebnerMatrix(6,80)/(groebnerMatrix(6,19))-groebnerMatrix(7,80)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,81) = (groebnerMatrix(6,81)/(groebnerMatrix(6,19))-groebnerMatrix(7,81)/(groebnerMatrix(7,19)));
  groebnerMatrix(14,82) = groebnerMatrix(6,82)/(groebnerMatrix(6,19));
  groebnerMatrix(14,83) = -groebnerMatrix(7,83)/(groebnerMatrix(7,19));
  groebnerMatrix(14,84) = (groebnerMatrix(6,84)/(groebnerMatrix(6,19))-groebnerMatrix(7,84)/(groebnerMatrix(7,19)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial15( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(15,21) = (groebnerMatrix(7,21)/(groebnerMatrix(7,19))-groebnerMatrix(8,21)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,33) = (groebnerMatrix(7,33)/(groebnerMatrix(7,19))-groebnerMatrix(8,33)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,53) = (groebnerMatrix(7,53)/(groebnerMatrix(7,19))-groebnerMatrix(8,53)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,66) = (groebnerMatrix(7,66)/(groebnerMatrix(7,19))-groebnerMatrix(8,66)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,70) = (groebnerMatrix(7,70)/(groebnerMatrix(7,19))-groebnerMatrix(8,70)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,71) = (groebnerMatrix(7,71)/(groebnerMatrix(7,19))-groebnerMatrix(8,71)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,75) = -groebnerMatrix(8,75)/(groebnerMatrix(8,19));
  groebnerMatrix(15,76) = groebnerMatrix(7,76)/(groebnerMatrix(7,19));
  groebnerMatrix(15,77) = (groebnerMatrix(7,77)/(groebnerMatrix(7,19))-groebnerMatrix(8,77)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,80) = (groebnerMatrix(7,80)/(groebnerMatrix(7,19))-groebnerMatrix(8,80)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,81) = groebnerMatrix(7,81)/(groebnerMatrix(7,19));
  groebnerMatrix(15,82) = -groebnerMatrix(8,82)/(groebnerMatrix(8,19));
  groebnerMatrix(15,83) = (groebnerMatrix(7,83)/(groebnerMatrix(7,19))-groebnerMatrix(8,83)/(groebnerMatrix(8,19)));
  groebnerMatrix(15,84) = (groebnerMatrix(7,84)/(groebnerMatrix(7,19))-groebnerMatrix(8,84)/(groebnerMatrix(8,19)));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial16( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(16,35) = groebnerMatrix(12,71)/(groebnerMatrix(12,70));
  groebnerMatrix(16,45) = -groebnerMatrix(15,75)/(groebnerMatrix(15,71));
  groebnerMatrix(16,49) = (groebnerMatrix(12,75)/(groebnerMatrix(12,70))-groebnerMatrix(15,76)/(groebnerMatrix(15,71)));
  groebnerMatrix(16,50) = groebnerMatrix(12,76)/(groebnerMatrix(12,70));
  groebnerMatrix(16,54) = -groebnerMatrix(15,77)/(groebnerMatrix(15,71));
  groebnerMatrix(16,55) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(16,66) = -groebnerMatrix(15,81)/(groebnerMatrix(15,71));
  groebnerMatrix(16,70) = (groebnerMatrix(12,81)/(groebnerMatrix(12,70))-groebnerMatrix(15,82)/(groebnerMatrix(15,71)));
  groebnerMatrix(16,71) = groebnerMatrix(12,82)/(groebnerMatrix(12,70));
  groebnerMatrix(16,75) = -groebnerMatrix(15,83)/(groebnerMatrix(15,71));
  groebnerMatrix(16,76) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(16,81) = -groebnerMatrix(15,84)/(groebnerMatrix(15,71));
  groebnerMatrix(16,82) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial17( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(17,44) = (groebnerMatrix(14,44)/(groebnerMatrix(14,33))-groebnerMatrix(15,75)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,48) = (groebnerMatrix(14,48)/(groebnerMatrix(14,33))-groebnerMatrix(15,76)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,53) = (groebnerMatrix(14,53)/(groebnerMatrix(14,33))-groebnerMatrix(15,77)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,65) = (groebnerMatrix(14,65)/(groebnerMatrix(14,33))-groebnerMatrix(15,81)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,69) = (groebnerMatrix(14,69)/(groebnerMatrix(14,33))-groebnerMatrix(15,82)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,71) = groebnerMatrix(14,71)/(groebnerMatrix(14,33));
  groebnerMatrix(17,74) = (groebnerMatrix(14,74)/(groebnerMatrix(14,33))-groebnerMatrix(15,83)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,75) = groebnerMatrix(14,75)/(groebnerMatrix(14,33));
  groebnerMatrix(17,76) = groebnerMatrix(14,76)/(groebnerMatrix(14,33));
  groebnerMatrix(17,77) = groebnerMatrix(14,77)/(groebnerMatrix(14,33));
  groebnerMatrix(17,80) = (groebnerMatrix(14,80)/(groebnerMatrix(14,33))-groebnerMatrix(15,84)/(groebnerMatrix(15,71)));
  groebnerMatrix(17,81) = groebnerMatrix(14,81)/(groebnerMatrix(14,33));
  groebnerMatrix(17,82) = groebnerMatrix(14,82)/(groebnerMatrix(14,33));
  groebnerMatrix(17,83) = groebnerMatrix(14,83)/(groebnerMatrix(14,33));
  groebnerMatrix(17,84) = groebnerMatrix(14,84)/(groebnerMatrix(14,33));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial18( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(18,33) = groebnerMatrix(13,33)/(groebnerMatrix(13,32));
  groebnerMatrix(18,43) = (groebnerMatrix(13,43)/(groebnerMatrix(13,32))-groebnerMatrix(15,75)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,44) = groebnerMatrix(13,44)/(groebnerMatrix(13,32));
  groebnerMatrix(18,47) = (groebnerMatrix(13,47)/(groebnerMatrix(13,32))-groebnerMatrix(15,76)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,48) = groebnerMatrix(13,48)/(groebnerMatrix(13,32));
  groebnerMatrix(18,52) = (groebnerMatrix(13,52)/(groebnerMatrix(13,32))-groebnerMatrix(15,77)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,53) = groebnerMatrix(13,53)/(groebnerMatrix(13,32));
  groebnerMatrix(18,64) = (groebnerMatrix(13,64)/(groebnerMatrix(13,32))-groebnerMatrix(15,81)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,65) = groebnerMatrix(13,65)/(groebnerMatrix(13,32));
  groebnerMatrix(18,68) = (groebnerMatrix(13,68)/(groebnerMatrix(13,32))-groebnerMatrix(15,82)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,69) = groebnerMatrix(13,69)/(groebnerMatrix(13,32));
  groebnerMatrix(18,71) = groebnerMatrix(13,71)/(groebnerMatrix(13,32));
  groebnerMatrix(18,73) = (groebnerMatrix(13,73)/(groebnerMatrix(13,32))-groebnerMatrix(15,83)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,74) = groebnerMatrix(13,74)/(groebnerMatrix(13,32));
  groebnerMatrix(18,75) = groebnerMatrix(13,75)/(groebnerMatrix(13,32));
  groebnerMatrix(18,76) = groebnerMatrix(13,76)/(groebnerMatrix(13,32));
  groebnerMatrix(18,77) = groebnerMatrix(13,77)/(groebnerMatrix(13,32));
  groebnerMatrix(18,79) = (groebnerMatrix(13,79)/(groebnerMatrix(13,32))-groebnerMatrix(15,84)/(groebnerMatrix(15,71)));
  groebnerMatrix(18,80) = groebnerMatrix(13,80)/(groebnerMatrix(13,32));
  groebnerMatrix(18,81) = groebnerMatrix(13,81)/(groebnerMatrix(13,32));
  groebnerMatrix(18,82) = groebnerMatrix(13,82)/(groebnerMatrix(13,32));
  groebnerMatrix(18,83) = groebnerMatrix(13,83)/(groebnerMatrix(13,32));
  groebnerMatrix(18,84) = groebnerMatrix(13,84)/(groebnerMatrix(13,32));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial19( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(19,34) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(12,71)/(groebnerMatrix(12,70)));
  groebnerMatrix(19,35) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(19,45) = -groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(19,49) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(12,76)/(groebnerMatrix(12,70)));
  groebnerMatrix(19,50) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(19,54) = -groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(19,55) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(19,66) = -groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(19,70) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(12,82)/(groebnerMatrix(12,70)));
  groebnerMatrix(19,71) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(19,75) = -groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(19,76) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(19,81) = -groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(19,82) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial20( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(20,21) = groebnerMatrix(8,21)/(groebnerMatrix(8,19));
  groebnerMatrix(20,28) = -groebnerMatrix(10,70)/(groebnerMatrix(10,66));
  groebnerMatrix(20,31) = (groebnerMatrix(8,31)/(groebnerMatrix(8,19))-groebnerMatrix(10,71)/(groebnerMatrix(10,66)));
  groebnerMatrix(20,33) = groebnerMatrix(8,33)/(groebnerMatrix(8,19));
  groebnerMatrix(20,42) = -groebnerMatrix(10,75)/(groebnerMatrix(10,66));
  groebnerMatrix(20,46) = -groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(20,51) = (groebnerMatrix(8,51)/(groebnerMatrix(8,19))-groebnerMatrix(10,77)/(groebnerMatrix(10,66)));
  groebnerMatrix(20,53) = groebnerMatrix(8,53)/(groebnerMatrix(8,19));
  groebnerMatrix(20,63) = -groebnerMatrix(10,81)/(groebnerMatrix(10,66));
  groebnerMatrix(20,66) = groebnerMatrix(8,66)/(groebnerMatrix(8,19));
  groebnerMatrix(20,67) = -groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(20,70) = groebnerMatrix(8,70)/(groebnerMatrix(8,19));
  groebnerMatrix(20,71) = groebnerMatrix(8,71)/(groebnerMatrix(8,19));
  groebnerMatrix(20,72) = -groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(20,75) = groebnerMatrix(8,75)/(groebnerMatrix(8,19));
  groebnerMatrix(20,77) = groebnerMatrix(8,77)/(groebnerMatrix(8,19));
  groebnerMatrix(20,78) = (groebnerMatrix(8,78)/(groebnerMatrix(8,19))-groebnerMatrix(10,84)/(groebnerMatrix(10,66)));
  groebnerMatrix(20,80) = groebnerMatrix(8,80)/(groebnerMatrix(8,19));
  groebnerMatrix(20,82) = groebnerMatrix(8,82)/(groebnerMatrix(8,19));
  groebnerMatrix(20,83) = groebnerMatrix(8,83)/(groebnerMatrix(8,19));
  groebnerMatrix(20,84) = groebnerMatrix(8,84)/(groebnerMatrix(8,19));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial21( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(21,11) = (groebnerMatrix(16,55)/(groebnerMatrix(16,54))-groebnerMatrix(17,48)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,14) = (groebnerMatrix(16,56)/(groebnerMatrix(16,54))-groebnerMatrix(17,53)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,44) = (groebnerMatrix(16,75)/(groebnerMatrix(16,54))-groebnerMatrix(17,65)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,48) = (groebnerMatrix(16,76)/(groebnerMatrix(16,54))-groebnerMatrix(17,69)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,53) = (groebnerMatrix(16,77)/(groebnerMatrix(16,54))-groebnerMatrix(17,74)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,54) = -groebnerMatrix(17,75)/(groebnerMatrix(17,44));
  groebnerMatrix(21,55) = -groebnerMatrix(17,76)/(groebnerMatrix(17,44));
  groebnerMatrix(21,56) = -groebnerMatrix(17,77)/(groebnerMatrix(17,44));
  groebnerMatrix(21,65) = groebnerMatrix(16,81)/(groebnerMatrix(16,54));
  groebnerMatrix(21,69) = groebnerMatrix(16,82)/(groebnerMatrix(16,54));
  groebnerMatrix(21,74) = (groebnerMatrix(16,83)/(groebnerMatrix(16,54))-groebnerMatrix(17,80)/(groebnerMatrix(17,44)));
  groebnerMatrix(21,75) = -groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(21,76) = -groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(21,77) = -groebnerMatrix(17,83)/(groebnerMatrix(17,44));
  groebnerMatrix(21,80) = groebnerMatrix(16,84)/(groebnerMatrix(16,54));
  groebnerMatrix(21,83) = -groebnerMatrix(17,84)/(groebnerMatrix(17,44));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial22( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(22,10) = (groebnerMatrix(16,55)/(groebnerMatrix(16,54))-groebnerMatrix(18,47)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,13) = (groebnerMatrix(16,56)/(groebnerMatrix(16,54))-groebnerMatrix(18,52)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,43) = (groebnerMatrix(16,75)/(groebnerMatrix(16,54))-groebnerMatrix(18,64)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,47) = (groebnerMatrix(16,76)/(groebnerMatrix(16,54))-groebnerMatrix(18,68)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,52) = (groebnerMatrix(16,77)/(groebnerMatrix(16,54))-groebnerMatrix(18,73)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,54) = -groebnerMatrix(18,75)/(groebnerMatrix(18,43));
  groebnerMatrix(22,55) = -groebnerMatrix(18,76)/(groebnerMatrix(18,43));
  groebnerMatrix(22,56) = -groebnerMatrix(18,77)/(groebnerMatrix(18,43));
  groebnerMatrix(22,64) = groebnerMatrix(16,81)/(groebnerMatrix(16,54));
  groebnerMatrix(22,68) = groebnerMatrix(16,82)/(groebnerMatrix(16,54));
  groebnerMatrix(22,73) = (groebnerMatrix(16,83)/(groebnerMatrix(16,54))-groebnerMatrix(18,79)/(groebnerMatrix(18,43)));
  groebnerMatrix(22,75) = -groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(22,76) = -groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(22,77) = -groebnerMatrix(18,83)/(groebnerMatrix(18,43));
  groebnerMatrix(22,79) = groebnerMatrix(16,84)/(groebnerMatrix(16,54));
  groebnerMatrix(22,83) = -groebnerMatrix(18,84)/(groebnerMatrix(18,43));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial23( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(23,9) = (groebnerMatrix(16,55)/(groebnerMatrix(16,54))-groebnerMatrix(20,46)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,12) = (groebnerMatrix(16,56)/(groebnerMatrix(16,54))-groebnerMatrix(20,51)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,42) = (groebnerMatrix(16,75)/(groebnerMatrix(16,54))-groebnerMatrix(20,63)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,46) = (groebnerMatrix(16,76)/(groebnerMatrix(16,54))-groebnerMatrix(20,67)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,51) = (groebnerMatrix(16,77)/(groebnerMatrix(16,54))-groebnerMatrix(20,72)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,54) = -groebnerMatrix(20,75)/(groebnerMatrix(20,42));
  groebnerMatrix(23,55) = -groebnerMatrix(20,76)/(groebnerMatrix(20,42));
  groebnerMatrix(23,56) = -groebnerMatrix(20,77)/(groebnerMatrix(20,42));
  groebnerMatrix(23,63) = groebnerMatrix(16,81)/(groebnerMatrix(16,54));
  groebnerMatrix(23,67) = groebnerMatrix(16,82)/(groebnerMatrix(16,54));
  groebnerMatrix(23,72) = (groebnerMatrix(16,83)/(groebnerMatrix(16,54))-groebnerMatrix(20,78)/(groebnerMatrix(20,42)));
  groebnerMatrix(23,75) = -groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(23,76) = -groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(23,77) = -groebnerMatrix(20,83)/(groebnerMatrix(20,42));
  groebnerMatrix(23,78) = groebnerMatrix(16,84)/(groebnerMatrix(16,54));
  groebnerMatrix(23,83) = -groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial24( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(24,5) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(17,48)/(groebnerMatrix(17,44)));
  groebnerMatrix(24,8) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(24,11) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(17,53)/(groebnerMatrix(17,44)));
  groebnerMatrix(24,14) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(24,30) = -groebnerMatrix(17,65)/(groebnerMatrix(17,44));
  groebnerMatrix(24,33) = -groebnerMatrix(17,69)/(groebnerMatrix(17,44));
  groebnerMatrix(24,44) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(24,48) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(17,74)/(groebnerMatrix(17,44)));
  groebnerMatrix(24,49) = -groebnerMatrix(17,75)/(groebnerMatrix(17,44));
  groebnerMatrix(24,50) = -groebnerMatrix(17,76)/(groebnerMatrix(17,44));
  groebnerMatrix(24,53) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(24,55) = -groebnerMatrix(17,77)/(groebnerMatrix(17,44));
  groebnerMatrix(24,69) = -groebnerMatrix(17,80)/(groebnerMatrix(17,44));
  groebnerMatrix(24,70) = -groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(24,71) = -groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(24,74) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(24,76) = -groebnerMatrix(17,83)/(groebnerMatrix(17,44));
  groebnerMatrix(24,82) = -groebnerMatrix(17,84)/(groebnerMatrix(17,44));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial25( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(25,4) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(18,47)/(groebnerMatrix(18,43)));
  groebnerMatrix(25,7) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(25,10) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(18,52)/(groebnerMatrix(18,43)));
  groebnerMatrix(25,13) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(25,29) = -groebnerMatrix(18,64)/(groebnerMatrix(18,43));
  groebnerMatrix(25,32) = -groebnerMatrix(18,68)/(groebnerMatrix(18,43));
  groebnerMatrix(25,43) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(25,47) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(18,73)/(groebnerMatrix(18,43)));
  groebnerMatrix(25,49) = -groebnerMatrix(18,75)/(groebnerMatrix(18,43));
  groebnerMatrix(25,50) = -groebnerMatrix(18,76)/(groebnerMatrix(18,43));
  groebnerMatrix(25,52) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(25,55) = -groebnerMatrix(18,77)/(groebnerMatrix(18,43));
  groebnerMatrix(25,68) = -groebnerMatrix(18,79)/(groebnerMatrix(18,43));
  groebnerMatrix(25,70) = -groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(25,71) = -groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(25,73) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(25,76) = -groebnerMatrix(18,83)/(groebnerMatrix(18,43));
  groebnerMatrix(25,82) = -groebnerMatrix(18,84)/(groebnerMatrix(18,43));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial26( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(26,3) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(20,46)/(groebnerMatrix(20,42)));
  groebnerMatrix(26,6) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(26,9) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(20,51)/(groebnerMatrix(20,42)));
  groebnerMatrix(26,12) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(26,28) = -groebnerMatrix(20,63)/(groebnerMatrix(20,42));
  groebnerMatrix(26,31) = -groebnerMatrix(20,67)/(groebnerMatrix(20,42));
  groebnerMatrix(26,42) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(26,46) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(20,72)/(groebnerMatrix(20,42)));
  groebnerMatrix(26,49) = -groebnerMatrix(20,75)/(groebnerMatrix(20,42));
  groebnerMatrix(26,50) = -groebnerMatrix(20,76)/(groebnerMatrix(20,42));
  groebnerMatrix(26,51) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(26,55) = -groebnerMatrix(20,77)/(groebnerMatrix(20,42));
  groebnerMatrix(26,67) = -groebnerMatrix(20,78)/(groebnerMatrix(20,42));
  groebnerMatrix(26,70) = -groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(26,71) = -groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(26,72) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(26,76) = -groebnerMatrix(20,83)/(groebnerMatrix(20,42));
  groebnerMatrix(26,82) = -groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial27( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(27,2) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(17,48)/(groebnerMatrix(17,44)));
  groebnerMatrix(27,5) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(27,8) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(17,53)/(groebnerMatrix(17,44)));
  groebnerMatrix(27,11) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(27,14) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(27,21) = -groebnerMatrix(17,65)/(groebnerMatrix(17,44));
  groebnerMatrix(27,30) = -groebnerMatrix(17,69)/(groebnerMatrix(17,44));
  groebnerMatrix(27,44) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(17,74)/(groebnerMatrix(17,44)));
  groebnerMatrix(27,45) = -groebnerMatrix(17,75)/(groebnerMatrix(17,44));
  groebnerMatrix(27,48) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(27,49) = -groebnerMatrix(17,76)/(groebnerMatrix(17,44));
  groebnerMatrix(27,53) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(27,54) = -groebnerMatrix(17,77)/(groebnerMatrix(17,44));
  groebnerMatrix(27,65) = -groebnerMatrix(17,80)/(groebnerMatrix(17,44));
  groebnerMatrix(27,66) = -groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(27,70) = -groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(27,74) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(27,75) = -groebnerMatrix(17,83)/(groebnerMatrix(17,44));
  groebnerMatrix(27,81) = -groebnerMatrix(17,84)/(groebnerMatrix(17,44));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial28( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(28,1) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(18,47)/(groebnerMatrix(18,43)));
  groebnerMatrix(28,4) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(28,7) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(18,52)/(groebnerMatrix(18,43)));
  groebnerMatrix(28,10) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(28,13) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(28,20) = -groebnerMatrix(18,64)/(groebnerMatrix(18,43));
  groebnerMatrix(28,29) = -groebnerMatrix(18,68)/(groebnerMatrix(18,43));
  groebnerMatrix(28,43) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(18,73)/(groebnerMatrix(18,43)));
  groebnerMatrix(28,45) = -groebnerMatrix(18,75)/(groebnerMatrix(18,43));
  groebnerMatrix(28,47) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(28,49) = -groebnerMatrix(18,76)/(groebnerMatrix(18,43));
  groebnerMatrix(28,52) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(28,54) = -groebnerMatrix(18,77)/(groebnerMatrix(18,43));
  groebnerMatrix(28,64) = -groebnerMatrix(18,79)/(groebnerMatrix(18,43));
  groebnerMatrix(28,66) = -groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(28,70) = -groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(28,73) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(28,75) = -groebnerMatrix(18,83)/(groebnerMatrix(18,43));
  groebnerMatrix(28,81) = -groebnerMatrix(18,84)/(groebnerMatrix(18,43));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial29( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(29,0) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(20,46)/(groebnerMatrix(20,42)));
  groebnerMatrix(29,3) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(29,6) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(20,51)/(groebnerMatrix(20,42)));
  groebnerMatrix(29,9) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(29,12) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(29,19) = -groebnerMatrix(20,63)/(groebnerMatrix(20,42));
  groebnerMatrix(29,28) = -groebnerMatrix(20,67)/(groebnerMatrix(20,42));
  groebnerMatrix(29,42) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(20,72)/(groebnerMatrix(20,42)));
  groebnerMatrix(29,45) = -groebnerMatrix(20,75)/(groebnerMatrix(20,42));
  groebnerMatrix(29,46) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(29,49) = -groebnerMatrix(20,76)/(groebnerMatrix(20,42));
  groebnerMatrix(29,51) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(29,54) = -groebnerMatrix(20,77)/(groebnerMatrix(20,42));
  groebnerMatrix(29,63) = -groebnerMatrix(20,78)/(groebnerMatrix(20,42));
  groebnerMatrix(29,66) = -groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(29,70) = -groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(29,72) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(29,75) = -groebnerMatrix(20,83)/(groebnerMatrix(20,42));
  groebnerMatrix(29,81) = -groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial30( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(30,43) = groebnerMatrix(17,75)/(groebnerMatrix(17,44));
  groebnerMatrix(30,44) = -groebnerMatrix(18,75)/(groebnerMatrix(18,43));
  groebnerMatrix(30,47) = groebnerMatrix(17,76)/(groebnerMatrix(17,44));
  groebnerMatrix(30,48) = -groebnerMatrix(18,76)/(groebnerMatrix(18,43));
  groebnerMatrix(30,52) = groebnerMatrix(17,77)/(groebnerMatrix(17,44));
  groebnerMatrix(30,53) = -groebnerMatrix(18,77)/(groebnerMatrix(18,43));
  groebnerMatrix(30,64) = groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(30,65) = -groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(30,68) = groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(30,69) = -groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(30,73) = groebnerMatrix(17,83)/(groebnerMatrix(17,44));
  groebnerMatrix(30,74) = -groebnerMatrix(18,83)/(groebnerMatrix(18,43));
  groebnerMatrix(30,79) = groebnerMatrix(17,84)/(groebnerMatrix(17,44));
  groebnerMatrix(30,80) = -groebnerMatrix(18,84)/(groebnerMatrix(18,43));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial31( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(31,42) = groebnerMatrix(17,75)/(groebnerMatrix(17,44));
  groebnerMatrix(31,44) = -groebnerMatrix(20,75)/(groebnerMatrix(20,42));
  groebnerMatrix(31,46) = groebnerMatrix(17,76)/(groebnerMatrix(17,44));
  groebnerMatrix(31,48) = -groebnerMatrix(20,76)/(groebnerMatrix(20,42));
  groebnerMatrix(31,51) = groebnerMatrix(17,77)/(groebnerMatrix(17,44));
  groebnerMatrix(31,53) = -groebnerMatrix(20,77)/(groebnerMatrix(20,42));
  groebnerMatrix(31,63) = groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(31,65) = -groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(31,67) = groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(31,69) = -groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(31,72) = groebnerMatrix(17,83)/(groebnerMatrix(17,44));
  groebnerMatrix(31,74) = -groebnerMatrix(20,83)/(groebnerMatrix(20,42));
  groebnerMatrix(31,78) = groebnerMatrix(17,84)/(groebnerMatrix(17,44));
  groebnerMatrix(31,80) = -groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial32( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(32,42) = groebnerMatrix(18,75)/(groebnerMatrix(18,43));
  groebnerMatrix(32,43) = -groebnerMatrix(20,75)/(groebnerMatrix(20,42));
  groebnerMatrix(32,46) = groebnerMatrix(18,76)/(groebnerMatrix(18,43));
  groebnerMatrix(32,47) = -groebnerMatrix(20,76)/(groebnerMatrix(20,42));
  groebnerMatrix(32,51) = groebnerMatrix(18,77)/(groebnerMatrix(18,43));
  groebnerMatrix(32,52) = -groebnerMatrix(20,77)/(groebnerMatrix(20,42));
  groebnerMatrix(32,63) = groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(32,64) = -groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(32,67) = groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(32,68) = -groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(32,72) = groebnerMatrix(18,83)/(groebnerMatrix(18,43));
  groebnerMatrix(32,73) = -groebnerMatrix(20,83)/(groebnerMatrix(20,42));
  groebnerMatrix(32,78) = groebnerMatrix(18,84)/(groebnerMatrix(18,43));
  groebnerMatrix(32,79) = -groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial33( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(33,46) = -groebnerMatrix(32,67)/(groebnerMatrix(32,65));
  groebnerMatrix(33,47) = -groebnerMatrix(32,68)/(groebnerMatrix(32,65));
  groebnerMatrix(33,48) = (groebnerMatrix(17,48)/(groebnerMatrix(17,44))-groebnerMatrix(32,69)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,51) = -groebnerMatrix(32,72)/(groebnerMatrix(32,65));
  groebnerMatrix(33,52) = -groebnerMatrix(32,73)/(groebnerMatrix(32,65));
  groebnerMatrix(33,53) = (groebnerMatrix(17,53)/(groebnerMatrix(17,44))-groebnerMatrix(32,74)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,54) = -groebnerMatrix(32,75)/(groebnerMatrix(32,65));
  groebnerMatrix(33,55) = -groebnerMatrix(32,76)/(groebnerMatrix(32,65));
  groebnerMatrix(33,56) = -groebnerMatrix(32,77)/(groebnerMatrix(32,65));
  groebnerMatrix(33,65) = groebnerMatrix(17,65)/(groebnerMatrix(17,44));
  groebnerMatrix(33,69) = groebnerMatrix(17,69)/(groebnerMatrix(17,44));
  groebnerMatrix(33,72) = -groebnerMatrix(32,78)/(groebnerMatrix(32,65));
  groebnerMatrix(33,73) = -groebnerMatrix(32,79)/(groebnerMatrix(32,65));
  groebnerMatrix(33,74) = (groebnerMatrix(17,74)/(groebnerMatrix(17,44))-groebnerMatrix(32,80)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,75) = (groebnerMatrix(17,75)/(groebnerMatrix(17,44))-groebnerMatrix(32,81)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,76) = (groebnerMatrix(17,76)/(groebnerMatrix(17,44))-groebnerMatrix(32,82)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,77) = (groebnerMatrix(17,77)/(groebnerMatrix(17,44))-groebnerMatrix(32,83)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,80) = groebnerMatrix(17,80)/(groebnerMatrix(17,44));
  groebnerMatrix(33,81) = groebnerMatrix(17,81)/(groebnerMatrix(17,44));
  groebnerMatrix(33,82) = groebnerMatrix(17,82)/(groebnerMatrix(17,44));
  groebnerMatrix(33,83) = (groebnerMatrix(17,83)/(groebnerMatrix(17,44))-groebnerMatrix(32,84)/(groebnerMatrix(32,65)));
  groebnerMatrix(33,84) = groebnerMatrix(17,84)/(groebnerMatrix(17,44));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial34( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(34,44) = -groebnerMatrix(30,65)/(groebnerMatrix(30,64));
  groebnerMatrix(34,47) = (groebnerMatrix(18,47)/(groebnerMatrix(18,43))-groebnerMatrix(30,68)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,48) = -groebnerMatrix(30,69)/(groebnerMatrix(30,64));
  groebnerMatrix(34,52) = (groebnerMatrix(18,52)/(groebnerMatrix(18,43))-groebnerMatrix(30,73)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,53) = -groebnerMatrix(30,74)/(groebnerMatrix(30,64));
  groebnerMatrix(34,54) = -groebnerMatrix(30,75)/(groebnerMatrix(30,64));
  groebnerMatrix(34,55) = -groebnerMatrix(30,76)/(groebnerMatrix(30,64));
  groebnerMatrix(34,56) = -groebnerMatrix(30,77)/(groebnerMatrix(30,64));
  groebnerMatrix(34,64) = groebnerMatrix(18,64)/(groebnerMatrix(18,43));
  groebnerMatrix(34,68) = groebnerMatrix(18,68)/(groebnerMatrix(18,43));
  groebnerMatrix(34,73) = (groebnerMatrix(18,73)/(groebnerMatrix(18,43))-groebnerMatrix(30,79)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,74) = -groebnerMatrix(30,80)/(groebnerMatrix(30,64));
  groebnerMatrix(34,75) = (groebnerMatrix(18,75)/(groebnerMatrix(18,43))-groebnerMatrix(30,81)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,76) = (groebnerMatrix(18,76)/(groebnerMatrix(18,43))-groebnerMatrix(30,82)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,77) = (groebnerMatrix(18,77)/(groebnerMatrix(18,43))-groebnerMatrix(30,83)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,79) = groebnerMatrix(18,79)/(groebnerMatrix(18,43));
  groebnerMatrix(34,81) = groebnerMatrix(18,81)/(groebnerMatrix(18,43));
  groebnerMatrix(34,82) = groebnerMatrix(18,82)/(groebnerMatrix(18,43));
  groebnerMatrix(34,83) = (groebnerMatrix(18,83)/(groebnerMatrix(18,43))-groebnerMatrix(30,84)/(groebnerMatrix(30,64)));
  groebnerMatrix(34,84) = groebnerMatrix(18,84)/(groebnerMatrix(18,43));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial35( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(35,44) = -groebnerMatrix(31,65)/(groebnerMatrix(31,63));
  groebnerMatrix(35,46) = (groebnerMatrix(20,46)/(groebnerMatrix(20,42))-groebnerMatrix(31,67)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,48) = -groebnerMatrix(31,69)/(groebnerMatrix(31,63));
  groebnerMatrix(35,51) = (groebnerMatrix(20,51)/(groebnerMatrix(20,42))-groebnerMatrix(31,72)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,53) = -groebnerMatrix(31,74)/(groebnerMatrix(31,63));
  groebnerMatrix(35,54) = -groebnerMatrix(31,75)/(groebnerMatrix(31,63));
  groebnerMatrix(35,55) = -groebnerMatrix(31,76)/(groebnerMatrix(31,63));
  groebnerMatrix(35,56) = -groebnerMatrix(31,77)/(groebnerMatrix(31,63));
  groebnerMatrix(35,63) = groebnerMatrix(20,63)/(groebnerMatrix(20,42));
  groebnerMatrix(35,67) = groebnerMatrix(20,67)/(groebnerMatrix(20,42));
  groebnerMatrix(35,72) = (groebnerMatrix(20,72)/(groebnerMatrix(20,42))-groebnerMatrix(31,78)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,74) = -groebnerMatrix(31,80)/(groebnerMatrix(31,63));
  groebnerMatrix(35,75) = (groebnerMatrix(20,75)/(groebnerMatrix(20,42))-groebnerMatrix(31,81)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,76) = (groebnerMatrix(20,76)/(groebnerMatrix(20,42))-groebnerMatrix(31,82)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,77) = (groebnerMatrix(20,77)/(groebnerMatrix(20,42))-groebnerMatrix(31,83)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,78) = groebnerMatrix(20,78)/(groebnerMatrix(20,42));
  groebnerMatrix(35,81) = groebnerMatrix(20,81)/(groebnerMatrix(20,42));
  groebnerMatrix(35,82) = groebnerMatrix(20,82)/(groebnerMatrix(20,42));
  groebnerMatrix(35,83) = (groebnerMatrix(20,83)/(groebnerMatrix(20,42))-groebnerMatrix(31,84)/(groebnerMatrix(31,63)));
  groebnerMatrix(35,84) = groebnerMatrix(20,84)/(groebnerMatrix(20,42));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial36( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(36,31) = -groebnerMatrix(32,67)/(groebnerMatrix(32,65));
  groebnerMatrix(36,32) = -groebnerMatrix(32,68)/(groebnerMatrix(32,65));
  groebnerMatrix(36,33) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(32,69)/(groebnerMatrix(32,65)));
  groebnerMatrix(36,44) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(36,46) = -groebnerMatrix(32,72)/(groebnerMatrix(32,65));
  groebnerMatrix(36,47) = -groebnerMatrix(32,73)/(groebnerMatrix(32,65));
  groebnerMatrix(36,48) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(32,74)/(groebnerMatrix(32,65)));
  groebnerMatrix(36,49) = -groebnerMatrix(32,75)/(groebnerMatrix(32,65));
  groebnerMatrix(36,50) = -groebnerMatrix(32,76)/(groebnerMatrix(32,65));
  groebnerMatrix(36,53) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(36,55) = -groebnerMatrix(32,77)/(groebnerMatrix(32,65));
  groebnerMatrix(36,65) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(36,67) = -groebnerMatrix(32,78)/(groebnerMatrix(32,65));
  groebnerMatrix(36,68) = -groebnerMatrix(32,79)/(groebnerMatrix(32,65));
  groebnerMatrix(36,69) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(32,80)/(groebnerMatrix(32,65)));
  groebnerMatrix(36,70) = -groebnerMatrix(32,81)/(groebnerMatrix(32,65));
  groebnerMatrix(36,71) = -groebnerMatrix(32,82)/(groebnerMatrix(32,65));
  groebnerMatrix(36,74) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(36,76) = -groebnerMatrix(32,83)/(groebnerMatrix(32,65));
  groebnerMatrix(36,80) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(36,82) = -groebnerMatrix(32,84)/(groebnerMatrix(32,65));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial37( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(37,30) = -groebnerMatrix(30,65)/(groebnerMatrix(30,64));
  groebnerMatrix(37,32) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(30,68)/(groebnerMatrix(30,64)));
  groebnerMatrix(37,33) = -groebnerMatrix(30,69)/(groebnerMatrix(30,64));
  groebnerMatrix(37,43) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(37,47) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(30,73)/(groebnerMatrix(30,64)));
  groebnerMatrix(37,48) = -groebnerMatrix(30,74)/(groebnerMatrix(30,64));
  groebnerMatrix(37,49) = -groebnerMatrix(30,75)/(groebnerMatrix(30,64));
  groebnerMatrix(37,50) = -groebnerMatrix(30,76)/(groebnerMatrix(30,64));
  groebnerMatrix(37,52) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(37,55) = -groebnerMatrix(30,77)/(groebnerMatrix(30,64));
  groebnerMatrix(37,64) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(37,68) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(30,79)/(groebnerMatrix(30,64)));
  groebnerMatrix(37,69) = -groebnerMatrix(30,80)/(groebnerMatrix(30,64));
  groebnerMatrix(37,70) = -groebnerMatrix(30,81)/(groebnerMatrix(30,64));
  groebnerMatrix(37,71) = -groebnerMatrix(30,82)/(groebnerMatrix(30,64));
  groebnerMatrix(37,73) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(37,76) = -groebnerMatrix(30,83)/(groebnerMatrix(30,64));
  groebnerMatrix(37,79) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(37,82) = -groebnerMatrix(30,84)/(groebnerMatrix(30,64));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial38( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(38,30) = -groebnerMatrix(31,65)/(groebnerMatrix(31,63));
  groebnerMatrix(38,31) = (groebnerMatrix(12,71)/(groebnerMatrix(12,70))-groebnerMatrix(31,67)/(groebnerMatrix(31,63)));
  groebnerMatrix(38,33) = -groebnerMatrix(31,69)/(groebnerMatrix(31,63));
  groebnerMatrix(38,42) = groebnerMatrix(12,75)/(groebnerMatrix(12,70));
  groebnerMatrix(38,46) = (groebnerMatrix(12,76)/(groebnerMatrix(12,70))-groebnerMatrix(31,72)/(groebnerMatrix(31,63)));
  groebnerMatrix(38,48) = -groebnerMatrix(31,74)/(groebnerMatrix(31,63));
  groebnerMatrix(38,49) = -groebnerMatrix(31,75)/(groebnerMatrix(31,63));
  groebnerMatrix(38,50) = -groebnerMatrix(31,76)/(groebnerMatrix(31,63));
  groebnerMatrix(38,51) = groebnerMatrix(12,77)/(groebnerMatrix(12,70));
  groebnerMatrix(38,55) = -groebnerMatrix(31,77)/(groebnerMatrix(31,63));
  groebnerMatrix(38,63) = groebnerMatrix(12,81)/(groebnerMatrix(12,70));
  groebnerMatrix(38,67) = (groebnerMatrix(12,82)/(groebnerMatrix(12,70))-groebnerMatrix(31,78)/(groebnerMatrix(31,63)));
  groebnerMatrix(38,69) = -groebnerMatrix(31,80)/(groebnerMatrix(31,63));
  groebnerMatrix(38,70) = -groebnerMatrix(31,81)/(groebnerMatrix(31,63));
  groebnerMatrix(38,71) = -groebnerMatrix(31,82)/(groebnerMatrix(31,63));
  groebnerMatrix(38,72) = groebnerMatrix(12,83)/(groebnerMatrix(12,70));
  groebnerMatrix(38,76) = -groebnerMatrix(31,83)/(groebnerMatrix(31,63));
  groebnerMatrix(38,78) = groebnerMatrix(12,84)/(groebnerMatrix(12,70));
  groebnerMatrix(38,82) = -groebnerMatrix(31,84)/(groebnerMatrix(31,63));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial39( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(39,28) = -groebnerMatrix(32,67)/(groebnerMatrix(32,65));
  groebnerMatrix(39,29) = -groebnerMatrix(32,68)/(groebnerMatrix(32,65));
  groebnerMatrix(39,30) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(32,69)/(groebnerMatrix(32,65)));
  groebnerMatrix(39,33) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(39,42) = -groebnerMatrix(32,72)/(groebnerMatrix(32,65));
  groebnerMatrix(39,43) = -groebnerMatrix(32,73)/(groebnerMatrix(32,65));
  groebnerMatrix(39,44) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(32,74)/(groebnerMatrix(32,65)));
  groebnerMatrix(39,45) = -groebnerMatrix(32,75)/(groebnerMatrix(32,65));
  groebnerMatrix(39,48) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(39,49) = -groebnerMatrix(32,76)/(groebnerMatrix(32,65));
  groebnerMatrix(39,53) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(39,54) = -groebnerMatrix(32,77)/(groebnerMatrix(32,65));
  groebnerMatrix(39,63) = -groebnerMatrix(32,78)/(groebnerMatrix(32,65));
  groebnerMatrix(39,64) = -groebnerMatrix(32,79)/(groebnerMatrix(32,65));
  groebnerMatrix(39,65) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(32,80)/(groebnerMatrix(32,65)));
  groebnerMatrix(39,66) = -groebnerMatrix(32,81)/(groebnerMatrix(32,65));
  groebnerMatrix(39,69) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(39,70) = -groebnerMatrix(32,82)/(groebnerMatrix(32,65));
  groebnerMatrix(39,74) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(39,75) = -groebnerMatrix(32,83)/(groebnerMatrix(32,65));
  groebnerMatrix(39,80) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(39,81) = -groebnerMatrix(32,84)/(groebnerMatrix(32,65));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial40( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(40,21) = -groebnerMatrix(30,65)/(groebnerMatrix(30,64));
  groebnerMatrix(40,29) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(30,68)/(groebnerMatrix(30,64)));
  groebnerMatrix(40,30) = -groebnerMatrix(30,69)/(groebnerMatrix(30,64));
  groebnerMatrix(40,32) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(40,43) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(30,73)/(groebnerMatrix(30,64)));
  groebnerMatrix(40,44) = -groebnerMatrix(30,74)/(groebnerMatrix(30,64));
  groebnerMatrix(40,45) = -groebnerMatrix(30,75)/(groebnerMatrix(30,64));
  groebnerMatrix(40,47) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(40,49) = -groebnerMatrix(30,76)/(groebnerMatrix(30,64));
  groebnerMatrix(40,52) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(40,54) = -groebnerMatrix(30,77)/(groebnerMatrix(30,64));
  groebnerMatrix(40,64) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(30,79)/(groebnerMatrix(30,64)));
  groebnerMatrix(40,65) = -groebnerMatrix(30,80)/(groebnerMatrix(30,64));
  groebnerMatrix(40,66) = -groebnerMatrix(30,81)/(groebnerMatrix(30,64));
  groebnerMatrix(40,68) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(40,70) = -groebnerMatrix(30,82)/(groebnerMatrix(30,64));
  groebnerMatrix(40,73) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(40,75) = -groebnerMatrix(30,83)/(groebnerMatrix(30,64));
  groebnerMatrix(40,79) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(40,81) = -groebnerMatrix(30,84)/(groebnerMatrix(30,64));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial41( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(41,21) = -groebnerMatrix(31,65)/(groebnerMatrix(31,63));
  groebnerMatrix(41,28) = (groebnerMatrix(10,70)/(groebnerMatrix(10,66))-groebnerMatrix(31,67)/(groebnerMatrix(31,63)));
  groebnerMatrix(41,30) = -groebnerMatrix(31,69)/(groebnerMatrix(31,63));
  groebnerMatrix(41,31) = groebnerMatrix(10,71)/(groebnerMatrix(10,66));
  groebnerMatrix(41,42) = (groebnerMatrix(10,75)/(groebnerMatrix(10,66))-groebnerMatrix(31,72)/(groebnerMatrix(31,63)));
  groebnerMatrix(41,44) = -groebnerMatrix(31,74)/(groebnerMatrix(31,63));
  groebnerMatrix(41,45) = -groebnerMatrix(31,75)/(groebnerMatrix(31,63));
  groebnerMatrix(41,46) = groebnerMatrix(10,76)/(groebnerMatrix(10,66));
  groebnerMatrix(41,49) = -groebnerMatrix(31,76)/(groebnerMatrix(31,63));
  groebnerMatrix(41,51) = groebnerMatrix(10,77)/(groebnerMatrix(10,66));
  groebnerMatrix(41,54) = -groebnerMatrix(31,77)/(groebnerMatrix(31,63));
  groebnerMatrix(41,63) = (groebnerMatrix(10,81)/(groebnerMatrix(10,66))-groebnerMatrix(31,78)/(groebnerMatrix(31,63)));
  groebnerMatrix(41,65) = -groebnerMatrix(31,80)/(groebnerMatrix(31,63));
  groebnerMatrix(41,66) = -groebnerMatrix(31,81)/(groebnerMatrix(31,63));
  groebnerMatrix(41,67) = groebnerMatrix(10,82)/(groebnerMatrix(10,66));
  groebnerMatrix(41,70) = -groebnerMatrix(31,82)/(groebnerMatrix(31,63));
  groebnerMatrix(41,72) = groebnerMatrix(10,83)/(groebnerMatrix(10,66));
  groebnerMatrix(41,75) = -groebnerMatrix(31,83)/(groebnerMatrix(31,63));
  groebnerMatrix(41,78) = groebnerMatrix(10,84)/(groebnerMatrix(10,66));
  groebnerMatrix(41,81) = -groebnerMatrix(31,84)/(groebnerMatrix(31,63));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial42( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(42,18) = groebnerMatrix(30,65)/(groebnerMatrix(30,64));
  groebnerMatrix(42,23) = -groebnerMatrix(32,67)/(groebnerMatrix(32,65));
  groebnerMatrix(42,24) = -groebnerMatrix(32,68)/(groebnerMatrix(32,65));
  groebnerMatrix(42,26) = (groebnerMatrix(30,68)/(groebnerMatrix(30,64))-groebnerMatrix(32,69)/(groebnerMatrix(32,65)));
  groebnerMatrix(42,27) = groebnerMatrix(30,69)/(groebnerMatrix(30,64));
  groebnerMatrix(42,37) = -groebnerMatrix(32,72)/(groebnerMatrix(32,65));
  groebnerMatrix(42,38) = -groebnerMatrix(32,73)/(groebnerMatrix(32,65));
  groebnerMatrix(42,40) = (groebnerMatrix(30,73)/(groebnerMatrix(30,64))-groebnerMatrix(32,74)/(groebnerMatrix(32,65)));
  groebnerMatrix(42,41) = groebnerMatrix(30,74)/(groebnerMatrix(30,64));
  groebnerMatrix(42,43) = -groebnerMatrix(32,75)/(groebnerMatrix(32,65));
  groebnerMatrix(42,44) = groebnerMatrix(30,75)/(groebnerMatrix(30,64));
  groebnerMatrix(42,47) = -groebnerMatrix(32,76)/(groebnerMatrix(32,65));
  groebnerMatrix(42,48) = groebnerMatrix(30,76)/(groebnerMatrix(30,64));
  groebnerMatrix(42,52) = -groebnerMatrix(32,77)/(groebnerMatrix(32,65));
  groebnerMatrix(42,53) = groebnerMatrix(30,77)/(groebnerMatrix(30,64));
  groebnerMatrix(42,58) = -groebnerMatrix(32,78)/(groebnerMatrix(32,65));
  groebnerMatrix(42,59) = -groebnerMatrix(32,79)/(groebnerMatrix(32,65));
  groebnerMatrix(42,61) = (groebnerMatrix(30,79)/(groebnerMatrix(30,64))-groebnerMatrix(32,80)/(groebnerMatrix(32,65)));
  groebnerMatrix(42,62) = groebnerMatrix(30,80)/(groebnerMatrix(30,64));
  groebnerMatrix(42,64) = -groebnerMatrix(32,81)/(groebnerMatrix(32,65));
  groebnerMatrix(42,65) = groebnerMatrix(30,81)/(groebnerMatrix(30,64));
  groebnerMatrix(42,68) = -groebnerMatrix(32,82)/(groebnerMatrix(32,65));
  groebnerMatrix(42,69) = groebnerMatrix(30,82)/(groebnerMatrix(30,64));
  groebnerMatrix(42,73) = -groebnerMatrix(32,83)/(groebnerMatrix(32,65));
  groebnerMatrix(42,74) = groebnerMatrix(30,83)/(groebnerMatrix(30,64));
  groebnerMatrix(42,79) = -groebnerMatrix(32,84)/(groebnerMatrix(32,65));
  groebnerMatrix(42,80) = groebnerMatrix(30,84)/(groebnerMatrix(30,64));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial43( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(43,18) = groebnerMatrix(31,65)/(groebnerMatrix(31,63));
  groebnerMatrix(43,22) = -groebnerMatrix(32,67)/(groebnerMatrix(32,65));
  groebnerMatrix(43,23) = -groebnerMatrix(32,68)/(groebnerMatrix(32,65));
  groebnerMatrix(43,25) = (groebnerMatrix(31,67)/(groebnerMatrix(31,63))-groebnerMatrix(32,69)/(groebnerMatrix(32,65)));
  groebnerMatrix(43,27) = groebnerMatrix(31,69)/(groebnerMatrix(31,63));
  groebnerMatrix(43,36) = -groebnerMatrix(32,72)/(groebnerMatrix(32,65));
  groebnerMatrix(43,37) = -groebnerMatrix(32,73)/(groebnerMatrix(32,65));
  groebnerMatrix(43,39) = (groebnerMatrix(31,72)/(groebnerMatrix(31,63))-groebnerMatrix(32,74)/(groebnerMatrix(32,65)));
  groebnerMatrix(43,41) = groebnerMatrix(31,74)/(groebnerMatrix(31,63));
  groebnerMatrix(43,42) = -groebnerMatrix(32,75)/(groebnerMatrix(32,65));
  groebnerMatrix(43,44) = groebnerMatrix(31,75)/(groebnerMatrix(31,63));
  groebnerMatrix(43,46) = -groebnerMatrix(32,76)/(groebnerMatrix(32,65));
  groebnerMatrix(43,48) = groebnerMatrix(31,76)/(groebnerMatrix(31,63));
  groebnerMatrix(43,51) = -groebnerMatrix(32,77)/(groebnerMatrix(32,65));
  groebnerMatrix(43,53) = groebnerMatrix(31,77)/(groebnerMatrix(31,63));
  groebnerMatrix(43,57) = -groebnerMatrix(32,78)/(groebnerMatrix(32,65));
  groebnerMatrix(43,58) = -groebnerMatrix(32,79)/(groebnerMatrix(32,65));
  groebnerMatrix(43,60) = (groebnerMatrix(31,78)/(groebnerMatrix(31,63))-groebnerMatrix(32,80)/(groebnerMatrix(32,65)));
  groebnerMatrix(43,62) = groebnerMatrix(31,80)/(groebnerMatrix(31,63));
  groebnerMatrix(43,63) = -groebnerMatrix(32,81)/(groebnerMatrix(32,65));
  groebnerMatrix(43,65) = groebnerMatrix(31,81)/(groebnerMatrix(31,63));
  groebnerMatrix(43,67) = -groebnerMatrix(32,82)/(groebnerMatrix(32,65));
  groebnerMatrix(43,69) = groebnerMatrix(31,82)/(groebnerMatrix(31,63));
  groebnerMatrix(43,72) = -groebnerMatrix(32,83)/(groebnerMatrix(32,65));
  groebnerMatrix(43,74) = groebnerMatrix(31,83)/(groebnerMatrix(31,63));
  groebnerMatrix(43,78) = -groebnerMatrix(32,84)/(groebnerMatrix(32,65));
  groebnerMatrix(43,80) = groebnerMatrix(31,84)/(groebnerMatrix(31,63));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial44( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(44,16) = groebnerMatrix(30,65)/(groebnerMatrix(30,64));
  groebnerMatrix(44,17) = -groebnerMatrix(31,65)/(groebnerMatrix(31,63));
  groebnerMatrix(44,25) = groebnerMatrix(30,69)/(groebnerMatrix(30,64));
  groebnerMatrix(44,26) = -groebnerMatrix(31,69)/(groebnerMatrix(31,63));
  groebnerMatrix(44,39) = groebnerMatrix(30,74)/(groebnerMatrix(30,64));
  groebnerMatrix(44,40) = -groebnerMatrix(31,74)/(groebnerMatrix(31,63));
  groebnerMatrix(44,42) = groebnerMatrix(30,75)/(groebnerMatrix(30,64));
  groebnerMatrix(44,43) = -groebnerMatrix(31,75)/(groebnerMatrix(31,63));
  groebnerMatrix(44,46) = groebnerMatrix(30,76)/(groebnerMatrix(30,64));
  groebnerMatrix(44,47) = -groebnerMatrix(31,76)/(groebnerMatrix(31,63));
  groebnerMatrix(44,51) = groebnerMatrix(30,77)/(groebnerMatrix(30,64));
  groebnerMatrix(44,52) = -groebnerMatrix(31,77)/(groebnerMatrix(31,63));
  groebnerMatrix(44,60) = groebnerMatrix(30,80)/(groebnerMatrix(30,64));
  groebnerMatrix(44,61) = -groebnerMatrix(31,80)/(groebnerMatrix(31,63));
  groebnerMatrix(44,63) = groebnerMatrix(30,81)/(groebnerMatrix(30,64));
  groebnerMatrix(44,64) = -groebnerMatrix(31,81)/(groebnerMatrix(31,63));
  groebnerMatrix(44,67) = groebnerMatrix(30,82)/(groebnerMatrix(30,64));
  groebnerMatrix(44,68) = -groebnerMatrix(31,82)/(groebnerMatrix(31,63));
  groebnerMatrix(44,72) = groebnerMatrix(30,83)/(groebnerMatrix(30,64));
  groebnerMatrix(44,73) = -groebnerMatrix(31,83)/(groebnerMatrix(31,63));
  groebnerMatrix(44,78) = groebnerMatrix(30,84)/(groebnerMatrix(30,64));
  groebnerMatrix(44,79) = -groebnerMatrix(31,84)/(groebnerMatrix(31,63));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial45( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(45,14) = groebnerMatrix(19,56)/(groebnerMatrix(19,55));
  groebnerMatrix(45,15) = -groebnerMatrix(27,56)/(groebnerMatrix(27,53));
  groebnerMatrix(45,30) = -groebnerMatrix(27,65)/(groebnerMatrix(27,53));
  groebnerMatrix(45,33) = -groebnerMatrix(27,69)/(groebnerMatrix(27,53));
  groebnerMatrix(45,44) = groebnerMatrix(19,75)/(groebnerMatrix(19,55));
  groebnerMatrix(45,48) = (groebnerMatrix(19,76)/(groebnerMatrix(19,55))-groebnerMatrix(27,74)/(groebnerMatrix(27,53)));
  groebnerMatrix(45,49) = -groebnerMatrix(27,75)/(groebnerMatrix(27,53));
  groebnerMatrix(45,50) = -groebnerMatrix(27,76)/(groebnerMatrix(27,53));
  groebnerMatrix(45,53) = groebnerMatrix(19,77)/(groebnerMatrix(19,55));
  groebnerMatrix(45,55) = -groebnerMatrix(27,77)/(groebnerMatrix(27,53));
  groebnerMatrix(45,65) = groebnerMatrix(19,81)/(groebnerMatrix(19,55));
  groebnerMatrix(45,69) = (groebnerMatrix(19,82)/(groebnerMatrix(19,55))-groebnerMatrix(27,80)/(groebnerMatrix(27,53)));
  groebnerMatrix(45,70) = -groebnerMatrix(27,81)/(groebnerMatrix(27,53));
  groebnerMatrix(45,71) = -groebnerMatrix(27,82)/(groebnerMatrix(27,53));
  groebnerMatrix(45,74) = groebnerMatrix(19,83)/(groebnerMatrix(19,55));
  groebnerMatrix(45,76) = -groebnerMatrix(27,83)/(groebnerMatrix(27,53));
  groebnerMatrix(45,80) = groebnerMatrix(19,84)/(groebnerMatrix(19,55));
  groebnerMatrix(45,82) = -groebnerMatrix(27,84)/(groebnerMatrix(27,53));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial46( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(46,13) = groebnerMatrix(19,56)/(groebnerMatrix(19,55));
  groebnerMatrix(46,15) = -groebnerMatrix(28,56)/(groebnerMatrix(28,52));
  groebnerMatrix(46,29) = -groebnerMatrix(28,64)/(groebnerMatrix(28,52));
  groebnerMatrix(46,32) = -groebnerMatrix(28,68)/(groebnerMatrix(28,52));
  groebnerMatrix(46,43) = groebnerMatrix(19,75)/(groebnerMatrix(19,55));
  groebnerMatrix(46,47) = (groebnerMatrix(19,76)/(groebnerMatrix(19,55))-groebnerMatrix(28,73)/(groebnerMatrix(28,52)));
  groebnerMatrix(46,49) = -groebnerMatrix(28,75)/(groebnerMatrix(28,52));
  groebnerMatrix(46,50) = -groebnerMatrix(28,76)/(groebnerMatrix(28,52));
  groebnerMatrix(46,52) = groebnerMatrix(19,77)/(groebnerMatrix(19,55));
  groebnerMatrix(46,55) = -groebnerMatrix(28,77)/(groebnerMatrix(28,52));
  groebnerMatrix(46,64) = groebnerMatrix(19,81)/(groebnerMatrix(19,55));
  groebnerMatrix(46,68) = (groebnerMatrix(19,82)/(groebnerMatrix(19,55))-groebnerMatrix(28,79)/(groebnerMatrix(28,52)));
  groebnerMatrix(46,70) = -groebnerMatrix(28,81)/(groebnerMatrix(28,52));
  groebnerMatrix(46,71) = -groebnerMatrix(28,82)/(groebnerMatrix(28,52));
  groebnerMatrix(46,73) = groebnerMatrix(19,83)/(groebnerMatrix(19,55));
  groebnerMatrix(46,76) = -groebnerMatrix(28,83)/(groebnerMatrix(28,52));
  groebnerMatrix(46,79) = groebnerMatrix(19,84)/(groebnerMatrix(19,55));
  groebnerMatrix(46,82) = -groebnerMatrix(28,84)/(groebnerMatrix(28,52));
}

void
opengv::absolute_pose::modules::gp3p::sPolynomial47( Eigen::Matrix<double,48,85> & groebnerMatrix )
{
  groebnerMatrix(47,12) = groebnerMatrix(19,56)/(groebnerMatrix(19,55));
  groebnerMatrix(47,15) = -groebnerMatrix(29,56)/(groebnerMatrix(29,51));
  groebnerMatrix(47,28) = -groebnerMatrix(29,63)/(groebnerMatrix(29,51));
  groebnerMatrix(47,31) = -groebnerMatrix(29,67)/(groebnerMatrix(29,51));
  groebnerMatrix(47,42) = groebnerMatrix(19,75)/(groebnerMatrix(19,55));
  groebnerMatrix(47,46) = (groebnerMatrix(19,76)/(groebnerMatrix(19,55))-groebnerMatrix(29,72)/(groebnerMatrix(29,51)));
  groebnerMatrix(47,49) = -groebnerMatrix(29,75)/(groebnerMatrix(29,51));
  groebnerMatrix(47,50) = -groebnerMatrix(29,76)/(groebnerMatrix(29,51));
  groebnerMatrix(47,51) = groebnerMatrix(19,77)/(groebnerMatrix(19,55));
  groebnerMatrix(47,55) = -groebnerMatrix(29,77)/(groebnerMatrix(29,51));
  groebnerMatrix(47,63) = groebnerMatrix(19,81)/(groebnerMatrix(19,55));
  groebnerMatrix(47,67) = (groebnerMatrix(19,82)/(groebnerMatrix(19,55))-groebnerMatrix(29,78)/(groebnerMatrix(29,51)));
  groebnerMatrix(47,70) = -groebnerMatrix(29,81)/(groebnerMatrix(29,51));
  groebnerMatrix(47,71) = -groebnerMatrix(29,82)/(groebnerMatrix(29,51));
  groebnerMatrix(47,72) = groebnerMatrix(19,83)/(groebnerMatrix(19,55));
  groebnerMatrix(47,76) = -groebnerMatrix(29,83)/(groebnerMatrix(29,51));
  groebnerMatrix(47,78) = groebnerMatrix(19,84)/(groebnerMatrix(19,55));
  groebnerMatrix(47,82) = -groebnerMatrix(29,84)/(groebnerMatrix(29,51));
}

