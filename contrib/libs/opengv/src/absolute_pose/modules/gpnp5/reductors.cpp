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


#include <opengv/absolute_pose/modules/gpnp5/modules.hpp>


void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,60) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,60) = 0.0;
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,61) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,61) = 0.0;
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,62) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,62) = 0.0;
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,63) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,63) = 0.0;
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,33) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,33) = 0.0;
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,35) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,35) = 0.0;
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,36) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,36) = 0.0;
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,37) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,37) = 0.0;
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,38) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,38) = 0.0;
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,39) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,39) = 0.0;
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,45) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,45) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,46) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,46) = 0.0;
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,47) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,47) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,48) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,48) = 0.0;
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,49) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,49) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,64) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,64) = 0.0;
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,32) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,32) = 0.0;
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow5_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,34) / groebnerMatrix(5,59);
  groebnerMatrix(targetRow,34) = 0.0;
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(5,60);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(5,61);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(5,62);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(5,63);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(5,64);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(5,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(5,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(5,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(5,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(5,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(5,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(5,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(5,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(5,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(5,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(5,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(5,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(5,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(5,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(5,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow11_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,40) / groebnerMatrix(11,40);
  groebnerMatrix(targetRow,40) = 0.0;
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(11,41);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(11,42);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(11,43);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(11,50);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(11,51);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(11,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(11,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(11,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(11,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(11,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(11,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(11,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(11,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(11,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(11,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(11,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(11,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(11,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(11,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(11,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(11,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(11,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(11,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(11,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(11,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(11,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(11,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow5_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,44) / groebnerMatrix(5,59);
  groebnerMatrix(targetRow,44) = 0.0;
  groebnerMatrix(targetRow,45) -= factor * groebnerMatrix(5,60);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(5,61);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(5,62);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(5,63);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(5,64);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(5,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(5,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(5,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(5,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(5,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(5,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(5,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(5,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(5,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(5,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(5,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(5,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(5,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(5,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(5,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow5_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,59) / groebnerMatrix(5,59);
  groebnerMatrix(targetRow,59) = 0.0;
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(5,60);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(5,61);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(5,62);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(5,63);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(5,64);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(5,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(5,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(5,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(5,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(5,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(5,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(5,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(5,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(5,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(5,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(5,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(5,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(5,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(5,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(5,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,31) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,31) = 0.0;
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow12_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,41) / groebnerMatrix(12,41);
  groebnerMatrix(targetRow,41) = 0.0;
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(12,42);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(12,43);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(12,50);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(12,51);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(12,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(12,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(12,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(12,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(12,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(12,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(12,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(12,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(12,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(12,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(12,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(12,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(12,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(12,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(12,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(12,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(12,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(12,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,30) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,30) = 0.0;
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow13_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(13,42);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(13,43);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(13,50);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(13,51);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(13,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(13,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(13,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(13,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(13,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(13,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(13,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(13,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(13,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(13,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(13,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(13,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(13,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(13,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(13,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(13,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(13,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(13,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(13,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(13,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(13,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(13,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow14_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,43) / groebnerMatrix(14,43);
  groebnerMatrix(targetRow,43) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(14,50);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(14,51);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(14,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(14,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(14,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(14,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(14,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(14,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(14,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(14,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(14,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(14,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(14,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(14,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(14,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(14,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(14,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(14,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(14,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(14,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(14,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(14,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(14,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(14,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,29) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,29) = 0.0;
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow15_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,50) / groebnerMatrix(15,50);
  groebnerMatrix(targetRow,50) = 0.0;
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(15,51);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(15,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(15,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(15,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(15,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(15,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(15,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(15,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(15,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(15,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(15,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(15,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(15,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(15,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(15,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(15,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(15,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(15,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(15,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(15,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,27) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,27) = 0.0;
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,45) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow5_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,28) / groebnerMatrix(5,59);
  groebnerMatrix(targetRow,28) = 0.0;
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(5,60);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(5,61);
  groebnerMatrix(targetRow,31) -= factor * groebnerMatrix(5,62);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(5,63);
  groebnerMatrix(targetRow,33) -= factor * groebnerMatrix(5,64);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(5,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(5,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(5,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(5,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(5,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(5,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(5,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(5,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(5,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(5,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(5,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(5,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(5,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(5,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(5,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow16_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(16,51);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(16,52);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(16,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(16,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(16,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(16,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(16,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(16,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(16,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(16,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(16,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(16,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(16,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(16,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(16,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(16,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(16,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(16,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,26) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,26) = 0.0;
  groebnerMatrix(targetRow,27) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,29) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,30) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,32) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,45) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow17_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(17,52);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(17,53);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(17,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(17,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(17,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(17,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(17,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(17,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(17,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(17,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(17,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(17,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(17,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(17,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(17,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(17,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(17,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(17,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(17,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(17,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(17,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(17,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow15_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(15,50);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(15,51);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(15,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(15,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(15,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(15,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(15,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(15,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(15,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(15,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(15,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(15,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(15,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(15,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(15,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(15,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(15,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(15,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(15,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(15,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(15,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(15,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(15,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(15,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow16_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(16,51);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(16,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(16,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(16,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(16,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(16,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(16,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(16,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(16,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(16,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(16,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(16,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(16,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(16,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(16,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(16,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(16,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(16,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(16,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(16,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(16,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(16,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(16,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow17_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(17,52);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(17,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(17,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(17,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(17,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(17,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(17,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(17,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(17,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(17,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(17,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(17,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(17,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(17,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(17,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(17,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(17,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(17,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(17,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(17,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(17,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(17,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow18_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,20) / groebnerMatrix(18,53);
  groebnerMatrix(targetRow,20) = 0.0;
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(18,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(18,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(18,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(18,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(18,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(18,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(18,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(18,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(18,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(18,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(18,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(18,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(18,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(18,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(18,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow18_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,53) / groebnerMatrix(18,53);
  groebnerMatrix(targetRow,53) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(18,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(18,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(18,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(18,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(18,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(18,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(18,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(18,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(18,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(18,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(18,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(18,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(18,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(18,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(18,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow14_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,10) / groebnerMatrix(14,43);
  groebnerMatrix(targetRow,10) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(14,50);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(14,51);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(14,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(14,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(14,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(14,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(14,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(14,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(14,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(14,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(14,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(14,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(14,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(14,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(14,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(14,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(14,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(14,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(14,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(14,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(14,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(14,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(14,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(14,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow19_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(19,21);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(19,22);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(19,23);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(19,24);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(19,25);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(19,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(19,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(19,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(19,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(19,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(19,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(19,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(19,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(19,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(19,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(19,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(19,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(19,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(19,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(19,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(19,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(19,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(19,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(19,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(19,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,14) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,14) = 0.0;
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,15) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,15) = 0.0;
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,16) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,16) = 0.0;
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow20_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,22) / groebnerMatrix(20,22);
  groebnerMatrix(targetRow,22) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(20,23);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(20,24);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(20,25);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(20,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(20,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(20,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(20,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(20,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(20,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(20,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(20,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(20,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(20,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(20,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(20,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(20,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(20,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(20,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(20,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(20,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(20,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(20,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(20,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow13_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(13,42);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(13,43);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(13,50);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(13,51);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(13,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(13,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(13,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(13,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(13,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(13,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(13,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(13,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(13,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(13,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(13,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(13,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(13,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(13,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(13,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(13,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(13,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(13,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(13,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(13,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(13,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(13,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow21_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,23) / groebnerMatrix(21,23);
  groebnerMatrix(targetRow,23) = 0.0;
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(21,24);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(21,25);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(21,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(21,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(21,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(21,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(21,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(21,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(21,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(21,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(21,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(21,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(21,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(21,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(21,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(21,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(21,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(21,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(21,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(21,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(21,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(21,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,12) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,12) = 0.0;
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,13) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,13) = 0.0;
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow22_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,24) / groebnerMatrix(22,24);
  groebnerMatrix(targetRow,24) = 0.0;
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(22,25);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(22,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(22,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(22,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(22,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(22,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(22,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(22,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(22,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(22,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(22,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(22,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(22,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(22,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(22,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(22,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(22,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(22,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(22,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(22,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(22,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow12_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,8) / groebnerMatrix(12,41);
  groebnerMatrix(targetRow,8) = 0.0;
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(12,42);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(12,43);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(12,50);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(12,51);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(12,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(12,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(12,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(12,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(12,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(12,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(12,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(12,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(12,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(12,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(12,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(12,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(12,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(12,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(12,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(12,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(12,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(12,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(12,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(12,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(12,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(12,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow23_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,25) / groebnerMatrix(23,25);
  groebnerMatrix(targetRow,25) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(23,54);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(23,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(23,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(23,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(23,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(23,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(23,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(23,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(23,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(23,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(23,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(23,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(23,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(23,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(23,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(23,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(23,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(23,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(23,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(23,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow5_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,11) / groebnerMatrix(5,59);
  groebnerMatrix(targetRow,11) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(5,60);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(5,61);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(5,62);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(5,63);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(5,64);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(5,65);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(5,66);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(5,67);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(5,68);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(5,69);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(5,70);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(5,71);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(5,72);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(5,73);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(5,74);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(5,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(5,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(5,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(5,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(5,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow24_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,21) / groebnerMatrix(24,54);
  groebnerMatrix(targetRow,21) = 0.0;
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(24,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(24,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(24,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(24,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(24,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(24,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(24,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(24,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(24,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(24,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(24,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(24,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(24,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(24,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(24,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(24,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(24,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(24,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(24,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow24_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(24,54);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(24,55);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(24,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(24,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(24,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(24,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(24,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(24,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(24,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(24,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(24,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(24,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(24,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(24,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(24,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(24,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(24,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(24,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(24,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(24,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow11_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(11,40);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(11,41);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(11,42);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(11,43);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(11,50);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(11,51);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(11,52);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(11,53);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(11,54);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(11,55);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(11,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(11,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(11,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(11,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(11,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(11,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(11,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(11,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(11,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(11,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(11,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(11,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(11,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(11,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(11,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(11,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(11,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(11,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow25_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,22) / groebnerMatrix(25,55);
  groebnerMatrix(targetRow,22) = 0.0;
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(25,56);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(25,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(25,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(25,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(25,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(25,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(25,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(25,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(25,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(25,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(25,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(25,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(25,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(25,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(25,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(25,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(25,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(25,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow25_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(25,55);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(25,56);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(25,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(25,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(25,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(25,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(25,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(25,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(25,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(25,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(25,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(25,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(25,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(25,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(25,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(25,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(25,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(25,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(25,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow10_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,6) / groebnerMatrix(10,64);
  groebnerMatrix(targetRow,6) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(10,65);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(10,66);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(10,67);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(10,68);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(10,69);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(10,70);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(10,71);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(10,72);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(10,73);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(10,74);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(10,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(10,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(10,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(10,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(10,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow26_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,23) / groebnerMatrix(26,56);
  groebnerMatrix(targetRow,23) = 0.0;
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(26,57);
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(26,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(26,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(26,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(26,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(26,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(26,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(26,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(26,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(26,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(26,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(26,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(26,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(26,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(26,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(26,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(26,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow26_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,56) / groebnerMatrix(26,56);
  groebnerMatrix(targetRow,56) = 0.0;
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(26,57);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(26,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(26,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(26,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(26,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(26,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(26,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(26,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(26,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(26,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(26,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(26,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(26,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(26,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(26,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(26,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(26,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow27_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,24) / groebnerMatrix(27,57);
  groebnerMatrix(targetRow,24) = 0.0;
  groebnerMatrix(targetRow,25) -= factor * groebnerMatrix(27,58);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(27,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(27,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(27,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(27,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(27,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(27,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(27,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(27,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(27,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(27,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(27,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(27,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(27,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(27,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(27,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow27_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,57) / groebnerMatrix(27,57);
  groebnerMatrix(targetRow,57) = 0.0;
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(27,58);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(27,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(27,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(27,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(27,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(27,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(27,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(27,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(27,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(27,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(27,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(27,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(27,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(27,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(27,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(27,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow9_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,5) / groebnerMatrix(9,63);
  groebnerMatrix(targetRow,5) = 0.0;
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(9,64);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(9,65);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(9,66);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(9,67);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(9,68);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(9,69);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(9,70);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(9,71);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(9,72);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(9,73);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(9,74);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(9,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(9,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(9,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(9,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(9,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow28_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,25) / groebnerMatrix(28,58);
  groebnerMatrix(targetRow,25) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(28,65);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(28,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(28,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(28,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(28,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(28,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(28,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(28,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(28,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(28,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(28,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(28,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(28,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(28,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(28,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow28_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,58) / groebnerMatrix(28,58);
  groebnerMatrix(targetRow,58) = 0.0;
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(28,65);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(28,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(28,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(28,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(28,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(28,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(28,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(28,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(28,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(28,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(28,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(28,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(28,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(28,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(28,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow18_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,7) / groebnerMatrix(18,53);
  groebnerMatrix(targetRow,7) = 0.0;
  groebnerMatrix(targetRow,11) -= factor * groebnerMatrix(18,54);
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(18,55);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(18,56);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(18,57);
  groebnerMatrix(targetRow,21) -= factor * groebnerMatrix(18,58);
  groebnerMatrix(targetRow,34) -= factor * groebnerMatrix(18,65);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(18,66);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(18,67);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,44) -= factor * groebnerMatrix(18,69);
  groebnerMatrix(targetRow,45) -= factor * groebnerMatrix(18,70);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(18,71);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(18,72);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(18,74);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(18,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(18,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow24_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,17) / groebnerMatrix(24,54);
  groebnerMatrix(targetRow,17) = 0.0;
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(24,55);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(24,56);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(24,57);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(24,58);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(24,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(24,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(24,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(24,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(24,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(24,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(24,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(24,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(24,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(24,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(24,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(24,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(24,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(24,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(24,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow29_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,40) / groebnerMatrix(29,65);
  groebnerMatrix(targetRow,40) = 0.0;
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(29,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(29,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(29,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(29,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(29,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(29,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(29,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(29,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(29,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(29,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(29,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(29,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(29,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(29,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow29_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,50) / groebnerMatrix(29,65);
  groebnerMatrix(targetRow,50) = 0.0;
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(29,66);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(29,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(29,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(29,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(29,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(29,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(29,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(29,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(29,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(29,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(29,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(29,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(29,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(29,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow29_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,65) / groebnerMatrix(29,65);
  groebnerMatrix(targetRow,65) = 0.0;
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(29,66);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(29,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(29,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(29,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(29,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(29,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(29,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(29,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(29,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(29,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(29,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(29,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(29,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(29,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow14_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(14,43);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(14,50);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(14,51);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(14,52);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(14,53);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(14,54);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(14,55);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(14,56);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(14,57);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(14,58);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(14,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(14,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(14,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(14,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(14,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(14,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(14,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(14,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(14,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(14,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(14,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(14,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(14,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(14,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(14,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow8_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,4) / groebnerMatrix(8,62);
  groebnerMatrix(targetRow,4) = 0.0;
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(8,63);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(8,64);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(8,65);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(8,66);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(8,67);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(8,68);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(8,69);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(8,70);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(8,71);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(8,72);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(8,73);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(8,74);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(8,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(8,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(8,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(8,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(8,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow18_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,8) / groebnerMatrix(18,53);
  groebnerMatrix(targetRow,8) = 0.0;
  groebnerMatrix(targetRow,12) -= factor * groebnerMatrix(18,54);
  groebnerMatrix(targetRow,13) -= factor * groebnerMatrix(18,55);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(18,56);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(18,57);
  groebnerMatrix(targetRow,22) -= factor * groebnerMatrix(18,58);
  groebnerMatrix(targetRow,35) -= factor * groebnerMatrix(18,65);
  groebnerMatrix(targetRow,36) -= factor * groebnerMatrix(18,66);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(18,67);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,45) -= factor * groebnerMatrix(18,69);
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(18,70);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(18,71);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(18,72);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(18,74);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(18,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(18,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow25_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,18) / groebnerMatrix(25,55);
  groebnerMatrix(targetRow,18) = 0.0;
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(25,56);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(25,57);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(25,58);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(25,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(25,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(25,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(25,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(25,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(25,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(25,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(25,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(25,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(25,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(25,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(25,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(25,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(25,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(25,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow30_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,41) / groebnerMatrix(30,66);
  groebnerMatrix(targetRow,41) = 0.0;
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(30,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(30,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(30,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(30,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(30,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(30,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow30_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(30,66);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(30,67);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(30,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(30,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(30,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(30,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(30,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow30_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,66) / groebnerMatrix(30,66);
  groebnerMatrix(targetRow,66) = 0.0;
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(30,67);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(30,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(30,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(30,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(30,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(30,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(30,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(30,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(30,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(30,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(30,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(30,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(30,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow14_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,0) / groebnerMatrix(14,43);
  groebnerMatrix(targetRow,0) = 0.0;
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(14,50);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(14,51);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(14,52);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(14,53);
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(14,54);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(14,55);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(14,56);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(14,57);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(14,58);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(14,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(14,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(14,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(14,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(14,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(14,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(14,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(14,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(14,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(14,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(14,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(14,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(14,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(14,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(14,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow6_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,2) / groebnerMatrix(6,60);
  groebnerMatrix(targetRow,2) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(6,61);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(6,62);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(6,63);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(6,64);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(6,65);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(6,66);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(6,67);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(6,68);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(6,69);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(6,70);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(6,71);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(6,72);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(6,73);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(6,74);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(6,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(6,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(6,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(6,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(6,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow7_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,3) / groebnerMatrix(7,61);
  groebnerMatrix(targetRow,3) = 0.0;
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(7,62);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(7,63);
  groebnerMatrix(targetRow,6) -= factor * groebnerMatrix(7,64);
  groebnerMatrix(targetRow,7) -= factor * groebnerMatrix(7,65);
  groebnerMatrix(targetRow,8) -= factor * groebnerMatrix(7,66);
  groebnerMatrix(targetRow,9) -= factor * groebnerMatrix(7,67);
  groebnerMatrix(targetRow,10) -= factor * groebnerMatrix(7,68);
  groebnerMatrix(targetRow,17) -= factor * groebnerMatrix(7,69);
  groebnerMatrix(targetRow,18) -= factor * groebnerMatrix(7,70);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(7,71);
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(7,72);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(7,73);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(7,74);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(7,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(7,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(7,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(7,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(7,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow18_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,9) / groebnerMatrix(18,53);
  groebnerMatrix(targetRow,9) = 0.0;
  groebnerMatrix(targetRow,14) -= factor * groebnerMatrix(18,54);
  groebnerMatrix(targetRow,15) -= factor * groebnerMatrix(18,55);
  groebnerMatrix(targetRow,16) -= factor * groebnerMatrix(18,56);
  groebnerMatrix(targetRow,19) -= factor * groebnerMatrix(18,57);
  groebnerMatrix(targetRow,23) -= factor * groebnerMatrix(18,58);
  groebnerMatrix(targetRow,37) -= factor * groebnerMatrix(18,65);
  groebnerMatrix(targetRow,38) -= factor * groebnerMatrix(18,66);
  groebnerMatrix(targetRow,39) -= factor * groebnerMatrix(18,67);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(18,68);
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(18,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(18,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(18,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(18,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(18,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(18,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(18,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(18,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(18,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(18,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(18,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow26_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,19) / groebnerMatrix(26,56);
  groebnerMatrix(targetRow,19) = 0.0;
  groebnerMatrix(targetRow,20) -= factor * groebnerMatrix(26,57);
  groebnerMatrix(targetRow,24) -= factor * groebnerMatrix(26,58);
  groebnerMatrix(targetRow,40) -= factor * groebnerMatrix(26,65);
  groebnerMatrix(targetRow,41) -= factor * groebnerMatrix(26,66);
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(26,67);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(26,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(26,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(26,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(26,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(26,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(26,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(26,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(26,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(26,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(26,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(26,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(26,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow31_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(31,67);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(31,68);
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(31,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(31,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(31,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(31,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow31_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(31,67);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(31,68);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(31,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(31,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(31,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(31,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow31_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,67) / groebnerMatrix(31,67);
  groebnerMatrix(targetRow,67) = 0.0;
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(31,68);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(31,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(31,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(31,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(31,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(31,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(31,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(31,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(31,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(31,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(31,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(31,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow32_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,68) / groebnerMatrix(32,68);
  groebnerMatrix(targetRow,68) = 0.0;
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(32,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(32,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(32,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow32_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,53) / groebnerMatrix(32,68);
  groebnerMatrix(targetRow,53) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(32,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(32,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(32,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow33_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(33,69);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(33,70);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(33,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(33,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow33_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,69) / groebnerMatrix(33,69);
  groebnerMatrix(targetRow,69) = 0.0;
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(33,70);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(33,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(33,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(33,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(33,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(33,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(33,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(33,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(33,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(33,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow34_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(34,70);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(34,71);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(34,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow34_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,70) / groebnerMatrix(34,70);
  groebnerMatrix(targetRow,70) = 0.0;
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(34,71);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(34,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(34,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(34,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(34,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(34,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(34,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(34,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(34,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow35_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,56) / groebnerMatrix(35,71);
  groebnerMatrix(targetRow,56) = 0.0;
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(35,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow35_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,71) / groebnerMatrix(35,71);
  groebnerMatrix(targetRow,71) = 0.0;
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(35,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow36_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,57) / groebnerMatrix(36,72);
  groebnerMatrix(targetRow,57) = 0.0;
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(36,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow36_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,72) / groebnerMatrix(36,72);
  groebnerMatrix(targetRow,72) = 0.0;
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(36,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow32_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,43) / groebnerMatrix(32,68);
  groebnerMatrix(targetRow,43) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(32,70);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(32,71);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(32,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,58) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,58) = 0.0;
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,73) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,73) = 0.0;
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow35_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,47) / groebnerMatrix(35,71);
  groebnerMatrix(targetRow,47) = 0.0;
  groebnerMatrix(targetRow,50) -= factor * groebnerMatrix(35,72);
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(35,73);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(35,74);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(35,75);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(35,76);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(35,77);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(35,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(35,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow36_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,50) / groebnerMatrix(36,72);
  groebnerMatrix(targetRow,50) = 0.0;
  groebnerMatrix(targetRow,54) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(36,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,59) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,59) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,59) = 0.0;
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,69) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,74) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,60) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,60) = 0.0;
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,62) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,62) = 0.0;
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,65) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,65) = 0.0;
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,69) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,69) = 0.0;
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,74) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,74) = 0.0;
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow36_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(36,72);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(36,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,60) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,61) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,61) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,61) = 0.0;
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,75) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,63) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,63) = 0.0;
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,66) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,66) = 0.0;
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,70) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,70) = 0.0;
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,75) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,75) = 0.0;
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow32_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(32,68);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,47) -= factor * groebnerMatrix(32,69);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(32,70);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(32,71);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(32,72);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(32,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(32,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(32,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(32,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(32,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(32,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(32,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_10010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,45) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,45) = 0.0;
  groebnerMatrix(targetRow,46) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_10010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,46) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,46) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,70) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,47) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,47) = 0.0;
  groebnerMatrix(targetRow,48) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,48) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,48) = 0.0;
  groebnerMatrix(targetRow,49) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,49) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,49) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow36_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(36,72);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(36,73);
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(36,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(36,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(36,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(36,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(36,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(36,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,56) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,56) = 0.0;
  groebnerMatrix(targetRow,62) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,63) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,64) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,64) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,64) = 0.0;
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,71) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,76) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,67) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,67) = 0.0;
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,71) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,71) = 0.0;
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,76) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,76) = 0.0;
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,41) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,41) = 0.0;
  groebnerMatrix(targetRow,42) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,42) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,42) = 0.0;
  groebnerMatrix(targetRow,43) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,43) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,43) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,50) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,50) = 0.0;
  groebnerMatrix(targetRow,51) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,51) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,51) = 0.0;
  groebnerMatrix(targetRow,52) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,52) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,52) = 0.0;
  groebnerMatrix(targetRow,53) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,53) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,53) = 0.0;
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow37_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,57) / groebnerMatrix(37,73);
  groebnerMatrix(targetRow,57) = 0.0;
  groebnerMatrix(targetRow,65) -= factor * groebnerMatrix(37,74);
  groebnerMatrix(targetRow,66) -= factor * groebnerMatrix(37,75);
  groebnerMatrix(targetRow,67) -= factor * groebnerMatrix(37,76);
  groebnerMatrix(targetRow,68) -= factor * groebnerMatrix(37,77);
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(37,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(37,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,68) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,68) = 0.0;
  groebnerMatrix(targetRow,72) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,77) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,72) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,72) = 0.0;
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,77) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,77) = 0.0;
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow38_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,54) / groebnerMatrix(38,74);
  groebnerMatrix(targetRow,54) = 0.0;
  groebnerMatrix(targetRow,55) -= factor * groebnerMatrix(38,75);
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(38,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(38,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(38,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(38,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow39_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,55) / groebnerMatrix(39,75);
  groebnerMatrix(targetRow,55) = 0.0;
  groebnerMatrix(targetRow,56) -= factor * groebnerMatrix(39,76);
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(39,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(39,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(39,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow40_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,56) / groebnerMatrix(40,76);
  groebnerMatrix(targetRow,56) = 0.0;
  groebnerMatrix(targetRow,57) -= factor * groebnerMatrix(40,77);
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(40,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(40,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow41_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,57) / groebnerMatrix(41,77);
  groebnerMatrix(targetRow,57) = 0.0;
  groebnerMatrix(targetRow,58) -= factor * groebnerMatrix(41,78);
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(41,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow42_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,58) / groebnerMatrix(42,78);
  groebnerMatrix(targetRow,58) = 0.0;
  groebnerMatrix(targetRow,73) -= factor * groebnerMatrix(42,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow42_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,73) / groebnerMatrix(42,78);
  groebnerMatrix(targetRow,73) = 0.0;
  groebnerMatrix(targetRow,78) -= factor * groebnerMatrix(42,79);
}

void
opengv::absolute_pose::modules::gpnp5::groebnerRow42_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,78) / groebnerMatrix(42,78);
  groebnerMatrix(targetRow,78) = 0.0;
  groebnerMatrix(targetRow,79) -= factor * groebnerMatrix(42,79);
}

