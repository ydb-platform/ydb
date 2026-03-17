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
opengv::absolute_pose::modules::gpnp5::sPolynomial6( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(6,60) = (groebnerMatrix(0,60)/(groebnerMatrix(0,59))-groebnerMatrix(1,60)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,61) = (groebnerMatrix(0,61)/(groebnerMatrix(0,59))-groebnerMatrix(1,61)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,62) = (groebnerMatrix(0,62)/(groebnerMatrix(0,59))-groebnerMatrix(1,62)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,63) = (groebnerMatrix(0,63)/(groebnerMatrix(0,59))-groebnerMatrix(1,63)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,64) = (groebnerMatrix(0,64)/(groebnerMatrix(0,59))-groebnerMatrix(1,64)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,65) = (groebnerMatrix(0,65)/(groebnerMatrix(0,59))-groebnerMatrix(1,65)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,66) = (groebnerMatrix(0,66)/(groebnerMatrix(0,59))-groebnerMatrix(1,66)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,67) = (groebnerMatrix(0,67)/(groebnerMatrix(0,59))-groebnerMatrix(1,67)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,68) = (groebnerMatrix(0,68)/(groebnerMatrix(0,59))-groebnerMatrix(1,68)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,69) = (groebnerMatrix(0,69)/(groebnerMatrix(0,59))-groebnerMatrix(1,69)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,70) = (groebnerMatrix(0,70)/(groebnerMatrix(0,59))-groebnerMatrix(1,70)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,71) = (groebnerMatrix(0,71)/(groebnerMatrix(0,59))-groebnerMatrix(1,71)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,72) = (groebnerMatrix(0,72)/(groebnerMatrix(0,59))-groebnerMatrix(1,72)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,73) = (groebnerMatrix(0,73)/(groebnerMatrix(0,59))-groebnerMatrix(1,73)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,74) = (groebnerMatrix(0,74)/(groebnerMatrix(0,59))-groebnerMatrix(1,74)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,75) = (groebnerMatrix(0,75)/(groebnerMatrix(0,59))-groebnerMatrix(1,75)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,76) = (groebnerMatrix(0,76)/(groebnerMatrix(0,59))-groebnerMatrix(1,76)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,77) = (groebnerMatrix(0,77)/(groebnerMatrix(0,59))-groebnerMatrix(1,77)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,78) = (groebnerMatrix(0,78)/(groebnerMatrix(0,59))-groebnerMatrix(1,78)/(groebnerMatrix(1,59)));
  groebnerMatrix(6,79) = (groebnerMatrix(0,79)/(groebnerMatrix(0,59))-groebnerMatrix(1,79)/(groebnerMatrix(1,59)));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial7( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(7,60) = (groebnerMatrix(1,60)/(groebnerMatrix(1,59))-groebnerMatrix(2,60)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,61) = (groebnerMatrix(1,61)/(groebnerMatrix(1,59))-groebnerMatrix(2,61)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,62) = (groebnerMatrix(1,62)/(groebnerMatrix(1,59))-groebnerMatrix(2,62)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,63) = (groebnerMatrix(1,63)/(groebnerMatrix(1,59))-groebnerMatrix(2,63)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,64) = (groebnerMatrix(1,64)/(groebnerMatrix(1,59))-groebnerMatrix(2,64)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,65) = (groebnerMatrix(1,65)/(groebnerMatrix(1,59))-groebnerMatrix(2,65)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,66) = (groebnerMatrix(1,66)/(groebnerMatrix(1,59))-groebnerMatrix(2,66)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,67) = (groebnerMatrix(1,67)/(groebnerMatrix(1,59))-groebnerMatrix(2,67)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,68) = (groebnerMatrix(1,68)/(groebnerMatrix(1,59))-groebnerMatrix(2,68)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,69) = (groebnerMatrix(1,69)/(groebnerMatrix(1,59))-groebnerMatrix(2,69)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,70) = (groebnerMatrix(1,70)/(groebnerMatrix(1,59))-groebnerMatrix(2,70)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,71) = (groebnerMatrix(1,71)/(groebnerMatrix(1,59))-groebnerMatrix(2,71)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,72) = (groebnerMatrix(1,72)/(groebnerMatrix(1,59))-groebnerMatrix(2,72)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,73) = (groebnerMatrix(1,73)/(groebnerMatrix(1,59))-groebnerMatrix(2,73)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,74) = (groebnerMatrix(1,74)/(groebnerMatrix(1,59))-groebnerMatrix(2,74)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,75) = (groebnerMatrix(1,75)/(groebnerMatrix(1,59))-groebnerMatrix(2,75)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,76) = (groebnerMatrix(1,76)/(groebnerMatrix(1,59))-groebnerMatrix(2,76)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,77) = (groebnerMatrix(1,77)/(groebnerMatrix(1,59))-groebnerMatrix(2,77)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,78) = (groebnerMatrix(1,78)/(groebnerMatrix(1,59))-groebnerMatrix(2,78)/(groebnerMatrix(2,59)));
  groebnerMatrix(7,79) = (groebnerMatrix(1,79)/(groebnerMatrix(1,59))-groebnerMatrix(2,79)/(groebnerMatrix(2,59)));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial8( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(8,60) = (groebnerMatrix(2,60)/(groebnerMatrix(2,59))-groebnerMatrix(3,60)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,61) = (groebnerMatrix(2,61)/(groebnerMatrix(2,59))-groebnerMatrix(3,61)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,62) = (groebnerMatrix(2,62)/(groebnerMatrix(2,59))-groebnerMatrix(3,62)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,63) = (groebnerMatrix(2,63)/(groebnerMatrix(2,59))-groebnerMatrix(3,63)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,64) = (groebnerMatrix(2,64)/(groebnerMatrix(2,59))-groebnerMatrix(3,64)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,65) = (groebnerMatrix(2,65)/(groebnerMatrix(2,59))-groebnerMatrix(3,65)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,66) = (groebnerMatrix(2,66)/(groebnerMatrix(2,59))-groebnerMatrix(3,66)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,67) = (groebnerMatrix(2,67)/(groebnerMatrix(2,59))-groebnerMatrix(3,67)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,68) = (groebnerMatrix(2,68)/(groebnerMatrix(2,59))-groebnerMatrix(3,68)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,69) = (groebnerMatrix(2,69)/(groebnerMatrix(2,59))-groebnerMatrix(3,69)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,70) = (groebnerMatrix(2,70)/(groebnerMatrix(2,59))-groebnerMatrix(3,70)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,71) = (groebnerMatrix(2,71)/(groebnerMatrix(2,59))-groebnerMatrix(3,71)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,72) = (groebnerMatrix(2,72)/(groebnerMatrix(2,59))-groebnerMatrix(3,72)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,73) = (groebnerMatrix(2,73)/(groebnerMatrix(2,59))-groebnerMatrix(3,73)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,74) = (groebnerMatrix(2,74)/(groebnerMatrix(2,59))-groebnerMatrix(3,74)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,75) = (groebnerMatrix(2,75)/(groebnerMatrix(2,59))-groebnerMatrix(3,75)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,76) = (groebnerMatrix(2,76)/(groebnerMatrix(2,59))-groebnerMatrix(3,76)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,77) = (groebnerMatrix(2,77)/(groebnerMatrix(2,59))-groebnerMatrix(3,77)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,78) = (groebnerMatrix(2,78)/(groebnerMatrix(2,59))-groebnerMatrix(3,78)/(groebnerMatrix(3,59)));
  groebnerMatrix(8,79) = (groebnerMatrix(2,79)/(groebnerMatrix(2,59))-groebnerMatrix(3,79)/(groebnerMatrix(3,59)));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial9( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(9,60) = (groebnerMatrix(3,60)/(groebnerMatrix(3,59))-groebnerMatrix(4,60)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,61) = (groebnerMatrix(3,61)/(groebnerMatrix(3,59))-groebnerMatrix(4,61)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,62) = (groebnerMatrix(3,62)/(groebnerMatrix(3,59))-groebnerMatrix(4,62)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,63) = (groebnerMatrix(3,63)/(groebnerMatrix(3,59))-groebnerMatrix(4,63)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,64) = (groebnerMatrix(3,64)/(groebnerMatrix(3,59))-groebnerMatrix(4,64)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,65) = (groebnerMatrix(3,65)/(groebnerMatrix(3,59))-groebnerMatrix(4,65)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,66) = (groebnerMatrix(3,66)/(groebnerMatrix(3,59))-groebnerMatrix(4,66)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,67) = (groebnerMatrix(3,67)/(groebnerMatrix(3,59))-groebnerMatrix(4,67)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,68) = (groebnerMatrix(3,68)/(groebnerMatrix(3,59))-groebnerMatrix(4,68)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,69) = (groebnerMatrix(3,69)/(groebnerMatrix(3,59))-groebnerMatrix(4,69)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,70) = (groebnerMatrix(3,70)/(groebnerMatrix(3,59))-groebnerMatrix(4,70)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,71) = (groebnerMatrix(3,71)/(groebnerMatrix(3,59))-groebnerMatrix(4,71)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,72) = (groebnerMatrix(3,72)/(groebnerMatrix(3,59))-groebnerMatrix(4,72)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,73) = (groebnerMatrix(3,73)/(groebnerMatrix(3,59))-groebnerMatrix(4,73)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,74) = (groebnerMatrix(3,74)/(groebnerMatrix(3,59))-groebnerMatrix(4,74)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,75) = (groebnerMatrix(3,75)/(groebnerMatrix(3,59))-groebnerMatrix(4,75)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,76) = (groebnerMatrix(3,76)/(groebnerMatrix(3,59))-groebnerMatrix(4,76)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,77) = (groebnerMatrix(3,77)/(groebnerMatrix(3,59))-groebnerMatrix(4,77)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,78) = (groebnerMatrix(3,78)/(groebnerMatrix(3,59))-groebnerMatrix(4,78)/(groebnerMatrix(4,59)));
  groebnerMatrix(9,79) = (groebnerMatrix(3,79)/(groebnerMatrix(3,59))-groebnerMatrix(4,79)/(groebnerMatrix(4,59)));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial10( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(10,60) = (groebnerMatrix(4,60)/(groebnerMatrix(4,59))-groebnerMatrix(5,60)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,61) = (groebnerMatrix(4,61)/(groebnerMatrix(4,59))-groebnerMatrix(5,61)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,62) = (groebnerMatrix(4,62)/(groebnerMatrix(4,59))-groebnerMatrix(5,62)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,63) = (groebnerMatrix(4,63)/(groebnerMatrix(4,59))-groebnerMatrix(5,63)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,64) = (groebnerMatrix(4,64)/(groebnerMatrix(4,59))-groebnerMatrix(5,64)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,65) = (groebnerMatrix(4,65)/(groebnerMatrix(4,59))-groebnerMatrix(5,65)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,66) = (groebnerMatrix(4,66)/(groebnerMatrix(4,59))-groebnerMatrix(5,66)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,67) = (groebnerMatrix(4,67)/(groebnerMatrix(4,59))-groebnerMatrix(5,67)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,68) = (groebnerMatrix(4,68)/(groebnerMatrix(4,59))-groebnerMatrix(5,68)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,69) = (groebnerMatrix(4,69)/(groebnerMatrix(4,59))-groebnerMatrix(5,69)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,70) = (groebnerMatrix(4,70)/(groebnerMatrix(4,59))-groebnerMatrix(5,70)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,71) = (groebnerMatrix(4,71)/(groebnerMatrix(4,59))-groebnerMatrix(5,71)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,72) = (groebnerMatrix(4,72)/(groebnerMatrix(4,59))-groebnerMatrix(5,72)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,73) = (groebnerMatrix(4,73)/(groebnerMatrix(4,59))-groebnerMatrix(5,73)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,74) = (groebnerMatrix(4,74)/(groebnerMatrix(4,59))-groebnerMatrix(5,74)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,75) = (groebnerMatrix(4,75)/(groebnerMatrix(4,59))-groebnerMatrix(5,75)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,76) = (groebnerMatrix(4,76)/(groebnerMatrix(4,59))-groebnerMatrix(5,76)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,77) = (groebnerMatrix(4,77)/(groebnerMatrix(4,59))-groebnerMatrix(5,77)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,78) = (groebnerMatrix(4,78)/(groebnerMatrix(4,59))-groebnerMatrix(5,78)/(groebnerMatrix(5,59)));
  groebnerMatrix(10,79) = (groebnerMatrix(4,79)/(groebnerMatrix(4,59))-groebnerMatrix(5,79)/(groebnerMatrix(5,59)));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial11( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(11,33) = groebnerMatrix(9,64)/(groebnerMatrix(9,63));
  groebnerMatrix(11,35) = -groebnerMatrix(10,65)/(groebnerMatrix(10,64));
  groebnerMatrix(11,36) = -groebnerMatrix(10,66)/(groebnerMatrix(10,64));
  groebnerMatrix(11,37) = groebnerMatrix(9,65)/(groebnerMatrix(9,63));
  groebnerMatrix(11,38) = (groebnerMatrix(9,66)/(groebnerMatrix(9,63))-groebnerMatrix(10,67)/(groebnerMatrix(10,64)));
  groebnerMatrix(11,39) = groebnerMatrix(9,67)/(groebnerMatrix(9,63));
  groebnerMatrix(11,41) = -groebnerMatrix(10,68)/(groebnerMatrix(10,64));
  groebnerMatrix(11,42) = groebnerMatrix(9,68)/(groebnerMatrix(9,63));
  groebnerMatrix(11,45) = -groebnerMatrix(10,69)/(groebnerMatrix(10,64));
  groebnerMatrix(11,46) = -groebnerMatrix(10,70)/(groebnerMatrix(10,64));
  groebnerMatrix(11,47) = groebnerMatrix(9,69)/(groebnerMatrix(9,63));
  groebnerMatrix(11,48) = (groebnerMatrix(9,70)/(groebnerMatrix(9,63))-groebnerMatrix(10,71)/(groebnerMatrix(10,64)));
  groebnerMatrix(11,49) = groebnerMatrix(9,71)/(groebnerMatrix(9,63));
  groebnerMatrix(11,51) = -groebnerMatrix(10,72)/(groebnerMatrix(10,64));
  groebnerMatrix(11,52) = groebnerMatrix(9,72)/(groebnerMatrix(9,63));
  groebnerMatrix(11,55) = -groebnerMatrix(10,73)/(groebnerMatrix(10,64));
  groebnerMatrix(11,56) = groebnerMatrix(9,73)/(groebnerMatrix(9,63));
  groebnerMatrix(11,60) = -groebnerMatrix(10,74)/(groebnerMatrix(10,64));
  groebnerMatrix(11,61) = -groebnerMatrix(10,75)/(groebnerMatrix(10,64));
  groebnerMatrix(11,62) = groebnerMatrix(9,74)/(groebnerMatrix(9,63));
  groebnerMatrix(11,63) = (groebnerMatrix(9,75)/(groebnerMatrix(9,63))-groebnerMatrix(10,76)/(groebnerMatrix(10,64)));
  groebnerMatrix(11,64) = groebnerMatrix(9,76)/(groebnerMatrix(9,63));
  groebnerMatrix(11,66) = -groebnerMatrix(10,77)/(groebnerMatrix(10,64));
  groebnerMatrix(11,67) = groebnerMatrix(9,77)/(groebnerMatrix(9,63));
  groebnerMatrix(11,70) = -groebnerMatrix(10,78)/(groebnerMatrix(10,64));
  groebnerMatrix(11,71) = groebnerMatrix(9,78)/(groebnerMatrix(9,63));
  groebnerMatrix(11,75) = -groebnerMatrix(10,79)/(groebnerMatrix(10,64));
  groebnerMatrix(11,76) = groebnerMatrix(9,79)/(groebnerMatrix(9,63));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial12( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(12,32) = groebnerMatrix(8,63)/(groebnerMatrix(8,62));
  groebnerMatrix(12,33) = groebnerMatrix(8,64)/(groebnerMatrix(8,62));
  groebnerMatrix(12,34) = -groebnerMatrix(10,65)/(groebnerMatrix(10,64));
  groebnerMatrix(12,35) = -groebnerMatrix(10,66)/(groebnerMatrix(10,64));
  groebnerMatrix(12,37) = (groebnerMatrix(8,65)/(groebnerMatrix(8,62))-groebnerMatrix(10,67)/(groebnerMatrix(10,64)));
  groebnerMatrix(12,38) = groebnerMatrix(8,66)/(groebnerMatrix(8,62));
  groebnerMatrix(12,39) = groebnerMatrix(8,67)/(groebnerMatrix(8,62));
  groebnerMatrix(12,40) = -groebnerMatrix(10,68)/(groebnerMatrix(10,64));
  groebnerMatrix(12,42) = groebnerMatrix(8,68)/(groebnerMatrix(8,62));
  groebnerMatrix(12,44) = -groebnerMatrix(10,69)/(groebnerMatrix(10,64));
  groebnerMatrix(12,45) = -groebnerMatrix(10,70)/(groebnerMatrix(10,64));
  groebnerMatrix(12,47) = (groebnerMatrix(8,69)/(groebnerMatrix(8,62))-groebnerMatrix(10,71)/(groebnerMatrix(10,64)));
  groebnerMatrix(12,48) = groebnerMatrix(8,70)/(groebnerMatrix(8,62));
  groebnerMatrix(12,49) = groebnerMatrix(8,71)/(groebnerMatrix(8,62));
  groebnerMatrix(12,50) = -groebnerMatrix(10,72)/(groebnerMatrix(10,64));
  groebnerMatrix(12,52) = groebnerMatrix(8,72)/(groebnerMatrix(8,62));
  groebnerMatrix(12,54) = -groebnerMatrix(10,73)/(groebnerMatrix(10,64));
  groebnerMatrix(12,56) = groebnerMatrix(8,73)/(groebnerMatrix(8,62));
  groebnerMatrix(12,59) = -groebnerMatrix(10,74)/(groebnerMatrix(10,64));
  groebnerMatrix(12,60) = -groebnerMatrix(10,75)/(groebnerMatrix(10,64));
  groebnerMatrix(12,62) = (groebnerMatrix(8,74)/(groebnerMatrix(8,62))-groebnerMatrix(10,76)/(groebnerMatrix(10,64)));
  groebnerMatrix(12,63) = groebnerMatrix(8,75)/(groebnerMatrix(8,62));
  groebnerMatrix(12,64) = groebnerMatrix(8,76)/(groebnerMatrix(8,62));
  groebnerMatrix(12,65) = -groebnerMatrix(10,77)/(groebnerMatrix(10,64));
  groebnerMatrix(12,67) = groebnerMatrix(8,77)/(groebnerMatrix(8,62));
  groebnerMatrix(12,69) = -groebnerMatrix(10,78)/(groebnerMatrix(10,64));
  groebnerMatrix(12,71) = groebnerMatrix(8,78)/(groebnerMatrix(8,62));
  groebnerMatrix(12,74) = -groebnerMatrix(10,79)/(groebnerMatrix(10,64));
  groebnerMatrix(12,76) = groebnerMatrix(8,79)/(groebnerMatrix(8,62));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial13( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(13,31) = groebnerMatrix(7,62)/(groebnerMatrix(7,61));
  groebnerMatrix(13,32) = (groebnerMatrix(7,63)/(groebnerMatrix(7,61))-groebnerMatrix(9,64)/(groebnerMatrix(9,63)));
  groebnerMatrix(13,33) = groebnerMatrix(7,64)/(groebnerMatrix(7,61));
  groebnerMatrix(13,35) = -groebnerMatrix(9,65)/(groebnerMatrix(9,63));
  groebnerMatrix(13,36) = -groebnerMatrix(9,66)/(groebnerMatrix(9,63));
  groebnerMatrix(13,37) = groebnerMatrix(7,65)/(groebnerMatrix(7,61));
  groebnerMatrix(13,38) = (groebnerMatrix(7,66)/(groebnerMatrix(7,61))-groebnerMatrix(9,67)/(groebnerMatrix(9,63)));
  groebnerMatrix(13,39) = groebnerMatrix(7,67)/(groebnerMatrix(7,61));
  groebnerMatrix(13,41) = -groebnerMatrix(9,68)/(groebnerMatrix(9,63));
  groebnerMatrix(13,42) = groebnerMatrix(7,68)/(groebnerMatrix(7,61));
  groebnerMatrix(13,45) = -groebnerMatrix(9,69)/(groebnerMatrix(9,63));
  groebnerMatrix(13,46) = -groebnerMatrix(9,70)/(groebnerMatrix(9,63));
  groebnerMatrix(13,47) = groebnerMatrix(7,69)/(groebnerMatrix(7,61));
  groebnerMatrix(13,48) = (groebnerMatrix(7,70)/(groebnerMatrix(7,61))-groebnerMatrix(9,71)/(groebnerMatrix(9,63)));
  groebnerMatrix(13,49) = groebnerMatrix(7,71)/(groebnerMatrix(7,61));
  groebnerMatrix(13,51) = -groebnerMatrix(9,72)/(groebnerMatrix(9,63));
  groebnerMatrix(13,52) = groebnerMatrix(7,72)/(groebnerMatrix(7,61));
  groebnerMatrix(13,55) = -groebnerMatrix(9,73)/(groebnerMatrix(9,63));
  groebnerMatrix(13,56) = groebnerMatrix(7,73)/(groebnerMatrix(7,61));
  groebnerMatrix(13,60) = -groebnerMatrix(9,74)/(groebnerMatrix(9,63));
  groebnerMatrix(13,61) = -groebnerMatrix(9,75)/(groebnerMatrix(9,63));
  groebnerMatrix(13,62) = groebnerMatrix(7,74)/(groebnerMatrix(7,61));
  groebnerMatrix(13,63) = (groebnerMatrix(7,75)/(groebnerMatrix(7,61))-groebnerMatrix(9,76)/(groebnerMatrix(9,63)));
  groebnerMatrix(13,64) = groebnerMatrix(7,76)/(groebnerMatrix(7,61));
  groebnerMatrix(13,66) = -groebnerMatrix(9,77)/(groebnerMatrix(9,63));
  groebnerMatrix(13,67) = groebnerMatrix(7,77)/(groebnerMatrix(7,61));
  groebnerMatrix(13,70) = -groebnerMatrix(9,78)/(groebnerMatrix(9,63));
  groebnerMatrix(13,71) = groebnerMatrix(7,78)/(groebnerMatrix(7,61));
  groebnerMatrix(13,75) = -groebnerMatrix(9,79)/(groebnerMatrix(9,63));
  groebnerMatrix(13,76) = groebnerMatrix(7,79)/(groebnerMatrix(7,61));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial14( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(14,30) = (groebnerMatrix(6,61)/(groebnerMatrix(6,60))-groebnerMatrix(8,63)/(groebnerMatrix(8,62)));
  groebnerMatrix(14,31) = groebnerMatrix(6,62)/(groebnerMatrix(6,60));
  groebnerMatrix(14,32) = (groebnerMatrix(6,63)/(groebnerMatrix(6,60))-groebnerMatrix(8,64)/(groebnerMatrix(8,62)));
  groebnerMatrix(14,33) = groebnerMatrix(6,64)/(groebnerMatrix(6,60));
  groebnerMatrix(14,35) = -groebnerMatrix(8,65)/(groebnerMatrix(8,62));
  groebnerMatrix(14,36) = -groebnerMatrix(8,66)/(groebnerMatrix(8,62));
  groebnerMatrix(14,37) = groebnerMatrix(6,65)/(groebnerMatrix(6,60));
  groebnerMatrix(14,38) = (groebnerMatrix(6,66)/(groebnerMatrix(6,60))-groebnerMatrix(8,67)/(groebnerMatrix(8,62)));
  groebnerMatrix(14,39) = groebnerMatrix(6,67)/(groebnerMatrix(6,60));
  groebnerMatrix(14,41) = -groebnerMatrix(8,68)/(groebnerMatrix(8,62));
  groebnerMatrix(14,42) = groebnerMatrix(6,68)/(groebnerMatrix(6,60));
  groebnerMatrix(14,45) = -groebnerMatrix(8,69)/(groebnerMatrix(8,62));
  groebnerMatrix(14,46) = -groebnerMatrix(8,70)/(groebnerMatrix(8,62));
  groebnerMatrix(14,47) = groebnerMatrix(6,69)/(groebnerMatrix(6,60));
  groebnerMatrix(14,48) = (groebnerMatrix(6,70)/(groebnerMatrix(6,60))-groebnerMatrix(8,71)/(groebnerMatrix(8,62)));
  groebnerMatrix(14,49) = groebnerMatrix(6,71)/(groebnerMatrix(6,60));
  groebnerMatrix(14,51) = -groebnerMatrix(8,72)/(groebnerMatrix(8,62));
  groebnerMatrix(14,52) = groebnerMatrix(6,72)/(groebnerMatrix(6,60));
  groebnerMatrix(14,55) = -groebnerMatrix(8,73)/(groebnerMatrix(8,62));
  groebnerMatrix(14,56) = groebnerMatrix(6,73)/(groebnerMatrix(6,60));
  groebnerMatrix(14,60) = -groebnerMatrix(8,74)/(groebnerMatrix(8,62));
  groebnerMatrix(14,61) = -groebnerMatrix(8,75)/(groebnerMatrix(8,62));
  groebnerMatrix(14,62) = groebnerMatrix(6,74)/(groebnerMatrix(6,60));
  groebnerMatrix(14,63) = (groebnerMatrix(6,75)/(groebnerMatrix(6,60))-groebnerMatrix(8,76)/(groebnerMatrix(8,62)));
  groebnerMatrix(14,64) = groebnerMatrix(6,76)/(groebnerMatrix(6,60));
  groebnerMatrix(14,66) = -groebnerMatrix(8,77)/(groebnerMatrix(8,62));
  groebnerMatrix(14,67) = groebnerMatrix(6,77)/(groebnerMatrix(6,60));
  groebnerMatrix(14,70) = -groebnerMatrix(8,78)/(groebnerMatrix(8,62));
  groebnerMatrix(14,71) = groebnerMatrix(6,78)/(groebnerMatrix(6,60));
  groebnerMatrix(14,75) = -groebnerMatrix(8,79)/(groebnerMatrix(8,62));
  groebnerMatrix(14,76) = groebnerMatrix(6,79)/(groebnerMatrix(6,60));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial15( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(15,30) = groebnerMatrix(6,61)/(groebnerMatrix(6,60));
  groebnerMatrix(15,31) = (groebnerMatrix(6,62)/(groebnerMatrix(6,60))-groebnerMatrix(9,64)/(groebnerMatrix(9,63)));
  groebnerMatrix(15,32) = groebnerMatrix(6,63)/(groebnerMatrix(6,60));
  groebnerMatrix(15,33) = groebnerMatrix(6,64)/(groebnerMatrix(6,60));
  groebnerMatrix(15,34) = -groebnerMatrix(9,65)/(groebnerMatrix(9,63));
  groebnerMatrix(15,35) = -groebnerMatrix(9,66)/(groebnerMatrix(9,63));
  groebnerMatrix(15,37) = (groebnerMatrix(6,65)/(groebnerMatrix(6,60))-groebnerMatrix(9,67)/(groebnerMatrix(9,63)));
  groebnerMatrix(15,38) = groebnerMatrix(6,66)/(groebnerMatrix(6,60));
  groebnerMatrix(15,39) = groebnerMatrix(6,67)/(groebnerMatrix(6,60));
  groebnerMatrix(15,40) = -groebnerMatrix(9,68)/(groebnerMatrix(9,63));
  groebnerMatrix(15,42) = groebnerMatrix(6,68)/(groebnerMatrix(6,60));
  groebnerMatrix(15,44) = -groebnerMatrix(9,69)/(groebnerMatrix(9,63));
  groebnerMatrix(15,45) = -groebnerMatrix(9,70)/(groebnerMatrix(9,63));
  groebnerMatrix(15,47) = (groebnerMatrix(6,69)/(groebnerMatrix(6,60))-groebnerMatrix(9,71)/(groebnerMatrix(9,63)));
  groebnerMatrix(15,48) = groebnerMatrix(6,70)/(groebnerMatrix(6,60));
  groebnerMatrix(15,49) = groebnerMatrix(6,71)/(groebnerMatrix(6,60));
  groebnerMatrix(15,50) = -groebnerMatrix(9,72)/(groebnerMatrix(9,63));
  groebnerMatrix(15,52) = groebnerMatrix(6,72)/(groebnerMatrix(6,60));
  groebnerMatrix(15,54) = -groebnerMatrix(9,73)/(groebnerMatrix(9,63));
  groebnerMatrix(15,56) = groebnerMatrix(6,73)/(groebnerMatrix(6,60));
  groebnerMatrix(15,59) = -groebnerMatrix(9,74)/(groebnerMatrix(9,63));
  groebnerMatrix(15,60) = -groebnerMatrix(9,75)/(groebnerMatrix(9,63));
  groebnerMatrix(15,62) = (groebnerMatrix(6,74)/(groebnerMatrix(6,60))-groebnerMatrix(9,76)/(groebnerMatrix(9,63)));
  groebnerMatrix(15,63) = groebnerMatrix(6,75)/(groebnerMatrix(6,60));
  groebnerMatrix(15,64) = groebnerMatrix(6,76)/(groebnerMatrix(6,60));
  groebnerMatrix(15,65) = -groebnerMatrix(9,77)/(groebnerMatrix(9,63));
  groebnerMatrix(15,67) = groebnerMatrix(6,77)/(groebnerMatrix(6,60));
  groebnerMatrix(15,69) = -groebnerMatrix(9,78)/(groebnerMatrix(9,63));
  groebnerMatrix(15,71) = groebnerMatrix(6,78)/(groebnerMatrix(6,60));
  groebnerMatrix(15,74) = -groebnerMatrix(9,79)/(groebnerMatrix(9,63));
  groebnerMatrix(15,76) = groebnerMatrix(6,79)/(groebnerMatrix(6,60));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial16( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(16,29) = (groebnerMatrix(5,60)/(groebnerMatrix(5,59))-groebnerMatrix(8,63)/(groebnerMatrix(8,62)));
  groebnerMatrix(16,30) = groebnerMatrix(5,61)/(groebnerMatrix(5,59));
  groebnerMatrix(16,31) = (groebnerMatrix(5,62)/(groebnerMatrix(5,59))-groebnerMatrix(8,64)/(groebnerMatrix(8,62)));
  groebnerMatrix(16,32) = groebnerMatrix(5,63)/(groebnerMatrix(5,59));
  groebnerMatrix(16,33) = groebnerMatrix(5,64)/(groebnerMatrix(5,59));
  groebnerMatrix(16,34) = -groebnerMatrix(8,65)/(groebnerMatrix(8,62));
  groebnerMatrix(16,35) = -groebnerMatrix(8,66)/(groebnerMatrix(8,62));
  groebnerMatrix(16,37) = (groebnerMatrix(5,65)/(groebnerMatrix(5,59))-groebnerMatrix(8,67)/(groebnerMatrix(8,62)));
  groebnerMatrix(16,38) = groebnerMatrix(5,66)/(groebnerMatrix(5,59));
  groebnerMatrix(16,39) = groebnerMatrix(5,67)/(groebnerMatrix(5,59));
  groebnerMatrix(16,40) = -groebnerMatrix(8,68)/(groebnerMatrix(8,62));
  groebnerMatrix(16,42) = groebnerMatrix(5,68)/(groebnerMatrix(5,59));
  groebnerMatrix(16,44) = -groebnerMatrix(8,69)/(groebnerMatrix(8,62));
  groebnerMatrix(16,45) = -groebnerMatrix(8,70)/(groebnerMatrix(8,62));
  groebnerMatrix(16,47) = (groebnerMatrix(5,69)/(groebnerMatrix(5,59))-groebnerMatrix(8,71)/(groebnerMatrix(8,62)));
  groebnerMatrix(16,48) = groebnerMatrix(5,70)/(groebnerMatrix(5,59));
  groebnerMatrix(16,49) = groebnerMatrix(5,71)/(groebnerMatrix(5,59));
  groebnerMatrix(16,50) = -groebnerMatrix(8,72)/(groebnerMatrix(8,62));
  groebnerMatrix(16,52) = groebnerMatrix(5,72)/(groebnerMatrix(5,59));
  groebnerMatrix(16,54) = -groebnerMatrix(8,73)/(groebnerMatrix(8,62));
  groebnerMatrix(16,56) = groebnerMatrix(5,73)/(groebnerMatrix(5,59));
  groebnerMatrix(16,59) = -groebnerMatrix(8,74)/(groebnerMatrix(8,62));
  groebnerMatrix(16,60) = -groebnerMatrix(8,75)/(groebnerMatrix(8,62));
  groebnerMatrix(16,62) = (groebnerMatrix(5,74)/(groebnerMatrix(5,59))-groebnerMatrix(8,76)/(groebnerMatrix(8,62)));
  groebnerMatrix(16,63) = groebnerMatrix(5,75)/(groebnerMatrix(5,59));
  groebnerMatrix(16,64) = groebnerMatrix(5,76)/(groebnerMatrix(5,59));
  groebnerMatrix(16,65) = -groebnerMatrix(8,77)/(groebnerMatrix(8,62));
  groebnerMatrix(16,67) = groebnerMatrix(5,77)/(groebnerMatrix(5,59));
  groebnerMatrix(16,69) = -groebnerMatrix(8,78)/(groebnerMatrix(8,62));
  groebnerMatrix(16,71) = groebnerMatrix(5,78)/(groebnerMatrix(5,59));
  groebnerMatrix(16,74) = -groebnerMatrix(8,79)/(groebnerMatrix(8,62));
  groebnerMatrix(16,76) = groebnerMatrix(5,79)/(groebnerMatrix(5,59));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial17( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(17,27) = groebnerMatrix(6,61)/(groebnerMatrix(6,60));
  groebnerMatrix(17,28) = -groebnerMatrix(7,62)/(groebnerMatrix(7,61));
  groebnerMatrix(17,29) = (groebnerMatrix(6,62)/(groebnerMatrix(6,60))-groebnerMatrix(7,63)/(groebnerMatrix(7,61)));
  groebnerMatrix(17,30) = groebnerMatrix(6,63)/(groebnerMatrix(6,60));
  groebnerMatrix(17,31) = -groebnerMatrix(7,64)/(groebnerMatrix(7,61));
  groebnerMatrix(17,32) = groebnerMatrix(6,64)/(groebnerMatrix(6,60));
  groebnerMatrix(17,34) = -groebnerMatrix(7,65)/(groebnerMatrix(7,61));
  groebnerMatrix(17,35) = (groebnerMatrix(6,65)/(groebnerMatrix(6,60))-groebnerMatrix(7,66)/(groebnerMatrix(7,61)));
  groebnerMatrix(17,36) = groebnerMatrix(6,66)/(groebnerMatrix(6,60));
  groebnerMatrix(17,37) = -groebnerMatrix(7,67)/(groebnerMatrix(7,61));
  groebnerMatrix(17,38) = groebnerMatrix(6,67)/(groebnerMatrix(6,60));
  groebnerMatrix(17,40) = -groebnerMatrix(7,68)/(groebnerMatrix(7,61));
  groebnerMatrix(17,41) = groebnerMatrix(6,68)/(groebnerMatrix(6,60));
  groebnerMatrix(17,44) = -groebnerMatrix(7,69)/(groebnerMatrix(7,61));
  groebnerMatrix(17,45) = (groebnerMatrix(6,69)/(groebnerMatrix(6,60))-groebnerMatrix(7,70)/(groebnerMatrix(7,61)));
  groebnerMatrix(17,46) = groebnerMatrix(6,70)/(groebnerMatrix(6,60));
  groebnerMatrix(17,47) = -groebnerMatrix(7,71)/(groebnerMatrix(7,61));
  groebnerMatrix(17,48) = groebnerMatrix(6,71)/(groebnerMatrix(6,60));
  groebnerMatrix(17,50) = -groebnerMatrix(7,72)/(groebnerMatrix(7,61));
  groebnerMatrix(17,51) = groebnerMatrix(6,72)/(groebnerMatrix(6,60));
  groebnerMatrix(17,54) = -groebnerMatrix(7,73)/(groebnerMatrix(7,61));
  groebnerMatrix(17,55) = groebnerMatrix(6,73)/(groebnerMatrix(6,60));
  groebnerMatrix(17,59) = -groebnerMatrix(7,74)/(groebnerMatrix(7,61));
  groebnerMatrix(17,60) = (groebnerMatrix(6,74)/(groebnerMatrix(6,60))-groebnerMatrix(7,75)/(groebnerMatrix(7,61)));
  groebnerMatrix(17,61) = groebnerMatrix(6,75)/(groebnerMatrix(6,60));
  groebnerMatrix(17,62) = -groebnerMatrix(7,76)/(groebnerMatrix(7,61));
  groebnerMatrix(17,63) = groebnerMatrix(6,76)/(groebnerMatrix(6,60));
  groebnerMatrix(17,65) = -groebnerMatrix(7,77)/(groebnerMatrix(7,61));
  groebnerMatrix(17,66) = groebnerMatrix(6,77)/(groebnerMatrix(6,60));
  groebnerMatrix(17,69) = -groebnerMatrix(7,78)/(groebnerMatrix(7,61));
  groebnerMatrix(17,70) = groebnerMatrix(6,78)/(groebnerMatrix(6,60));
  groebnerMatrix(17,74) = -groebnerMatrix(7,79)/(groebnerMatrix(7,61));
  groebnerMatrix(17,75) = groebnerMatrix(6,79)/(groebnerMatrix(6,60));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial18( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(18,26) = (groebnerMatrix(5,60)/(groebnerMatrix(5,59))-groebnerMatrix(6,61)/(groebnerMatrix(6,60)));
  groebnerMatrix(18,27) = groebnerMatrix(5,61)/(groebnerMatrix(5,59));
  groebnerMatrix(18,28) = -groebnerMatrix(6,62)/(groebnerMatrix(6,60));
  groebnerMatrix(18,29) = (groebnerMatrix(5,62)/(groebnerMatrix(5,59))-groebnerMatrix(6,63)/(groebnerMatrix(6,60)));
  groebnerMatrix(18,30) = groebnerMatrix(5,63)/(groebnerMatrix(5,59));
  groebnerMatrix(18,31) = -groebnerMatrix(6,64)/(groebnerMatrix(6,60));
  groebnerMatrix(18,32) = groebnerMatrix(5,64)/(groebnerMatrix(5,59));
  groebnerMatrix(18,34) = -groebnerMatrix(6,65)/(groebnerMatrix(6,60));
  groebnerMatrix(18,35) = (groebnerMatrix(5,65)/(groebnerMatrix(5,59))-groebnerMatrix(6,66)/(groebnerMatrix(6,60)));
  groebnerMatrix(18,36) = groebnerMatrix(5,66)/(groebnerMatrix(5,59));
  groebnerMatrix(18,37) = -groebnerMatrix(6,67)/(groebnerMatrix(6,60));
  groebnerMatrix(18,38) = groebnerMatrix(5,67)/(groebnerMatrix(5,59));
  groebnerMatrix(18,40) = -groebnerMatrix(6,68)/(groebnerMatrix(6,60));
  groebnerMatrix(18,41) = groebnerMatrix(5,68)/(groebnerMatrix(5,59));
  groebnerMatrix(18,44) = -groebnerMatrix(6,69)/(groebnerMatrix(6,60));
  groebnerMatrix(18,45) = (groebnerMatrix(5,69)/(groebnerMatrix(5,59))-groebnerMatrix(6,70)/(groebnerMatrix(6,60)));
  groebnerMatrix(18,46) = groebnerMatrix(5,70)/(groebnerMatrix(5,59));
  groebnerMatrix(18,47) = -groebnerMatrix(6,71)/(groebnerMatrix(6,60));
  groebnerMatrix(18,48) = groebnerMatrix(5,71)/(groebnerMatrix(5,59));
  groebnerMatrix(18,50) = -groebnerMatrix(6,72)/(groebnerMatrix(6,60));
  groebnerMatrix(18,51) = groebnerMatrix(5,72)/(groebnerMatrix(5,59));
  groebnerMatrix(18,54) = -groebnerMatrix(6,73)/(groebnerMatrix(6,60));
  groebnerMatrix(18,55) = groebnerMatrix(5,73)/(groebnerMatrix(5,59));
  groebnerMatrix(18,59) = -groebnerMatrix(6,74)/(groebnerMatrix(6,60));
  groebnerMatrix(18,60) = (groebnerMatrix(5,74)/(groebnerMatrix(5,59))-groebnerMatrix(6,75)/(groebnerMatrix(6,60)));
  groebnerMatrix(18,61) = groebnerMatrix(5,75)/(groebnerMatrix(5,59));
  groebnerMatrix(18,62) = -groebnerMatrix(6,76)/(groebnerMatrix(6,60));
  groebnerMatrix(18,63) = groebnerMatrix(5,76)/(groebnerMatrix(5,59));
  groebnerMatrix(18,65) = -groebnerMatrix(6,77)/(groebnerMatrix(6,60));
  groebnerMatrix(18,66) = groebnerMatrix(5,77)/(groebnerMatrix(5,59));
  groebnerMatrix(18,69) = -groebnerMatrix(6,78)/(groebnerMatrix(6,60));
  groebnerMatrix(18,70) = groebnerMatrix(5,78)/(groebnerMatrix(5,59));
  groebnerMatrix(18,74) = -groebnerMatrix(6,79)/(groebnerMatrix(6,60));
  groebnerMatrix(18,75) = groebnerMatrix(5,79)/(groebnerMatrix(5,59));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial19( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(19,17) = (groebnerMatrix(14,50)/(groebnerMatrix(14,43))-groebnerMatrix(18,54)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,18) = (groebnerMatrix(14,51)/(groebnerMatrix(14,43))-groebnerMatrix(18,55)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,19) = (groebnerMatrix(14,52)/(groebnerMatrix(14,43))-groebnerMatrix(18,56)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,20) = (groebnerMatrix(14,53)/(groebnerMatrix(14,43))-groebnerMatrix(18,57)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,21) = groebnerMatrix(14,54)/(groebnerMatrix(14,43));
  groebnerMatrix(19,22) = groebnerMatrix(14,55)/(groebnerMatrix(14,43));
  groebnerMatrix(19,23) = groebnerMatrix(14,56)/(groebnerMatrix(14,43));
  groebnerMatrix(19,24) = (groebnerMatrix(14,57)/(groebnerMatrix(14,43))-groebnerMatrix(18,58)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,25) = groebnerMatrix(14,58)/(groebnerMatrix(14,43));
  groebnerMatrix(19,40) = -groebnerMatrix(18,65)/(groebnerMatrix(18,53));
  groebnerMatrix(19,41) = -groebnerMatrix(18,66)/(groebnerMatrix(18,53));
  groebnerMatrix(19,42) = -groebnerMatrix(18,67)/(groebnerMatrix(18,53));
  groebnerMatrix(19,43) = -groebnerMatrix(18,68)/(groebnerMatrix(18,53));
  groebnerMatrix(19,50) = (groebnerMatrix(14,65)/(groebnerMatrix(14,43))-groebnerMatrix(18,69)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,51) = (groebnerMatrix(14,66)/(groebnerMatrix(14,43))-groebnerMatrix(18,70)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,52) = (groebnerMatrix(14,67)/(groebnerMatrix(14,43))-groebnerMatrix(18,71)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,53) = (groebnerMatrix(14,68)/(groebnerMatrix(14,43))-groebnerMatrix(18,72)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,54) = groebnerMatrix(14,69)/(groebnerMatrix(14,43));
  groebnerMatrix(19,55) = groebnerMatrix(14,70)/(groebnerMatrix(14,43));
  groebnerMatrix(19,56) = groebnerMatrix(14,71)/(groebnerMatrix(14,43));
  groebnerMatrix(19,57) = (groebnerMatrix(14,72)/(groebnerMatrix(14,43))-groebnerMatrix(18,73)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,58) = groebnerMatrix(14,73)/(groebnerMatrix(14,43));
  groebnerMatrix(19,65) = -groebnerMatrix(18,74)/(groebnerMatrix(18,53));
  groebnerMatrix(19,66) = -groebnerMatrix(18,75)/(groebnerMatrix(18,53));
  groebnerMatrix(19,67) = -groebnerMatrix(18,76)/(groebnerMatrix(18,53));
  groebnerMatrix(19,68) = -groebnerMatrix(18,77)/(groebnerMatrix(18,53));
  groebnerMatrix(19,69) = groebnerMatrix(14,74)/(groebnerMatrix(14,43));
  groebnerMatrix(19,70) = groebnerMatrix(14,75)/(groebnerMatrix(14,43));
  groebnerMatrix(19,71) = groebnerMatrix(14,76)/(groebnerMatrix(14,43));
  groebnerMatrix(19,72) = (groebnerMatrix(14,77)/(groebnerMatrix(14,43))-groebnerMatrix(18,78)/(groebnerMatrix(18,53)));
  groebnerMatrix(19,73) = groebnerMatrix(14,78)/(groebnerMatrix(14,43));
  groebnerMatrix(19,77) = -groebnerMatrix(18,79)/(groebnerMatrix(18,53));
  groebnerMatrix(19,78) = groebnerMatrix(14,79)/(groebnerMatrix(14,43));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial20( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(20,10) = (groebnerMatrix(13,43)/(groebnerMatrix(13,42))-groebnerMatrix(17,53)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,17) = (groebnerMatrix(13,50)/(groebnerMatrix(13,42))-groebnerMatrix(17,54)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,18) = (groebnerMatrix(13,51)/(groebnerMatrix(13,42))-groebnerMatrix(17,55)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,19) = (groebnerMatrix(13,52)/(groebnerMatrix(13,42))-groebnerMatrix(17,56)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,20) = (groebnerMatrix(13,53)/(groebnerMatrix(13,42))-groebnerMatrix(17,57)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,21) = groebnerMatrix(13,54)/(groebnerMatrix(13,42));
  groebnerMatrix(20,22) = groebnerMatrix(13,55)/(groebnerMatrix(13,42));
  groebnerMatrix(20,23) = groebnerMatrix(13,56)/(groebnerMatrix(13,42));
  groebnerMatrix(20,24) = (groebnerMatrix(13,57)/(groebnerMatrix(13,42))-groebnerMatrix(17,58)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,25) = groebnerMatrix(13,58)/(groebnerMatrix(13,42));
  groebnerMatrix(20,40) = -groebnerMatrix(17,65)/(groebnerMatrix(17,52));
  groebnerMatrix(20,41) = -groebnerMatrix(17,66)/(groebnerMatrix(17,52));
  groebnerMatrix(20,42) = -groebnerMatrix(17,67)/(groebnerMatrix(17,52));
  groebnerMatrix(20,43) = -groebnerMatrix(17,68)/(groebnerMatrix(17,52));
  groebnerMatrix(20,50) = (groebnerMatrix(13,65)/(groebnerMatrix(13,42))-groebnerMatrix(17,69)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,51) = (groebnerMatrix(13,66)/(groebnerMatrix(13,42))-groebnerMatrix(17,70)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,52) = (groebnerMatrix(13,67)/(groebnerMatrix(13,42))-groebnerMatrix(17,71)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,53) = (groebnerMatrix(13,68)/(groebnerMatrix(13,42))-groebnerMatrix(17,72)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,54) = groebnerMatrix(13,69)/(groebnerMatrix(13,42));
  groebnerMatrix(20,55) = groebnerMatrix(13,70)/(groebnerMatrix(13,42));
  groebnerMatrix(20,56) = groebnerMatrix(13,71)/(groebnerMatrix(13,42));
  groebnerMatrix(20,57) = (groebnerMatrix(13,72)/(groebnerMatrix(13,42))-groebnerMatrix(17,73)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,58) = groebnerMatrix(13,73)/(groebnerMatrix(13,42));
  groebnerMatrix(20,65) = -groebnerMatrix(17,74)/(groebnerMatrix(17,52));
  groebnerMatrix(20,66) = -groebnerMatrix(17,75)/(groebnerMatrix(17,52));
  groebnerMatrix(20,67) = -groebnerMatrix(17,76)/(groebnerMatrix(17,52));
  groebnerMatrix(20,68) = -groebnerMatrix(17,77)/(groebnerMatrix(17,52));
  groebnerMatrix(20,69) = groebnerMatrix(13,74)/(groebnerMatrix(13,42));
  groebnerMatrix(20,70) = groebnerMatrix(13,75)/(groebnerMatrix(13,42));
  groebnerMatrix(20,71) = groebnerMatrix(13,76)/(groebnerMatrix(13,42));
  groebnerMatrix(20,72) = (groebnerMatrix(13,77)/(groebnerMatrix(13,42))-groebnerMatrix(17,78)/(groebnerMatrix(17,52)));
  groebnerMatrix(20,73) = groebnerMatrix(13,78)/(groebnerMatrix(13,42));
  groebnerMatrix(20,77) = -groebnerMatrix(17,79)/(groebnerMatrix(17,52));
  groebnerMatrix(20,78) = groebnerMatrix(13,79)/(groebnerMatrix(13,42));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial21( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(21,10) = groebnerMatrix(13,43)/(groebnerMatrix(13,42));
  groebnerMatrix(21,14) = -groebnerMatrix(18,54)/(groebnerMatrix(18,53));
  groebnerMatrix(21,15) = -groebnerMatrix(18,55)/(groebnerMatrix(18,53));
  groebnerMatrix(21,16) = -groebnerMatrix(18,56)/(groebnerMatrix(18,53));
  groebnerMatrix(21,17) = groebnerMatrix(13,50)/(groebnerMatrix(13,42));
  groebnerMatrix(21,18) = groebnerMatrix(13,51)/(groebnerMatrix(13,42));
  groebnerMatrix(21,19) = (groebnerMatrix(13,52)/(groebnerMatrix(13,42))-groebnerMatrix(18,57)/(groebnerMatrix(18,53)));
  groebnerMatrix(21,20) = groebnerMatrix(13,53)/(groebnerMatrix(13,42));
  groebnerMatrix(21,21) = groebnerMatrix(13,54)/(groebnerMatrix(13,42));
  groebnerMatrix(21,22) = groebnerMatrix(13,55)/(groebnerMatrix(13,42));
  groebnerMatrix(21,23) = (groebnerMatrix(13,56)/(groebnerMatrix(13,42))-groebnerMatrix(18,58)/(groebnerMatrix(18,53)));
  groebnerMatrix(21,24) = groebnerMatrix(13,57)/(groebnerMatrix(13,42));
  groebnerMatrix(21,25) = groebnerMatrix(13,58)/(groebnerMatrix(13,42));
  groebnerMatrix(21,37) = -groebnerMatrix(18,65)/(groebnerMatrix(18,53));
  groebnerMatrix(21,38) = -groebnerMatrix(18,66)/(groebnerMatrix(18,53));
  groebnerMatrix(21,39) = -groebnerMatrix(18,67)/(groebnerMatrix(18,53));
  groebnerMatrix(21,42) = -groebnerMatrix(18,68)/(groebnerMatrix(18,53));
  groebnerMatrix(21,47) = -groebnerMatrix(18,69)/(groebnerMatrix(18,53));
  groebnerMatrix(21,48) = -groebnerMatrix(18,70)/(groebnerMatrix(18,53));
  groebnerMatrix(21,49) = -groebnerMatrix(18,71)/(groebnerMatrix(18,53));
  groebnerMatrix(21,50) = groebnerMatrix(13,65)/(groebnerMatrix(13,42));
  groebnerMatrix(21,51) = groebnerMatrix(13,66)/(groebnerMatrix(13,42));
  groebnerMatrix(21,52) = (groebnerMatrix(13,67)/(groebnerMatrix(13,42))-groebnerMatrix(18,72)/(groebnerMatrix(18,53)));
  groebnerMatrix(21,53) = groebnerMatrix(13,68)/(groebnerMatrix(13,42));
  groebnerMatrix(21,54) = groebnerMatrix(13,69)/(groebnerMatrix(13,42));
  groebnerMatrix(21,55) = groebnerMatrix(13,70)/(groebnerMatrix(13,42));
  groebnerMatrix(21,56) = (groebnerMatrix(13,71)/(groebnerMatrix(13,42))-groebnerMatrix(18,73)/(groebnerMatrix(18,53)));
  groebnerMatrix(21,57) = groebnerMatrix(13,72)/(groebnerMatrix(13,42));
  groebnerMatrix(21,58) = groebnerMatrix(13,73)/(groebnerMatrix(13,42));
  groebnerMatrix(21,62) = -groebnerMatrix(18,74)/(groebnerMatrix(18,53));
  groebnerMatrix(21,63) = -groebnerMatrix(18,75)/(groebnerMatrix(18,53));
  groebnerMatrix(21,64) = -groebnerMatrix(18,76)/(groebnerMatrix(18,53));
  groebnerMatrix(21,67) = -groebnerMatrix(18,77)/(groebnerMatrix(18,53));
  groebnerMatrix(21,69) = groebnerMatrix(13,74)/(groebnerMatrix(13,42));
  groebnerMatrix(21,70) = groebnerMatrix(13,75)/(groebnerMatrix(13,42));
  groebnerMatrix(21,71) = (groebnerMatrix(13,76)/(groebnerMatrix(13,42))-groebnerMatrix(18,78)/(groebnerMatrix(18,53)));
  groebnerMatrix(21,72) = groebnerMatrix(13,77)/(groebnerMatrix(13,42));
  groebnerMatrix(21,73) = groebnerMatrix(13,78)/(groebnerMatrix(13,42));
  groebnerMatrix(21,76) = -groebnerMatrix(18,79)/(groebnerMatrix(18,53));
  groebnerMatrix(21,78) = groebnerMatrix(13,79)/(groebnerMatrix(13,42));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial22( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(22,9) = (groebnerMatrix(12,42)/(groebnerMatrix(12,41))-groebnerMatrix(16,52)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,10) = (groebnerMatrix(12,43)/(groebnerMatrix(12,41))-groebnerMatrix(16,53)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,17) = (groebnerMatrix(12,50)/(groebnerMatrix(12,41))-groebnerMatrix(16,54)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,18) = (groebnerMatrix(12,51)/(groebnerMatrix(12,41))-groebnerMatrix(16,55)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,19) = (groebnerMatrix(12,52)/(groebnerMatrix(12,41))-groebnerMatrix(16,56)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,20) = (groebnerMatrix(12,53)/(groebnerMatrix(12,41))-groebnerMatrix(16,57)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,21) = groebnerMatrix(12,54)/(groebnerMatrix(12,41));
  groebnerMatrix(22,22) = groebnerMatrix(12,55)/(groebnerMatrix(12,41));
  groebnerMatrix(22,23) = groebnerMatrix(12,56)/(groebnerMatrix(12,41));
  groebnerMatrix(22,24) = (groebnerMatrix(12,57)/(groebnerMatrix(12,41))-groebnerMatrix(16,58)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,25) = groebnerMatrix(12,58)/(groebnerMatrix(12,41));
  groebnerMatrix(22,40) = -groebnerMatrix(16,65)/(groebnerMatrix(16,51));
  groebnerMatrix(22,41) = -groebnerMatrix(16,66)/(groebnerMatrix(16,51));
  groebnerMatrix(22,42) = -groebnerMatrix(16,67)/(groebnerMatrix(16,51));
  groebnerMatrix(22,43) = -groebnerMatrix(16,68)/(groebnerMatrix(16,51));
  groebnerMatrix(22,50) = (groebnerMatrix(12,65)/(groebnerMatrix(12,41))-groebnerMatrix(16,69)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,51) = (groebnerMatrix(12,66)/(groebnerMatrix(12,41))-groebnerMatrix(16,70)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,52) = (groebnerMatrix(12,67)/(groebnerMatrix(12,41))-groebnerMatrix(16,71)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,53) = (groebnerMatrix(12,68)/(groebnerMatrix(12,41))-groebnerMatrix(16,72)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,54) = groebnerMatrix(12,69)/(groebnerMatrix(12,41));
  groebnerMatrix(22,55) = groebnerMatrix(12,70)/(groebnerMatrix(12,41));
  groebnerMatrix(22,56) = groebnerMatrix(12,71)/(groebnerMatrix(12,41));
  groebnerMatrix(22,57) = (groebnerMatrix(12,72)/(groebnerMatrix(12,41))-groebnerMatrix(16,73)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,58) = groebnerMatrix(12,73)/(groebnerMatrix(12,41));
  groebnerMatrix(22,65) = -groebnerMatrix(16,74)/(groebnerMatrix(16,51));
  groebnerMatrix(22,66) = -groebnerMatrix(16,75)/(groebnerMatrix(16,51));
  groebnerMatrix(22,67) = -groebnerMatrix(16,76)/(groebnerMatrix(16,51));
  groebnerMatrix(22,68) = -groebnerMatrix(16,77)/(groebnerMatrix(16,51));
  groebnerMatrix(22,69) = groebnerMatrix(12,74)/(groebnerMatrix(12,41));
  groebnerMatrix(22,70) = groebnerMatrix(12,75)/(groebnerMatrix(12,41));
  groebnerMatrix(22,71) = groebnerMatrix(12,76)/(groebnerMatrix(12,41));
  groebnerMatrix(22,72) = (groebnerMatrix(12,77)/(groebnerMatrix(12,41))-groebnerMatrix(16,78)/(groebnerMatrix(16,51)));
  groebnerMatrix(22,73) = groebnerMatrix(12,78)/(groebnerMatrix(12,41));
  groebnerMatrix(22,77) = -groebnerMatrix(16,79)/(groebnerMatrix(16,51));
  groebnerMatrix(22,78) = groebnerMatrix(12,79)/(groebnerMatrix(12,41));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial23( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(23,9) = groebnerMatrix(12,42)/(groebnerMatrix(12,41));
  groebnerMatrix(23,10) = groebnerMatrix(12,43)/(groebnerMatrix(12,41));
  groebnerMatrix(23,12) = -groebnerMatrix(18,54)/(groebnerMatrix(18,53));
  groebnerMatrix(23,13) = -groebnerMatrix(18,55)/(groebnerMatrix(18,53));
  groebnerMatrix(23,15) = -groebnerMatrix(18,56)/(groebnerMatrix(18,53));
  groebnerMatrix(23,17) = groebnerMatrix(12,50)/(groebnerMatrix(12,41));
  groebnerMatrix(23,18) = (groebnerMatrix(12,51)/(groebnerMatrix(12,41))-groebnerMatrix(18,57)/(groebnerMatrix(18,53)));
  groebnerMatrix(23,19) = groebnerMatrix(12,52)/(groebnerMatrix(12,41));
  groebnerMatrix(23,20) = groebnerMatrix(12,53)/(groebnerMatrix(12,41));
  groebnerMatrix(23,21) = groebnerMatrix(12,54)/(groebnerMatrix(12,41));
  groebnerMatrix(23,22) = (groebnerMatrix(12,55)/(groebnerMatrix(12,41))-groebnerMatrix(18,58)/(groebnerMatrix(18,53)));
  groebnerMatrix(23,23) = groebnerMatrix(12,56)/(groebnerMatrix(12,41));
  groebnerMatrix(23,24) = groebnerMatrix(12,57)/(groebnerMatrix(12,41));
  groebnerMatrix(23,25) = groebnerMatrix(12,58)/(groebnerMatrix(12,41));
  groebnerMatrix(23,35) = -groebnerMatrix(18,65)/(groebnerMatrix(18,53));
  groebnerMatrix(23,36) = -groebnerMatrix(18,66)/(groebnerMatrix(18,53));
  groebnerMatrix(23,38) = -groebnerMatrix(18,67)/(groebnerMatrix(18,53));
  groebnerMatrix(23,41) = -groebnerMatrix(18,68)/(groebnerMatrix(18,53));
  groebnerMatrix(23,45) = -groebnerMatrix(18,69)/(groebnerMatrix(18,53));
  groebnerMatrix(23,46) = -groebnerMatrix(18,70)/(groebnerMatrix(18,53));
  groebnerMatrix(23,48) = -groebnerMatrix(18,71)/(groebnerMatrix(18,53));
  groebnerMatrix(23,50) = groebnerMatrix(12,65)/(groebnerMatrix(12,41));
  groebnerMatrix(23,51) = (groebnerMatrix(12,66)/(groebnerMatrix(12,41))-groebnerMatrix(18,72)/(groebnerMatrix(18,53)));
  groebnerMatrix(23,52) = groebnerMatrix(12,67)/(groebnerMatrix(12,41));
  groebnerMatrix(23,53) = groebnerMatrix(12,68)/(groebnerMatrix(12,41));
  groebnerMatrix(23,54) = groebnerMatrix(12,69)/(groebnerMatrix(12,41));
  groebnerMatrix(23,55) = (groebnerMatrix(12,70)/(groebnerMatrix(12,41))-groebnerMatrix(18,73)/(groebnerMatrix(18,53)));
  groebnerMatrix(23,56) = groebnerMatrix(12,71)/(groebnerMatrix(12,41));
  groebnerMatrix(23,57) = groebnerMatrix(12,72)/(groebnerMatrix(12,41));
  groebnerMatrix(23,58) = groebnerMatrix(12,73)/(groebnerMatrix(12,41));
  groebnerMatrix(23,60) = -groebnerMatrix(18,74)/(groebnerMatrix(18,53));
  groebnerMatrix(23,61) = -groebnerMatrix(18,75)/(groebnerMatrix(18,53));
  groebnerMatrix(23,63) = -groebnerMatrix(18,76)/(groebnerMatrix(18,53));
  groebnerMatrix(23,66) = -groebnerMatrix(18,77)/(groebnerMatrix(18,53));
  groebnerMatrix(23,69) = groebnerMatrix(12,74)/(groebnerMatrix(12,41));
  groebnerMatrix(23,70) = (groebnerMatrix(12,75)/(groebnerMatrix(12,41))-groebnerMatrix(18,78)/(groebnerMatrix(18,53)));
  groebnerMatrix(23,71) = groebnerMatrix(12,76)/(groebnerMatrix(12,41));
  groebnerMatrix(23,72) = groebnerMatrix(12,77)/(groebnerMatrix(12,41));
  groebnerMatrix(23,73) = groebnerMatrix(12,78)/(groebnerMatrix(12,41));
  groebnerMatrix(23,75) = -groebnerMatrix(18,79)/(groebnerMatrix(18,53));
  groebnerMatrix(23,78) = groebnerMatrix(12,79)/(groebnerMatrix(12,41));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial24( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(24,8) = (groebnerMatrix(11,41)/(groebnerMatrix(11,40))-groebnerMatrix(15,51)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,9) = (groebnerMatrix(11,42)/(groebnerMatrix(11,40))-groebnerMatrix(15,52)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,10) = (groebnerMatrix(11,43)/(groebnerMatrix(11,40))-groebnerMatrix(15,53)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,17) = (groebnerMatrix(11,50)/(groebnerMatrix(11,40))-groebnerMatrix(15,54)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,18) = (groebnerMatrix(11,51)/(groebnerMatrix(11,40))-groebnerMatrix(15,55)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,19) = (groebnerMatrix(11,52)/(groebnerMatrix(11,40))-groebnerMatrix(15,56)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,20) = (groebnerMatrix(11,53)/(groebnerMatrix(11,40))-groebnerMatrix(15,57)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,21) = groebnerMatrix(11,54)/(groebnerMatrix(11,40));
  groebnerMatrix(24,22) = groebnerMatrix(11,55)/(groebnerMatrix(11,40));
  groebnerMatrix(24,23) = groebnerMatrix(11,56)/(groebnerMatrix(11,40));
  groebnerMatrix(24,24) = (groebnerMatrix(11,57)/(groebnerMatrix(11,40))-groebnerMatrix(15,58)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,25) = groebnerMatrix(11,58)/(groebnerMatrix(11,40));
  groebnerMatrix(24,40) = -groebnerMatrix(15,65)/(groebnerMatrix(15,50));
  groebnerMatrix(24,41) = -groebnerMatrix(15,66)/(groebnerMatrix(15,50));
  groebnerMatrix(24,42) = -groebnerMatrix(15,67)/(groebnerMatrix(15,50));
  groebnerMatrix(24,43) = -groebnerMatrix(15,68)/(groebnerMatrix(15,50));
  groebnerMatrix(24,50) = (groebnerMatrix(11,65)/(groebnerMatrix(11,40))-groebnerMatrix(15,69)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,51) = (groebnerMatrix(11,66)/(groebnerMatrix(11,40))-groebnerMatrix(15,70)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,52) = (groebnerMatrix(11,67)/(groebnerMatrix(11,40))-groebnerMatrix(15,71)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,53) = (groebnerMatrix(11,68)/(groebnerMatrix(11,40))-groebnerMatrix(15,72)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,54) = groebnerMatrix(11,69)/(groebnerMatrix(11,40));
  groebnerMatrix(24,55) = groebnerMatrix(11,70)/(groebnerMatrix(11,40));
  groebnerMatrix(24,56) = groebnerMatrix(11,71)/(groebnerMatrix(11,40));
  groebnerMatrix(24,57) = (groebnerMatrix(11,72)/(groebnerMatrix(11,40))-groebnerMatrix(15,73)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,58) = groebnerMatrix(11,73)/(groebnerMatrix(11,40));
  groebnerMatrix(24,65) = -groebnerMatrix(15,74)/(groebnerMatrix(15,50));
  groebnerMatrix(24,66) = -groebnerMatrix(15,75)/(groebnerMatrix(15,50));
  groebnerMatrix(24,67) = -groebnerMatrix(15,76)/(groebnerMatrix(15,50));
  groebnerMatrix(24,68) = -groebnerMatrix(15,77)/(groebnerMatrix(15,50));
  groebnerMatrix(24,69) = groebnerMatrix(11,74)/(groebnerMatrix(11,40));
  groebnerMatrix(24,70) = groebnerMatrix(11,75)/(groebnerMatrix(11,40));
  groebnerMatrix(24,71) = groebnerMatrix(11,76)/(groebnerMatrix(11,40));
  groebnerMatrix(24,72) = (groebnerMatrix(11,77)/(groebnerMatrix(11,40))-groebnerMatrix(15,78)/(groebnerMatrix(15,50)));
  groebnerMatrix(24,73) = groebnerMatrix(11,78)/(groebnerMatrix(11,40));
  groebnerMatrix(24,77) = -groebnerMatrix(15,79)/(groebnerMatrix(15,50));
  groebnerMatrix(24,78) = groebnerMatrix(11,79)/(groebnerMatrix(11,40));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial25( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(25,8) = groebnerMatrix(11,41)/(groebnerMatrix(11,40));
  groebnerMatrix(25,9) = groebnerMatrix(11,42)/(groebnerMatrix(11,40));
  groebnerMatrix(25,10) = groebnerMatrix(11,43)/(groebnerMatrix(11,40));
  groebnerMatrix(25,11) = -groebnerMatrix(18,54)/(groebnerMatrix(18,53));
  groebnerMatrix(25,12) = -groebnerMatrix(18,55)/(groebnerMatrix(18,53));
  groebnerMatrix(25,14) = -groebnerMatrix(18,56)/(groebnerMatrix(18,53));
  groebnerMatrix(25,17) = (groebnerMatrix(11,50)/(groebnerMatrix(11,40))-groebnerMatrix(18,57)/(groebnerMatrix(18,53)));
  groebnerMatrix(25,18) = groebnerMatrix(11,51)/(groebnerMatrix(11,40));
  groebnerMatrix(25,19) = groebnerMatrix(11,52)/(groebnerMatrix(11,40));
  groebnerMatrix(25,20) = groebnerMatrix(11,53)/(groebnerMatrix(11,40));
  groebnerMatrix(25,21) = (groebnerMatrix(11,54)/(groebnerMatrix(11,40))-groebnerMatrix(18,58)/(groebnerMatrix(18,53)));
  groebnerMatrix(25,22) = groebnerMatrix(11,55)/(groebnerMatrix(11,40));
  groebnerMatrix(25,23) = groebnerMatrix(11,56)/(groebnerMatrix(11,40));
  groebnerMatrix(25,24) = groebnerMatrix(11,57)/(groebnerMatrix(11,40));
  groebnerMatrix(25,25) = groebnerMatrix(11,58)/(groebnerMatrix(11,40));
  groebnerMatrix(25,34) = -groebnerMatrix(18,65)/(groebnerMatrix(18,53));
  groebnerMatrix(25,35) = -groebnerMatrix(18,66)/(groebnerMatrix(18,53));
  groebnerMatrix(25,37) = -groebnerMatrix(18,67)/(groebnerMatrix(18,53));
  groebnerMatrix(25,40) = -groebnerMatrix(18,68)/(groebnerMatrix(18,53));
  groebnerMatrix(25,44) = -groebnerMatrix(18,69)/(groebnerMatrix(18,53));
  groebnerMatrix(25,45) = -groebnerMatrix(18,70)/(groebnerMatrix(18,53));
  groebnerMatrix(25,47) = -groebnerMatrix(18,71)/(groebnerMatrix(18,53));
  groebnerMatrix(25,50) = (groebnerMatrix(11,65)/(groebnerMatrix(11,40))-groebnerMatrix(18,72)/(groebnerMatrix(18,53)));
  groebnerMatrix(25,51) = groebnerMatrix(11,66)/(groebnerMatrix(11,40));
  groebnerMatrix(25,52) = groebnerMatrix(11,67)/(groebnerMatrix(11,40));
  groebnerMatrix(25,53) = groebnerMatrix(11,68)/(groebnerMatrix(11,40));
  groebnerMatrix(25,54) = (groebnerMatrix(11,69)/(groebnerMatrix(11,40))-groebnerMatrix(18,73)/(groebnerMatrix(18,53)));
  groebnerMatrix(25,55) = groebnerMatrix(11,70)/(groebnerMatrix(11,40));
  groebnerMatrix(25,56) = groebnerMatrix(11,71)/(groebnerMatrix(11,40));
  groebnerMatrix(25,57) = groebnerMatrix(11,72)/(groebnerMatrix(11,40));
  groebnerMatrix(25,58) = groebnerMatrix(11,73)/(groebnerMatrix(11,40));
  groebnerMatrix(25,59) = -groebnerMatrix(18,74)/(groebnerMatrix(18,53));
  groebnerMatrix(25,60) = -groebnerMatrix(18,75)/(groebnerMatrix(18,53));
  groebnerMatrix(25,62) = -groebnerMatrix(18,76)/(groebnerMatrix(18,53));
  groebnerMatrix(25,65) = -groebnerMatrix(18,77)/(groebnerMatrix(18,53));
  groebnerMatrix(25,69) = (groebnerMatrix(11,74)/(groebnerMatrix(11,40))-groebnerMatrix(18,78)/(groebnerMatrix(18,53)));
  groebnerMatrix(25,70) = groebnerMatrix(11,75)/(groebnerMatrix(11,40));
  groebnerMatrix(25,71) = groebnerMatrix(11,76)/(groebnerMatrix(11,40));
  groebnerMatrix(25,72) = groebnerMatrix(11,77)/(groebnerMatrix(11,40));
  groebnerMatrix(25,73) = groebnerMatrix(11,78)/(groebnerMatrix(11,40));
  groebnerMatrix(25,74) = -groebnerMatrix(18,79)/(groebnerMatrix(18,53));
  groebnerMatrix(25,78) = groebnerMatrix(11,79)/(groebnerMatrix(11,40));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial26( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(26,7) = groebnerMatrix(10,65)/(groebnerMatrix(10,64));
  groebnerMatrix(26,8) = groebnerMatrix(10,66)/(groebnerMatrix(10,64));
  groebnerMatrix(26,9) = (groebnerMatrix(10,67)/(groebnerMatrix(10,64))-groebnerMatrix(17,53)/(groebnerMatrix(17,52)));
  groebnerMatrix(26,10) = groebnerMatrix(10,68)/(groebnerMatrix(10,64));
  groebnerMatrix(26,14) = -groebnerMatrix(17,54)/(groebnerMatrix(17,52));
  groebnerMatrix(26,15) = -groebnerMatrix(17,55)/(groebnerMatrix(17,52));
  groebnerMatrix(26,16) = -groebnerMatrix(17,56)/(groebnerMatrix(17,52));
  groebnerMatrix(26,17) = groebnerMatrix(10,69)/(groebnerMatrix(10,64));
  groebnerMatrix(26,18) = groebnerMatrix(10,70)/(groebnerMatrix(10,64));
  groebnerMatrix(26,19) = (groebnerMatrix(10,71)/(groebnerMatrix(10,64))-groebnerMatrix(17,57)/(groebnerMatrix(17,52)));
  groebnerMatrix(26,20) = groebnerMatrix(10,72)/(groebnerMatrix(10,64));
  groebnerMatrix(26,23) = -groebnerMatrix(17,58)/(groebnerMatrix(17,52));
  groebnerMatrix(26,24) = groebnerMatrix(10,73)/(groebnerMatrix(10,64));
  groebnerMatrix(26,37) = -groebnerMatrix(17,65)/(groebnerMatrix(17,52));
  groebnerMatrix(26,38) = -groebnerMatrix(17,66)/(groebnerMatrix(17,52));
  groebnerMatrix(26,39) = -groebnerMatrix(17,67)/(groebnerMatrix(17,52));
  groebnerMatrix(26,42) = -groebnerMatrix(17,68)/(groebnerMatrix(17,52));
  groebnerMatrix(26,47) = -groebnerMatrix(17,69)/(groebnerMatrix(17,52));
  groebnerMatrix(26,48) = -groebnerMatrix(17,70)/(groebnerMatrix(17,52));
  groebnerMatrix(26,49) = -groebnerMatrix(17,71)/(groebnerMatrix(17,52));
  groebnerMatrix(26,50) = groebnerMatrix(10,74)/(groebnerMatrix(10,64));
  groebnerMatrix(26,51) = groebnerMatrix(10,75)/(groebnerMatrix(10,64));
  groebnerMatrix(26,52) = (groebnerMatrix(10,76)/(groebnerMatrix(10,64))-groebnerMatrix(17,72)/(groebnerMatrix(17,52)));
  groebnerMatrix(26,53) = groebnerMatrix(10,77)/(groebnerMatrix(10,64));
  groebnerMatrix(26,56) = -groebnerMatrix(17,73)/(groebnerMatrix(17,52));
  groebnerMatrix(26,57) = groebnerMatrix(10,78)/(groebnerMatrix(10,64));
  groebnerMatrix(26,62) = -groebnerMatrix(17,74)/(groebnerMatrix(17,52));
  groebnerMatrix(26,63) = -groebnerMatrix(17,75)/(groebnerMatrix(17,52));
  groebnerMatrix(26,64) = -groebnerMatrix(17,76)/(groebnerMatrix(17,52));
  groebnerMatrix(26,67) = -groebnerMatrix(17,77)/(groebnerMatrix(17,52));
  groebnerMatrix(26,71) = -groebnerMatrix(17,78)/(groebnerMatrix(17,52));
  groebnerMatrix(26,72) = groebnerMatrix(10,79)/(groebnerMatrix(10,64));
  groebnerMatrix(26,76) = -groebnerMatrix(17,79)/(groebnerMatrix(17,52));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial27( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(27,6) = (groebnerMatrix(9,64)/(groebnerMatrix(9,63))-groebnerMatrix(16,52)/(groebnerMatrix(16,51)));
  groebnerMatrix(27,7) = groebnerMatrix(9,65)/(groebnerMatrix(9,63));
  groebnerMatrix(27,8) = groebnerMatrix(9,66)/(groebnerMatrix(9,63));
  groebnerMatrix(27,9) = (groebnerMatrix(9,67)/(groebnerMatrix(9,63))-groebnerMatrix(16,53)/(groebnerMatrix(16,51)));
  groebnerMatrix(27,10) = groebnerMatrix(9,68)/(groebnerMatrix(9,63));
  groebnerMatrix(27,14) = -groebnerMatrix(16,54)/(groebnerMatrix(16,51));
  groebnerMatrix(27,15) = -groebnerMatrix(16,55)/(groebnerMatrix(16,51));
  groebnerMatrix(27,16) = -groebnerMatrix(16,56)/(groebnerMatrix(16,51));
  groebnerMatrix(27,17) = groebnerMatrix(9,69)/(groebnerMatrix(9,63));
  groebnerMatrix(27,18) = groebnerMatrix(9,70)/(groebnerMatrix(9,63));
  groebnerMatrix(27,19) = (groebnerMatrix(9,71)/(groebnerMatrix(9,63))-groebnerMatrix(16,57)/(groebnerMatrix(16,51)));
  groebnerMatrix(27,20) = groebnerMatrix(9,72)/(groebnerMatrix(9,63));
  groebnerMatrix(27,23) = -groebnerMatrix(16,58)/(groebnerMatrix(16,51));
  groebnerMatrix(27,24) = groebnerMatrix(9,73)/(groebnerMatrix(9,63));
  groebnerMatrix(27,37) = -groebnerMatrix(16,65)/(groebnerMatrix(16,51));
  groebnerMatrix(27,38) = -groebnerMatrix(16,66)/(groebnerMatrix(16,51));
  groebnerMatrix(27,39) = -groebnerMatrix(16,67)/(groebnerMatrix(16,51));
  groebnerMatrix(27,42) = -groebnerMatrix(16,68)/(groebnerMatrix(16,51));
  groebnerMatrix(27,47) = -groebnerMatrix(16,69)/(groebnerMatrix(16,51));
  groebnerMatrix(27,48) = -groebnerMatrix(16,70)/(groebnerMatrix(16,51));
  groebnerMatrix(27,49) = -groebnerMatrix(16,71)/(groebnerMatrix(16,51));
  groebnerMatrix(27,50) = groebnerMatrix(9,74)/(groebnerMatrix(9,63));
  groebnerMatrix(27,51) = groebnerMatrix(9,75)/(groebnerMatrix(9,63));
  groebnerMatrix(27,52) = (groebnerMatrix(9,76)/(groebnerMatrix(9,63))-groebnerMatrix(16,72)/(groebnerMatrix(16,51)));
  groebnerMatrix(27,53) = groebnerMatrix(9,77)/(groebnerMatrix(9,63));
  groebnerMatrix(27,56) = -groebnerMatrix(16,73)/(groebnerMatrix(16,51));
  groebnerMatrix(27,57) = groebnerMatrix(9,78)/(groebnerMatrix(9,63));
  groebnerMatrix(27,62) = -groebnerMatrix(16,74)/(groebnerMatrix(16,51));
  groebnerMatrix(27,63) = -groebnerMatrix(16,75)/(groebnerMatrix(16,51));
  groebnerMatrix(27,64) = -groebnerMatrix(16,76)/(groebnerMatrix(16,51));
  groebnerMatrix(27,67) = -groebnerMatrix(16,77)/(groebnerMatrix(16,51));
  groebnerMatrix(27,71) = -groebnerMatrix(16,78)/(groebnerMatrix(16,51));
  groebnerMatrix(27,72) = groebnerMatrix(9,79)/(groebnerMatrix(9,63));
  groebnerMatrix(27,76) = -groebnerMatrix(16,79)/(groebnerMatrix(16,51));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial28( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(28,6) = groebnerMatrix(9,64)/(groebnerMatrix(9,63));
  groebnerMatrix(28,7) = groebnerMatrix(9,65)/(groebnerMatrix(9,63));
  groebnerMatrix(28,8) = (groebnerMatrix(9,66)/(groebnerMatrix(9,63))-groebnerMatrix(17,53)/(groebnerMatrix(17,52)));
  groebnerMatrix(28,9) = groebnerMatrix(9,67)/(groebnerMatrix(9,63));
  groebnerMatrix(28,10) = groebnerMatrix(9,68)/(groebnerMatrix(9,63));
  groebnerMatrix(28,12) = -groebnerMatrix(17,54)/(groebnerMatrix(17,52));
  groebnerMatrix(28,13) = -groebnerMatrix(17,55)/(groebnerMatrix(17,52));
  groebnerMatrix(28,15) = -groebnerMatrix(17,56)/(groebnerMatrix(17,52));
  groebnerMatrix(28,17) = groebnerMatrix(9,69)/(groebnerMatrix(9,63));
  groebnerMatrix(28,18) = (groebnerMatrix(9,70)/(groebnerMatrix(9,63))-groebnerMatrix(17,57)/(groebnerMatrix(17,52)));
  groebnerMatrix(28,19) = groebnerMatrix(9,71)/(groebnerMatrix(9,63));
  groebnerMatrix(28,20) = groebnerMatrix(9,72)/(groebnerMatrix(9,63));
  groebnerMatrix(28,22) = -groebnerMatrix(17,58)/(groebnerMatrix(17,52));
  groebnerMatrix(28,24) = groebnerMatrix(9,73)/(groebnerMatrix(9,63));
  groebnerMatrix(28,35) = -groebnerMatrix(17,65)/(groebnerMatrix(17,52));
  groebnerMatrix(28,36) = -groebnerMatrix(17,66)/(groebnerMatrix(17,52));
  groebnerMatrix(28,38) = -groebnerMatrix(17,67)/(groebnerMatrix(17,52));
  groebnerMatrix(28,41) = -groebnerMatrix(17,68)/(groebnerMatrix(17,52));
  groebnerMatrix(28,45) = -groebnerMatrix(17,69)/(groebnerMatrix(17,52));
  groebnerMatrix(28,46) = -groebnerMatrix(17,70)/(groebnerMatrix(17,52));
  groebnerMatrix(28,48) = -groebnerMatrix(17,71)/(groebnerMatrix(17,52));
  groebnerMatrix(28,50) = groebnerMatrix(9,74)/(groebnerMatrix(9,63));
  groebnerMatrix(28,51) = (groebnerMatrix(9,75)/(groebnerMatrix(9,63))-groebnerMatrix(17,72)/(groebnerMatrix(17,52)));
  groebnerMatrix(28,52) = groebnerMatrix(9,76)/(groebnerMatrix(9,63));
  groebnerMatrix(28,53) = groebnerMatrix(9,77)/(groebnerMatrix(9,63));
  groebnerMatrix(28,55) = -groebnerMatrix(17,73)/(groebnerMatrix(17,52));
  groebnerMatrix(28,57) = groebnerMatrix(9,78)/(groebnerMatrix(9,63));
  groebnerMatrix(28,60) = -groebnerMatrix(17,74)/(groebnerMatrix(17,52));
  groebnerMatrix(28,61) = -groebnerMatrix(17,75)/(groebnerMatrix(17,52));
  groebnerMatrix(28,63) = -groebnerMatrix(17,76)/(groebnerMatrix(17,52));
  groebnerMatrix(28,66) = -groebnerMatrix(17,77)/(groebnerMatrix(17,52));
  groebnerMatrix(28,70) = -groebnerMatrix(17,78)/(groebnerMatrix(17,52));
  groebnerMatrix(28,72) = groebnerMatrix(9,79)/(groebnerMatrix(9,63));
  groebnerMatrix(28,75) = -groebnerMatrix(17,79)/(groebnerMatrix(17,52));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial29( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(29,5) = (groebnerMatrix(8,63)/(groebnerMatrix(8,62))-groebnerMatrix(15,51)/(groebnerMatrix(15,50)));
  groebnerMatrix(29,6) = (groebnerMatrix(8,64)/(groebnerMatrix(8,62))-groebnerMatrix(15,52)/(groebnerMatrix(15,50)));
  groebnerMatrix(29,7) = groebnerMatrix(8,65)/(groebnerMatrix(8,62));
  groebnerMatrix(29,8) = groebnerMatrix(8,66)/(groebnerMatrix(8,62));
  groebnerMatrix(29,9) = (groebnerMatrix(8,67)/(groebnerMatrix(8,62))-groebnerMatrix(15,53)/(groebnerMatrix(15,50)));
  groebnerMatrix(29,10) = groebnerMatrix(8,68)/(groebnerMatrix(8,62));
  groebnerMatrix(29,14) = -groebnerMatrix(15,54)/(groebnerMatrix(15,50));
  groebnerMatrix(29,15) = -groebnerMatrix(15,55)/(groebnerMatrix(15,50));
  groebnerMatrix(29,16) = -groebnerMatrix(15,56)/(groebnerMatrix(15,50));
  groebnerMatrix(29,17) = groebnerMatrix(8,69)/(groebnerMatrix(8,62));
  groebnerMatrix(29,18) = groebnerMatrix(8,70)/(groebnerMatrix(8,62));
  groebnerMatrix(29,19) = (groebnerMatrix(8,71)/(groebnerMatrix(8,62))-groebnerMatrix(15,57)/(groebnerMatrix(15,50)));
  groebnerMatrix(29,20) = groebnerMatrix(8,72)/(groebnerMatrix(8,62));
  groebnerMatrix(29,23) = -groebnerMatrix(15,58)/(groebnerMatrix(15,50));
  groebnerMatrix(29,24) = groebnerMatrix(8,73)/(groebnerMatrix(8,62));
  groebnerMatrix(29,37) = -groebnerMatrix(15,65)/(groebnerMatrix(15,50));
  groebnerMatrix(29,38) = -groebnerMatrix(15,66)/(groebnerMatrix(15,50));
  groebnerMatrix(29,39) = -groebnerMatrix(15,67)/(groebnerMatrix(15,50));
  groebnerMatrix(29,42) = -groebnerMatrix(15,68)/(groebnerMatrix(15,50));
  groebnerMatrix(29,47) = -groebnerMatrix(15,69)/(groebnerMatrix(15,50));
  groebnerMatrix(29,48) = -groebnerMatrix(15,70)/(groebnerMatrix(15,50));
  groebnerMatrix(29,49) = -groebnerMatrix(15,71)/(groebnerMatrix(15,50));
  groebnerMatrix(29,50) = groebnerMatrix(8,74)/(groebnerMatrix(8,62));
  groebnerMatrix(29,51) = groebnerMatrix(8,75)/(groebnerMatrix(8,62));
  groebnerMatrix(29,52) = (groebnerMatrix(8,76)/(groebnerMatrix(8,62))-groebnerMatrix(15,72)/(groebnerMatrix(15,50)));
  groebnerMatrix(29,53) = groebnerMatrix(8,77)/(groebnerMatrix(8,62));
  groebnerMatrix(29,56) = -groebnerMatrix(15,73)/(groebnerMatrix(15,50));
  groebnerMatrix(29,57) = groebnerMatrix(8,78)/(groebnerMatrix(8,62));
  groebnerMatrix(29,62) = -groebnerMatrix(15,74)/(groebnerMatrix(15,50));
  groebnerMatrix(29,63) = -groebnerMatrix(15,75)/(groebnerMatrix(15,50));
  groebnerMatrix(29,64) = -groebnerMatrix(15,76)/(groebnerMatrix(15,50));
  groebnerMatrix(29,67) = -groebnerMatrix(15,77)/(groebnerMatrix(15,50));
  groebnerMatrix(29,71) = -groebnerMatrix(15,78)/(groebnerMatrix(15,50));
  groebnerMatrix(29,72) = groebnerMatrix(8,79)/(groebnerMatrix(8,62));
  groebnerMatrix(29,76) = -groebnerMatrix(15,79)/(groebnerMatrix(15,50));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial30( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(30,5) = groebnerMatrix(8,63)/(groebnerMatrix(8,62));
  groebnerMatrix(30,6) = groebnerMatrix(8,64)/(groebnerMatrix(8,62));
  groebnerMatrix(30,7) = (groebnerMatrix(8,65)/(groebnerMatrix(8,62))-groebnerMatrix(17,53)/(groebnerMatrix(17,52)));
  groebnerMatrix(30,8) = groebnerMatrix(8,66)/(groebnerMatrix(8,62));
  groebnerMatrix(30,9) = groebnerMatrix(8,67)/(groebnerMatrix(8,62));
  groebnerMatrix(30,10) = groebnerMatrix(8,68)/(groebnerMatrix(8,62));
  groebnerMatrix(30,11) = -groebnerMatrix(17,54)/(groebnerMatrix(17,52));
  groebnerMatrix(30,12) = -groebnerMatrix(17,55)/(groebnerMatrix(17,52));
  groebnerMatrix(30,14) = -groebnerMatrix(17,56)/(groebnerMatrix(17,52));
  groebnerMatrix(30,17) = (groebnerMatrix(8,69)/(groebnerMatrix(8,62))-groebnerMatrix(17,57)/(groebnerMatrix(17,52)));
  groebnerMatrix(30,18) = groebnerMatrix(8,70)/(groebnerMatrix(8,62));
  groebnerMatrix(30,19) = groebnerMatrix(8,71)/(groebnerMatrix(8,62));
  groebnerMatrix(30,20) = groebnerMatrix(8,72)/(groebnerMatrix(8,62));
  groebnerMatrix(30,21) = -groebnerMatrix(17,58)/(groebnerMatrix(17,52));
  groebnerMatrix(30,24) = groebnerMatrix(8,73)/(groebnerMatrix(8,62));
  groebnerMatrix(30,34) = -groebnerMatrix(17,65)/(groebnerMatrix(17,52));
  groebnerMatrix(30,35) = -groebnerMatrix(17,66)/(groebnerMatrix(17,52));
  groebnerMatrix(30,37) = -groebnerMatrix(17,67)/(groebnerMatrix(17,52));
  groebnerMatrix(30,40) = -groebnerMatrix(17,68)/(groebnerMatrix(17,52));
  groebnerMatrix(30,44) = -groebnerMatrix(17,69)/(groebnerMatrix(17,52));
  groebnerMatrix(30,45) = -groebnerMatrix(17,70)/(groebnerMatrix(17,52));
  groebnerMatrix(30,47) = -groebnerMatrix(17,71)/(groebnerMatrix(17,52));
  groebnerMatrix(30,50) = (groebnerMatrix(8,74)/(groebnerMatrix(8,62))-groebnerMatrix(17,72)/(groebnerMatrix(17,52)));
  groebnerMatrix(30,51) = groebnerMatrix(8,75)/(groebnerMatrix(8,62));
  groebnerMatrix(30,52) = groebnerMatrix(8,76)/(groebnerMatrix(8,62));
  groebnerMatrix(30,53) = groebnerMatrix(8,77)/(groebnerMatrix(8,62));
  groebnerMatrix(30,54) = -groebnerMatrix(17,73)/(groebnerMatrix(17,52));
  groebnerMatrix(30,57) = groebnerMatrix(8,78)/(groebnerMatrix(8,62));
  groebnerMatrix(30,59) = -groebnerMatrix(17,74)/(groebnerMatrix(17,52));
  groebnerMatrix(30,60) = -groebnerMatrix(17,75)/(groebnerMatrix(17,52));
  groebnerMatrix(30,62) = -groebnerMatrix(17,76)/(groebnerMatrix(17,52));
  groebnerMatrix(30,65) = -groebnerMatrix(17,77)/(groebnerMatrix(17,52));
  groebnerMatrix(30,69) = -groebnerMatrix(17,78)/(groebnerMatrix(17,52));
  groebnerMatrix(30,72) = groebnerMatrix(8,79)/(groebnerMatrix(8,62));
  groebnerMatrix(30,74) = -groebnerMatrix(17,79)/(groebnerMatrix(17,52));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial31( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(31,1) = groebnerMatrix(13,43)/(groebnerMatrix(13,42));
  groebnerMatrix(31,4) = -groebnerMatrix(14,50)/(groebnerMatrix(14,43));
  groebnerMatrix(31,5) = -groebnerMatrix(14,51)/(groebnerMatrix(14,43));
  groebnerMatrix(31,6) = -groebnerMatrix(14,52)/(groebnerMatrix(14,43));
  groebnerMatrix(31,7) = groebnerMatrix(13,50)/(groebnerMatrix(13,42));
  groebnerMatrix(31,8) = groebnerMatrix(13,51)/(groebnerMatrix(13,42));
  groebnerMatrix(31,9) = (groebnerMatrix(13,52)/(groebnerMatrix(13,42))-groebnerMatrix(14,53)/(groebnerMatrix(14,43)));
  groebnerMatrix(31,10) = groebnerMatrix(13,53)/(groebnerMatrix(13,42));
  groebnerMatrix(31,14) = -groebnerMatrix(14,54)/(groebnerMatrix(14,43));
  groebnerMatrix(31,15) = -groebnerMatrix(14,55)/(groebnerMatrix(14,43));
  groebnerMatrix(31,16) = -groebnerMatrix(14,56)/(groebnerMatrix(14,43));
  groebnerMatrix(31,17) = groebnerMatrix(13,54)/(groebnerMatrix(13,42));
  groebnerMatrix(31,18) = groebnerMatrix(13,55)/(groebnerMatrix(13,42));
  groebnerMatrix(31,19) = (groebnerMatrix(13,56)/(groebnerMatrix(13,42))-groebnerMatrix(14,57)/(groebnerMatrix(14,43)));
  groebnerMatrix(31,20) = groebnerMatrix(13,57)/(groebnerMatrix(13,42));
  groebnerMatrix(31,23) = -groebnerMatrix(14,58)/(groebnerMatrix(14,43));
  groebnerMatrix(31,24) = groebnerMatrix(13,58)/(groebnerMatrix(13,42));
  groebnerMatrix(31,37) = -groebnerMatrix(14,65)/(groebnerMatrix(14,43));
  groebnerMatrix(31,38) = -groebnerMatrix(14,66)/(groebnerMatrix(14,43));
  groebnerMatrix(31,39) = -groebnerMatrix(14,67)/(groebnerMatrix(14,43));
  groebnerMatrix(31,40) = groebnerMatrix(13,65)/(groebnerMatrix(13,42));
  groebnerMatrix(31,41) = groebnerMatrix(13,66)/(groebnerMatrix(13,42));
  groebnerMatrix(31,42) = (groebnerMatrix(13,67)/(groebnerMatrix(13,42))-groebnerMatrix(14,68)/(groebnerMatrix(14,43)));
  groebnerMatrix(31,43) = groebnerMatrix(13,68)/(groebnerMatrix(13,42));
  groebnerMatrix(31,47) = -groebnerMatrix(14,69)/(groebnerMatrix(14,43));
  groebnerMatrix(31,48) = -groebnerMatrix(14,70)/(groebnerMatrix(14,43));
  groebnerMatrix(31,49) = -groebnerMatrix(14,71)/(groebnerMatrix(14,43));
  groebnerMatrix(31,50) = groebnerMatrix(13,69)/(groebnerMatrix(13,42));
  groebnerMatrix(31,51) = groebnerMatrix(13,70)/(groebnerMatrix(13,42));
  groebnerMatrix(31,52) = (groebnerMatrix(13,71)/(groebnerMatrix(13,42))-groebnerMatrix(14,72)/(groebnerMatrix(14,43)));
  groebnerMatrix(31,53) = groebnerMatrix(13,72)/(groebnerMatrix(13,42));
  groebnerMatrix(31,56) = -groebnerMatrix(14,73)/(groebnerMatrix(14,43));
  groebnerMatrix(31,57) = groebnerMatrix(13,73)/(groebnerMatrix(13,42));
  groebnerMatrix(31,62) = -groebnerMatrix(14,74)/(groebnerMatrix(14,43));
  groebnerMatrix(31,63) = -groebnerMatrix(14,75)/(groebnerMatrix(14,43));
  groebnerMatrix(31,64) = -groebnerMatrix(14,76)/(groebnerMatrix(14,43));
  groebnerMatrix(31,65) = groebnerMatrix(13,74)/(groebnerMatrix(13,42));
  groebnerMatrix(31,66) = groebnerMatrix(13,75)/(groebnerMatrix(13,42));
  groebnerMatrix(31,67) = (groebnerMatrix(13,76)/(groebnerMatrix(13,42))-groebnerMatrix(14,77)/(groebnerMatrix(14,43)));
  groebnerMatrix(31,68) = groebnerMatrix(13,77)/(groebnerMatrix(13,42));
  groebnerMatrix(31,71) = -groebnerMatrix(14,78)/(groebnerMatrix(14,43));
  groebnerMatrix(31,72) = groebnerMatrix(13,78)/(groebnerMatrix(13,42));
  groebnerMatrix(31,76) = -groebnerMatrix(14,79)/(groebnerMatrix(14,43));
  groebnerMatrix(31,77) = groebnerMatrix(13,79)/(groebnerMatrix(13,42));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial32( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(32,0) = groebnerMatrix(12,42)/(groebnerMatrix(12,41));
  groebnerMatrix(32,1) = groebnerMatrix(12,43)/(groebnerMatrix(12,41));
  groebnerMatrix(32,2) = -groebnerMatrix(14,50)/(groebnerMatrix(14,43));
  groebnerMatrix(32,3) = -groebnerMatrix(14,51)/(groebnerMatrix(14,43));
  groebnerMatrix(32,5) = -groebnerMatrix(14,52)/(groebnerMatrix(14,43));
  groebnerMatrix(32,7) = groebnerMatrix(12,50)/(groebnerMatrix(12,41));
  groebnerMatrix(32,8) = (groebnerMatrix(12,51)/(groebnerMatrix(12,41))-groebnerMatrix(14,53)/(groebnerMatrix(14,43)));
  groebnerMatrix(32,9) = groebnerMatrix(12,52)/(groebnerMatrix(12,41));
  groebnerMatrix(32,10) = groebnerMatrix(12,53)/(groebnerMatrix(12,41));
  groebnerMatrix(32,12) = -groebnerMatrix(14,54)/(groebnerMatrix(14,43));
  groebnerMatrix(32,13) = -groebnerMatrix(14,55)/(groebnerMatrix(14,43));
  groebnerMatrix(32,15) = -groebnerMatrix(14,56)/(groebnerMatrix(14,43));
  groebnerMatrix(32,17) = groebnerMatrix(12,54)/(groebnerMatrix(12,41));
  groebnerMatrix(32,18) = (groebnerMatrix(12,55)/(groebnerMatrix(12,41))-groebnerMatrix(14,57)/(groebnerMatrix(14,43)));
  groebnerMatrix(32,19) = groebnerMatrix(12,56)/(groebnerMatrix(12,41));
  groebnerMatrix(32,20) = groebnerMatrix(12,57)/(groebnerMatrix(12,41));
  groebnerMatrix(32,22) = -groebnerMatrix(14,58)/(groebnerMatrix(14,43));
  groebnerMatrix(32,24) = groebnerMatrix(12,58)/(groebnerMatrix(12,41));
  groebnerMatrix(32,35) = -groebnerMatrix(14,65)/(groebnerMatrix(14,43));
  groebnerMatrix(32,36) = -groebnerMatrix(14,66)/(groebnerMatrix(14,43));
  groebnerMatrix(32,38) = -groebnerMatrix(14,67)/(groebnerMatrix(14,43));
  groebnerMatrix(32,40) = groebnerMatrix(12,65)/(groebnerMatrix(12,41));
  groebnerMatrix(32,41) = (groebnerMatrix(12,66)/(groebnerMatrix(12,41))-groebnerMatrix(14,68)/(groebnerMatrix(14,43)));
  groebnerMatrix(32,42) = groebnerMatrix(12,67)/(groebnerMatrix(12,41));
  groebnerMatrix(32,43) = groebnerMatrix(12,68)/(groebnerMatrix(12,41));
  groebnerMatrix(32,45) = -groebnerMatrix(14,69)/(groebnerMatrix(14,43));
  groebnerMatrix(32,46) = -groebnerMatrix(14,70)/(groebnerMatrix(14,43));
  groebnerMatrix(32,48) = -groebnerMatrix(14,71)/(groebnerMatrix(14,43));
  groebnerMatrix(32,50) = groebnerMatrix(12,69)/(groebnerMatrix(12,41));
  groebnerMatrix(32,51) = (groebnerMatrix(12,70)/(groebnerMatrix(12,41))-groebnerMatrix(14,72)/(groebnerMatrix(14,43)));
  groebnerMatrix(32,52) = groebnerMatrix(12,71)/(groebnerMatrix(12,41));
  groebnerMatrix(32,53) = groebnerMatrix(12,72)/(groebnerMatrix(12,41));
  groebnerMatrix(32,55) = -groebnerMatrix(14,73)/(groebnerMatrix(14,43));
  groebnerMatrix(32,57) = groebnerMatrix(12,73)/(groebnerMatrix(12,41));
  groebnerMatrix(32,60) = -groebnerMatrix(14,74)/(groebnerMatrix(14,43));
  groebnerMatrix(32,61) = -groebnerMatrix(14,75)/(groebnerMatrix(14,43));
  groebnerMatrix(32,63) = -groebnerMatrix(14,76)/(groebnerMatrix(14,43));
  groebnerMatrix(32,65) = groebnerMatrix(12,74)/(groebnerMatrix(12,41));
  groebnerMatrix(32,66) = (groebnerMatrix(12,75)/(groebnerMatrix(12,41))-groebnerMatrix(14,77)/(groebnerMatrix(14,43)));
  groebnerMatrix(32,67) = groebnerMatrix(12,76)/(groebnerMatrix(12,41));
  groebnerMatrix(32,68) = groebnerMatrix(12,77)/(groebnerMatrix(12,41));
  groebnerMatrix(32,70) = -groebnerMatrix(14,78)/(groebnerMatrix(14,43));
  groebnerMatrix(32,72) = groebnerMatrix(12,78)/(groebnerMatrix(12,41));
  groebnerMatrix(32,75) = -groebnerMatrix(14,79)/(groebnerMatrix(14,43));
  groebnerMatrix(32,77) = groebnerMatrix(12,79)/(groebnerMatrix(12,41));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial33( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(33,54) = (groebnerMatrix(18,54)/(groebnerMatrix(18,53))-groebnerMatrix(32,69)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,55) = (groebnerMatrix(18,55)/(groebnerMatrix(18,53))-groebnerMatrix(32,70)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,56) = (groebnerMatrix(18,56)/(groebnerMatrix(18,53))-groebnerMatrix(32,71)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,57) = (groebnerMatrix(18,57)/(groebnerMatrix(18,53))-groebnerMatrix(32,72)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,58) = (groebnerMatrix(18,58)/(groebnerMatrix(18,53))-groebnerMatrix(32,73)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,65) = groebnerMatrix(18,65)/(groebnerMatrix(18,53));
  groebnerMatrix(33,66) = groebnerMatrix(18,66)/(groebnerMatrix(18,53));
  groebnerMatrix(33,67) = groebnerMatrix(18,67)/(groebnerMatrix(18,53));
  groebnerMatrix(33,68) = groebnerMatrix(18,68)/(groebnerMatrix(18,53));
  groebnerMatrix(33,69) = (groebnerMatrix(18,69)/(groebnerMatrix(18,53))-groebnerMatrix(32,74)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,70) = (groebnerMatrix(18,70)/(groebnerMatrix(18,53))-groebnerMatrix(32,75)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,71) = (groebnerMatrix(18,71)/(groebnerMatrix(18,53))-groebnerMatrix(32,76)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,72) = (groebnerMatrix(18,72)/(groebnerMatrix(18,53))-groebnerMatrix(32,77)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,73) = (groebnerMatrix(18,73)/(groebnerMatrix(18,53))-groebnerMatrix(32,78)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,74) = groebnerMatrix(18,74)/(groebnerMatrix(18,53));
  groebnerMatrix(33,75) = groebnerMatrix(18,75)/(groebnerMatrix(18,53));
  groebnerMatrix(33,76) = groebnerMatrix(18,76)/(groebnerMatrix(18,53));
  groebnerMatrix(33,77) = groebnerMatrix(18,77)/(groebnerMatrix(18,53));
  groebnerMatrix(33,78) = (groebnerMatrix(18,78)/(groebnerMatrix(18,53))-groebnerMatrix(32,79)/(groebnerMatrix(32,68)));
  groebnerMatrix(33,79) = groebnerMatrix(18,79)/(groebnerMatrix(18,53));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial34( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(34,53) = (groebnerMatrix(17,53)/(groebnerMatrix(17,52))-groebnerMatrix(31,68)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,54) = (groebnerMatrix(17,54)/(groebnerMatrix(17,52))-groebnerMatrix(31,69)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,55) = (groebnerMatrix(17,55)/(groebnerMatrix(17,52))-groebnerMatrix(31,70)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,56) = (groebnerMatrix(17,56)/(groebnerMatrix(17,52))-groebnerMatrix(31,71)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,57) = (groebnerMatrix(17,57)/(groebnerMatrix(17,52))-groebnerMatrix(31,72)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,58) = (groebnerMatrix(17,58)/(groebnerMatrix(17,52))-groebnerMatrix(31,73)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,65) = groebnerMatrix(17,65)/(groebnerMatrix(17,52));
  groebnerMatrix(34,66) = groebnerMatrix(17,66)/(groebnerMatrix(17,52));
  groebnerMatrix(34,67) = groebnerMatrix(17,67)/(groebnerMatrix(17,52));
  groebnerMatrix(34,68) = groebnerMatrix(17,68)/(groebnerMatrix(17,52));
  groebnerMatrix(34,69) = (groebnerMatrix(17,69)/(groebnerMatrix(17,52))-groebnerMatrix(31,74)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,70) = (groebnerMatrix(17,70)/(groebnerMatrix(17,52))-groebnerMatrix(31,75)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,71) = (groebnerMatrix(17,71)/(groebnerMatrix(17,52))-groebnerMatrix(31,76)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,72) = (groebnerMatrix(17,72)/(groebnerMatrix(17,52))-groebnerMatrix(31,77)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,73) = (groebnerMatrix(17,73)/(groebnerMatrix(17,52))-groebnerMatrix(31,78)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,74) = groebnerMatrix(17,74)/(groebnerMatrix(17,52));
  groebnerMatrix(34,75) = groebnerMatrix(17,75)/(groebnerMatrix(17,52));
  groebnerMatrix(34,76) = groebnerMatrix(17,76)/(groebnerMatrix(17,52));
  groebnerMatrix(34,77) = groebnerMatrix(17,77)/(groebnerMatrix(17,52));
  groebnerMatrix(34,78) = (groebnerMatrix(17,78)/(groebnerMatrix(17,52))-groebnerMatrix(31,79)/(groebnerMatrix(31,67)));
  groebnerMatrix(34,79) = groebnerMatrix(17,79)/(groebnerMatrix(17,52));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial35( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(35,52) = (groebnerMatrix(16,52)/(groebnerMatrix(16,51))-groebnerMatrix(30,67)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,53) = (groebnerMatrix(16,53)/(groebnerMatrix(16,51))-groebnerMatrix(30,68)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,54) = (groebnerMatrix(16,54)/(groebnerMatrix(16,51))-groebnerMatrix(30,69)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,55) = (groebnerMatrix(16,55)/(groebnerMatrix(16,51))-groebnerMatrix(30,70)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,56) = (groebnerMatrix(16,56)/(groebnerMatrix(16,51))-groebnerMatrix(30,71)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,57) = (groebnerMatrix(16,57)/(groebnerMatrix(16,51))-groebnerMatrix(30,72)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,58) = (groebnerMatrix(16,58)/(groebnerMatrix(16,51))-groebnerMatrix(30,73)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,65) = groebnerMatrix(16,65)/(groebnerMatrix(16,51));
  groebnerMatrix(35,66) = groebnerMatrix(16,66)/(groebnerMatrix(16,51));
  groebnerMatrix(35,67) = groebnerMatrix(16,67)/(groebnerMatrix(16,51));
  groebnerMatrix(35,68) = groebnerMatrix(16,68)/(groebnerMatrix(16,51));
  groebnerMatrix(35,69) = (groebnerMatrix(16,69)/(groebnerMatrix(16,51))-groebnerMatrix(30,74)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,70) = (groebnerMatrix(16,70)/(groebnerMatrix(16,51))-groebnerMatrix(30,75)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,71) = (groebnerMatrix(16,71)/(groebnerMatrix(16,51))-groebnerMatrix(30,76)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,72) = (groebnerMatrix(16,72)/(groebnerMatrix(16,51))-groebnerMatrix(30,77)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,73) = (groebnerMatrix(16,73)/(groebnerMatrix(16,51))-groebnerMatrix(30,78)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,74) = groebnerMatrix(16,74)/(groebnerMatrix(16,51));
  groebnerMatrix(35,75) = groebnerMatrix(16,75)/(groebnerMatrix(16,51));
  groebnerMatrix(35,76) = groebnerMatrix(16,76)/(groebnerMatrix(16,51));
  groebnerMatrix(35,77) = groebnerMatrix(16,77)/(groebnerMatrix(16,51));
  groebnerMatrix(35,78) = (groebnerMatrix(16,78)/(groebnerMatrix(16,51))-groebnerMatrix(30,79)/(groebnerMatrix(30,66)));
  groebnerMatrix(35,79) = groebnerMatrix(16,79)/(groebnerMatrix(16,51));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial36( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(36,51) = (groebnerMatrix(15,51)/(groebnerMatrix(15,50))-groebnerMatrix(29,66)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,52) = (groebnerMatrix(15,52)/(groebnerMatrix(15,50))-groebnerMatrix(29,67)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,53) = (groebnerMatrix(15,53)/(groebnerMatrix(15,50))-groebnerMatrix(29,68)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,54) = (groebnerMatrix(15,54)/(groebnerMatrix(15,50))-groebnerMatrix(29,69)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,55) = (groebnerMatrix(15,55)/(groebnerMatrix(15,50))-groebnerMatrix(29,70)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,56) = (groebnerMatrix(15,56)/(groebnerMatrix(15,50))-groebnerMatrix(29,71)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,57) = (groebnerMatrix(15,57)/(groebnerMatrix(15,50))-groebnerMatrix(29,72)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,58) = (groebnerMatrix(15,58)/(groebnerMatrix(15,50))-groebnerMatrix(29,73)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,65) = groebnerMatrix(15,65)/(groebnerMatrix(15,50));
  groebnerMatrix(36,66) = groebnerMatrix(15,66)/(groebnerMatrix(15,50));
  groebnerMatrix(36,67) = groebnerMatrix(15,67)/(groebnerMatrix(15,50));
  groebnerMatrix(36,68) = groebnerMatrix(15,68)/(groebnerMatrix(15,50));
  groebnerMatrix(36,69) = (groebnerMatrix(15,69)/(groebnerMatrix(15,50))-groebnerMatrix(29,74)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,70) = (groebnerMatrix(15,70)/(groebnerMatrix(15,50))-groebnerMatrix(29,75)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,71) = (groebnerMatrix(15,71)/(groebnerMatrix(15,50))-groebnerMatrix(29,76)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,72) = (groebnerMatrix(15,72)/(groebnerMatrix(15,50))-groebnerMatrix(29,77)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,73) = (groebnerMatrix(15,73)/(groebnerMatrix(15,50))-groebnerMatrix(29,78)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,74) = groebnerMatrix(15,74)/(groebnerMatrix(15,50));
  groebnerMatrix(36,75) = groebnerMatrix(15,75)/(groebnerMatrix(15,50));
  groebnerMatrix(36,76) = groebnerMatrix(15,76)/(groebnerMatrix(15,50));
  groebnerMatrix(36,77) = groebnerMatrix(15,77)/(groebnerMatrix(15,50));
  groebnerMatrix(36,78) = (groebnerMatrix(15,78)/(groebnerMatrix(15,50))-groebnerMatrix(29,79)/(groebnerMatrix(29,65)));
  groebnerMatrix(36,79) = groebnerMatrix(15,79)/(groebnerMatrix(15,50));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial37( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(37,50) = (groebnerMatrix(14,50)/(groebnerMatrix(14,43))-groebnerMatrix(32,69)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,51) = (groebnerMatrix(14,51)/(groebnerMatrix(14,43))-groebnerMatrix(32,70)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,52) = (groebnerMatrix(14,52)/(groebnerMatrix(14,43))-groebnerMatrix(32,71)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,53) = (groebnerMatrix(14,53)/(groebnerMatrix(14,43))-groebnerMatrix(32,72)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,54) = groebnerMatrix(14,54)/(groebnerMatrix(14,43));
  groebnerMatrix(37,55) = groebnerMatrix(14,55)/(groebnerMatrix(14,43));
  groebnerMatrix(37,56) = groebnerMatrix(14,56)/(groebnerMatrix(14,43));
  groebnerMatrix(37,57) = (groebnerMatrix(14,57)/(groebnerMatrix(14,43))-groebnerMatrix(32,73)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,58) = groebnerMatrix(14,58)/(groebnerMatrix(14,43));
  groebnerMatrix(37,65) = (groebnerMatrix(14,65)/(groebnerMatrix(14,43))-groebnerMatrix(32,74)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,66) = (groebnerMatrix(14,66)/(groebnerMatrix(14,43))-groebnerMatrix(32,75)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,67) = (groebnerMatrix(14,67)/(groebnerMatrix(14,43))-groebnerMatrix(32,76)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,68) = (groebnerMatrix(14,68)/(groebnerMatrix(14,43))-groebnerMatrix(32,77)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,69) = groebnerMatrix(14,69)/(groebnerMatrix(14,43));
  groebnerMatrix(37,70) = groebnerMatrix(14,70)/(groebnerMatrix(14,43));
  groebnerMatrix(37,71) = groebnerMatrix(14,71)/(groebnerMatrix(14,43));
  groebnerMatrix(37,72) = (groebnerMatrix(14,72)/(groebnerMatrix(14,43))-groebnerMatrix(32,78)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,73) = groebnerMatrix(14,73)/(groebnerMatrix(14,43));
  groebnerMatrix(37,74) = groebnerMatrix(14,74)/(groebnerMatrix(14,43));
  groebnerMatrix(37,75) = groebnerMatrix(14,75)/(groebnerMatrix(14,43));
  groebnerMatrix(37,76) = groebnerMatrix(14,76)/(groebnerMatrix(14,43));
  groebnerMatrix(37,77) = (groebnerMatrix(14,77)/(groebnerMatrix(14,43))-groebnerMatrix(32,79)/(groebnerMatrix(32,68)));
  groebnerMatrix(37,78) = groebnerMatrix(14,78)/(groebnerMatrix(14,43));
  groebnerMatrix(37,79) = groebnerMatrix(14,79)/(groebnerMatrix(14,43));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial38( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(38,43) = (groebnerMatrix(13,43)/(groebnerMatrix(13,42))-groebnerMatrix(31,68)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,50) = (groebnerMatrix(13,50)/(groebnerMatrix(13,42))-groebnerMatrix(31,69)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,51) = (groebnerMatrix(13,51)/(groebnerMatrix(13,42))-groebnerMatrix(31,70)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,52) = (groebnerMatrix(13,52)/(groebnerMatrix(13,42))-groebnerMatrix(31,71)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,53) = (groebnerMatrix(13,53)/(groebnerMatrix(13,42))-groebnerMatrix(31,72)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,54) = groebnerMatrix(13,54)/(groebnerMatrix(13,42));
  groebnerMatrix(38,55) = groebnerMatrix(13,55)/(groebnerMatrix(13,42));
  groebnerMatrix(38,56) = groebnerMatrix(13,56)/(groebnerMatrix(13,42));
  groebnerMatrix(38,57) = (groebnerMatrix(13,57)/(groebnerMatrix(13,42))-groebnerMatrix(31,73)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,58) = groebnerMatrix(13,58)/(groebnerMatrix(13,42));
  groebnerMatrix(38,65) = (groebnerMatrix(13,65)/(groebnerMatrix(13,42))-groebnerMatrix(31,74)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,66) = (groebnerMatrix(13,66)/(groebnerMatrix(13,42))-groebnerMatrix(31,75)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,67) = (groebnerMatrix(13,67)/(groebnerMatrix(13,42))-groebnerMatrix(31,76)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,68) = (groebnerMatrix(13,68)/(groebnerMatrix(13,42))-groebnerMatrix(31,77)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,69) = groebnerMatrix(13,69)/(groebnerMatrix(13,42));
  groebnerMatrix(38,70) = groebnerMatrix(13,70)/(groebnerMatrix(13,42));
  groebnerMatrix(38,71) = groebnerMatrix(13,71)/(groebnerMatrix(13,42));
  groebnerMatrix(38,72) = (groebnerMatrix(13,72)/(groebnerMatrix(13,42))-groebnerMatrix(31,78)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,73) = groebnerMatrix(13,73)/(groebnerMatrix(13,42));
  groebnerMatrix(38,74) = groebnerMatrix(13,74)/(groebnerMatrix(13,42));
  groebnerMatrix(38,75) = groebnerMatrix(13,75)/(groebnerMatrix(13,42));
  groebnerMatrix(38,76) = groebnerMatrix(13,76)/(groebnerMatrix(13,42));
  groebnerMatrix(38,77) = (groebnerMatrix(13,77)/(groebnerMatrix(13,42))-groebnerMatrix(31,79)/(groebnerMatrix(31,67)));
  groebnerMatrix(38,78) = groebnerMatrix(13,78)/(groebnerMatrix(13,42));
  groebnerMatrix(38,79) = groebnerMatrix(13,79)/(groebnerMatrix(13,42));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial39( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(39,43) = groebnerMatrix(31,68)/(groebnerMatrix(31,67));
  groebnerMatrix(39,47) = -groebnerMatrix(32,69)/(groebnerMatrix(32,68));
  groebnerMatrix(39,48) = -groebnerMatrix(32,70)/(groebnerMatrix(32,68));
  groebnerMatrix(39,49) = -groebnerMatrix(32,71)/(groebnerMatrix(32,68));
  groebnerMatrix(39,50) = groebnerMatrix(31,69)/(groebnerMatrix(31,67));
  groebnerMatrix(39,51) = groebnerMatrix(31,70)/(groebnerMatrix(31,67));
  groebnerMatrix(39,52) = (groebnerMatrix(31,71)/(groebnerMatrix(31,67))-groebnerMatrix(32,72)/(groebnerMatrix(32,68)));
  groebnerMatrix(39,53) = groebnerMatrix(31,72)/(groebnerMatrix(31,67));
  groebnerMatrix(39,56) = -groebnerMatrix(32,73)/(groebnerMatrix(32,68));
  groebnerMatrix(39,57) = groebnerMatrix(31,73)/(groebnerMatrix(31,67));
  groebnerMatrix(39,62) = -groebnerMatrix(32,74)/(groebnerMatrix(32,68));
  groebnerMatrix(39,63) = -groebnerMatrix(32,75)/(groebnerMatrix(32,68));
  groebnerMatrix(39,64) = -groebnerMatrix(32,76)/(groebnerMatrix(32,68));
  groebnerMatrix(39,65) = groebnerMatrix(31,74)/(groebnerMatrix(31,67));
  groebnerMatrix(39,66) = groebnerMatrix(31,75)/(groebnerMatrix(31,67));
  groebnerMatrix(39,67) = (groebnerMatrix(31,76)/(groebnerMatrix(31,67))-groebnerMatrix(32,77)/(groebnerMatrix(32,68)));
  groebnerMatrix(39,68) = groebnerMatrix(31,77)/(groebnerMatrix(31,67));
  groebnerMatrix(39,71) = -groebnerMatrix(32,78)/(groebnerMatrix(32,68));
  groebnerMatrix(39,72) = groebnerMatrix(31,78)/(groebnerMatrix(31,67));
  groebnerMatrix(39,76) = -groebnerMatrix(32,79)/(groebnerMatrix(32,68));
  groebnerMatrix(39,77) = groebnerMatrix(31,79)/(groebnerMatrix(31,67));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial40( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(40,42) = (groebnerMatrix(12,42)/(groebnerMatrix(12,41))-groebnerMatrix(30,67)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,43) = (groebnerMatrix(12,43)/(groebnerMatrix(12,41))-groebnerMatrix(30,68)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,50) = (groebnerMatrix(12,50)/(groebnerMatrix(12,41))-groebnerMatrix(30,69)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,51) = (groebnerMatrix(12,51)/(groebnerMatrix(12,41))-groebnerMatrix(30,70)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,52) = (groebnerMatrix(12,52)/(groebnerMatrix(12,41))-groebnerMatrix(30,71)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,53) = (groebnerMatrix(12,53)/(groebnerMatrix(12,41))-groebnerMatrix(30,72)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,54) = groebnerMatrix(12,54)/(groebnerMatrix(12,41));
  groebnerMatrix(40,55) = groebnerMatrix(12,55)/(groebnerMatrix(12,41));
  groebnerMatrix(40,56) = groebnerMatrix(12,56)/(groebnerMatrix(12,41));
  groebnerMatrix(40,57) = (groebnerMatrix(12,57)/(groebnerMatrix(12,41))-groebnerMatrix(30,73)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,58) = groebnerMatrix(12,58)/(groebnerMatrix(12,41));
  groebnerMatrix(40,65) = (groebnerMatrix(12,65)/(groebnerMatrix(12,41))-groebnerMatrix(30,74)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,66) = (groebnerMatrix(12,66)/(groebnerMatrix(12,41))-groebnerMatrix(30,75)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,67) = (groebnerMatrix(12,67)/(groebnerMatrix(12,41))-groebnerMatrix(30,76)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,68) = (groebnerMatrix(12,68)/(groebnerMatrix(12,41))-groebnerMatrix(30,77)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,69) = groebnerMatrix(12,69)/(groebnerMatrix(12,41));
  groebnerMatrix(40,70) = groebnerMatrix(12,70)/(groebnerMatrix(12,41));
  groebnerMatrix(40,71) = groebnerMatrix(12,71)/(groebnerMatrix(12,41));
  groebnerMatrix(40,72) = (groebnerMatrix(12,72)/(groebnerMatrix(12,41))-groebnerMatrix(30,78)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,73) = groebnerMatrix(12,73)/(groebnerMatrix(12,41));
  groebnerMatrix(40,74) = groebnerMatrix(12,74)/(groebnerMatrix(12,41));
  groebnerMatrix(40,75) = groebnerMatrix(12,75)/(groebnerMatrix(12,41));
  groebnerMatrix(40,76) = groebnerMatrix(12,76)/(groebnerMatrix(12,41));
  groebnerMatrix(40,77) = (groebnerMatrix(12,77)/(groebnerMatrix(12,41))-groebnerMatrix(30,79)/(groebnerMatrix(30,66)));
  groebnerMatrix(40,78) = groebnerMatrix(12,78)/(groebnerMatrix(12,41));
  groebnerMatrix(40,79) = groebnerMatrix(12,79)/(groebnerMatrix(12,41));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial41( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(41,42) = groebnerMatrix(30,67)/(groebnerMatrix(30,66));
  groebnerMatrix(41,43) = groebnerMatrix(30,68)/(groebnerMatrix(30,66));
  groebnerMatrix(41,45) = -groebnerMatrix(32,69)/(groebnerMatrix(32,68));
  groebnerMatrix(41,46) = -groebnerMatrix(32,70)/(groebnerMatrix(32,68));
  groebnerMatrix(41,48) = -groebnerMatrix(32,71)/(groebnerMatrix(32,68));
  groebnerMatrix(41,50) = groebnerMatrix(30,69)/(groebnerMatrix(30,66));
  groebnerMatrix(41,51) = (groebnerMatrix(30,70)/(groebnerMatrix(30,66))-groebnerMatrix(32,72)/(groebnerMatrix(32,68)));
  groebnerMatrix(41,52) = groebnerMatrix(30,71)/(groebnerMatrix(30,66));
  groebnerMatrix(41,53) = groebnerMatrix(30,72)/(groebnerMatrix(30,66));
  groebnerMatrix(41,55) = -groebnerMatrix(32,73)/(groebnerMatrix(32,68));
  groebnerMatrix(41,57) = groebnerMatrix(30,73)/(groebnerMatrix(30,66));
  groebnerMatrix(41,60) = -groebnerMatrix(32,74)/(groebnerMatrix(32,68));
  groebnerMatrix(41,61) = -groebnerMatrix(32,75)/(groebnerMatrix(32,68));
  groebnerMatrix(41,63) = -groebnerMatrix(32,76)/(groebnerMatrix(32,68));
  groebnerMatrix(41,65) = groebnerMatrix(30,74)/(groebnerMatrix(30,66));
  groebnerMatrix(41,66) = (groebnerMatrix(30,75)/(groebnerMatrix(30,66))-groebnerMatrix(32,77)/(groebnerMatrix(32,68)));
  groebnerMatrix(41,67) = groebnerMatrix(30,76)/(groebnerMatrix(30,66));
  groebnerMatrix(41,68) = groebnerMatrix(30,77)/(groebnerMatrix(30,66));
  groebnerMatrix(41,70) = -groebnerMatrix(32,78)/(groebnerMatrix(32,68));
  groebnerMatrix(41,72) = groebnerMatrix(30,78)/(groebnerMatrix(30,66));
  groebnerMatrix(41,75) = -groebnerMatrix(32,79)/(groebnerMatrix(32,68));
  groebnerMatrix(41,77) = groebnerMatrix(30,79)/(groebnerMatrix(30,66));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial42( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(42,41) = (groebnerMatrix(11,41)/(groebnerMatrix(11,40))-groebnerMatrix(29,66)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,42) = (groebnerMatrix(11,42)/(groebnerMatrix(11,40))-groebnerMatrix(29,67)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,43) = (groebnerMatrix(11,43)/(groebnerMatrix(11,40))-groebnerMatrix(29,68)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,50) = (groebnerMatrix(11,50)/(groebnerMatrix(11,40))-groebnerMatrix(29,69)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,51) = (groebnerMatrix(11,51)/(groebnerMatrix(11,40))-groebnerMatrix(29,70)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,52) = (groebnerMatrix(11,52)/(groebnerMatrix(11,40))-groebnerMatrix(29,71)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,53) = (groebnerMatrix(11,53)/(groebnerMatrix(11,40))-groebnerMatrix(29,72)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,54) = groebnerMatrix(11,54)/(groebnerMatrix(11,40));
  groebnerMatrix(42,55) = groebnerMatrix(11,55)/(groebnerMatrix(11,40));
  groebnerMatrix(42,56) = groebnerMatrix(11,56)/(groebnerMatrix(11,40));
  groebnerMatrix(42,57) = (groebnerMatrix(11,57)/(groebnerMatrix(11,40))-groebnerMatrix(29,73)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,58) = groebnerMatrix(11,58)/(groebnerMatrix(11,40));
  groebnerMatrix(42,65) = (groebnerMatrix(11,65)/(groebnerMatrix(11,40))-groebnerMatrix(29,74)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,66) = (groebnerMatrix(11,66)/(groebnerMatrix(11,40))-groebnerMatrix(29,75)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,67) = (groebnerMatrix(11,67)/(groebnerMatrix(11,40))-groebnerMatrix(29,76)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,68) = (groebnerMatrix(11,68)/(groebnerMatrix(11,40))-groebnerMatrix(29,77)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,69) = groebnerMatrix(11,69)/(groebnerMatrix(11,40));
  groebnerMatrix(42,70) = groebnerMatrix(11,70)/(groebnerMatrix(11,40));
  groebnerMatrix(42,71) = groebnerMatrix(11,71)/(groebnerMatrix(11,40));
  groebnerMatrix(42,72) = (groebnerMatrix(11,72)/(groebnerMatrix(11,40))-groebnerMatrix(29,78)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,73) = groebnerMatrix(11,73)/(groebnerMatrix(11,40));
  groebnerMatrix(42,74) = groebnerMatrix(11,74)/(groebnerMatrix(11,40));
  groebnerMatrix(42,75) = groebnerMatrix(11,75)/(groebnerMatrix(11,40));
  groebnerMatrix(42,76) = groebnerMatrix(11,76)/(groebnerMatrix(11,40));
  groebnerMatrix(42,77) = (groebnerMatrix(11,77)/(groebnerMatrix(11,40))-groebnerMatrix(29,79)/(groebnerMatrix(29,65)));
  groebnerMatrix(42,78) = groebnerMatrix(11,78)/(groebnerMatrix(11,40));
  groebnerMatrix(42,79) = groebnerMatrix(11,79)/(groebnerMatrix(11,40));
}

void
opengv::absolute_pose::modules::gpnp5::sPolynomial43( Eigen::Matrix<double,44,80> & groebnerMatrix )
{
  groebnerMatrix(43,50) = -groebnerMatrix(28,65)/(groebnerMatrix(28,58));
  groebnerMatrix(43,51) = -groebnerMatrix(28,66)/(groebnerMatrix(28,58));
  groebnerMatrix(43,52) = -groebnerMatrix(28,67)/(groebnerMatrix(28,58));
  groebnerMatrix(43,53) = -groebnerMatrix(28,68)/(groebnerMatrix(28,58));
  groebnerMatrix(43,54) = (groebnerMatrix(23,54)/(groebnerMatrix(23,25))-groebnerMatrix(28,69)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,55) = (groebnerMatrix(23,55)/(groebnerMatrix(23,25))-groebnerMatrix(28,70)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,56) = (groebnerMatrix(23,56)/(groebnerMatrix(23,25))-groebnerMatrix(28,71)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,57) = (groebnerMatrix(23,57)/(groebnerMatrix(23,25))-groebnerMatrix(28,72)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,58) = (groebnerMatrix(23,58)/(groebnerMatrix(23,25))-groebnerMatrix(28,73)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,65) = groebnerMatrix(23,65)/(groebnerMatrix(23,25));
  groebnerMatrix(43,66) = groebnerMatrix(23,66)/(groebnerMatrix(23,25));
  groebnerMatrix(43,67) = groebnerMatrix(23,67)/(groebnerMatrix(23,25));
  groebnerMatrix(43,68) = groebnerMatrix(23,68)/(groebnerMatrix(23,25));
  groebnerMatrix(43,69) = (groebnerMatrix(23,69)/(groebnerMatrix(23,25))-groebnerMatrix(28,74)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,70) = (groebnerMatrix(23,70)/(groebnerMatrix(23,25))-groebnerMatrix(28,75)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,71) = (groebnerMatrix(23,71)/(groebnerMatrix(23,25))-groebnerMatrix(28,76)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,72) = (groebnerMatrix(23,72)/(groebnerMatrix(23,25))-groebnerMatrix(28,77)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,73) = (groebnerMatrix(23,73)/(groebnerMatrix(23,25))-groebnerMatrix(28,78)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,74) = groebnerMatrix(23,74)/(groebnerMatrix(23,25));
  groebnerMatrix(43,75) = groebnerMatrix(23,75)/(groebnerMatrix(23,25));
  groebnerMatrix(43,76) = groebnerMatrix(23,76)/(groebnerMatrix(23,25));
  groebnerMatrix(43,77) = groebnerMatrix(23,77)/(groebnerMatrix(23,25));
  groebnerMatrix(43,78) = (groebnerMatrix(23,78)/(groebnerMatrix(23,25))-groebnerMatrix(28,79)/(groebnerMatrix(28,58)));
  groebnerMatrix(43,79) = groebnerMatrix(23,79)/(groebnerMatrix(23,25));
}

