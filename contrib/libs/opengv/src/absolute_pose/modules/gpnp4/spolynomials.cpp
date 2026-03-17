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
opengv::absolute_pose::modules::gpnp4::sPolynomial5( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(5,23) = (groebnerMatrix(0,23)/(groebnerMatrix(0,22))-groebnerMatrix(1,23)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,24) = (groebnerMatrix(0,24)/(groebnerMatrix(0,22))-groebnerMatrix(1,24)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,25) = (groebnerMatrix(0,25)/(groebnerMatrix(0,22))-groebnerMatrix(1,25)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,26) = (groebnerMatrix(0,26)/(groebnerMatrix(0,22))-groebnerMatrix(1,26)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,27) = (groebnerMatrix(0,27)/(groebnerMatrix(0,22))-groebnerMatrix(1,27)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,28) = (groebnerMatrix(0,28)/(groebnerMatrix(0,22))-groebnerMatrix(1,28)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,29) = (groebnerMatrix(0,29)/(groebnerMatrix(0,22))-groebnerMatrix(1,29)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,30) = (groebnerMatrix(0,30)/(groebnerMatrix(0,22))-groebnerMatrix(1,30)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,31) = (groebnerMatrix(0,31)/(groebnerMatrix(0,22))-groebnerMatrix(1,31)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,32) = (groebnerMatrix(0,32)/(groebnerMatrix(0,22))-groebnerMatrix(1,32)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,33) = (groebnerMatrix(0,33)/(groebnerMatrix(0,22))-groebnerMatrix(1,33)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,34) = (groebnerMatrix(0,34)/(groebnerMatrix(0,22))-groebnerMatrix(1,34)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,35) = (groebnerMatrix(0,35)/(groebnerMatrix(0,22))-groebnerMatrix(1,35)/(groebnerMatrix(1,22)));
  groebnerMatrix(5,36) = (groebnerMatrix(0,36)/(groebnerMatrix(0,22))-groebnerMatrix(1,36)/(groebnerMatrix(1,22)));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial6( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(6,23) = (groebnerMatrix(1,23)/(groebnerMatrix(1,22))-groebnerMatrix(2,23)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,24) = (groebnerMatrix(1,24)/(groebnerMatrix(1,22))-groebnerMatrix(2,24)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,25) = (groebnerMatrix(1,25)/(groebnerMatrix(1,22))-groebnerMatrix(2,25)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,26) = (groebnerMatrix(1,26)/(groebnerMatrix(1,22))-groebnerMatrix(2,26)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,27) = (groebnerMatrix(1,27)/(groebnerMatrix(1,22))-groebnerMatrix(2,27)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,28) = (groebnerMatrix(1,28)/(groebnerMatrix(1,22))-groebnerMatrix(2,28)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,29) = (groebnerMatrix(1,29)/(groebnerMatrix(1,22))-groebnerMatrix(2,29)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,30) = (groebnerMatrix(1,30)/(groebnerMatrix(1,22))-groebnerMatrix(2,30)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,31) = (groebnerMatrix(1,31)/(groebnerMatrix(1,22))-groebnerMatrix(2,31)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,32) = (groebnerMatrix(1,32)/(groebnerMatrix(1,22))-groebnerMatrix(2,32)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,33) = (groebnerMatrix(1,33)/(groebnerMatrix(1,22))-groebnerMatrix(2,33)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,34) = (groebnerMatrix(1,34)/(groebnerMatrix(1,22))-groebnerMatrix(2,34)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,35) = (groebnerMatrix(1,35)/(groebnerMatrix(1,22))-groebnerMatrix(2,35)/(groebnerMatrix(2,22)));
  groebnerMatrix(6,36) = (groebnerMatrix(1,36)/(groebnerMatrix(1,22))-groebnerMatrix(2,36)/(groebnerMatrix(2,22)));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial7( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(7,23) = (groebnerMatrix(2,23)/(groebnerMatrix(2,22))-groebnerMatrix(3,23)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,24) = (groebnerMatrix(2,24)/(groebnerMatrix(2,22))-groebnerMatrix(3,24)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,25) = (groebnerMatrix(2,25)/(groebnerMatrix(2,22))-groebnerMatrix(3,25)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,26) = (groebnerMatrix(2,26)/(groebnerMatrix(2,22))-groebnerMatrix(3,26)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,27) = (groebnerMatrix(2,27)/(groebnerMatrix(2,22))-groebnerMatrix(3,27)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,28) = (groebnerMatrix(2,28)/(groebnerMatrix(2,22))-groebnerMatrix(3,28)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,29) = (groebnerMatrix(2,29)/(groebnerMatrix(2,22))-groebnerMatrix(3,29)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,30) = (groebnerMatrix(2,30)/(groebnerMatrix(2,22))-groebnerMatrix(3,30)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,31) = (groebnerMatrix(2,31)/(groebnerMatrix(2,22))-groebnerMatrix(3,31)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,32) = (groebnerMatrix(2,32)/(groebnerMatrix(2,22))-groebnerMatrix(3,32)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,33) = (groebnerMatrix(2,33)/(groebnerMatrix(2,22))-groebnerMatrix(3,33)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,34) = (groebnerMatrix(2,34)/(groebnerMatrix(2,22))-groebnerMatrix(3,34)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,35) = (groebnerMatrix(2,35)/(groebnerMatrix(2,22))-groebnerMatrix(3,35)/(groebnerMatrix(3,22)));
  groebnerMatrix(7,36) = (groebnerMatrix(2,36)/(groebnerMatrix(2,22))-groebnerMatrix(3,36)/(groebnerMatrix(3,22)));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial8( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(8,23) = (groebnerMatrix(3,23)/(groebnerMatrix(3,22))-groebnerMatrix(4,23)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,24) = (groebnerMatrix(3,24)/(groebnerMatrix(3,22))-groebnerMatrix(4,24)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,25) = (groebnerMatrix(3,25)/(groebnerMatrix(3,22))-groebnerMatrix(4,25)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,26) = (groebnerMatrix(3,26)/(groebnerMatrix(3,22))-groebnerMatrix(4,26)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,27) = (groebnerMatrix(3,27)/(groebnerMatrix(3,22))-groebnerMatrix(4,27)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,28) = (groebnerMatrix(3,28)/(groebnerMatrix(3,22))-groebnerMatrix(4,28)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,29) = (groebnerMatrix(3,29)/(groebnerMatrix(3,22))-groebnerMatrix(4,29)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,30) = (groebnerMatrix(3,30)/(groebnerMatrix(3,22))-groebnerMatrix(4,30)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,31) = (groebnerMatrix(3,31)/(groebnerMatrix(3,22))-groebnerMatrix(4,31)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,32) = (groebnerMatrix(3,32)/(groebnerMatrix(3,22))-groebnerMatrix(4,32)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,33) = (groebnerMatrix(3,33)/(groebnerMatrix(3,22))-groebnerMatrix(4,33)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,34) = (groebnerMatrix(3,34)/(groebnerMatrix(3,22))-groebnerMatrix(4,34)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,35) = (groebnerMatrix(3,35)/(groebnerMatrix(3,22))-groebnerMatrix(4,35)/(groebnerMatrix(4,22)));
  groebnerMatrix(8,36) = (groebnerMatrix(3,36)/(groebnerMatrix(3,22))-groebnerMatrix(4,36)/(groebnerMatrix(4,22)));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial9( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(9,9) = groebnerMatrix(6,25)/(groebnerMatrix(6,24));
  groebnerMatrix(9,10) = (groebnerMatrix(6,26)/(groebnerMatrix(6,24))-groebnerMatrix(8,27)/(groebnerMatrix(8,26)));
  groebnerMatrix(9,11) = groebnerMatrix(6,27)/(groebnerMatrix(6,24));
  groebnerMatrix(9,13) = -groebnerMatrix(8,28)/(groebnerMatrix(8,26));
  groebnerMatrix(9,14) = -groebnerMatrix(8,29)/(groebnerMatrix(8,26));
  groebnerMatrix(9,15) = groebnerMatrix(6,28)/(groebnerMatrix(6,24));
  groebnerMatrix(9,16) = (groebnerMatrix(6,29)/(groebnerMatrix(6,24))-groebnerMatrix(8,30)/(groebnerMatrix(8,26)));
  groebnerMatrix(9,17) = groebnerMatrix(6,30)/(groebnerMatrix(6,24));
  groebnerMatrix(9,19) = -groebnerMatrix(8,31)/(groebnerMatrix(8,26));
  groebnerMatrix(9,20) = groebnerMatrix(6,31)/(groebnerMatrix(6,24));
  groebnerMatrix(9,23) = -groebnerMatrix(8,32)/(groebnerMatrix(8,26));
  groebnerMatrix(9,24) = -groebnerMatrix(8,33)/(groebnerMatrix(8,26));
  groebnerMatrix(9,25) = groebnerMatrix(6,32)/(groebnerMatrix(6,24));
  groebnerMatrix(9,26) = (groebnerMatrix(6,33)/(groebnerMatrix(6,24))-groebnerMatrix(8,34)/(groebnerMatrix(8,26)));
  groebnerMatrix(9,27) = groebnerMatrix(6,34)/(groebnerMatrix(6,24));
  groebnerMatrix(9,29) = -groebnerMatrix(8,35)/(groebnerMatrix(8,26));
  groebnerMatrix(9,30) = groebnerMatrix(6,35)/(groebnerMatrix(6,24));
  groebnerMatrix(9,33) = -groebnerMatrix(8,36)/(groebnerMatrix(8,26));
  groebnerMatrix(9,34) = groebnerMatrix(6,36)/(groebnerMatrix(6,24));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial10( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(10,8) = (groebnerMatrix(5,24)/(groebnerMatrix(5,23))-groebnerMatrix(7,26)/(groebnerMatrix(7,25)));
  groebnerMatrix(10,9) = groebnerMatrix(5,25)/(groebnerMatrix(5,23));
  groebnerMatrix(10,10) = (groebnerMatrix(5,26)/(groebnerMatrix(5,23))-groebnerMatrix(7,27)/(groebnerMatrix(7,25)));
  groebnerMatrix(10,11) = groebnerMatrix(5,27)/(groebnerMatrix(5,23));
  groebnerMatrix(10,13) = -groebnerMatrix(7,28)/(groebnerMatrix(7,25));
  groebnerMatrix(10,14) = -groebnerMatrix(7,29)/(groebnerMatrix(7,25));
  groebnerMatrix(10,15) = groebnerMatrix(5,28)/(groebnerMatrix(5,23));
  groebnerMatrix(10,16) = (groebnerMatrix(5,29)/(groebnerMatrix(5,23))-groebnerMatrix(7,30)/(groebnerMatrix(7,25)));
  groebnerMatrix(10,17) = groebnerMatrix(5,30)/(groebnerMatrix(5,23));
  groebnerMatrix(10,19) = -groebnerMatrix(7,31)/(groebnerMatrix(7,25));
  groebnerMatrix(10,20) = groebnerMatrix(5,31)/(groebnerMatrix(5,23));
  groebnerMatrix(10,23) = -groebnerMatrix(7,32)/(groebnerMatrix(7,25));
  groebnerMatrix(10,24) = -groebnerMatrix(7,33)/(groebnerMatrix(7,25));
  groebnerMatrix(10,25) = groebnerMatrix(5,32)/(groebnerMatrix(5,23));
  groebnerMatrix(10,26) = (groebnerMatrix(5,33)/(groebnerMatrix(5,23))-groebnerMatrix(7,34)/(groebnerMatrix(7,25)));
  groebnerMatrix(10,27) = groebnerMatrix(5,34)/(groebnerMatrix(5,23));
  groebnerMatrix(10,29) = -groebnerMatrix(7,35)/(groebnerMatrix(7,25));
  groebnerMatrix(10,30) = groebnerMatrix(5,35)/(groebnerMatrix(5,23));
  groebnerMatrix(10,33) = -groebnerMatrix(7,36)/(groebnerMatrix(7,25));
  groebnerMatrix(10,34) = groebnerMatrix(5,36)/(groebnerMatrix(5,23));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial11( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(11,8) = groebnerMatrix(5,24)/(groebnerMatrix(5,23));
  groebnerMatrix(11,9) = (groebnerMatrix(5,25)/(groebnerMatrix(5,23))-groebnerMatrix(8,27)/(groebnerMatrix(8,26)));
  groebnerMatrix(11,10) = groebnerMatrix(5,26)/(groebnerMatrix(5,23));
  groebnerMatrix(11,11) = groebnerMatrix(5,27)/(groebnerMatrix(5,23));
  groebnerMatrix(11,12) = -groebnerMatrix(8,28)/(groebnerMatrix(8,26));
  groebnerMatrix(11,13) = -groebnerMatrix(8,29)/(groebnerMatrix(8,26));
  groebnerMatrix(11,15) = (groebnerMatrix(5,28)/(groebnerMatrix(5,23))-groebnerMatrix(8,30)/(groebnerMatrix(8,26)));
  groebnerMatrix(11,16) = groebnerMatrix(5,29)/(groebnerMatrix(5,23));
  groebnerMatrix(11,17) = groebnerMatrix(5,30)/(groebnerMatrix(5,23));
  groebnerMatrix(11,18) = -groebnerMatrix(8,31)/(groebnerMatrix(8,26));
  groebnerMatrix(11,20) = groebnerMatrix(5,31)/(groebnerMatrix(5,23));
  groebnerMatrix(11,22) = -groebnerMatrix(8,32)/(groebnerMatrix(8,26));
  groebnerMatrix(11,23) = -groebnerMatrix(8,33)/(groebnerMatrix(8,26));
  groebnerMatrix(11,25) = (groebnerMatrix(5,32)/(groebnerMatrix(5,23))-groebnerMatrix(8,34)/(groebnerMatrix(8,26)));
  groebnerMatrix(11,26) = groebnerMatrix(5,33)/(groebnerMatrix(5,23));
  groebnerMatrix(11,27) = groebnerMatrix(5,34)/(groebnerMatrix(5,23));
  groebnerMatrix(11,28) = -groebnerMatrix(8,35)/(groebnerMatrix(8,26));
  groebnerMatrix(11,30) = groebnerMatrix(5,35)/(groebnerMatrix(5,23));
  groebnerMatrix(11,32) = -groebnerMatrix(8,36)/(groebnerMatrix(8,26));
  groebnerMatrix(11,34) = groebnerMatrix(5,36)/(groebnerMatrix(5,23));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial12( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(12,7) = (groebnerMatrix(4,23)/(groebnerMatrix(4,22))-groebnerMatrix(7,26)/(groebnerMatrix(7,25)));
  groebnerMatrix(12,8) = groebnerMatrix(4,24)/(groebnerMatrix(4,22));
  groebnerMatrix(12,9) = (groebnerMatrix(4,25)/(groebnerMatrix(4,22))-groebnerMatrix(7,27)/(groebnerMatrix(7,25)));
  groebnerMatrix(12,10) = groebnerMatrix(4,26)/(groebnerMatrix(4,22));
  groebnerMatrix(12,11) = groebnerMatrix(4,27)/(groebnerMatrix(4,22));
  groebnerMatrix(12,12) = -groebnerMatrix(7,28)/(groebnerMatrix(7,25));
  groebnerMatrix(12,13) = -groebnerMatrix(7,29)/(groebnerMatrix(7,25));
  groebnerMatrix(12,15) = (groebnerMatrix(4,28)/(groebnerMatrix(4,22))-groebnerMatrix(7,30)/(groebnerMatrix(7,25)));
  groebnerMatrix(12,16) = groebnerMatrix(4,29)/(groebnerMatrix(4,22));
  groebnerMatrix(12,17) = groebnerMatrix(4,30)/(groebnerMatrix(4,22));
  groebnerMatrix(12,18) = -groebnerMatrix(7,31)/(groebnerMatrix(7,25));
  groebnerMatrix(12,20) = groebnerMatrix(4,31)/(groebnerMatrix(4,22));
  groebnerMatrix(12,22) = -groebnerMatrix(7,32)/(groebnerMatrix(7,25));
  groebnerMatrix(12,23) = -groebnerMatrix(7,33)/(groebnerMatrix(7,25));
  groebnerMatrix(12,25) = (groebnerMatrix(4,32)/(groebnerMatrix(4,22))-groebnerMatrix(7,34)/(groebnerMatrix(7,25)));
  groebnerMatrix(12,26) = groebnerMatrix(4,33)/(groebnerMatrix(4,22));
  groebnerMatrix(12,27) = groebnerMatrix(4,34)/(groebnerMatrix(4,22));
  groebnerMatrix(12,28) = -groebnerMatrix(7,35)/(groebnerMatrix(7,25));
  groebnerMatrix(12,30) = groebnerMatrix(4,35)/(groebnerMatrix(4,22));
  groebnerMatrix(12,32) = -groebnerMatrix(7,36)/(groebnerMatrix(7,25));
  groebnerMatrix(12,34) = groebnerMatrix(4,36)/(groebnerMatrix(4,22));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial13( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(13,5) = groebnerMatrix(5,24)/(groebnerMatrix(5,23));
  groebnerMatrix(13,6) = -groebnerMatrix(6,25)/(groebnerMatrix(6,24));
  groebnerMatrix(13,7) = (groebnerMatrix(5,25)/(groebnerMatrix(5,23))-groebnerMatrix(6,26)/(groebnerMatrix(6,24)));
  groebnerMatrix(13,8) = groebnerMatrix(5,26)/(groebnerMatrix(5,23));
  groebnerMatrix(13,9) = -groebnerMatrix(6,27)/(groebnerMatrix(6,24));
  groebnerMatrix(13,10) = groebnerMatrix(5,27)/(groebnerMatrix(5,23));
  groebnerMatrix(13,12) = -groebnerMatrix(6,28)/(groebnerMatrix(6,24));
  groebnerMatrix(13,13) = (groebnerMatrix(5,28)/(groebnerMatrix(5,23))-groebnerMatrix(6,29)/(groebnerMatrix(6,24)));
  groebnerMatrix(13,14) = groebnerMatrix(5,29)/(groebnerMatrix(5,23));
  groebnerMatrix(13,15) = -groebnerMatrix(6,30)/(groebnerMatrix(6,24));
  groebnerMatrix(13,16) = groebnerMatrix(5,30)/(groebnerMatrix(5,23));
  groebnerMatrix(13,18) = -groebnerMatrix(6,31)/(groebnerMatrix(6,24));
  groebnerMatrix(13,19) = groebnerMatrix(5,31)/(groebnerMatrix(5,23));
  groebnerMatrix(13,22) = -groebnerMatrix(6,32)/(groebnerMatrix(6,24));
  groebnerMatrix(13,23) = (groebnerMatrix(5,32)/(groebnerMatrix(5,23))-groebnerMatrix(6,33)/(groebnerMatrix(6,24)));
  groebnerMatrix(13,24) = groebnerMatrix(5,33)/(groebnerMatrix(5,23));
  groebnerMatrix(13,25) = -groebnerMatrix(6,34)/(groebnerMatrix(6,24));
  groebnerMatrix(13,26) = groebnerMatrix(5,34)/(groebnerMatrix(5,23));
  groebnerMatrix(13,28) = -groebnerMatrix(6,35)/(groebnerMatrix(6,24));
  groebnerMatrix(13,29) = groebnerMatrix(5,35)/(groebnerMatrix(5,23));
  groebnerMatrix(13,32) = -groebnerMatrix(6,36)/(groebnerMatrix(6,24));
  groebnerMatrix(13,33) = groebnerMatrix(5,36)/(groebnerMatrix(5,23));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial14( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(14,4) = (groebnerMatrix(4,23)/(groebnerMatrix(4,22))-groebnerMatrix(5,24)/(groebnerMatrix(5,23)));
  groebnerMatrix(14,5) = groebnerMatrix(4,24)/(groebnerMatrix(4,22));
  groebnerMatrix(14,6) = -groebnerMatrix(5,25)/(groebnerMatrix(5,23));
  groebnerMatrix(14,7) = (groebnerMatrix(4,25)/(groebnerMatrix(4,22))-groebnerMatrix(5,26)/(groebnerMatrix(5,23)));
  groebnerMatrix(14,8) = groebnerMatrix(4,26)/(groebnerMatrix(4,22));
  groebnerMatrix(14,9) = -groebnerMatrix(5,27)/(groebnerMatrix(5,23));
  groebnerMatrix(14,10) = groebnerMatrix(4,27)/(groebnerMatrix(4,22));
  groebnerMatrix(14,12) = -groebnerMatrix(5,28)/(groebnerMatrix(5,23));
  groebnerMatrix(14,13) = (groebnerMatrix(4,28)/(groebnerMatrix(4,22))-groebnerMatrix(5,29)/(groebnerMatrix(5,23)));
  groebnerMatrix(14,14) = groebnerMatrix(4,29)/(groebnerMatrix(4,22));
  groebnerMatrix(14,15) = -groebnerMatrix(5,30)/(groebnerMatrix(5,23));
  groebnerMatrix(14,16) = groebnerMatrix(4,30)/(groebnerMatrix(4,22));
  groebnerMatrix(14,18) = -groebnerMatrix(5,31)/(groebnerMatrix(5,23));
  groebnerMatrix(14,19) = groebnerMatrix(4,31)/(groebnerMatrix(4,22));
  groebnerMatrix(14,22) = -groebnerMatrix(5,32)/(groebnerMatrix(5,23));
  groebnerMatrix(14,23) = (groebnerMatrix(4,32)/(groebnerMatrix(4,22))-groebnerMatrix(5,33)/(groebnerMatrix(5,23)));
  groebnerMatrix(14,24) = groebnerMatrix(4,33)/(groebnerMatrix(4,22));
  groebnerMatrix(14,25) = -groebnerMatrix(5,34)/(groebnerMatrix(5,23));
  groebnerMatrix(14,26) = groebnerMatrix(4,34)/(groebnerMatrix(4,22));
  groebnerMatrix(14,28) = -groebnerMatrix(5,35)/(groebnerMatrix(5,23));
  groebnerMatrix(14,29) = groebnerMatrix(4,35)/(groebnerMatrix(4,22));
  groebnerMatrix(14,32) = -groebnerMatrix(5,36)/(groebnerMatrix(5,23));
  groebnerMatrix(14,33) = groebnerMatrix(4,36)/(groebnerMatrix(4,22));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial15( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(15,3) = groebnerMatrix(13,21)/(groebnerMatrix(13,20));
  groebnerMatrix(15,11) = -groebnerMatrix(14,27)/(groebnerMatrix(14,21));
  groebnerMatrix(15,15) = -groebnerMatrix(14,28)/(groebnerMatrix(14,21));
  groebnerMatrix(15,16) = -groebnerMatrix(14,29)/(groebnerMatrix(14,21));
  groebnerMatrix(15,17) = (groebnerMatrix(13,27)/(groebnerMatrix(13,20))-groebnerMatrix(14,30)/(groebnerMatrix(14,21)));
  groebnerMatrix(15,18) = groebnerMatrix(13,28)/(groebnerMatrix(13,20));
  groebnerMatrix(15,19) = groebnerMatrix(13,29)/(groebnerMatrix(13,20));
  groebnerMatrix(15,20) = (groebnerMatrix(13,30)/(groebnerMatrix(13,20))-groebnerMatrix(14,31)/(groebnerMatrix(14,21)));
  groebnerMatrix(15,21) = groebnerMatrix(13,31)/(groebnerMatrix(13,20));
  groebnerMatrix(15,25) = -groebnerMatrix(14,32)/(groebnerMatrix(14,21));
  groebnerMatrix(15,26) = -groebnerMatrix(14,33)/(groebnerMatrix(14,21));
  groebnerMatrix(15,27) = -groebnerMatrix(14,34)/(groebnerMatrix(14,21));
  groebnerMatrix(15,28) = groebnerMatrix(13,32)/(groebnerMatrix(13,20));
  groebnerMatrix(15,29) = groebnerMatrix(13,33)/(groebnerMatrix(13,20));
  groebnerMatrix(15,30) = (groebnerMatrix(13,34)/(groebnerMatrix(13,20))-groebnerMatrix(14,35)/(groebnerMatrix(14,21)));
  groebnerMatrix(15,31) = groebnerMatrix(13,35)/(groebnerMatrix(13,20));
  groebnerMatrix(15,34) = -groebnerMatrix(14,36)/(groebnerMatrix(14,21));
  groebnerMatrix(15,35) = groebnerMatrix(13,36)/(groebnerMatrix(13,20));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial16( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(16,2) = groebnerMatrix(12,20)/(groebnerMatrix(12,19));
  groebnerMatrix(16,3) = groebnerMatrix(12,21)/(groebnerMatrix(12,19));
  groebnerMatrix(16,10) = -groebnerMatrix(14,27)/(groebnerMatrix(14,21));
  groebnerMatrix(16,13) = -groebnerMatrix(14,28)/(groebnerMatrix(14,21));
  groebnerMatrix(16,14) = -groebnerMatrix(14,29)/(groebnerMatrix(14,21));
  groebnerMatrix(16,16) = -groebnerMatrix(14,30)/(groebnerMatrix(14,21));
  groebnerMatrix(16,17) = groebnerMatrix(12,27)/(groebnerMatrix(12,19));
  groebnerMatrix(16,18) = groebnerMatrix(12,28)/(groebnerMatrix(12,19));
  groebnerMatrix(16,19) = (groebnerMatrix(12,29)/(groebnerMatrix(12,19))-groebnerMatrix(14,31)/(groebnerMatrix(14,21)));
  groebnerMatrix(16,20) = groebnerMatrix(12,30)/(groebnerMatrix(12,19));
  groebnerMatrix(16,21) = groebnerMatrix(12,31)/(groebnerMatrix(12,19));
  groebnerMatrix(16,23) = -groebnerMatrix(14,32)/(groebnerMatrix(14,21));
  groebnerMatrix(16,24) = -groebnerMatrix(14,33)/(groebnerMatrix(14,21));
  groebnerMatrix(16,26) = -groebnerMatrix(14,34)/(groebnerMatrix(14,21));
  groebnerMatrix(16,28) = groebnerMatrix(12,32)/(groebnerMatrix(12,19));
  groebnerMatrix(16,29) = (groebnerMatrix(12,33)/(groebnerMatrix(12,19))-groebnerMatrix(14,35)/(groebnerMatrix(14,21)));
  groebnerMatrix(16,30) = groebnerMatrix(12,34)/(groebnerMatrix(12,19));
  groebnerMatrix(16,31) = groebnerMatrix(12,35)/(groebnerMatrix(12,19));
  groebnerMatrix(16,33) = -groebnerMatrix(14,36)/(groebnerMatrix(14,21));
  groebnerMatrix(16,35) = groebnerMatrix(12,36)/(groebnerMatrix(12,19));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial17( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(17,1) = groebnerMatrix(11,19)/(groebnerMatrix(11,18));
  groebnerMatrix(17,2) = groebnerMatrix(11,20)/(groebnerMatrix(11,18));
  groebnerMatrix(17,3) = groebnerMatrix(11,21)/(groebnerMatrix(11,18));
  groebnerMatrix(17,9) = -groebnerMatrix(14,27)/(groebnerMatrix(14,21));
  groebnerMatrix(17,12) = -groebnerMatrix(14,28)/(groebnerMatrix(14,21));
  groebnerMatrix(17,13) = -groebnerMatrix(14,29)/(groebnerMatrix(14,21));
  groebnerMatrix(17,15) = -groebnerMatrix(14,30)/(groebnerMatrix(14,21));
  groebnerMatrix(17,17) = groebnerMatrix(11,27)/(groebnerMatrix(11,18));
  groebnerMatrix(17,18) = (groebnerMatrix(11,28)/(groebnerMatrix(11,18))-groebnerMatrix(14,31)/(groebnerMatrix(14,21)));
  groebnerMatrix(17,19) = groebnerMatrix(11,29)/(groebnerMatrix(11,18));
  groebnerMatrix(17,20) = groebnerMatrix(11,30)/(groebnerMatrix(11,18));
  groebnerMatrix(17,21) = groebnerMatrix(11,31)/(groebnerMatrix(11,18));
  groebnerMatrix(17,22) = -groebnerMatrix(14,32)/(groebnerMatrix(14,21));
  groebnerMatrix(17,23) = -groebnerMatrix(14,33)/(groebnerMatrix(14,21));
  groebnerMatrix(17,25) = -groebnerMatrix(14,34)/(groebnerMatrix(14,21));
  groebnerMatrix(17,28) = (groebnerMatrix(11,32)/(groebnerMatrix(11,18))-groebnerMatrix(14,35)/(groebnerMatrix(14,21)));
  groebnerMatrix(17,29) = groebnerMatrix(11,33)/(groebnerMatrix(11,18));
  groebnerMatrix(17,30) = groebnerMatrix(11,34)/(groebnerMatrix(11,18));
  groebnerMatrix(17,31) = groebnerMatrix(11,35)/(groebnerMatrix(11,18));
  groebnerMatrix(17,32) = -groebnerMatrix(14,36)/(groebnerMatrix(14,21));
  groebnerMatrix(17,35) = groebnerMatrix(11,36)/(groebnerMatrix(11,18));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial18( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(18,0) = groebnerMatrix(10,18)/(groebnerMatrix(10,17));
  groebnerMatrix(18,1) = groebnerMatrix(10,19)/(groebnerMatrix(10,17));
  groebnerMatrix(18,2) = (groebnerMatrix(10,20)/(groebnerMatrix(10,17))-groebnerMatrix(13,21)/(groebnerMatrix(13,20)));
  groebnerMatrix(18,3) = groebnerMatrix(10,21)/(groebnerMatrix(10,17));
  groebnerMatrix(18,11) = -groebnerMatrix(13,27)/(groebnerMatrix(13,20));
  groebnerMatrix(18,15) = -groebnerMatrix(13,28)/(groebnerMatrix(13,20));
  groebnerMatrix(18,16) = -groebnerMatrix(13,29)/(groebnerMatrix(13,20));
  groebnerMatrix(18,17) = (groebnerMatrix(10,27)/(groebnerMatrix(10,17))-groebnerMatrix(13,30)/(groebnerMatrix(13,20)));
  groebnerMatrix(18,18) = groebnerMatrix(10,28)/(groebnerMatrix(10,17));
  groebnerMatrix(18,19) = groebnerMatrix(10,29)/(groebnerMatrix(10,17));
  groebnerMatrix(18,20) = (groebnerMatrix(10,30)/(groebnerMatrix(10,17))-groebnerMatrix(13,31)/(groebnerMatrix(13,20)));
  groebnerMatrix(18,21) = groebnerMatrix(10,31)/(groebnerMatrix(10,17));
  groebnerMatrix(18,25) = -groebnerMatrix(13,32)/(groebnerMatrix(13,20));
  groebnerMatrix(18,26) = -groebnerMatrix(13,33)/(groebnerMatrix(13,20));
  groebnerMatrix(18,27) = -groebnerMatrix(13,34)/(groebnerMatrix(13,20));
  groebnerMatrix(18,28) = groebnerMatrix(10,32)/(groebnerMatrix(10,17));
  groebnerMatrix(18,29) = groebnerMatrix(10,33)/(groebnerMatrix(10,17));
  groebnerMatrix(18,30) = (groebnerMatrix(10,34)/(groebnerMatrix(10,17))-groebnerMatrix(13,35)/(groebnerMatrix(13,20)));
  groebnerMatrix(18,31) = groebnerMatrix(10,35)/(groebnerMatrix(10,17));
  groebnerMatrix(18,34) = -groebnerMatrix(13,36)/(groebnerMatrix(13,20));
  groebnerMatrix(18,35) = groebnerMatrix(10,36)/(groebnerMatrix(10,17));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial19( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(19,21) = (groebnerMatrix(13,21)/(groebnerMatrix(13,20))-groebnerMatrix(18,31)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,27) = groebnerMatrix(13,27)/(groebnerMatrix(13,20));
  groebnerMatrix(19,28) = (groebnerMatrix(13,28)/(groebnerMatrix(13,20))-groebnerMatrix(18,32)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,29) = (groebnerMatrix(13,29)/(groebnerMatrix(13,20))-groebnerMatrix(18,33)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,30) = (groebnerMatrix(13,30)/(groebnerMatrix(13,20))-groebnerMatrix(18,34)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,31) = (groebnerMatrix(13,31)/(groebnerMatrix(13,20))-groebnerMatrix(18,35)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,32) = groebnerMatrix(13,32)/(groebnerMatrix(13,20));
  groebnerMatrix(19,33) = groebnerMatrix(13,33)/(groebnerMatrix(13,20));
  groebnerMatrix(19,34) = groebnerMatrix(13,34)/(groebnerMatrix(13,20));
  groebnerMatrix(19,35) = (groebnerMatrix(13,35)/(groebnerMatrix(13,20))-groebnerMatrix(18,36)/(groebnerMatrix(18,30)));
  groebnerMatrix(19,36) = groebnerMatrix(13,36)/(groebnerMatrix(13,20));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial20( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(20,20) = (groebnerMatrix(12,20)/(groebnerMatrix(12,19))-groebnerMatrix(17,30)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,21) = (groebnerMatrix(12,21)/(groebnerMatrix(12,19))-groebnerMatrix(17,31)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,27) = groebnerMatrix(12,27)/(groebnerMatrix(12,19));
  groebnerMatrix(20,28) = (groebnerMatrix(12,28)/(groebnerMatrix(12,19))-groebnerMatrix(17,32)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,29) = (groebnerMatrix(12,29)/(groebnerMatrix(12,19))-groebnerMatrix(17,33)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,30) = (groebnerMatrix(12,30)/(groebnerMatrix(12,19))-groebnerMatrix(17,34)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,31) = (groebnerMatrix(12,31)/(groebnerMatrix(12,19))-groebnerMatrix(17,35)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,32) = groebnerMatrix(12,32)/(groebnerMatrix(12,19));
  groebnerMatrix(20,33) = groebnerMatrix(12,33)/(groebnerMatrix(12,19));
  groebnerMatrix(20,34) = groebnerMatrix(12,34)/(groebnerMatrix(12,19));
  groebnerMatrix(20,35) = (groebnerMatrix(12,35)/(groebnerMatrix(12,19))-groebnerMatrix(17,36)/(groebnerMatrix(17,29)));
  groebnerMatrix(20,36) = groebnerMatrix(12,36)/(groebnerMatrix(12,19));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial21( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(21,19) = (groebnerMatrix(11,19)/(groebnerMatrix(11,18))-groebnerMatrix(16,29)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,20) = (groebnerMatrix(11,20)/(groebnerMatrix(11,18))-groebnerMatrix(16,30)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,21) = (groebnerMatrix(11,21)/(groebnerMatrix(11,18))-groebnerMatrix(16,31)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,27) = groebnerMatrix(11,27)/(groebnerMatrix(11,18));
  groebnerMatrix(21,28) = (groebnerMatrix(11,28)/(groebnerMatrix(11,18))-groebnerMatrix(16,32)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,29) = (groebnerMatrix(11,29)/(groebnerMatrix(11,18))-groebnerMatrix(16,33)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,30) = (groebnerMatrix(11,30)/(groebnerMatrix(11,18))-groebnerMatrix(16,34)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,31) = (groebnerMatrix(11,31)/(groebnerMatrix(11,18))-groebnerMatrix(16,35)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,32) = groebnerMatrix(11,32)/(groebnerMatrix(11,18));
  groebnerMatrix(21,33) = groebnerMatrix(11,33)/(groebnerMatrix(11,18));
  groebnerMatrix(21,34) = groebnerMatrix(11,34)/(groebnerMatrix(11,18));
  groebnerMatrix(21,35) = (groebnerMatrix(11,35)/(groebnerMatrix(11,18))-groebnerMatrix(16,36)/(groebnerMatrix(16,28)));
  groebnerMatrix(21,36) = groebnerMatrix(11,36)/(groebnerMatrix(11,18));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial22( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(22,18) = (groebnerMatrix(10,18)/(groebnerMatrix(10,17))-groebnerMatrix(15,28)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,19) = (groebnerMatrix(10,19)/(groebnerMatrix(10,17))-groebnerMatrix(15,29)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,20) = (groebnerMatrix(10,20)/(groebnerMatrix(10,17))-groebnerMatrix(15,30)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,21) = (groebnerMatrix(10,21)/(groebnerMatrix(10,17))-groebnerMatrix(15,31)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,27) = groebnerMatrix(10,27)/(groebnerMatrix(10,17));
  groebnerMatrix(22,28) = (groebnerMatrix(10,28)/(groebnerMatrix(10,17))-groebnerMatrix(15,32)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,29) = (groebnerMatrix(10,29)/(groebnerMatrix(10,17))-groebnerMatrix(15,33)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,30) = (groebnerMatrix(10,30)/(groebnerMatrix(10,17))-groebnerMatrix(15,34)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,31) = (groebnerMatrix(10,31)/(groebnerMatrix(10,17))-groebnerMatrix(15,35)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,32) = groebnerMatrix(10,32)/(groebnerMatrix(10,17));
  groebnerMatrix(22,33) = groebnerMatrix(10,33)/(groebnerMatrix(10,17));
  groebnerMatrix(22,34) = groebnerMatrix(10,34)/(groebnerMatrix(10,17));
  groebnerMatrix(22,35) = (groebnerMatrix(10,35)/(groebnerMatrix(10,17))-groebnerMatrix(15,36)/(groebnerMatrix(15,27)));
  groebnerMatrix(22,36) = groebnerMatrix(10,36)/(groebnerMatrix(10,17));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial23( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(23,15) = -groebnerMatrix(15,28)/(groebnerMatrix(15,27));
  groebnerMatrix(23,16) = -groebnerMatrix(15,29)/(groebnerMatrix(15,27));
  groebnerMatrix(23,17) = (groebnerMatrix(9,17)/(groebnerMatrix(9,11))-groebnerMatrix(15,30)/(groebnerMatrix(15,27)));
  groebnerMatrix(23,18) = groebnerMatrix(9,18)/(groebnerMatrix(9,11));
  groebnerMatrix(23,19) = groebnerMatrix(9,19)/(groebnerMatrix(9,11));
  groebnerMatrix(23,20) = (groebnerMatrix(9,20)/(groebnerMatrix(9,11))-groebnerMatrix(15,31)/(groebnerMatrix(15,27)));
  groebnerMatrix(23,21) = groebnerMatrix(9,21)/(groebnerMatrix(9,11));
  groebnerMatrix(23,25) = -groebnerMatrix(15,32)/(groebnerMatrix(15,27));
  groebnerMatrix(23,26) = -groebnerMatrix(15,33)/(groebnerMatrix(15,27));
  groebnerMatrix(23,27) = (groebnerMatrix(9,27)/(groebnerMatrix(9,11))-groebnerMatrix(15,34)/(groebnerMatrix(15,27)));
  groebnerMatrix(23,28) = groebnerMatrix(9,28)/(groebnerMatrix(9,11));
  groebnerMatrix(23,29) = groebnerMatrix(9,29)/(groebnerMatrix(9,11));
  groebnerMatrix(23,30) = (groebnerMatrix(9,30)/(groebnerMatrix(9,11))-groebnerMatrix(15,35)/(groebnerMatrix(15,27)));
  groebnerMatrix(23,31) = groebnerMatrix(9,31)/(groebnerMatrix(9,11));
  groebnerMatrix(23,32) = groebnerMatrix(9,32)/(groebnerMatrix(9,11));
  groebnerMatrix(23,33) = groebnerMatrix(9,33)/(groebnerMatrix(9,11));
  groebnerMatrix(23,34) = (groebnerMatrix(9,34)/(groebnerMatrix(9,11))-groebnerMatrix(15,36)/(groebnerMatrix(15,27)));
  groebnerMatrix(23,35) = groebnerMatrix(9,35)/(groebnerMatrix(9,11));
  groebnerMatrix(23,36) = groebnerMatrix(9,36)/(groebnerMatrix(9,11));
}

void
opengv::absolute_pose::modules::gpnp4::sPolynomial24( Eigen::Matrix<double,25,37> & groebnerMatrix )
{
  groebnerMatrix(24,32) = groebnerMatrix(19,32)/(groebnerMatrix(19,31));
  groebnerMatrix(24,33) = groebnerMatrix(19,33)/(groebnerMatrix(19,31));
  groebnerMatrix(24,34) = groebnerMatrix(19,34)/(groebnerMatrix(19,31));
  groebnerMatrix(24,35) = (groebnerMatrix(19,35)/(groebnerMatrix(19,31))-groebnerMatrix(23,36)/(groebnerMatrix(23,35)));
  groebnerMatrix(24,36) = groebnerMatrix(19,36)/(groebnerMatrix(19,31));
}

