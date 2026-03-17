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
opengv::absolute_pose::modules::gpnp3::sPolynomial4( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(4,9) = (groebnerMatrix(0,9)/(groebnerMatrix(0,8))-groebnerMatrix(1,9)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,10) = (groebnerMatrix(0,10)/(groebnerMatrix(0,8))-groebnerMatrix(1,10)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,11) = (groebnerMatrix(0,11)/(groebnerMatrix(0,8))-groebnerMatrix(1,11)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,12) = (groebnerMatrix(0,12)/(groebnerMatrix(0,8))-groebnerMatrix(1,12)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,13) = (groebnerMatrix(0,13)/(groebnerMatrix(0,8))-groebnerMatrix(1,13)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,14) = (groebnerMatrix(0,14)/(groebnerMatrix(0,8))-groebnerMatrix(1,14)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,15) = (groebnerMatrix(0,15)/(groebnerMatrix(0,8))-groebnerMatrix(1,15)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,16) = (groebnerMatrix(0,16)/(groebnerMatrix(0,8))-groebnerMatrix(1,16)/(groebnerMatrix(1,8)));
  groebnerMatrix(4,17) = (groebnerMatrix(0,17)/(groebnerMatrix(0,8))-groebnerMatrix(1,17)/(groebnerMatrix(1,8)));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial5( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(5,9) = (groebnerMatrix(1,9)/(groebnerMatrix(1,8))-groebnerMatrix(2,9)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,10) = (groebnerMatrix(1,10)/(groebnerMatrix(1,8))-groebnerMatrix(2,10)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,11) = (groebnerMatrix(1,11)/(groebnerMatrix(1,8))-groebnerMatrix(2,11)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,12) = (groebnerMatrix(1,12)/(groebnerMatrix(1,8))-groebnerMatrix(2,12)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,13) = (groebnerMatrix(1,13)/(groebnerMatrix(1,8))-groebnerMatrix(2,13)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,14) = (groebnerMatrix(1,14)/(groebnerMatrix(1,8))-groebnerMatrix(2,14)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,15) = (groebnerMatrix(1,15)/(groebnerMatrix(1,8))-groebnerMatrix(2,15)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,16) = (groebnerMatrix(1,16)/(groebnerMatrix(1,8))-groebnerMatrix(2,16)/(groebnerMatrix(2,8)));
  groebnerMatrix(5,17) = (groebnerMatrix(1,17)/(groebnerMatrix(1,8))-groebnerMatrix(2,17)/(groebnerMatrix(2,8)));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial6( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(6,9) = (groebnerMatrix(2,9)/(groebnerMatrix(2,8))-groebnerMatrix(3,9)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,10) = (groebnerMatrix(2,10)/(groebnerMatrix(2,8))-groebnerMatrix(3,10)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,11) = (groebnerMatrix(2,11)/(groebnerMatrix(2,8))-groebnerMatrix(3,11)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,12) = (groebnerMatrix(2,12)/(groebnerMatrix(2,8))-groebnerMatrix(3,12)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,13) = (groebnerMatrix(2,13)/(groebnerMatrix(2,8))-groebnerMatrix(3,13)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,14) = (groebnerMatrix(2,14)/(groebnerMatrix(2,8))-groebnerMatrix(3,14)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,15) = (groebnerMatrix(2,15)/(groebnerMatrix(2,8))-groebnerMatrix(3,15)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,16) = (groebnerMatrix(2,16)/(groebnerMatrix(2,8))-groebnerMatrix(3,16)/(groebnerMatrix(3,8)));
  groebnerMatrix(6,17) = (groebnerMatrix(2,17)/(groebnerMatrix(2,8))-groebnerMatrix(3,17)/(groebnerMatrix(3,8)));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial7( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(7,4) = (groebnerMatrix(4,10)/(groebnerMatrix(4,9))-groebnerMatrix(6,12)/(groebnerMatrix(6,11)));
  groebnerMatrix(7,5) = groebnerMatrix(4,11)/(groebnerMatrix(4,9));
  groebnerMatrix(7,6) = (groebnerMatrix(4,12)/(groebnerMatrix(4,9))-groebnerMatrix(6,13)/(groebnerMatrix(6,11)));
  groebnerMatrix(7,7) = groebnerMatrix(4,13)/(groebnerMatrix(4,9));
  groebnerMatrix(7,9) = -groebnerMatrix(6,14)/(groebnerMatrix(6,11));
  groebnerMatrix(7,10) = -groebnerMatrix(6,15)/(groebnerMatrix(6,11));
  groebnerMatrix(7,11) = groebnerMatrix(4,14)/(groebnerMatrix(4,9));
  groebnerMatrix(7,12) = (groebnerMatrix(4,15)/(groebnerMatrix(4,9))-groebnerMatrix(6,16)/(groebnerMatrix(6,11)));
  groebnerMatrix(7,13) = groebnerMatrix(4,16)/(groebnerMatrix(4,9));
  groebnerMatrix(7,15) = -groebnerMatrix(6,17)/(groebnerMatrix(6,11));
  groebnerMatrix(7,16) = groebnerMatrix(4,17)/(groebnerMatrix(4,9));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial8( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(8,3) = (groebnerMatrix(3,9)/(groebnerMatrix(3,8))-groebnerMatrix(6,12)/(groebnerMatrix(6,11)));
  groebnerMatrix(8,4) = groebnerMatrix(3,10)/(groebnerMatrix(3,8));
  groebnerMatrix(8,5) = (groebnerMatrix(3,11)/(groebnerMatrix(3,8))-groebnerMatrix(6,13)/(groebnerMatrix(6,11)));
  groebnerMatrix(8,6) = groebnerMatrix(3,12)/(groebnerMatrix(3,8));
  groebnerMatrix(8,7) = groebnerMatrix(3,13)/(groebnerMatrix(3,8));
  groebnerMatrix(8,8) = -groebnerMatrix(6,14)/(groebnerMatrix(6,11));
  groebnerMatrix(8,9) = -groebnerMatrix(6,15)/(groebnerMatrix(6,11));
  groebnerMatrix(8,11) = (groebnerMatrix(3,14)/(groebnerMatrix(3,8))-groebnerMatrix(6,16)/(groebnerMatrix(6,11)));
  groebnerMatrix(8,12) = groebnerMatrix(3,15)/(groebnerMatrix(3,8));
  groebnerMatrix(8,13) = groebnerMatrix(3,16)/(groebnerMatrix(3,8));
  groebnerMatrix(8,14) = -groebnerMatrix(6,17)/(groebnerMatrix(6,11));
  groebnerMatrix(8,16) = groebnerMatrix(3,17)/(groebnerMatrix(3,8));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial9( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(9,1) = groebnerMatrix(4,10)/(groebnerMatrix(4,9));
  groebnerMatrix(9,2) = -groebnerMatrix(5,11)/(groebnerMatrix(5,10));
  groebnerMatrix(9,3) = (groebnerMatrix(4,11)/(groebnerMatrix(4,9))-groebnerMatrix(5,12)/(groebnerMatrix(5,10)));
  groebnerMatrix(9,4) = groebnerMatrix(4,12)/(groebnerMatrix(4,9));
  groebnerMatrix(9,5) = -groebnerMatrix(5,13)/(groebnerMatrix(5,10));
  groebnerMatrix(9,6) = groebnerMatrix(4,13)/(groebnerMatrix(4,9));
  groebnerMatrix(9,8) = -groebnerMatrix(5,14)/(groebnerMatrix(5,10));
  groebnerMatrix(9,9) = (groebnerMatrix(4,14)/(groebnerMatrix(4,9))-groebnerMatrix(5,15)/(groebnerMatrix(5,10)));
  groebnerMatrix(9,10) = groebnerMatrix(4,15)/(groebnerMatrix(4,9));
  groebnerMatrix(9,11) = -groebnerMatrix(5,16)/(groebnerMatrix(5,10));
  groebnerMatrix(9,12) = groebnerMatrix(4,16)/(groebnerMatrix(4,9));
  groebnerMatrix(9,14) = -groebnerMatrix(5,17)/(groebnerMatrix(5,10));
  groebnerMatrix(9,15) = groebnerMatrix(4,17)/(groebnerMatrix(4,9));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial10( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(10,0) = (groebnerMatrix(3,9)/(groebnerMatrix(3,8))-groebnerMatrix(4,10)/(groebnerMatrix(4,9)));
  groebnerMatrix(10,1) = groebnerMatrix(3,10)/(groebnerMatrix(3,8));
  groebnerMatrix(10,2) = -groebnerMatrix(4,11)/(groebnerMatrix(4,9));
  groebnerMatrix(10,3) = (groebnerMatrix(3,11)/(groebnerMatrix(3,8))-groebnerMatrix(4,12)/(groebnerMatrix(4,9)));
  groebnerMatrix(10,4) = groebnerMatrix(3,12)/(groebnerMatrix(3,8));
  groebnerMatrix(10,5) = -groebnerMatrix(4,13)/(groebnerMatrix(4,9));
  groebnerMatrix(10,6) = groebnerMatrix(3,13)/(groebnerMatrix(3,8));
  groebnerMatrix(10,8) = -groebnerMatrix(4,14)/(groebnerMatrix(4,9));
  groebnerMatrix(10,9) = (groebnerMatrix(3,14)/(groebnerMatrix(3,8))-groebnerMatrix(4,15)/(groebnerMatrix(4,9)));
  groebnerMatrix(10,10) = groebnerMatrix(3,15)/(groebnerMatrix(3,8));
  groebnerMatrix(10,11) = -groebnerMatrix(4,16)/(groebnerMatrix(4,9));
  groebnerMatrix(10,12) = groebnerMatrix(3,16)/(groebnerMatrix(3,8));
  groebnerMatrix(10,14) = -groebnerMatrix(4,17)/(groebnerMatrix(4,9));
  groebnerMatrix(10,15) = groebnerMatrix(3,17)/(groebnerMatrix(3,8));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial11( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(11,11) = -groebnerMatrix(10,14)/(groebnerMatrix(10,13));
  groebnerMatrix(11,12) = (groebnerMatrix(8,12)/(groebnerMatrix(8,7))-groebnerMatrix(10,15)/(groebnerMatrix(10,13)));
  groebnerMatrix(11,13) = (groebnerMatrix(8,13)/(groebnerMatrix(8,7))-groebnerMatrix(10,16)/(groebnerMatrix(10,13)));
  groebnerMatrix(11,14) = groebnerMatrix(8,14)/(groebnerMatrix(8,7));
  groebnerMatrix(11,15) = groebnerMatrix(8,15)/(groebnerMatrix(8,7));
  groebnerMatrix(11,16) = (groebnerMatrix(8,16)/(groebnerMatrix(8,7))-groebnerMatrix(10,17)/(groebnerMatrix(10,13)));
  groebnerMatrix(11,17) = groebnerMatrix(8,17)/(groebnerMatrix(8,7));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial12( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(12,7) = (groebnerMatrix(7,7)/(groebnerMatrix(7,6))-groebnerMatrix(9,13)/(groebnerMatrix(9,12)));
  groebnerMatrix(12,11) = -groebnerMatrix(9,14)/(groebnerMatrix(9,12));
  groebnerMatrix(12,12) = (groebnerMatrix(7,12)/(groebnerMatrix(7,6))-groebnerMatrix(9,15)/(groebnerMatrix(9,12)));
  groebnerMatrix(12,13) = (groebnerMatrix(7,13)/(groebnerMatrix(7,6))-groebnerMatrix(9,16)/(groebnerMatrix(9,12)));
  groebnerMatrix(12,14) = groebnerMatrix(7,14)/(groebnerMatrix(7,6));
  groebnerMatrix(12,15) = groebnerMatrix(7,15)/(groebnerMatrix(7,6));
  groebnerMatrix(12,16) = (groebnerMatrix(7,16)/(groebnerMatrix(7,6))-groebnerMatrix(9,17)/(groebnerMatrix(9,12)));
  groebnerMatrix(12,17) = groebnerMatrix(7,17)/(groebnerMatrix(7,6));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial13( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(13,7) = groebnerMatrix(9,13)/(groebnerMatrix(9,12));
  groebnerMatrix(13,9) = -groebnerMatrix(10,14)/(groebnerMatrix(10,13));
  groebnerMatrix(13,10) = -groebnerMatrix(10,15)/(groebnerMatrix(10,13));
  groebnerMatrix(13,11) = groebnerMatrix(9,14)/(groebnerMatrix(9,12));
  groebnerMatrix(13,12) = (groebnerMatrix(9,15)/(groebnerMatrix(9,12))-groebnerMatrix(10,16)/(groebnerMatrix(10,13)));
  groebnerMatrix(13,13) = groebnerMatrix(9,16)/(groebnerMatrix(9,12));
  groebnerMatrix(13,15) = -groebnerMatrix(10,17)/(groebnerMatrix(10,13));
  groebnerMatrix(13,16) = groebnerMatrix(9,17)/(groebnerMatrix(9,12));
}

void
opengv::absolute_pose::modules::gpnp3::sPolynomial14( Eigen::Matrix<double,15,18> & groebnerMatrix )
{
  groebnerMatrix(14,14) = groebnerMatrix(10,14)/(groebnerMatrix(10,13));
  groebnerMatrix(14,15) = groebnerMatrix(10,15)/(groebnerMatrix(10,13));
  groebnerMatrix(14,16) = (groebnerMatrix(10,16)/(groebnerMatrix(10,13))-groebnerMatrix(13,17)/(groebnerMatrix(13,16)));
  groebnerMatrix(14,17) = groebnerMatrix(10,17)/(groebnerMatrix(10,13));
}

