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


#include <opengv/absolute_pose/modules/gpnp2/modules.hpp>


void
opengv::absolute_pose::modules::gpnp2::sPolynomial5( Eigen::Matrix<double,10,6> & groebnerMatrix )
{
  groebnerMatrix(5,1) = (groebnerMatrix(0,1)/(groebnerMatrix(0,0))-groebnerMatrix(1,1)/(groebnerMatrix(1,0)));
  groebnerMatrix(5,2) = (groebnerMatrix(0,2)/(groebnerMatrix(0,0))-groebnerMatrix(1,2)/(groebnerMatrix(1,0)));
  groebnerMatrix(5,3) = (groebnerMatrix(0,3)/(groebnerMatrix(0,0))-groebnerMatrix(1,3)/(groebnerMatrix(1,0)));
  groebnerMatrix(5,4) = (groebnerMatrix(0,4)/(groebnerMatrix(0,0))-groebnerMatrix(1,4)/(groebnerMatrix(1,0)));
  groebnerMatrix(5,5) = (groebnerMatrix(0,5)/(groebnerMatrix(0,0))-groebnerMatrix(1,5)/(groebnerMatrix(1,0)));
}

void
opengv::absolute_pose::modules::gpnp2::sPolynomial6( Eigen::Matrix<double,10,6> & groebnerMatrix )
{
  groebnerMatrix(6,1) = (groebnerMatrix(1,1)/(groebnerMatrix(1,0))-groebnerMatrix(2,1)/(groebnerMatrix(2,0)));
  groebnerMatrix(6,2) = (groebnerMatrix(1,2)/(groebnerMatrix(1,0))-groebnerMatrix(2,2)/(groebnerMatrix(2,0)));
  groebnerMatrix(6,3) = (groebnerMatrix(1,3)/(groebnerMatrix(1,0))-groebnerMatrix(2,3)/(groebnerMatrix(2,0)));
  groebnerMatrix(6,4) = (groebnerMatrix(1,4)/(groebnerMatrix(1,0))-groebnerMatrix(2,4)/(groebnerMatrix(2,0)));
  groebnerMatrix(6,5) = (groebnerMatrix(1,5)/(groebnerMatrix(1,0))-groebnerMatrix(2,5)/(groebnerMatrix(2,0)));
}

void
opengv::absolute_pose::modules::gpnp2::sPolynomial7( Eigen::Matrix<double,10,6> & groebnerMatrix )
{
  groebnerMatrix(7,1) = (groebnerMatrix(2,1)/(groebnerMatrix(2,0))-groebnerMatrix(3,1)/(groebnerMatrix(3,0)));
  groebnerMatrix(7,2) = (groebnerMatrix(2,2)/(groebnerMatrix(2,0))-groebnerMatrix(3,2)/(groebnerMatrix(3,0)));
  groebnerMatrix(7,3) = (groebnerMatrix(2,3)/(groebnerMatrix(2,0))-groebnerMatrix(3,3)/(groebnerMatrix(3,0)));
  groebnerMatrix(7,4) = (groebnerMatrix(2,4)/(groebnerMatrix(2,0))-groebnerMatrix(3,4)/(groebnerMatrix(3,0)));
  groebnerMatrix(7,5) = (groebnerMatrix(2,5)/(groebnerMatrix(2,0))-groebnerMatrix(3,5)/(groebnerMatrix(3,0)));
}

void
opengv::absolute_pose::modules::gpnp2::sPolynomial8( Eigen::Matrix<double,10,6> & groebnerMatrix )
{
  groebnerMatrix(8,1) = (groebnerMatrix(3,1)/(groebnerMatrix(3,0))-groebnerMatrix(4,1)/(groebnerMatrix(4,0)));
  groebnerMatrix(8,2) = (groebnerMatrix(3,2)/(groebnerMatrix(3,0))-groebnerMatrix(4,2)/(groebnerMatrix(4,0)));
  groebnerMatrix(8,3) = (groebnerMatrix(3,3)/(groebnerMatrix(3,0))-groebnerMatrix(4,3)/(groebnerMatrix(4,0)));
  groebnerMatrix(8,4) = (groebnerMatrix(3,4)/(groebnerMatrix(3,0))-groebnerMatrix(4,4)/(groebnerMatrix(4,0)));
  groebnerMatrix(8,5) = (groebnerMatrix(3,5)/(groebnerMatrix(3,0))-groebnerMatrix(4,5)/(groebnerMatrix(4,0)));
}

void
opengv::absolute_pose::modules::gpnp2::sPolynomial9( Eigen::Matrix<double,10,6> & groebnerMatrix )
{
  groebnerMatrix(9,3) = groebnerMatrix(6,3)/(groebnerMatrix(6,2));
  groebnerMatrix(9,4) = (groebnerMatrix(6,4)/(groebnerMatrix(6,2))-groebnerMatrix(8,5)/(groebnerMatrix(8,4)));
  groebnerMatrix(9,5) = groebnerMatrix(6,5)/(groebnerMatrix(6,2));
}

