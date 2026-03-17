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
opengv::absolute_pose::modules::gpnp2::groebnerRow5_00_f( Eigen::Matrix<double,10,6> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(5,1);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,2) -= factor * groebnerMatrix(5,2);
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(5,3);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(5,4);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(5,5);
}

void
opengv::absolute_pose::modules::gpnp2::groebnerRow6_00_f( Eigen::Matrix<double,10,6> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,2) / groebnerMatrix(6,2);
  groebnerMatrix(targetRow,2) = 0.0;
  groebnerMatrix(targetRow,3) -= factor * groebnerMatrix(6,3);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(6,4);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(6,5);
}

void
opengv::absolute_pose::modules::gpnp2::groebnerRow7_10_f( Eigen::Matrix<double,10,6> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,1) / groebnerMatrix(7,3);
  groebnerMatrix(targetRow,1) = 0.0;
  groebnerMatrix(targetRow,2) -= factor * groebnerMatrix(7,4);
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(7,5);
}

void
opengv::absolute_pose::modules::gpnp2::groebnerRow7_00_f( Eigen::Matrix<double,10,6> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,3) / groebnerMatrix(7,3);
  groebnerMatrix(targetRow,3) = 0.0;
  groebnerMatrix(targetRow,4) -= factor * groebnerMatrix(7,4);
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(7,5);
}

void
opengv::absolute_pose::modules::gpnp2::groebnerRow8_00_f( Eigen::Matrix<double,10,6> & groebnerMatrix, int targetRow )
{
  double factor = groebnerMatrix(targetRow,4) / groebnerMatrix(8,4);
  groebnerMatrix(targetRow,4) = 0.0;
  groebnerMatrix(targetRow,5) -= factor * groebnerMatrix(8,5);
}

