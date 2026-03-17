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
opengv::absolute_pose::modules::gp3p::init(
    Eigen::Matrix<double,48,85> & groebnerMatrix,
    const Eigen::Matrix<double,3,3> & f,
    const Eigen::Matrix<double,3,3> & v,
    const Eigen::Matrix<double,3,3> & p )
{
  groebnerMatrix(0,20) = -1*f(0,1);
  groebnerMatrix(0,21) = f(0,0);
  groebnerMatrix(0,32) = -1*f(0,1);
  groebnerMatrix(0,33) = f(0,0);
  groebnerMatrix(0,52) = -1*f(0,1);
  groebnerMatrix(0,53) = f(0,0);
  groebnerMatrix(0,66) = (((v(0,0)-v(0,1))+p(0,0))-p(0,1));
  groebnerMatrix(0,71) = (((v(0,0)-v(0,1))+p(0,0))-p(0,1));
  groebnerMatrix(0,75) = (-2*p(2,0)+2*p(2,1));
  groebnerMatrix(0,76) = (-2*p(1,0)+2*p(1,1));
  groebnerMatrix(0,77) = (((v(0,0)-v(0,1))-p(0,0))+p(0,1));
  groebnerMatrix(0,79) = -1*f(0,1);
  groebnerMatrix(0,80) = f(0,0);
  groebnerMatrix(0,81) = (2*p(1,0)-2*p(1,1));
  groebnerMatrix(0,82) = (-2*p(2,0)+2*p(2,1));
  groebnerMatrix(0,84) = (((-p(0,0)+p(0,1))+v(0,0))-v(0,1));
  groebnerMatrix(1,20) = -1*f(1,1);
  groebnerMatrix(1,21) = f(1,0);
  groebnerMatrix(1,32) = -1*f(1,1);
  groebnerMatrix(1,33) = f(1,0);
  groebnerMatrix(1,52) = -1*f(1,1);
  groebnerMatrix(1,53) = f(1,0);
  groebnerMatrix(1,66) = (((v(1,0)-v(1,1))+p(1,0))-p(1,1));
  groebnerMatrix(1,70) = (-2*p(2,0)+2*p(2,1));
  groebnerMatrix(1,71) = (((v(1,0)-v(1,1))-p(1,0))+p(1,1));
  groebnerMatrix(1,76) = (2*p(0,1)-2*p(0,0));
  groebnerMatrix(1,77) = (((v(1,0)-v(1,1))+p(1,0))-p(1,1));
  groebnerMatrix(1,79) = -1*f(1,1);
  groebnerMatrix(1,80) = f(1,0);
  groebnerMatrix(1,81) = (-2*p(0,0)+2*p(0,1));
  groebnerMatrix(1,83) = (2*p(2,0)-2*p(2,1));
  groebnerMatrix(1,84) = (((-p(1,0)+p(1,1))+v(1,0))-v(1,1));
  groebnerMatrix(2,20) = -1*f(2,1);
  groebnerMatrix(2,21) = f(2,0);
  groebnerMatrix(2,32) = -1*f(2,1);
  groebnerMatrix(2,33) = f(2,0);
  groebnerMatrix(2,52) = -1*f(2,1);
  groebnerMatrix(2,53) = f(2,0);
  groebnerMatrix(2,66) = (((v(2,0)-v(2,1))-p(2,0))+p(2,1));
  groebnerMatrix(2,70) = (-2*p(1,0)+2*p(1,1));
  groebnerMatrix(2,71) = (((v(2,0)-v(2,1))+p(2,0))-p(2,1));
  groebnerMatrix(2,75) = (-2*p(0,0)+2*p(0,1));
  groebnerMatrix(2,77) = (((v(2,0)-v(2,1))+p(2,0))-p(2,1));
  groebnerMatrix(2,79) = -1*f(2,1);
  groebnerMatrix(2,80) = f(2,0);
  groebnerMatrix(2,82) = (2*p(0,0)-2*p(0,1));
  groebnerMatrix(2,83) = (-2*p(1,0)+2*p(1,1));
  groebnerMatrix(2,84) = (((-p(2,0)+p(2,1))+v(2,0))-v(2,1));
  groebnerMatrix(3,19) = -1*f(0,2);
  groebnerMatrix(3,20) = f(0,1);
  groebnerMatrix(3,31) = -1*f(0,2);
  groebnerMatrix(3,32) = f(0,1);
  groebnerMatrix(3,51) = -1*f(0,2);
  groebnerMatrix(3,52) = f(0,1);
  groebnerMatrix(3,66) = (((v(0,1)+p(0,1))-v(0,2))-p(0,2));
  groebnerMatrix(3,71) = (((v(0,1)+p(0,1))-v(0,2))-p(0,2));
  groebnerMatrix(3,75) = (-2*p(2,1)+2*p(2,2));
  groebnerMatrix(3,76) = (-2*p(1,1)+2*p(1,2));
  groebnerMatrix(3,77) = (((v(0,1)-p(0,1))-v(0,2))+p(0,2));
  groebnerMatrix(3,78) = -1*f(0,2);
  groebnerMatrix(3,79) = f(0,1);
  groebnerMatrix(3,81) = (2*p(1,1)-2*p(1,2));
  groebnerMatrix(3,82) = (-2*p(2,1)+2*p(2,2));
  groebnerMatrix(3,84) = (((-p(0,1)+p(0,2))-v(0,2))+v(0,1));
  groebnerMatrix(4,19) = -1*f(1,2);
  groebnerMatrix(4,20) = f(1,1);
  groebnerMatrix(4,31) = -1*f(1,2);
  groebnerMatrix(4,32) = f(1,1);
  groebnerMatrix(4,51) = -1*f(1,2);
  groebnerMatrix(4,52) = f(1,1);
  groebnerMatrix(4,66) = (((v(1,1)+p(1,1))-v(1,2))-p(1,2));
  groebnerMatrix(4,70) = (2*p(2,2)-2*p(2,1));
  groebnerMatrix(4,71) = (((v(1,1)-p(1,1))-v(1,2))+p(1,2));
  groebnerMatrix(4,76) = (-2*p(0,1)+2*p(0,2));
  groebnerMatrix(4,77) = (((v(1,1)+p(1,1))-v(1,2))-p(1,2));
  groebnerMatrix(4,78) = -1*f(1,2);
  groebnerMatrix(4,79) = f(1,1);
  groebnerMatrix(4,81) = (-2*p(0,1)+2*p(0,2));
  groebnerMatrix(4,83) = (2*p(2,1)-2*p(2,2));
  groebnerMatrix(4,84) = (((-p(1,1)+p(1,2))-v(1,2))+v(1,1));
  groebnerMatrix(5,19) = -1*f(2,2);
  groebnerMatrix(5,20) = f(2,1);
  groebnerMatrix(5,31) = -1*f(2,2);
  groebnerMatrix(5,32) = f(2,1);
  groebnerMatrix(5,51) = -1*f(2,2);
  groebnerMatrix(5,52) = f(2,1);
  groebnerMatrix(5,66) = (((v(2,1)-p(2,1))-v(2,2))+p(2,2));
  groebnerMatrix(5,70) = (-2*p(1,1)+2*p(1,2));
  groebnerMatrix(5,71) = (((v(2,1)+p(2,1))-v(2,2))-p(2,2));
  groebnerMatrix(5,75) = (-2*p(0,1)+2*p(0,2));
  groebnerMatrix(5,77) = (((v(2,1)+p(2,1))-v(2,2))-p(2,2));
  groebnerMatrix(5,78) = -1*f(2,2);
  groebnerMatrix(5,79) = f(2,1);
  groebnerMatrix(5,82) = (2*p(0,1)-2*p(0,2));
  groebnerMatrix(5,83) = (-2*p(1,1)+2*p(1,2));
  groebnerMatrix(5,84) = (((-p(2,1)+p(2,2))-v(2,2))+v(2,1));
  groebnerMatrix(6,19) = f(0,2);
  groebnerMatrix(6,21) = -1*f(0,0);
  groebnerMatrix(6,31) = f(0,2);
  groebnerMatrix(6,33) = -1*f(0,0);
  groebnerMatrix(6,51) = f(0,2);
  groebnerMatrix(6,53) = -1*f(0,0);
  groebnerMatrix(6,66) = (((-v(0,0)-p(0,0))+v(0,2))+p(0,2));
  groebnerMatrix(6,71) = (((-v(0,0)-p(0,0))+v(0,2))+p(0,2));
  groebnerMatrix(6,75) = (2*p(2,0)-2*p(2,2));
  groebnerMatrix(6,76) = (2*p(1,0)-2*p(1,2));
  groebnerMatrix(6,77) = (((-v(0,0)+p(0,0))+v(0,2))-p(0,2));
  groebnerMatrix(6,78) = f(0,2);
  groebnerMatrix(6,80) = -1*f(0,0);
  groebnerMatrix(6,81) = (-2*p(1,0)+2*p(1,2));
  groebnerMatrix(6,82) = (2*p(2,0)-2*p(2,2));
  groebnerMatrix(6,84) = (((p(0,0)-p(0,2))-v(0,0))+v(0,2));
  groebnerMatrix(7,19) = f(1,2);
  groebnerMatrix(7,21) = -1*f(1,0);
  groebnerMatrix(7,31) = f(1,2);
  groebnerMatrix(7,33) = -1*f(1,0);
  groebnerMatrix(7,51) = f(1,2);
  groebnerMatrix(7,53) = -1*f(1,0);
  groebnerMatrix(7,66) = (((-v(1,0)-p(1,0))+v(1,2))+p(1,2));
  groebnerMatrix(7,70) = (-2*p(2,2)+2*p(2,0));
  groebnerMatrix(7,71) = (((-v(1,0)+p(1,0))+v(1,2))-p(1,2));
  groebnerMatrix(7,76) = (2*p(0,0)-2*p(0,2));
  groebnerMatrix(7,77) = (((-v(1,0)-p(1,0))+v(1,2))+p(1,2));
  groebnerMatrix(7,78) = f(1,2);
  groebnerMatrix(7,80) = -1*f(1,0);
  groebnerMatrix(7,81) = (2*p(0,0)-2*p(0,2));
  groebnerMatrix(7,83) = (-2*p(2,0)+2*p(2,2));
  groebnerMatrix(7,84) = (((p(1,0)-p(1,2))-v(1,0))+v(1,2));
  groebnerMatrix(8,19) = f(2,2);
  groebnerMatrix(8,21) = -1*f(2,0);
  groebnerMatrix(8,31) = f(2,2);
  groebnerMatrix(8,33) = -1*f(2,0);
  groebnerMatrix(8,51) = f(2,2);
  groebnerMatrix(8,53) = -1*f(2,0);
  groebnerMatrix(8,66) = (((-v(2,0)+p(2,0))+v(2,2))-p(2,2));
  groebnerMatrix(8,70) = (2*p(1,0)-2*p(1,2));
  groebnerMatrix(8,71) = (((-v(2,0)-p(2,0))+v(2,2))+p(2,2));
  groebnerMatrix(8,75) = (2*p(0,0)-2*p(0,2));
  groebnerMatrix(8,77) = (((-v(2,0)-p(2,0))+v(2,2))+p(2,2));
  groebnerMatrix(8,78) = f(2,2);
  groebnerMatrix(8,80) = -1*f(2,0);
  groebnerMatrix(8,82) = (-2*p(0,0)+2*p(0,2));
  groebnerMatrix(8,83) = (2*p(1,0)-2*p(1,2));
  groebnerMatrix(8,84) = (((p(2,0)-p(2,2))-v(2,0))+v(2,2));
}
