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


#include <opengv/math/cayley.hpp>

opengv::rotation_t
opengv::math::cayley2rot( const cayley_t & cayley)
{
  rotation_t R;
  double scale = 1+pow(cayley[0],2)+pow(cayley[1],2)+pow(cayley[2],2);

  R(0,0) = 1+pow(cayley[0],2)-pow(cayley[1],2)-pow(cayley[2],2);
  R(0,1) = 2*(cayley[0]*cayley[1]-cayley[2]);
  R(0,2) = 2*(cayley[0]*cayley[2]+cayley[1]);
  R(1,0) = 2*(cayley[0]*cayley[1]+cayley[2]);
  R(1,1) = 1-pow(cayley[0],2)+pow(cayley[1],2)-pow(cayley[2],2);
  R(1,2) = 2*(cayley[1]*cayley[2]-cayley[0]);
  R(2,0) = 2*(cayley[0]*cayley[2]-cayley[1]);
  R(2,1) = 2*(cayley[1]*cayley[2]+cayley[0]);
  R(2,2) = 1-pow(cayley[0],2)-pow(cayley[1],2)+pow(cayley[2],2);

  R = (1/scale) * R;
  return R;
}

opengv::rotation_t
opengv::math::cayley2rot_reduced( const cayley_t & cayley)
{
  rotation_t R;

  R(0,0) = 1+pow(cayley[0],2)-pow(cayley[1],2)-pow(cayley[2],2);
  R(0,1) = 2*(cayley[0]*cayley[1]-cayley[2]);
  R(0,2) = 2*(cayley[0]*cayley[2]+cayley[1]);
  R(1,0) = 2*(cayley[0]*cayley[1]+cayley[2]);
  R(1,1) = 1-pow(cayley[0],2)+pow(cayley[1],2)-pow(cayley[2],2);
  R(1,2) = 2*(cayley[1]*cayley[2]-cayley[0]);
  R(2,0) = 2*(cayley[0]*cayley[2]-cayley[1]);
  R(2,1) = 2*(cayley[1]*cayley[2]+cayley[0]);
  R(2,2) = 1-pow(cayley[0],2)-pow(cayley[1],2)+pow(cayley[2],2);

  return R;
}

opengv::cayley_t
opengv::math::rot2cayley( const rotation_t & R )
{
  Eigen::Matrix3d C1;
  Eigen::Matrix3d C2;
  Eigen::Matrix3d C;
  C1 = R-Eigen::Matrix3d::Identity();
  C2 = R+Eigen::Matrix3d::Identity();
  C = C1 * C2.inverse();

  cayley_t cayley;
  cayley[0] = -C(1,2);
  cayley[1] = C(0,2);
  cayley[2] = -C(0,1);

  return cayley;
}
