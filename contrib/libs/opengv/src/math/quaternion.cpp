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


#include <opengv/math/quaternion.hpp>

opengv::rotation_t
opengv::math::quaternion2rot( const quaternion_t & q)
{
  rotation_t R;

  R(0,0) = pow(q[0],2)+pow(q[1],2)-pow(q[2],2)-pow(q[3],2);
  R(0,1) = 2.0*(q[1]*q[2]-q[0]*q[3]);
  R(0,2) = 2.0*(q[1]*q[3]+q[0]*q[2]);
  R(1,0) = 2.0*(q[1]*q[2]+q[0]*q[3]);
  R(1,1) = pow(q[0],2)-pow(q[1],2)+pow(q[2],2)-pow(q[3],2);
  R(1,2) = 2.0*(q[2]*q[3]-q[0]*q[1]);
  R(2,0) = 2.0*(q[1]*q[3]-q[0]*q[2]);
  R(2,1) = 2.0*(q[2]*q[3]+q[0]*q[1]);
  R(2,2) = pow(q[0],2)-pow(q[1],2)-pow(q[2],2)+pow(q[3],2);

  return R;
}

opengv::quaternion_t
opengv::math::rot2quaternion( const rotation_t & R )
{
  quaternion_t q;
    
  q[0] = ( R(0,0) + R(1,1) + R(2,2) + 1.0) / 4.0;
  q[1] = ( R(0,0) - R(1,1) - R(2,2) + 1.0) / 4.0;
  q[2] = (-R(0,0) + R(1,1) - R(2,2) + 1.0) / 4.0;
  q[3] = (-R(0,0) - R(1,1) + R(2,2) + 1.0) / 4.0;
  if(q[0] < 0.0) q[0] = 0.0;
  if(q[1] < 0.0) q[1] = 0.0;
  if(q[2] < 0.0) q[2] = 0.0;
  if(q[3] < 0.0) q[3] = 0.0;
  q[0] = sqrt(q[0]);
  q[1] = sqrt(q[1]);
  q[2] = sqrt(q[2]);
  q[3] = sqrt(q[3]);
  if(q[0] >= q[1] && q[0] >= q[2] && q[0] >= q[3])
  {
    q[0] *= +1.0;
    q[1] *= ((R(2,1)-R(1,2))/fabs(R(2,1)-R(1,2)));
    q[2] *= ((R(0,2)-R(2,0))/fabs(R(0,2)-R(2,0)));
    q[3] *= ((R(1,0)-R(0,1))/fabs(R(1,0)-R(0,1)));
  }
  else if(q[1] >= q[0] && q[1] >= q[2] && q[1] >= q[3])
  {
    q[0] *= ((R(2,1)-R(1,2))/fabs(R(2,1)-R(1,2)));
    q[1] *= +1.0;
    q[2] *= ((R(1,0)+R(0,1))/fabs(R(1,0)+R(0,1)));
    q[3] *= ((R(0,2)+R(2,0))/fabs(R(0,2)+R(2,0)));
  }
  else if(q[2] >= q[0] && q[2] >= q[1] && q[2] >= q[3])
  {
    q[0] *= ((R(0,2)-R(2,0))/fabs(R(0,2)-R(2,0)));
    q[1] *= ((R(1,0)+R(0,1))/fabs(R(1,0)+R(0,1)));
    q[2] *= +1.0;
    q[3] *= ((R(2,1)+R(1,2))/fabs(R(2,1)+R(1,2)));
  }
  else if(q[3] >= q[0] && q[3] >= q[1] && q[3] >= q[2])
  {
    q[0] *= ((R(1,0)-R(0,1))/fabs(R(1,0)-R(0,1)));
    q[1] *= ((R(2,0)+R(0,2))/fabs(R(2,0)+R(0,2)));
    q[2] *= ((R(2,1)+R(1,2))/fabs(R(2,1)+R(1,2)));  
    q[3] *= +1.0;
  }
  else
    {};/*std::cout << "quaternion error" << std::endl;*/
  
  double scale = sqrt(q[0]*q[0]+q[1]*q[1]+q[2]*q[2]+q[3]*q[3]);
  q[0] /= scale;
  q[1] /= scale;
  q[2] /= scale;
  q[3] /= scale;
  
  return q;
}
