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


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_GPNP3_MODULES_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_GPNP3_MODULES_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>
#include <vector>

namespace opengv
{
namespace absolute_pose
{
namespace modules
{
namespace gpnp3
{

void init(
    Eigen::Matrix<double,15,18> & groebnerMatrix,
    const Eigen::Matrix<double,12,1> & a,
    Eigen::Matrix<double,12,1> & n,
    Eigen::Matrix<double,12,1> & m,
    Eigen::Matrix<double,12,1> & k,
    Eigen::Vector3d & c0,
    Eigen::Vector3d & c1,
    Eigen::Vector3d & c2,
    Eigen::Vector3d & c3 );
void compute( Eigen::Matrix<double,15,18> & groebnerMatrix );
void sPolynomial4( Eigen::Matrix<double,15,18> & groebnerMatrix );
void sPolynomial5( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow4_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial6( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow5_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial7( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow5_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow6_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow6_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial8( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow4_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow7_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow3_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial9( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow5_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow3_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow8_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial10( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow4_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow9_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow9_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial11( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow10_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial12( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow10_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow11_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow11_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial13( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow11_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow12_010_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow12_100_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void groebnerRow12_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );
void sPolynomial14( Eigen::Matrix<double,15,18> & groebnerMatrix );
void groebnerRow13_000_f( Eigen::Matrix<double,15,18> & groebnerMatrix, int targetRow );

}
}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_GPNP3_MODULES_HPP_ */
