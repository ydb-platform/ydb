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


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_GPNP4_MODULES_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_GPNP4_MODULES_HPP_

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
namespace gpnp4
{

void init(
    Eigen::Matrix<double,25,37> & groebnerMatrix,
    const Eigen::Matrix<double,12,1> & a,
    Eigen::Matrix<double,12,1> & n,
    Eigen::Matrix<double,12,1> & m,
    Eigen::Matrix<double,12,1> & k,
    Eigen::Matrix<double,12,1> & l,
    Eigen::Vector3d & c0,
    Eigen::Vector3d & c1,
    Eigen::Vector3d & c2,
    Eigen::Vector3d & c3 );
void compute( Eigen::Matrix<double,25,37> & groebnerMatrix );
void sPolynomial5( Eigen::Matrix<double,25,37> & groebnerMatrix );
void sPolynomial6( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow5_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial7( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow6_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial8( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow7_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial9( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow7_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow8_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow5_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow6_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow7_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow8_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow8_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial10( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow6_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow9_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial11( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow4_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow10_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow4_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial12( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow5_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow11_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial13( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow6_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow4_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow12_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial14( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow5_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow13_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial15( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow14_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow14_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial16( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow13_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow15_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow15_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow15_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial17( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow12_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow16_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow16_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial18( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow14_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow14_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow17_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow17_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial19( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow18_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial20( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow18_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow19_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow19_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial21( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow20_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow20_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial22( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow19_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow19_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow20_0001_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow20_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow21_0010_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow20_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow21_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow21_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow21_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial23( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow20_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow21_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow22_1100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow19_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow22_0100_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow22_1000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void groebnerRow22_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );
void sPolynomial24( Eigen::Matrix<double,25,37> & groebnerMatrix );
void groebnerRow23_0000_f( Eigen::Matrix<double,25,37> & groebnerMatrix, int targetRow );

}
}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_GPNP4_MODULES_HPP_ */
