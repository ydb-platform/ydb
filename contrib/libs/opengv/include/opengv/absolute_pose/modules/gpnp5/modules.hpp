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


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_GPNP5_MODULES_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_GPNP5_MODULES_HPP_

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
namespace gpnp5
{

void init(
    Eigen::Matrix<double,44,80> & groebnerMatrix,
    const Eigen::Matrix<double,12,1> & a,
    Eigen::Matrix<double,12,1> & n,
    Eigen::Matrix<double,12,1> & m,
    Eigen::Matrix<double,12,1> & k,
    Eigen::Matrix<double,12,1> & l,
    Eigen::Matrix<double,12,1> & p,
    Eigen::Vector3d & c0,
    Eigen::Vector3d & c1,
    Eigen::Vector3d & c2,
    Eigen::Vector3d & c3 );
void compute( Eigen::Matrix<double,44,80> & groebnerMatrix );
void sPolynomial6( Eigen::Matrix<double,44,80> & groebnerMatrix );
void sPolynomial7( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow6_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial8( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow7_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial9( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow8_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial10( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow9_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial11( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow10_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow6_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow7_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow8_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow9_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow10_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow6_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow7_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow8_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow9_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow10_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow10_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial12( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow9_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow5_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow11_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow5_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow5_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial13( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow8_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow12_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial14( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow7_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow13_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial15( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow14_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial16( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow6_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow15_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial17( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow7_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow5_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow16_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial18( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow6_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow17_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial19( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow15_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow16_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow17_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow18_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow18_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial20( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow14_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow19_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial21( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow8_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow9_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow10_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow20_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial22( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow13_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow21_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial23( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow6_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow7_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow22_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial24( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow12_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow23_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial25( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow5_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow24_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow24_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial26( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow11_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow25_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow25_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial27( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow10_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow26_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow26_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial28( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow27_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow27_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial29( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow9_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow28_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow28_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial30( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow18_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow24_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow29_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow29_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow29_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial31( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow14_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow8_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow18_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow25_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow30_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow30_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow30_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial32( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow14_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow6_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow7_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow18_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow26_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow31_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow31_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow31_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial33( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow32_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial34( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow32_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow33_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow33_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial35( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow34_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow34_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial36( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow35_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow35_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial37( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow36_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow36_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial38( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow32_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial39( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow35_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow36_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_00001_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial40( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow36_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_00010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial41( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow32_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_10010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_10010_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_10100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow36_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_00100_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial42( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow39_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_02000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow38_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_11000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow37_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_01000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void sPolynomial43( Eigen::Matrix<double,44,80> & groebnerMatrix );
void groebnerRow38_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow39_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow40_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow41_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow42_20000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow42_10000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );
void groebnerRow42_00000_f( Eigen::Matrix<double,44,80> & groebnerMatrix, int targetRow );

}
}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_GPNP5_MODULES_HPP_ */
