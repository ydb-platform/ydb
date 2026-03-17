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


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_GP3P_MODULES_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_GP3P_MODULES_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>


namespace opengv
{
namespace absolute_pose
{
namespace modules
{
namespace gp3p
{

void init(
    Eigen::Matrix<double,48,85> & groebnerMatrix,
    const Eigen::Matrix<double,3,3> & f,
    const Eigen::Matrix<double,3,3> & v,
    const Eigen::Matrix<double,3,3> & p );
void compute( Eigen::Matrix<double,48,85> & groebnerMatrix );
void sPolynomial9( Eigen::Matrix<double,48,85> & groebnerMatrix );
void sPolynomial10( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow9_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial11( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow10_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow10_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial12( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow11_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial13( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow10_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow12_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow12_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow12_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial14( Eigen::Matrix<double,48,85> & groebnerMatrix );
void sPolynomial15( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow14_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial16( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow15_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow10_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow12_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow15_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow15_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial17( Eigen::Matrix<double,48,85> & groebnerMatrix );
void sPolynomial18( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow15_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow17_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial19( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow12_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow16_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial20( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow12_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow15_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial21( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow19_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow19_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial22( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow19_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow18_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial23( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow19_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow20_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial24( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow15_100100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow16_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow21_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial25( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow15_100010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow16_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow22_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow15_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial26( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow15_100001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow16_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow23_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial27( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow12_100100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow24_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial28( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow12_100010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow25_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial29( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow12_100001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow10_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow26_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial30( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow28_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow27_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial31( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow29_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial32( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow31_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow30_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow31_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow30_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial33( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow32_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial34( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow32_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow33_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow33_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial35( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow34_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow34_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial36( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow35_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow35_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial37( Eigen::Matrix<double,48,85> & groebnerMatrix );
void sPolynomial38( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow37_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow36_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow37_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial39( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow38_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow38_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial40( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow39_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow39_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial41( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow40_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow40_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial42( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow32_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow33_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow34_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow33_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow34_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow35_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow37_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow38_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow37_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow38_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow39_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow41_100000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow41_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial43( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow33_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow37_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow42_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial44( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow31_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow30_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial45( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow36_000100_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow36_010000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow44_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial46( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow36_000010_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow45_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void sPolynomial47( Eigen::Matrix<double,48,85> & groebnerMatrix );
void groebnerRow36_000001_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow43_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );
void groebnerRow46_000000_f( Eigen::Matrix<double,48,85> & groebnerMatrix, int targetRow );

}
}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_GP3P_MODULES_HPP_ */
