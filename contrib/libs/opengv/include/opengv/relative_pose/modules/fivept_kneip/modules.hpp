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


#ifndef OPENGV_RELATIVE_POSE_MODULES_FIVEPT_KNEIP_MODULES_HPP_
#define OPENGV_RELATIVE_POSE_MODULES_FIVEPT_KNEIP_MODULES_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>
#include <vector>

namespace opengv
{
namespace relative_pose
{
namespace modules
{
namespace fivept_kneip
{

Eigen::Matrix<double,1,197> initEpncpRowR(
    std::vector<Eigen::Matrix3d, Eigen::aligned_allocator<Eigen::Matrix3d> > & c123_1,
    std::vector<Eigen::Matrix3d, Eigen::aligned_allocator<Eigen::Matrix3d> > & c123_2);
void initMatrix( Eigen::Matrix<double,66,197> & groebnerMatrix );
void computeBasis( Eigen::Matrix<double,66,197> & groebnerMatrix );
void sPolynomial30( Eigen::Matrix<double,66,197> & groebnerMatrix );
void sPolynomial31( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow30_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial32( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow31_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial33( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow32_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial34( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow33_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial35( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow34_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial36( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow35_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial37( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow36_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial38( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow37_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial39( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow38_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial40( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow38_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow39_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow39_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial41( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow40_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow33_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow34_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow35_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow36_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow37_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial42( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow41_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow32_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial43( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow42_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial44( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow43_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow31_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial45( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow44_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow30_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial46( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow45_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial47( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow46_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial48( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow47_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial49( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow48_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial50( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow49_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial51( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow50_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial52( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow32_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow33_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow51_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial53( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow52_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial54( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow30_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow31_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow53_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial55( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow39_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow54_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial56( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow38_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow55_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial57( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow37_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow56_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial58( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow36_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow57_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial59( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow35_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow58_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial60( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow34_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow59_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial61( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow33_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow60_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial62( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow61_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow61_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow61_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial63( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow32_000100000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow62_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow62_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow62_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial64( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow63_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow63_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void sPolynomial65( Eigen::Matrix<double,66,197> & groebnerMatrix );
void groebnerRow63_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow64_010000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow64_100000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );
void groebnerRow64_000000000_f( Eigen::Matrix<double,66,197> & groebnerMatrix, int targetRow );

}
}
}
}

#endif /* OPENGV_RELATIVE_POSE_MODULES_FIVEPT_KNEIP_MODULES_HPP_ */
