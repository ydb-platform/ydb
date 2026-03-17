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


#ifndef OPENGV_RELATIVE_POSE_MODULES_SIXPT_MODULES_HPP_
#define OPENGV_RELATIVE_POSE_MODULES_SIXPT_MODULES_HPP_

#define EQS 60

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>
#include <opengv/types.hpp>

namespace opengv
{
namespace relative_pose
{
namespace modules
{
namespace sixpt
{

//my solver
void fillRow1(
    const Eigen::Matrix<double,6,1> & l01,
    const Eigen::Matrix<double,6,1> & l02,
    const Eigen::Matrix<double,6,1> & l11,
    const Eigen::Matrix<double,6,1> & l12,
    std::vector<double> & c0,
    std::vector<double> & c1,
    std::vector<double> & c2 );
void fillRow2(
    Eigen::Matrix<double,EQS,84> & M1,
    int row,
    const std::vector<double> (&m0)[3],
    const std::vector<double> (&m1)[3],
    const std::vector<double> (&m2)[3] );
void setupAction(
    const std::vector<Eigen::Matrix<double,6,1>,Eigen::aligned_allocator< Eigen::Matrix<double,6,1> > > & L1,
    const std::vector<Eigen::Matrix<double,6,1>,Eigen::aligned_allocator< Eigen::Matrix<double,6,1> > > & L2,
    Eigen::Matrix<double,64,64> & Action );

}
}
}
}

#endif /* OPENGV_RELATIVE_POSE_MODULES_FIVEPT_STEWENIUS_MODULES_HPP_ */


