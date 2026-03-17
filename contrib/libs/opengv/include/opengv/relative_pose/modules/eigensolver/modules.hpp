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


#ifndef OPENGV_RELATIVE_POSE_MODULES_EIGENSOLVER_MODULES_HPP_
#define OPENGV_RELATIVE_POSE_MODULES_EIGENSOLVER_MODULES_HPP_

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
namespace eigensolver
{

double getSmallestEV(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix3d & M );
double getSmallestEVwithJacobian(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix<double,1,3> & jacobian);
Eigen::Matrix3d composeM(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley);
Eigen::Matrix3d composeMwithJacobians(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const cayley_t & cayley,
    Eigen::Matrix3d & M_jac1,
    Eigen::Matrix3d & M_jac2,
    Eigen::Matrix3d & M_jac3 );

}
}
}
}

#endif /* OPENGV_RELATIVE_POSE_MODULES_EIGENSOLVER_MODULES_HPP_ */


