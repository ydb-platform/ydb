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


#ifndef OPENGV_RELATIVE_POSE_MODULES_MAIN_HPP_
#define OPENGV_RELATIVE_POSE_MODULES_MAIN_HPP_

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

void fivept_stewenius_main(
    const Eigen::Matrix<double,9,4> & EE,
    complexEssentials_t & complexEssentials );
void fivept_nister_main(
    const Eigen::Matrix<double,9,4> & EE,
    essentials_t & essentials );
void fivept_kneip_main(
    const Eigen::Matrix<double,3,5> & f1,
    const Eigen::Matrix<double,3,5> & f2,
    rotations_t & rotations );
void eigensolver_main(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    eigensolverOutput_t & output );
void sixpt_main(
    Eigen::Matrix<double,6,6> & L1,
    Eigen::Matrix<double,6,6> & L2,
    rotations_t & solutions);
void ge_main(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    const cayley_t & startingPoint,
    geOutput_t & output );
void ge_main2(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    const cayley_t & startingPoint,
    geOutput_t & output );
void ge_plot(
    const Eigen::Matrix3d & xxF,
    const Eigen::Matrix3d & yyF,
    const Eigen::Matrix3d & zzF,
    const Eigen::Matrix3d & xyF,
    const Eigen::Matrix3d & yzF,
    const Eigen::Matrix3d & zxF,
    const Eigen::Matrix<double,3,9> & x1P,
    const Eigen::Matrix<double,3,9> & y1P,
    const Eigen::Matrix<double,3,9> & z1P,
    const Eigen::Matrix<double,3,9> & x2P,
    const Eigen::Matrix<double,3,9> & y2P,
    const Eigen::Matrix<double,3,9> & z2P,
    const Eigen::Matrix<double,9,9> & m11P,
    const Eigen::Matrix<double,9,9> & m12P,
    const Eigen::Matrix<double,9,9> & m22P,
    geOutput_t & output );
}
}
}

#endif /* OPENGV_RELATIVE_POSE_MODULES_MAIN_HPP_ */


