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


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_MAIN_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_MAIN_HPP_

#include <stdlib.h>
#include <opengv/types.hpp>

namespace opengv
{
namespace absolute_pose
{
namespace modules
{

void p3p_kneip_main(
    const bearingVectors_t & f,
    const points_t & p,
    transformations_t & solutions );
void p3p_gao_main(
    const bearingVectors_t & f,
    const points_t & p,
    transformations_t & solutions );
void gp3p_main(
    const Eigen::Matrix3d & f,
    const Eigen::Matrix3d & v,
    const Eigen::Matrix3d & p,
    transformations_t & solutions );
void gpnp_main(
    const Eigen::Matrix<double,12,1> & a,
    const Eigen::Matrix<double,12,12> & V,
    const points_t & c,
    transformation_t & transformation );
double gpnp_evaluate(
    const Eigen::Matrix<double,12,1> & solution,
    const points_t & c,
    translation_t & t,
    rotation_t & R );
void gpnp_optimize(
    const Eigen::Matrix<double,12,1> & a,
    const Eigen::Matrix<double,12,12> & V,
    const points_t & c,
    std::vector<double> & factors );
void upnp_fill_s(
    const Eigen::Vector4d & quaternion,
    Eigen::Matrix<double,10,1> & s );
void upnp_main(
    const Eigen::Matrix<double,10,10> & M,
    const Eigen::Matrix<double,1,10> & C,
    double gamma,
    std::vector<
        std::pair<double,Eigen::Vector4d>,
        Eigen::aligned_allocator< std::pair<double,Eigen::Vector4d> >
        > & quaternions );
void upnp_main_sym(
    const Eigen::Matrix<double,10,10> & M,
    const Eigen::Matrix<double,1,10> & C,
    double gamma,
    std::vector<
        std::pair<double,Eigen::Vector4d>,
        Eigen::aligned_allocator< std::pair<double,Eigen::Vector4d> >
        > & quaternions );

}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_MAIN_HPP_ */
