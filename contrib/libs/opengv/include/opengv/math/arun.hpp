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

/**
 * \file arun.hpp
 * \brief Arun's method for computing the rotation between two point sets.
 */

#ifndef OPENGV_ARUN_HPP_
#define OPENGV_ARUN_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>
#include <opengv/types.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace of the math tools.
 */
namespace math
{

/**
 * \brief Arun's method for computing the rotation between two point sets.
 *        Core function [13].
 *
 * \param[in] Hcross The summation over the exterior products between the
 *            normalized points.
 * \return The rotation matrix that aligns the points.
 */
rotation_t arun( const Eigen::MatrixXd & Hcross );

/**
 * \brief Arun's method for complete point cloud alignment [13]. The method
 *        actually does the same than threept_arun, but has a different
 *        interface.
 *
 * \param[in] p1 The points expressed in the first frame.
 * \param[in] p2 The points expressed in the second frame.
 * \return The Transformation from frame 2 to frame 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of frame 2 seen from
 *         frame 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         frame 2 to frame 1).
 */
transformation_t arun_complete( const points_t & p1, const points_t & p2 );

}
}

#endif /* OPENGV_ARUN_HPP_ */
