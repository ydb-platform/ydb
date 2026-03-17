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
 * \file point_cloud/methods.hpp
 * \brief Methods for computing the transformation between two frames that
 *        contain point-clouds.
 */

#ifndef OPENGV_POINT_CLOUD_METHODS_HPP_
#define OPENGV_POINT_CLOUD_METHODS_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/point_cloud/PointCloudAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the point-cloud alignment methods.
 */
namespace point_cloud
{

/**
 * \brief Compute the transformation between two frames containing point clouds,
 *        following Arun's method [13]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding world-point correspondences.
 * \return Transformation from frame 2 back to frame 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of frame 2 seen from
 *         frame 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         frame 2 to frame 1).
 */
transformation_t threept_arun( const PointCloudAdapterBase & adapter );

/**
 * \brief Compute the transformation between two frames containing point clouds,
 *        following Arun's method [13].
 *
 * \param[in] adapter Visitor holding world-point correspondences.
 * \param[in] indices Indices of the correspondences used for deriving the
 *                    transformation.
 * \return Transformation from frame 2 back to frame 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of frame 2 seen from
 *         frame 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         frame 2 to frame 1).
 */
transformation_t threept_arun(
    const PointCloudAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the transformation between two frames containing point clouds
 *        using nonlinear optimization. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding world-point correspondences, plus the
 *                    initial values.
 * \return Transformation from frame 2 back to frame 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of frame 2 seen from
 *         frame 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         frame 2 to frame 1).
 */
transformation_t optimize_nonlinear( PointCloudAdapterBase & adapter );

/**
 * \brief Compute the transformation between two frames containing point clouds.
 *        Using nonlinear optimization.
 *
 * \param[in] adapter Visitor holding world-point correspondences, plus the
 *                    initial values.
 * \param[in] indices Indices of the correspondences used for optimization.
 * \return Transformation from frame 2 back to frame 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of frame 2 seen from
 *         frame 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         frame 2 to frame 1).
 */
transformation_t optimize_nonlinear(
    PointCloudAdapterBase & adapter,
    const std::vector<int> & indices );

}
}

#endif /* OPENGV_POINT_CLOUD_METHODS_HPP_ */
