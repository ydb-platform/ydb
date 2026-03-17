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
 * \file triangulation/methods.hpp
 * \brief Some triangulation methods. Not exhaustive.
 */

#ifndef OPENGV_TRIANGULATION_METHODS_HPP_
#define OPENGV_TRIANGULATION_METHODS_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/relative_pose/RelativeAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the triangulation methods.
 */
namespace triangulation
{

/**
 * \brief Compute the position of a 3D point seen from two viewpoints. Linear
 *        Method.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    relative transformation.
 * \param[in] index The index of the correspondence being triangulated.
 * \return The 3D point expressed in the first viewpoint.
 */
point_t triangulate(
    const relative_pose::RelativeAdapterBase & adapter, size_t index );

/**
 * \brief Compute the position of a 3D point seen from two viewpoints. Fast
 *        non-linear approximation (closed-form).
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    relative transformation.
 * \param[in] index The index of the correspondence being triangulated.
 * \return The 3D point expressed in the first viewpoint.
 */
point_t triangulate2(
    const relative_pose::RelativeAdapterBase & adapter, size_t index );

}

}

#endif /* OPENGV_TRIANGULATION_METHODS_HPP_ */
