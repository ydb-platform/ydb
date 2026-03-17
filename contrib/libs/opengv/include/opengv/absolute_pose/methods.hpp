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
 * \file absolute_pose/methods.hpp
 * \brief Methods for computing the absolute pose of a calibrated viewpoint.
 *
 * The collection includes both minimal and non-minimal solutions for
 * computing the absolute pose of a calibrated, either central or non-central
 * viewpoint.
 */

#ifndef OPENGV_ABSOLUTE_POSE_METHODS_HPP_
#define OPENGV_ABSOLUTE_POSE_METHODS_HPP_

#include <stdlib.h>
#include <vector>
#include <opengv/types.hpp>
#include <opengv/absolute_pose/AbsoluteAdapterBase.hpp>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{
/**
 * \brief The namespace for the absolute pose methods.
 */
namespace absolute_pose
{

/** \brief Compute the pose of a central viewpoint with known rotation using two
 *         point correspondences.
 *
 * \param[in] adapter Visitor holding bearing vectors, world points, and
 *                    known rotation.
 * \param[in] indices Indices of the two correspondences that are used for
 *                    deriving the translation.
 * \return The position of the viewpoint seen from the world frame.
 */
translation_t p2p(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a central viewpoint with known rotation using two
 *        point correspondences.
 *
 * \param[in] adapter Visitor holding bearing vectors, world points, and
 *                    known rotation.
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   translation (use default value if only two vectors provided
 *                   by the visitor anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   translation (use default value if only two vectors provided
 *                   by the visitor anyway).
 * \return The position of the viewpoint seen from the world frame.
 */
translation_t p2p(
    const AbsoluteAdapterBase & adapter,
    size_t index0 = 0,
    size_t index1 = 1 );

/**
 * \brief Compute the pose of a central viewpoint using three point
 *        correspondences and Kneip's method [1].
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \param[in] indices Indices of the three correspondences that are used for
 *                    deriving the pose.
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world, frame, maximum 4 solutions).
 */
transformations_t p3p_kneip(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a central viewpoint using three point
 *        correspondences and Kneip's method [1].
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index2 Index of the third correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame, maximum 4 solutions).
 */
transformations_t p3p_kneip(
    const AbsoluteAdapterBase & adapter,
    size_t index0 = 0,
    size_t index1 = 1,
    size_t index2 = 2 );

/**
 * \brief Compute the pose of a central viewpoint using three point correspondences
 *        and Gao's method [2].
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \param[in] indices Indices of the three correspondences that are used for
 *                    deriving the pose.
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame, maximum 4 solutions).
 */
transformations_t p3p_gao(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a central viewpoint using three point
 *        correspondences and Gao's method [2].
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index2 Index of the third correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame, maximum 4 solutions).
 */
transformations_t p3p_gao(
    const AbsoluteAdapterBase & adapter,
    size_t index0 = 0,
    size_t index1 = 1,
    size_t index2 = 2 );

/**
 * \brief Compute the pose of a non-central viewpoint using three point
 *        correspondences and Kneip's method [3].
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \param[in] indices Indices of the three correspondences that are used for
 *                    deriving the pose.
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from
 *         viewpoint to world frame, maximum 8 solutions).
 */
transformations_t gp3p(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a non-central viewpoint using three point
 *        correspondences and Kneip's method [3].
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \param[in] index2 Index of the third correspondence used for deriving the
 *                   pose (use default value if only three correspondences
 *                   provided anyway).
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from
 *         viewpoint to world frame, maximum 8 solutions).
 */
transformations_t gp3p(
    const AbsoluteAdapterBase & adapter,
    size_t index0 = 0,
    size_t index1 = 1,
    size_t index2 = 2 );

/**
 * \brief Compute the pose of a central viewpoint using the EPnP method [4].
 *        Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t epnp( const AbsoluteAdapterBase & adapter );

/**
 * \brief Compute the pose of a central viewpoint using the EPnP method [4].
 *
 * \param[in] adapter Visitor holding bearing vector to world point correspondences.
 * \param[in] indices Indices of the n correspondences that are used for
 *                    deriving the pose.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t epnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a non-central viewpoint using the gPnP method [3].
 *        Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t gpnp( const AbsoluteAdapterBase & adapter );

/**
 * \brief Compute the pose of a non-central viewpoint using the gPnP method [3].
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \param[in] indices Indices of the n correspondences that are used for
 *                    deriving the pose.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t gpnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the poses of a non-central viewpoint using the uPnP method.
 *        Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformations_t upnp( const AbsoluteAdapterBase & adapter );

/**
 * \brief Compute the poses of a non-central viewpoint using the uPnP method.
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, plus the multi-camera configuration.
 * \param[in] indices Indices of the n correspondences that are used for
 *                    deriving the pose.
 * \return Poses of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformations_t upnp(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose of a viewpoint using nonlinear optimization. Using
 *        all available correspondences. Works for central and non-central case.
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, the multi-camera configuration, plus
 *                    the initial values.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t optimize_nonlinear( const AbsoluteAdapterBase & adapter );

/**
 * \brief Compute the pose of a viewpoint using nonlinear optimization. Works
 *        for central and non-central viewpoints.
 *
 * \param[in] adapter Visitor holding bearing vector to world point
 *                    correspondences, the multi-camera configuration, plus
 *                    the initial values.
 * \param[in] indices Indices of the n correspondences that are used for
 *                    deriving the pose.
 * \return Pose of viewpoint (position seen from world frame and orientation
 *         from viewpoint to world frame, transforms points from viewpoint to
 *         world frame).
 */
transformation_t optimize_nonlinear(
    const AbsoluteAdapterBase & adapter,
    const std::vector<int> & indices );

}
}

#endif /* OPENGV_ABSOLUTE_POSE_METHODS_HPP_ */
