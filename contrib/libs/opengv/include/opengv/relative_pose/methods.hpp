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
 * \file relative_pose/methods.hpp
 * \brief A collection of methods for computing the relative pose between two
 *        calibrated central or non-central viewpoints.
 */

#ifndef OPENGV_RELATIVE_POSE_METHODS_HPP_
#define OPENGV_RELATIVE_POSE_METHODS_HPP_

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
 * \brief The namespace for the relative pose methods.
 */
namespace relative_pose
{

/**
 * \brief Compute the translation between two central viewpoints with known
 *        relative rotation, and using two correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences and known
 *                    relative rotation.
 * \param[in] unrotate Set to true if known rotation should be used to
 *                     unrotate bearing vectors from viewpoint 2 (not required
 *                     in case of identity rotation).
 * \param[in] indices Indices of the two correspondences used for deriving the
 *                    translation.
 * \return Position of viewpoint 2 seen from viewpoint 1.
 */
translation_t twopt(
    const RelativeAdapterBase & adapter,
    bool unrotate,
    const std::vector<int> & indices );

/**
 * \brief Compute the translation between two central viewpoints with known
 *        relative rotation, and using two correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences and known
 *                    relative rotation.
 * \param[in] unrotate Set to true if known rotation should be used to
 *                     unrotate bearing vectors from viewpoint 2 (not required
 *                     in case of identity rotation).
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   translation (use default value if only two provided
 *                   anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   translation (use default vector if only two provided
 *                   anyway).
 * \return Position of viewpoint 2 seen from viewpoint 1.
 */
translation_t twopt(
    const RelativeAdapterBase & adapter,
    bool unrotate,
    size_t index0 = 0,
    size_t index1 = 1 );

/**
 * \brief Compute the rotation between two central viewpoints with pure
 *        rotation change, using only two correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the two correspondences used for deriving the
 *                    rotation.
 * \return Rotation from viewpoint 2 to viewpoint 1.
 */
rotation_t twopt_rotationOnly(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the rotation between two central viewpoints with pure
 *        rotation change, using only two correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] index0 Index of the first correspondence used for deriving the
 *                   rotation (use default value if only two provided anyway).
 * \param[in] index1 Index of the second correspondence used for deriving the
 *                   rotation (use default value if only two provided anyway).
 * \return Rotation from viewpoint 2 to viewpoint 1.
 */
rotation_t twopt_rotationOnly(
    const RelativeAdapterBase & adapter,
    size_t index0 = 0,
    size_t index1 = 1 );

/**
 * \brief Compute the rotation between two central viewpoints with pure
 *        rotation change using Arun's method [13]. Using all available
 *        correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \return Rotation from viewpoint 2 to viewpoint 1.
 */
rotation_t rotationOnly( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the rotation between two central viewpoints with pure
 *        rotation change using Arun's method [13].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation.
 * \return Rotation from viewpoint 2 to viewpoint 1.
 */
rotation_t rotationOnly(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the complex essential matrices between two central viewpoints
 *        using Stewenius' method [5]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \return Complex essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum of 10 solutions).
 */
complexEssentials_t fivept_stewenius( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the complex essential matrices between two central viewpoints
 *        using Stewenius' method [5].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the essential matrices.
 * \return Complex essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum of 10 solutions).
 */
complexEssentials_t fivept_stewenius(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the essential matrices between two central viewpoints using
 *        Nister's method [6]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \return Real essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum of 10 solutions).
 */
essentials_t fivept_nister( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the essential matrices between two central viewpoints using
 *        Nister's method [6].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the essential matrices.
 * \return Real essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum of 10 solutions).
 */
essentials_t fivept_nister(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the rotation matrices between two central viewpoints using
 *        Kneips's method [7]. Only minimal case.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrices.
 * \return Rotation matrices from viewpoint 2 to viewpoint 1
 *         (maximum 20 solutions).
 */
rotations_t fivept_kneip(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the essential matrices between two central viewpoints using
 *        the seven-point algorithm [8]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \return Real essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum 3 solutions).
 */
essentials_t sevenpt( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the essential matrices between two central viewpoints using
 *        the seven-point algorithm [8].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the essential matrices.
 * \return Real essential matrices
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1, maximum 3 solutions).
 */
essentials_t sevenpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the essential matrix between two central viewpoints using
 *        the eight-point algorithm [9,10]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \return Real essential matrix
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1).
 */
essential_t eightpt( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the essential matrix between two central viewpoints using
 *        the eight-point algorithm [9,10].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the essential matrix.
 * \return Real essential matrix
 *         (\f$ \mathbf{E}= \f$ skew\f$(\mathbf{t})\mathbf{R}\f$, where
 *         \f$ \mathbf{t} \f$ is the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ is the rotation from viewpoint
 *         2 back to viewpoint 1).
 */
essential_t eightpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the rotation matrix between two central viewpoints as an
 *        iterative eigenproblem [11]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    initial rotation for the optimization.
 * \param[out] output Returns more complete information (position of viewpoint
 *                    2 seen from viewpoint 1, eigenvectors and eigenvalues)
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t eigensolver(
    const RelativeAdapterBase & adapter,
    eigensolverOutput_t & output,
    bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two central viewpoints as an
 *        iterative eigenproblem [11].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    initial rotation for the optimization.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrix.
 * \param[out] output Returns more complete information (position of viewpoint
 *                    2 seen from viewpoint 1, eigenvectors and eigenvalues)
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t eigensolver(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    eigensolverOutput_t & output,
    bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two central viewpoints as an
 *        iterative eigenproblem [11]. Using all available correspondences.
 *        Outputs only the rotation.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    initial rotation for the optimization.
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t eigensolver(
    const RelativeAdapterBase & adapter,
    bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two central viewpoints as an
 *        iterative eigenproblem [11]. Outputs only the rotation.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    initial rotation for the optimization.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrix.
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t eigensolver(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    bool useWeights = false );

/**
 * \brief Compute the relative rotation between two non-central viewpoints
 *        following Stewenius' method [16]. Assuming exactly 6 correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    multi-camera configuration.
 * \return Rotations from viewpoint 2 back to viewpoint 1
 */
rotations_t sixpt(
    const RelativeAdapterBase & adapter );

/**
 * \brief Compute the relative rotation between two non-central viewpoints
 *        following Stewenius' method using 6 correspondences [16].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    multi-camera configuration.
 * \param[in] indices Indices of the six correspondences used for deriving
 *                    the rotation matrix.
 * \return Rotations from viewpoint 2 back to viewpoint 1
 */
rotations_t sixpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the rotation matrix between two non-central viewpoints as an
 *        iterative eigenproblem. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the multi-
 *                    camera configuration, plus the initial rotation for the
 *                    optimization.
 * \param[out] output Returns more complete information (position of viewpoint
 *                    2 seen from viewpoint 1, eigenvectors and eigenvalues)
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t ge(
    const RelativeAdapterBase & adapter,
    geOutput_t & output,
    bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two non-central viewpoints as an
 *        iterative eigenproblem.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the multi-
 *                    camera configuration, plus the initial rotation for the
 *                    optimization.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrix.
 * \param[out] output Returns more complete information (position of viewpoint
 *                    2 seen from viewpoint 1, eigenvectors and eigenvalues)
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t ge(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    geOutput_t & output,
    bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two non-central viewpoints as an
 *        iterative eigenproblem. Using all available correspondences.
 *        Outputs only the rotation.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the multi-
 *                    camera configuration, plus the initial rotation for the
 *                    optimization.
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t ge( const RelativeAdapterBase & adapter, bool useWeights = false );

/**
 * \brief Compute the rotation matrix between two non-central viewpoints as an
 *        iterative eigenproblem. Outputs only the rotation.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the multi-
 *                    camera configuration, plus the initial rotation for the
 *                    optimization.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrix.
 * \param[in] useWeights Use weights to weight the summation terms?
 * \return Rotation matrix from viewpoint 2 to viewpoint 1.
 */
rotation_t ge(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices,
    bool useWeights = false );

/**
 * \brief Compute the relative pose between two non-central viewpoints
 *        following Li's method [12]. Using all available correspondences.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    multi-camera configuration.
 * \return Pose of viewpoint 2 seen from viewpoint 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         viewpoint 2 to viewpoint 1).
 */
transformation_t seventeenpt( const RelativeAdapterBase & adapter );

/**
 * \brief Compute the relative pose between two non-central viewpoints
 *        following Li's method [12].
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, plus the
 *                    multi-camera configuration.
 * \param[in] indices Indices of the correspondences used for deriving
 *                    the rotation matrix.
 * \return Pose of viewpoint 2 seen from viewpoint 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         viewpoint 2 to viewpoint 1).
 */
transformation_t seventeenpt(
    const RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

/**
 * \brief Compute the pose between two viewpoints using nonlinear optimization.
 *        Using all available correspondences. Works for both central and
 *        non-central case.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the
 *                    multi-camera configuration, plus the initial values.
 * \return Pose of viewpoint 2 seen from viewpoint 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         viewpoint 2 to viewpoint 1).
 */
transformation_t optimize_nonlinear( RelativeAdapterBase & adapter );

/**
 * \brief Compute the pose between two viewpoints using nonlinear optimization.
 *        Works for both central and non-central case.
 *
 * \param[in] adapter Visitor holding bearing-vector correspondences, the
 *                    multi-camera configuration, plus the initial values.
 * \param[in] indices Indices of the correspondences used for deriving the pose.
 * \return Pose of viewpoint 2 seen from viewpoint 1 (
 *         \f$ \mathbf{T} = \left(\begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array}\right) \f$,
 *         with \f$ \mathbf{t} \f$ being the position of viewpoint 2 seen from
 *         viewpoint 1, and \f$ \mathbf{R} \f$ being the rotation from
 *         viewpoint 2 to viewpoint 1).
 */
transformation_t optimize_nonlinear(
    RelativeAdapterBase & adapter,
    const std::vector<int> & indices );

}
}

#endif /* OPENGV_RELATIVE_POSE_METHODS_HPP_ */
