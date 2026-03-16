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
 * \file types.hpp
 * \brief A collection of variables used in geometric vision for the
 *        computation of calibrated absolute and relative pose.
 */

#ifndef OPENGV_TYPES_HPP_
#define OPENGV_TYPES_HPP_

#include <stdlib.h>
#include <vector>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>

/**
 * \brief The namespace of this library.
 */
namespace opengv
{

/** A 3-vector of unit length used to describe landmark observations/bearings
 *  in camera frames (always expressed in camera frames)
 */
typedef Eigen::Vector3d
    bearingVector_t;

/** An array of bearing-vectors */
typedef std::vector<bearingVector_t, Eigen::aligned_allocator<bearingVector_t> >
    bearingVectors_t;

/** A 3-vector describing a translation/camera position */
typedef Eigen::Vector3d
    translation_t;

/** An array of translations */
typedef std::vector<translation_t, Eigen::aligned_allocator<translation_t> >
    translations_t;

/** A rotation matrix */
typedef Eigen::Matrix3d
    rotation_t;

/** An array of rotation matrices as returned by fivept_kneip [7] */
typedef std::vector<rotation_t, Eigen::aligned_allocator<rotation_t> >
    rotations_t;

/** A 3x4 transformation matrix containing rotation \f$ \mathbf{R} \f$ and
 *  translation \f$ \mathbf{t} \f$ as follows:
 *  \f$ \left( \begin{array}{cc} \mathbf{R} & \mathbf{t} \end{array} \right) \f$
 */
typedef Eigen::Matrix<double,3,4>
    transformation_t;

/** An array of transformations */
typedef std::vector<transformation_t, Eigen::aligned_allocator<transformation_t> >
    transformations_t;

/** A 3-vector containing the cayley parameters of a rotation matrix */
typedef Eigen::Vector3d
    cayley_t;

/** A 4-vector containing the quaternion parameters of rotation matrix */
typedef Eigen::Vector4d
    quaternion_t;

/** Essential matrix \f$ \mathbf{E} \f$ between two viewpoints:
 *
 *  \f$ \mathbf{E} = \f$ skew(\f$\mathbf{t}\f$) \f$ \mathbf{R} \f$,
 *
 *  where \f$ \mathbf{t} \f$ describes the position of viewpoint 2 seen from
 *  viewpoint 1, and \f$\mathbf{R}\f$ describes the rotation from viewpoint 2
 *  to viewpoint 1.
 */
typedef Eigen::Matrix3d
    essential_t;

/** An array of essential matrices */
typedef std::vector<essential_t, Eigen::aligned_allocator<essential_t> >
    essentials_t;

/** An essential matrix with complex entires (as returned from
 *  fivept_stewenius [5])
 */
typedef Eigen::Matrix3cd
    complexEssential_t;

/** An array of complex-type essential matrices */
typedef std::vector< complexEssential_t, Eigen::aligned_allocator< complexEssential_t> >
    complexEssentials_t;

/** A 3-vector describing a point in 3D-space */
typedef Eigen::Vector3d
    point_t;

/** An array of 3D-points */
typedef std::vector<point_t, Eigen::aligned_allocator<point_t> >
    points_t;

/** A 3-vector containing the Eigenvalues of matrix \f$ \mathbf{M} \f$ in the
 *  eigensolver-algorithm (described in [11])
 */
typedef Eigen::Vector3d
    eigenvalues_t;

/** A 3x3 matrix containing the eigenvectors of matrix \f$ \mathbf{M} \f$ in the
 *  eigensolver-algorithm (described in [11])
 */
typedef Eigen::Matrix3d
    eigenvectors_t;

/** EigensolverOutput holds the output-parameters of the eigensolver-algorithm
 *  (described in [11])
 */
typedef struct EigensolverOutput
{
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW

  /** Position of viewpoint 2 seen from viewpoint 1 (unscaled) */
  translation_t   translation;
  /** Rotation from viewpoint 2 back to viewpoint 1 */
  rotation_t      rotation;
  /** The eigenvalues of matrix \f$ \mathbf{M} \f$ */
  eigenvalues_t   eigenvalues;
  /** The eigenvectors of matrix matrix \f$ \mathbf{M} \f$ */
  eigenvectors_t  eigenvectors;
} eigensolverOutput_t;

/** GeOutput holds the output-parameters of ge
 */
typedef struct GeOutput
{
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW
  
  /** Homogeneous position of viewpoint 2 seen from viewpoint 1 */
  Eigen::Vector4d   translation;
  /** Rotation from viewpoint 2 back to viewpoint 1 */
  rotation_t        rotation;
  /** The eigenvalues of matrix \f$ \mathbf{G} \f$ */
  Eigen::Vector4d   eigenvalues;
  /** The eigenvectors of matrix matrix \f$ \mathbf{G} \f$ */
  Eigen::Matrix4d   eigenvectors;
} geOutput_t;

}

#endif /* OPENGV_TYPES_HPP_ */
