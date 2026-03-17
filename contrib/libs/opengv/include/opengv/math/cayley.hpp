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
 * \file cayley.hpp
 * \brief Functions for back-and-forth transformation between rotation matrices
 *        and Cayley-parameters.
 */

#ifndef OPENGV_CAYLEY_HPP_
#define OPENGV_CAYLEY_HPP_

#include <stdlib.h>
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
 * \brief Compute a rotation matrix from Cayley-parameters, following [14].
 *
 * \param[in] cayley The Cayley-parameters of a rotation.
 * \return The 3x3 rotation matrix.
 */
rotation_t cayley2rot( const cayley_t & cayley);

/**
 * \brief Compute a fake rotation matrix from Cayley-parameters, following [14].
 *        The rotation matrix is missing the scaling parameter of the
 *        Cayley-transform. This short form is useful for the Jacobian-based
 *        iterative rotation optimization of the eigensolver [11].
 *
 * \param[in] cayley The Cayley-parameters of the rotation.
 * \return The false 3x3 rotation matrix.
 */
rotation_t cayley2rot_reduced( const cayley_t & cayley);

/**
 * \brief Compute the Cayley-parameters of a rotation matrix, following [14].
 *
 * \param[in] R The 3x3 rotation matrix.
 * \return The Cayley-parameters.
 */
cayley_t rot2cayley( const rotation_t & R );

}
}

#endif /* OPENGV_CAYLEY_HPP_ */
