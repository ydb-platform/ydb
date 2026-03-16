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
 * \file roots.hpp
 * \brief Closed-form solutions for computing the roots of a polynomial.
 */

#ifndef OPENGV_ROOTS_HPP_
#define OPENGV_ROOTS_HPP_

#include <stdlib.h>
#include <vector>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>

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
 * \brief The roots of a third-order polynomial.
 * 
 * \param[in] p The polynomial coefficients (poly = p[0]*x^3 + p[1]*x^2 ...).
 * \return The roots of the polynomial (only real ones).
 */
std::vector<double> o3_roots( const std::vector<double> & p );

/**
 * \brief Ferrari's method for computing the roots of a fourth order polynomial.
 *
 * \param[in] p The polynomial coefficients (poly = p(0,0)*x^4 + p(1,0)*x^3 ...).
 * \return The roots of the polynomial (only real ones).
 */
std::vector<double> o4_roots( const Eigen::MatrixXd & p );

/**
 * \brief Ferrari's method for computing the roots of a fourth order polynomial.
 *        With a different interface.
 *
 * \param[in] p The polynomial coefficients (poly = p[0]*x^4 + p[1]*x^3 ...).
 * \return The roots of the polynomial (only real ones).
 */
std::vector<double> o4_roots( const std::vector<double> & p );

}
}

#endif /* OPENGV_ROOTS_HPP_ */
