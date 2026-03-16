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
 * \file Sturm.hpp
 * \brief Class for evaluating Sturm chains on polynomials, and bracketing the
 *        real roots.
 */

#ifndef OPENGV_STURM_HPP_
#define OPENGV_STURM_HPP_

#include <memory>
#include <stdlib.h>
#include <vector>
#include <list>
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

class Bracket
{
public:
  typedef std::shared_ptr<Bracket> Ptr;
  typedef std::shared_ptr<const Bracket> ConstPtr;

  Bracket( double lowerBound, double upperBound );
  Bracket( double lowerBound, double upperBound, size_t changes, bool setUpperBoundChanges );
  virtual ~Bracket();

  bool dividable( double eps ) const;
  void divide( std::list<Ptr> & brackets ) const;
  double lowerBound() const;
  double upperBound() const;
  bool lowerBoundChangesComputed() const;
  bool upperBoundChangesComputed() const;
  void setLowerBoundChanges( size_t changes );
  void setUpperBoundChanges( size_t changes );
  size_t numberRoots() const;

private:
  double _lowerBound;
  double _upperBound;
  bool _lowerBoundChangesComputed;
  bool _upperBoundChangesComputed;
  size_t _lowerBoundChanges;
  size_t _upperBoundChanges;
};

/**
 * Sturm is initialized over polynomials of arbitrary order, and used to compute
 * the real roots of the polynomial.
 */
class Sturm
{

public:
  EIGEN_MAKE_ALIGNED_OPERATOR_NEW
  /** A pair of values bracketing a real root */
  typedef std::pair<double,double> bracket_t;

  /**
   * \brief Contructor.
   * \param[in] p The polynomial coefficients (poly = p(0,0)*x^n + p(0,1)*x^(n-1) ...).
   */
  Sturm( const Eigen::MatrixXd & p );
  /**
   * \brief Contructor.
   * \param[in] p The polynomial coefficients (poly = p[0]*x^n + p[1]*x^(n-1) ...).
   */
  Sturm( const std::vector<double> & p );
  /**
   * \brief Destructor.
   */
  virtual ~Sturm();

  void findRoots2( std::vector<double> & roots, double eps_x = 0.001, double eps_val = 0.001 );
  /**
   * \brief Finds the roots of the polynomial.
   * \return An array with the real roots of the polynomial.
   */
  std::vector<double> findRoots();
  /**
   * \brief Finds brackets for the real roots of the polynomial.
   * \return A list of brackets for the real roots of the polynomial.
   */
  void bracketRoots( std::vector<double> & roots, double eps = -1.0 );
  /**
   * \brief Evaluates the Sturm chain at a single bound.
   * \param[in] bound The bound.
   * \return The number of sign changes on the bound.
   */
  size_t evaluateChain( double bound );
  /**
   * \brief Evaluates the Sturm chain at a single bound.
   * \param[in] bound The bound.
   * \return The number of sign changes on the bound.
   */
  size_t evaluateChain2( double bound );
  /**
   * \brief Composes an initial bracket for all the roots of the polynomial.
   * \return The maximum of the absolute values of the bracket-values (That's
   *         what the Lagrangian bound is able to find).
   */
  double computeLagrangianBound();

private:
  /**
   * \brief Internal function used for composing the Sturm chain
   * \param[in] p1 First polynomial.
   * \param[in] p2 Second polynomial.
   * \param[out] r The negated remainder of the polynomial division p1/p2.
   */
  void computeNegatedRemainder(
      const Eigen::MatrixXd & p1,
      const Eigen::MatrixXd & p2,
      Eigen::MatrixXd & r );

  /** A matrix containing the coefficients of the Sturm-chain of the polynomial */
  Eigen::MatrixXd _C;
  /** The dimension _C, which corresponds to (polynomial order+1) */
  size_t _dimension;
};

}
}

#endif /* OPENGV_STURM_HPP_ */
