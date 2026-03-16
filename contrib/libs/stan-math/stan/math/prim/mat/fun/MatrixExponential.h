// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2009, 2010, 2013 Jitse Niesen <jitse@maths.leeds.ac.uk>
// Copyright (C) 2011, 2013 Chen-Pang He <jdh8@ms63.hinet.net>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// This file was edited for to the Stan math library to create
// the matrix exponential function (matrix_exp), 2016.

#ifndef STAN_MATH_PRIM_MAT_FUN_MATRIXEXPONENTIAL_H
#define STAN_MATH_PRIM_MAT_FUN_MATRIXEXPONENTIAL_H

#include <stan/math/prim/mat/fun/value_of_rec.hpp>

namespace Eigen {

  template <typename RealScalar>
  struct MatrixExponentialScalingOp
  {   
    /** \brief Constructor.
     *
     * \param[in] squarings  The integer \f$ s \f$ in this document.
     */
    MatrixExponentialScalingOp(int squarings) : m_squarings(squarings) { }   


    /** \brief Scale a matrix coefficient.
     *
     * \param[in,out] x  The scalar to be scaled, becoming \f$ 2^{-s} x \f$.
     */
    inline const RealScalar operator() (const RealScalar& x) const
    {  
    using std::ldexp;
    return ldexp(x, -m_squarings);
    }

    typedef std::complex<RealScalar> ComplexScalar;

    /** \brief Scale a matrix coefficient.
     *
     * \param[in,out] x  The scalar to be scaled, becoming \f$ 2^{-s} x \f$.
     */
    inline const ComplexScalar operator() (const ComplexScalar& x) const
    {  
    using std::ldexp;
    return ComplexScalar(ldexp(x.real(), -m_squarings), ldexp(x.imag(), -m_squarings));  
    }

    private:
    int m_squarings;
  };


  /** \brief Compute the (5,5)-Pad&eacute; approximant to the exponential.
   *
   *  After exit, \f$ (V+U)(V-U)^{-1} \f$ is the Pad&eacute;
   *  approximant of \f$ \exp(A) \f$ around \f$ A = 0 \f$.
   */
  template <typename MatrixType>
  void matrix_exp_pade3(const MatrixType &A, MatrixType &U, MatrixType &V)
  {  
    using Eigen::internal::traits; 
    typedef typename Eigen::NumTraits<typename traits<MatrixType>::Scalar>::Real RealScalar;  
    const RealScalar b[] = {120.L, 60.L, 12.L, 1.L};
    const MatrixType A2 = A * A;
    const MatrixType tmp = b[3] * A2 + b[1] * MatrixType::Identity(A.rows(), A.cols());  
    U.noalias() = A * tmp;
    V = b[2] * A2 + b[0] * MatrixType::Identity(A.rows(), A.cols());
  }

  /** \brief Compute the (5,5)-Pad&eacute; approximant to the exponential.
   *
   *  After exit, \f$ (V+U)(V-U)^{-1} \f$ is the Pad&eacute;
   *  approximant of \f$ \exp(A) \f$ around \f$ A = 0 \f$.
   */
  template <typename MatrixType>
  void matrix_exp_pade5(const MatrixType &A, MatrixType &U, MatrixType &V)
  {  
    typedef typename Eigen::NumTraits<typename Eigen::internal::traits<MatrixType>::Scalar>::Real RealScalar;  
    const RealScalar b[] = {30240.L, 15120.L, 3360.L, 420.L, 30.L, 1.L};
    const MatrixType A2 = A * A;
    const MatrixType A4 = A2 * A2;
    const MatrixType tmp = b[5] * A4 + b[3] * A2 + b[1] * MatrixType::Identity(A.rows(), A.cols());  
    U.noalias() = A * tmp;
    V = b[4] * A4 + b[2] * A2 + b[0] * MatrixType::Identity(A.rows(), A.cols());  
  }

  /** \brief Compute the (7,7)-Pad&eacute; approximant to the exponential.
   *
   *  After exit, \f$ (V+U)(V-U)^{-1} \f$ is the Pad&eacute;
   *  approximant of \f$ \exp(A) \f$ around \f$ A = 0 \f$.
   */
  template <typename MatrixType>
  void matrix_exp_pade7(const MatrixType &A, MatrixType &U, MatrixType &V)
  {  
    using Eigen::internal::traits; 
    typedef typename Eigen::NumTraits<typename traits<MatrixType>::Scalar>::Real RealScalar; 
    const RealScalar b[] = {17297280.L, 8648640.L, 1995840.L, 277200.L, 25200.L, 1512.L, 56.L, 1.L};  
    const MatrixType A2 = A * A;
    const MatrixType A4 = A2 * A2;
    const MatrixType A6 = A4 * A2;
    const MatrixType tmp = b[7] * A6 + b[5] * A4 + b[3] * A2
    + b[1] * MatrixType::Identity(A.rows(), A.cols());
    U.noalias() = A * tmp;
    V = b[6] * A6 + b[4] * A4 + b[2] * A2 + b[0] * MatrixType::Identity(A.rows(), A.cols());  
  }

  /** \brief Compute the (9,9)-Pad&eacute; approximant to the exponential.
   *
   *  After exit, \f$ (V+U)(V-U)^{-1} \f$ is the Pad&eacute;
   *  approximant of \f$ \exp(A) \f$ around \f$ A = 0 \f$.
   */
  template <typename MatrixType>
  void matrix_exp_pade9(const MatrixType &A, MatrixType &U, MatrixType &V)
  {  
    using Eigen::internal::traits; 
    typedef typename Eigen::NumTraits<typename traits<MatrixType>::Scalar>::Real RealScalar;  
    const RealScalar b[] = {17643225600.L, 8821612800.L, 2075673600.L, 302702400.L, 30270240.L,  
    2162160.L, 110880.L, 3960.L, 90.L, 1.L};
    const MatrixType A2 = A * A;
    const MatrixType A4 = A2 * A2;
    const MatrixType A6 = A4 * A2;
    const MatrixType A8 = A6 * A2;
    const MatrixType tmp = b[9] * A8 + b[7] * A6 + b[5] * A4 + b[3] * A2
    + b[1] * MatrixType::Identity(A.rows(), A.cols());
    U.noalias() = A * tmp;
    V = b[8] * A8 + b[6] * A6 + b[4] * A4 + b[2] * A2 + b[0] * MatrixType::Identity(A.rows(), A.cols());  
  }

  /** \brief Compute the (13,13)-Pad&eacute; approximant to the exponential.
   *
   *  After exit, \f$ (V+U)(V-U)^{-1} \f$ is the Pad&eacute;
   *  approximant of \f$ \exp(A) \f$ around \f$ A = 0 \f$.
   */
  template <typename MatrixType>
  void matrix_exp_pade13(const MatrixType &A, MatrixType &U, MatrixType &V)  
  {  
    using Eigen::internal::traits; 
    typedef typename Eigen::NumTraits<typename traits<MatrixType>::Scalar>::Real RealScalar;  
    const RealScalar b[] = {64764752532480000.L, 32382376266240000.L, 7771770303897600.L,  
    1187353796428800.L, 129060195264000.L, 10559470521600.L, 670442572800.L,  
    33522128640.L, 1323241920.L, 40840800.L, 960960.L, 16380.L, 182.L, 1.L};  
    const MatrixType A2 = A * A;
    const MatrixType A4 = A2 * A2;
    const MatrixType A6 = A4 * A2;
    V = b[13] * A6 + b[11] * A4 + b[9] * A2; // used for temporary storage  
    MatrixType tmp = A6 * V;
    tmp += b[7] * A6 + b[5] * A4 + b[3] * A2 + b[1] * MatrixType::Identity(A.rows(), A.cols());  
    U.noalias() = A * tmp;
    tmp = b[12] * A6 + b[10] * A4 + b[8] * A2;
    V.noalias() = A6 * tmp;
    V += b[6] * A6 + b[4] * A4 + b[2] * A2 + b[0] * MatrixType::Identity(A.rows(), A.cols());  
  }

  template <typename MatrixType, typename RealScalar = typename Eigen:: NumTraits<typename Eigen::internal::traits<MatrixType>::Scalar>::Real>  
  struct matrix_exp_computeUV
  {  
    /** \brief Compute Pad&eacute; approximant to the exponential.
     *
     * Computes \c U, \c V and \c squarings such that \f$ (V+U)(V-U)^{-1} \f$ is a Pad&eacute;
     * approximant of \f$ \exp(2^{-\mbox{squarings}}M) \f$ around \f$ M = 0 \f$, where \f$ M \f$
     * denotes the matrix \c arg. The degree of the Pad&eacute; approximant and the value of squarings
     * are chosen such that the approximation error is no more than the round-off error.
     *
     * <p> Edit for Stan: template ComputeUV::run so that it may used on
     * autodiff variables (var and fvar). This required adding the scalar_type
     * argument, which tells the function the type of elements used in the
     * matrix.
     */
    static void run(const MatrixType& arg, MatrixType& U, MatrixType& V, int& squarings);  
  };

  template <typename MatrixType>
  struct matrix_exp_computeUV<MatrixType>
  {  
    template <typename T>  
    static void run(const MatrixType& arg, MatrixType& U, MatrixType& V, int& squarings,  
    				T scalar_type)  
    {  
    using std::frexp;
    using std::pow;
    using stan::math::value_of_rec;
    const T l1norm = arg.cwiseAbs().colwise().sum().maxCoeff();
    squarings = 0;
    if (l1norm < 1.495585217958292e-002) {
      matrix_exp_pade3(arg, U, V);
    } else if (l1norm < 2.539398330063230e-001) {
      matrix_exp_pade5(arg, U, V);
    } else if (l1norm < 9.504178996162932e-001) {
      matrix_exp_pade7(arg, U, V);
    } else if (l1norm < 2.097847961257068e+000) {
      matrix_exp_pade9(arg, U, V);
    } else {
      const double maxnorm = 5.371920351148152;
      frexp(value_of_rec(l1norm) / value_of_rec(maxnorm), &squarings);
      if (squarings < 0) squarings = 0;
      MatrixType A = arg.unaryExpr(MatrixExponentialScalingOp<T>(squarings));  
      matrix_exp_pade13(A, U, V);
    }
    }
  };

}

#endif
