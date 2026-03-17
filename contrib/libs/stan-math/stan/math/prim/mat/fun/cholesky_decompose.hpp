#ifndef STAN_MATH_PRIM_MAT_FUN_CHOLESKY_DECOMPOSE_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOLESKY_DECOMPOSE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#ifdef STAN_OPENCL
#include <stan/math/opencl/opencl_context.hpp>
#include <stan/math/opencl/err/check_symmetric.hpp>
#include <stan/math/opencl/cholesky_decompose.hpp>
#include <stan/math/opencl/copy.hpp>
#endif

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the lower-triangular Cholesky factor (i.e., matrix
 * square root) of the specified square, symmetric matrix.  The return
 * value \f$L\f$ will be a lower-traingular matrix such that the
 * original matrix \f$A\f$ is given by
 * <p>\f$A = L \times L^T\f$.
 * @param m Symmetrix matrix.
 * @return Square root of matrix.
 * @note Because OpenCL only works on doubles there are two
 * <code>cholesky_decompose</code> functions. One that works on doubles
 * and another that works on all other types (this one).
 * @throw std::domain_error if m is not a symmetric matrix or
 *   if m is not positive definite (if m has more than 0 elements)
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cholesky_decompose(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m) {
  check_square("cholesky_decompose", "m", m);
  check_symmetric("cholesky_decompose", "m", m);
  Eigen::LLT<Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> > llt(m.rows());
  llt.compute(m);
  check_pos_definite("cholesky_decompose", "m", llt);
  return llt.matrixL();
}

/**
 * Return the lower-triangular Cholesky factor (i.e., matrix
 * square root) of the specified square, symmetric matrix.  The return
 * value \f$L\f$ will be a lower-traingular matrix such that the
 * original matrix \f$A\f$ is given by
 * <p>\f$A = L \times L^T\f$.
 * @param m Symmetrix matrix.
 * @return Square root of matrix.
 * @note Because OpenCL only works on doubles there are two
 * <code>cholesky_decompose</code> functions. One that works on doubles
 * (this one) and another that works on all other types.
 * @throw std::domain_error if m is not a symmetric matrix or
 *   if m is not positive definite (if m has more than 0 elements)
 */
template <>
inline Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> cholesky_decompose(
    const Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>& m) {
  check_square("cholesky_decompose", "m", m);
#ifdef STAN_OPENCL
  if (m.rows() >= opencl_context.tuning_opts().cholesky_size_worth_transfer) {
    matrix_cl m_cl(m);
    check_symmetric("cholesky_decompose", "m", m_cl);
    Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> m_chol(m.rows(),
                                                                 m.cols());
    m_cl = cholesky_decompose(m_cl);
    copy(m_chol, m_cl);  // NOLINT
    return m_chol;
  } else {
    check_symmetric("cholesky_decompose", "m", m);
    Eigen::LLT<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> > llt(
        m.rows());
    llt.compute(m);
    check_pos_definite("cholesky_decompose", "m", llt);
    return llt.matrixL();
  }
#else
  check_symmetric("cholesky_decompose", "m", m);
  Eigen::LLT<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> > llt(
      m.rows());
  llt.compute(m);
  check_pos_definite("cholesky_decompose", "m", llt);
  return llt.matrixL();
#endif
}
}  // namespace math

}  // namespace stan
#endif
