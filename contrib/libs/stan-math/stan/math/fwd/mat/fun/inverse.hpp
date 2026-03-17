#ifndef STAN_MATH_FWD_MAT_FUN_INVERSE_HPP
#define STAN_MATH_FWD_MAT_FUN_INVERSE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/inverse.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline Eigen::Matrix<fvar<T>, R, C> inverse(
    const Eigen::Matrix<fvar<T>, R, C>& m) {
  check_square("inverse", "m", m);
  Eigen::Matrix<T, R, C> m_deriv(m.rows(), m.cols());
  Eigen::Matrix<T, R, C> m_inv(m.rows(), m.cols());

  for (size_type i = 0; i < m.rows(); i++) {
    for (size_type j = 0; j < m.cols(); j++) {
      m_inv(i, j) = m(i, j).val_;
      m_deriv(i, j) = m(i, j).d_;
    }
  }

  m_inv = inverse(m_inv);

  m_deriv = multiply(multiply(m_inv, m_deriv), m_inv);
  m_deriv = -m_deriv;

  return to_fvar(m_inv, m_deriv);
}

}  // namespace math
}  // namespace stan
#endif
