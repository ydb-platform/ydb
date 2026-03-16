#ifndef STAN_MATH_FWD_MAT_FUN_DETERMINANT_HPP
#define STAN_MATH_FWD_MAT_FUN_DETERMINANT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/inverse.hpp>
#include <stan/math/fwd/mat/fun/inverse.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline fvar<T> determinant(const Eigen::Matrix<fvar<T>, R, C>& m) {
  check_square("determinant", "m", m);
  Eigen::Matrix<T, R, C> m_deriv(m.rows(), m.cols());
  Eigen::Matrix<T, R, C> m_val(m.rows(), m.cols());

  for (size_type i = 0; i < m.rows(); i++) {
    for (size_type j = 0; j < m.cols(); j++) {
      m_deriv(i, j) = m(i, j).d_;
      m_val(i, j) = m(i, j).val_;
    }
  }

  Eigen::Matrix<T, R, C> m_inv = inverse(m_val);
  m_deriv = multiply(m_inv, m_deriv);

  fvar<T> result;
  result.val_ = m_val.determinant();
  result.d_ = result.val_ * m_deriv.trace();

  // FIXME:  I think this will overcopy compared to retur fvar<T>(...);
  return result;
}

}  // namespace math
}  // namespace stan
#endif
