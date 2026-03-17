#ifndef STAN_MATH_REV_MAT_FUN_LOG_DETERMINANT_HPP
#define STAN_MATH_REV_MAT_FUN_LOG_DETERMINANT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

template <int R, int C>
inline var log_determinant(const Eigen::Matrix<var, R, C>& m) {
  using Eigen::Matrix;

  math::check_square("log_determinant", "m", m);

  Matrix<double, R, C> m_d(m.rows(), m.cols());
  for (int i = 0; i < m.size(); ++i)
    m_d(i) = m(i).val();

  Eigen::FullPivHouseholderQR<Matrix<double, R, C> > hh
      = m_d.fullPivHouseholderQr();

  double val = hh.logAbsDeterminant();

  vari** varis
      = ChainableStack::instance().memalloc_.alloc_array<vari*>(m.size());
  for (int i = 0; i < m.size(); ++i)
    varis[i] = m(i).vi_;

  Matrix<double, R, C> m_inv_transpose = hh.inverse().transpose();
  double* gradients
      = ChainableStack::instance().memalloc_.alloc_array<double>(m.size());
  for (int i = 0; i < m.size(); ++i)
    gradients[i] = m_inv_transpose(i);

  return var(new precomputed_gradients_vari(val, m.size(), varis, gradients));
}

}  // namespace math
}  // namespace stan
#endif
