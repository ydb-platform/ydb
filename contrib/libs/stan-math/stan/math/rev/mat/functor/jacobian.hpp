#ifndef STAN_MATH_REV_MAT_FUNCTOR_JACOBIAN_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_JACOBIAN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

template <typename F>
void jacobian(const F& f, const Eigen::Matrix<double, Eigen::Dynamic, 1>& x,
              Eigen::Matrix<double, Eigen::Dynamic, 1>& fx,
              Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>& J) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  start_nested();
  try {
    Matrix<var, Dynamic, 1> x_var(x.size());
    for (int k = 0; k < x.size(); ++k)
      x_var(k) = x(k);
    Matrix<var, Dynamic, 1> fx_var = f(x_var);
    fx.resize(fx_var.size());
    for (int i = 0; i < fx_var.size(); ++i)
      fx(i) = fx_var(i).val();
    J.resize(fx_var.size(), x.size());
    for (int i = 0; i < fx_var.size(); ++i) {
      if (i > 0)
        set_zero_all_adjoints_nested();
      grad(fx_var(i).vi_);
      for (int k = 0; k < x.size(); ++k)
        J(i, k) = x_var(k).adj();
    }
  } catch (const std::exception& e) {
    recover_memory_nested();
    throw;
  }
  recover_memory_nested();
}

}  // namespace math
}  // namespace stan
#endif
