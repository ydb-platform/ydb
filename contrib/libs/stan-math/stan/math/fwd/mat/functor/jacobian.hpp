#ifndef STAN_MATH_FWD_MAT_FUNCTOR_JACOBIAN_HPP
#define STAN_MATH_FWD_MAT_FUNCTOR_JACOBIAN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, typename F>
void jacobian(const F& f, const Eigen::Matrix<T, Eigen::Dynamic, 1>& x,
              Eigen::Matrix<T, Eigen::Dynamic, 1>& fx,
              Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& J) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  Matrix<fvar<T>, Dynamic, 1> x_fvar(x.size());
  for (int i = 0; i < x.size(); ++i) {
    for (int k = 0; k < x.size(); ++k)
      x_fvar(k) = fvar<T>(x(k), i == k);
    Matrix<fvar<T>, Dynamic, 1> fx_fvar = f(x_fvar);
    if (i == 0) {
      J.resize(fx_fvar.size(), x.size());
      fx.resize(fx_fvar.size());
      for (int k = 0; k < fx_fvar.size(); ++k)
        fx(k) = fx_fvar(k).val_;
    }
    for (int k = 0; k < fx_fvar.size(); ++k) {
      J(k, i) = fx_fvar(k).d_;
    }
  }
}

}  // namespace math
}  // namespace stan
#endif
