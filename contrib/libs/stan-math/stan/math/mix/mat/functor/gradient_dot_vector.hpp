#ifndef STAN_MATH_MIX_MAT_FUNCTOR_GRADIENT_DOT_VECTOR_HPP
#define STAN_MATH_MIX_MAT_FUNCTOR_GRADIENT_DOT_VECTOR_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T1, typename T2, typename F>
void gradient_dot_vector(const F& f,
                         const Eigen::Matrix<T1, Eigen::Dynamic, 1>& x,
                         const Eigen::Matrix<T2, Eigen::Dynamic, 1>& v, T1& fx,
                         T1& grad_fx_dot_v) {
  using Eigen::Matrix;
  Matrix<fvar<T1>, Eigen::Dynamic, 1> x_fvar(x.size());
  for (int i = 0; i < x.size(); ++i)
    x_fvar(i) = fvar<T1>(x(i), v(i));
  fvar<T1> fx_fvar = f(x_fvar);
  fx = fx_fvar.val_;
  grad_fx_dot_v = fx_fvar.d_;
}

}  // namespace math
}  // namespace stan
#endif
