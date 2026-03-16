#ifndef STAN_MATH_MIX_MAT_FUNCTOR_PARTIAL_DERIVATIVE_HPP
#define STAN_MATH_MIX_MAT_FUNCTOR_PARTIAL_DERIVATIVE_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the partial derivative of the specified multiivariate
 * function at the specified argument.
 *
 * @tparam T Argument type
 * @tparam F Function type
 * @param f Function
 * @param[in] x Argument vector
 * @param[in] n Index of argument with which to take derivative
 * @param[out] fx Value of function applied to argument
 * @param[out] dfx_dxn Value of partial derivative
 */
template <typename T, typename F>
void partial_derivative(const F& f,
                        const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, int n,
                        T& fx, T& dfx_dxn) {
  Eigen::Matrix<fvar<T>, Eigen::Dynamic, 1> x_fvar(x.size());
  for (int i = 0; i < x.size(); ++i)
    x_fvar(i) = fvar<T>(x(i), i == n);
  fvar<T> fx_fvar = f(x_fvar);
  fx = fx_fvar.val_;
  dfx_dxn = fx_fvar.d_;
}

}  // namespace math
}  // namespace stan
#endif
