#ifndef STAN_MATH_FWD_MAT_FUNCTOR_GRADIENT_HPP
#define STAN_MATH_FWD_MAT_FUNCTOR_GRADIENT_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Calculate the value and the gradient of the specified function
 * at the specified argument.
 *
 * <p>The functor must implement
 *
 * <code>
 * fvar<T>
 * operator()(const Eigen::Matrix<T, Eigen::Dynamic, 1>&)
 * </code>
 *
 * using only operations that are defined for
 * <code>fvar</code>.  This latter constraint usually
 * requires the functions to be defined in terms of the libraries
 * defined in Stan or in terms of functions with appropriately
 * general namespace imports that eventually depend on functions
 * defined in Stan.
 *
 * <p>Time and memory usage is on the order of the size of the
 * fully unfolded expression for the function applied to the
 * argument, independently of dimension.
 *
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] grad_fx Gradient of function at argument
 */
template <typename T, typename F>
void gradient(const F& f, const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, T& fx,
              Eigen::Matrix<T, Eigen::Dynamic, 1>& grad_fx) {
  Eigen::Matrix<fvar<T>, Eigen::Dynamic, 1> x_fvar(x.size());
  grad_fx.resize(x.size());
  for (int i = 0; i < x.size(); ++i) {
    for (int k = 0; k < x.size(); ++k)
      x_fvar(k) = fvar<T>(x(k), k == i);
    fvar<T> fx_fvar = f(x_fvar);
    if (i == 0)
      fx = fx_fvar.val_;
    grad_fx(i) = fx_fvar.d_;
  }
}

}  // namespace math
}  // namespace stan
#endif
