#ifndef STAN_MATH_FWD_MAT_FUNCTOR_HESSIAN_HPP
#define STAN_MATH_FWD_MAT_FUNCTOR_HESSIAN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Calculate the value, the gradient, and the Hessian,
 * of the specified function at the specified argument in
 * time O(N^3) time and O(N^2) space.  The advantage over the
 * mixed definition, which is faster for Hessians, is that this
 * version is itself differentiable.
 *
 * <p>The functor must implement
 *
 * <code>
 * fvar\<fvar\<T\> \>
 * operator()(const
 * Eigen::Matrix\<fvar\<fvar\<T\> \>, Eigen::Dynamic, 1\>&)
 * </code>
 *
 * using only operations that are defined for the argument type.
 *
 * This latter constraint usually requires the functions to be
 * defined in terms of the libraries defined in Stan or in terms
 * of functions with appropriately general namespace imports that
 * eventually depend on functions defined in Stan.
 *
 * @tparam T Type of underlying scalar
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] grad gradient of function at argument
 * @param[out] H Hessian of function at argument
 */
template <typename T, typename F>
void hessian(const F& f, const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, T& fx,
             Eigen::Matrix<T, Eigen::Dynamic, 1>& grad,
             Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& H) {
  H.resize(x.size(), x.size());
  grad.resize(x.size());
  // size 0 separate because nothing to loop over in main body
  if (x.size() == 0) {
    fx = f(x);
    return;
  }
  Eigen::Matrix<fvar<fvar<T> >, Eigen::Dynamic, 1> x_fvar(x.size());
  for (int i = 0; i < x.size(); ++i) {
    for (int j = i; j < x.size(); ++j) {
      for (int k = 0; k < x.size(); ++k)
        x_fvar(k) = fvar<fvar<T> >(fvar<T>(x(k), j == k), fvar<T>(i == k, 0));
      fvar<fvar<T> > fx_fvar = f(x_fvar);
      if (j == 0)
        fx = fx_fvar.val_.val_;
      if (i == j)
        grad(i) = fx_fvar.d_.val_;
      H(i, j) = fx_fvar.d_.d_;
      H(j, i) = H(i, j);
    }
  }
}

}  // namespace math
}  // namespace stan
#endif
