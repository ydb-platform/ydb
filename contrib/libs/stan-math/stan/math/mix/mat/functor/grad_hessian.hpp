#ifndef STAN_MATH_MIX_MAT_FUNCTOR_GRAD_HESSIAN_HPP
#define STAN_MATH_MIX_MAT_FUNCTOR_GRAD_HESSIAN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

/**
 * Calculate the value, the Hessian, and the gradient of the Hessian
 * of the specified function at the specified argument.
 *
 * <p>The functor must implement
 *
 * <code>
 * fvar\<fvar\<var\> \>
 * operator()(const Eigen::Matrix\<fvar\<fvar\<var\> \>,
 *            Eigen::Dynamic, 1\>&)
 * </code>
 *
 * using only operations that are defined for
 * <code>fvar</code> and <code>var</code>.
 *
 * This latter constraint usually
 * requires the functions to be defined in terms of the libraries
 * defined in Stan or in terms of functions with appropriately
 * general namespace imports that eventually depend on functions
 * defined in Stan.
 *
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] H Hessian of function at argument
 * @param[out] grad_H Gradient of the Hessian of function at argument
 */
template <typename F>
void grad_hessian(
    const F& f, const Eigen::Matrix<double, Eigen::Dynamic, 1>& x, double& fx,
    Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>& H,
    std::vector<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> >&
        grad_H) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  fx = f(x);
  int d = x.size();
  H.resize(d, d);
  grad_H.resize(d, Matrix<double, Dynamic, Dynamic>(d, d));
  try {
    for (int i = 0; i < d; ++i) {
      for (int j = i; j < d; ++j) {
        start_nested();
        Matrix<fvar<fvar<var> >, Dynamic, 1> x_ffvar(d);
        for (int k = 0; k < d; ++k)
          x_ffvar(k)
              = fvar<fvar<var> >(fvar<var>(x(k), i == k), fvar<var>(j == k, 0));
        fvar<fvar<var> > fx_ffvar = f(x_ffvar);
        H(i, j) = fx_ffvar.d_.d_.val();
        H(j, i) = H(i, j);
        grad(fx_ffvar.d_.d_.vi_);
        for (int k = 0; k < d; ++k) {
          grad_H[i](j, k) = x_ffvar(k).val_.val_.adj();
          grad_H[j](i, k) = grad_H[i](j, k);
        }
        recover_memory_nested();
      }
    }
  } catch (const std::exception& e) {
    recover_memory_nested();
    throw;
  }
}

}  // namespace math
}  // namespace stan
#endif
