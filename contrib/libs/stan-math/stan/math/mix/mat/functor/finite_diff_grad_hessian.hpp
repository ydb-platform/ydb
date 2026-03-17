#ifndef STAN_MATH_MIX_MAT_FUNCTOR_FINITE_DIFF_GRAD_HESSIAN_HPP
#define STAN_MATH_MIX_MAT_FUNCTOR_FINITE_DIFF_GRAD_HESSIAN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/mix/mat/functor/hessian.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Calculate the value and the gradient of the hessian of the specified
 * function at the specified argument using second-order autodiff and
 * first-order finite difference.
 *
 * <p>The functor must implement
 *
 * <code>
 * double
 * operator()(const
 * Eigen::Matrix<double, Eigen::Dynamic, 1>&)
 * </code>
 *
 * Reference:
 *
 * De Levie: An improved numerical approximation
 * for the first derivative, page 3
 *
 * 4 calls to the function, f.
 *
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] hess Hessian matrix
 * @param[out] grad_hess_fx gradient of Hessian of function at argument
 * @param[in] epsilon perturbation size
 */
template <typename F>
void finite_diff_grad_hessian(
    const F& f, const Eigen::Matrix<double, -1, 1>& x, double& fx,
    Eigen::Matrix<double, -1, -1>& hess,
    std::vector<Eigen::Matrix<double, -1, -1> >& grad_hess_fx,
    double epsilon = 1e-04) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  int d = x.size();
  double dummy_fx_eval;

  Matrix<double, Dynamic, 1> x_temp(x);
  Matrix<double, Dynamic, 1> grad_auto(d);
  Matrix<double, Dynamic, Dynamic> hess_auto(d, d);
  Matrix<double, Dynamic, Dynamic> hess_diff(d, d);

  hessian(f, x, fx, grad_auto, hess);
  for (int i = 0; i < d; ++i) {
    hess_diff.setZero();

    x_temp(i) = x(i) + 2.0 * epsilon;
    hessian(f, x_temp, dummy_fx_eval, grad_auto, hess_auto);
    hess_diff = -hess_auto;

    x_temp(i) = x(i) + -2.0 * epsilon;
    hessian(f, x_temp, dummy_fx_eval, grad_auto, hess_auto);
    hess_diff += hess_auto;

    x_temp(i) = x(i) + epsilon;
    hessian(f, x_temp, dummy_fx_eval, grad_auto, hess_auto);
    hess_diff += 8.0 * hess_auto;

    x_temp(i) = x(i) + -epsilon;
    hessian(f, x_temp, dummy_fx_eval, grad_auto, hess_auto);
    hess_diff -= 8.0 * hess_auto;

    x_temp(i) = x(i);
    hess_diff /= 12.0 * epsilon;

    grad_hess_fx.push_back(hess_diff);
  }
  fx = f(x);
}

}  // namespace math
}  // namespace stan
#endif
