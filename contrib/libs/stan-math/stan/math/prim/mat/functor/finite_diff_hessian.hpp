#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_FINITE_DIFF_HESSIAN_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_FINITE_DIFF_HESSIAN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/functor/finite_diff_gradient.hpp>

namespace stan {
namespace math {

template <typename F>
double finite_diff_hess_helper(
    const F& f, const Eigen::Matrix<double, Eigen::Dynamic, 1>& x, int lambda,
    double epsilon = 1e-03) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  Matrix<double, Dynamic, 1> x_temp(x);

  double grad = 0.0;
  x_temp(lambda) = x(lambda) + 2.0 * epsilon;
  grad = -f(x_temp);

  x_temp(lambda) = x(lambda) + -2.0 * epsilon;
  grad += f(x_temp);

  x_temp(lambda) = x(lambda) + epsilon;
  grad += 8.0 * f(x_temp);

  x_temp(lambda) = x(lambda) + -epsilon;
  grad -= 8.0 * f(x_temp);

  return grad;
}

/**
 * Calculate the value and the Hessian of the specified function
 * at the specified argument using second-order finite difference.
 *
 * <p>The functor must implement
 *
 * <code>
 * double
 * operator()(const
 * Eigen::Matrix<double, Eigen::Dynamic, 1>&)
 * </code>
 *
 * Error should be on order of epsilon ^ 4, with 4
 * calls to the function f.
 *
 * Reference:
 * Eberly: Derivative Approximation by Finite Differences
 * Page 6
 *
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] grad_fx Gradient of function at argument
 * @param[out] hess_fx Hessian of function at argument
 * @param[in] epsilon perturbation size
 */
template <typename F>
void finite_diff_hessian(const F& f, const Eigen::Matrix<double, -1, 1>& x,
                         double& fx, Eigen::Matrix<double, -1, 1>& grad_fx,
                         Eigen::Matrix<double, -1, -1>& hess_fx,
                         double epsilon = 1e-03) {
  using Eigen::Dynamic;
  using Eigen::Matrix;

  int d = x.size();

  Matrix<double, Dynamic, 1> x_temp(x);
  hess_fx.resize(d, d);

  finite_diff_gradient(f, x, fx, grad_fx);
  double f_diff(0.0);
  for (int i = 0; i < d; ++i) {
    for (int j = i; j < d; ++j) {
      x_temp(i) += 2.0 * epsilon;
      if (i != j) {
        f_diff = -finite_diff_hess_helper(f, x_temp, j);
        x_temp(i) = x(i) + -2.0 * epsilon;
        f_diff += finite_diff_hess_helper(f, x_temp, j);
        x_temp(i) = x(i) + epsilon;
        f_diff += 8.0 * finite_diff_hess_helper(f, x_temp, j);
        x_temp(i) = x(i) + -epsilon;
        f_diff -= 8.0 * finite_diff_hess_helper(f, x_temp, j);
        f_diff /= 12.0 * epsilon * 12.0 * epsilon;
      } else {
        f_diff = -f(x_temp);
        f_diff -= 30 * fx;
        x_temp(i) = x(i) + -2.0 * epsilon;
        f_diff -= f(x_temp);
        x_temp(i) = x(i) + epsilon;
        f_diff += 16.0 * f(x_temp);
        x_temp(i) = x(i) - epsilon;
        f_diff += 16.0 * f(x_temp);
        f_diff /= 12 * epsilon * epsilon;
      }

      x_temp(i) = x(i);

      hess_fx(j, i) = f_diff;
      hess_fx(i, j) = hess_fx(j, i);
    }
  }
}
}  // namespace math
}  // namespace stan
#endif
