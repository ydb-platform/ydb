#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_FINITE_DIFF_GRADIENT_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_FINITE_DIFF_GRADIENT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Calculate the value and the gradient of the specified function
 * at the specified argument using finite difference.
 *
 * <p>The functor must implement
 *
 * <code>
 * double
 * operator()(const
 * Eigen::Matrix<double, Eigen::Dynamic, 1>&)
 * </code>
 *
 * Error should be on order of epsilon ^ 6.
 * The reference for this algorithm is:
 *
 * De Levie: An improved numerical approximation
 * for the first derivative, page 3
 *
 * This function involves 6 calls to f.
 *
 * @tparam F Type of function
 * @param[in] f Function
 * @param[in] x Argument to function
 * @param[out] fx Function applied to argument
 * @param[out] grad_fx Gradient of function at argument
 * @param[in] epsilon perturbation size
 */
template <typename F>
void finite_diff_gradient(const F& f, const Eigen::Matrix<double, -1, 1>& x,
                          double& fx, Eigen::Matrix<double, -1, 1>& grad_fx,
                          double epsilon = 1e-03) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  Matrix<double, Dynamic, 1> x_temp(x);

  int d = x.size();
  grad_fx.resize(d);

  fx = f(x);

  for (int i = 0; i < d; ++i) {
    double delta_f = 0.0;

    x_temp(i) = x(i) + 3.0 * epsilon;
    delta_f = f(x_temp);

    x_temp(i) = x(i) + 2.0 * epsilon;
    delta_f -= 9.0 * f(x_temp);

    x_temp(i) = x(i) + epsilon;
    delta_f += 45.0 * f(x_temp);

    x_temp(i) = x(i) + -3.0 * epsilon;
    delta_f -= f(x_temp);

    x_temp(i) = x(i) + -2.0 * epsilon;
    delta_f += 9.0 * f(x_temp);

    x_temp(i) = x(i) + -epsilon;
    delta_f -= 45.0 * f(x_temp);

    delta_f /= 60 * epsilon;

    x_temp(i) = x(i);
    grad_fx(i) = delta_f;
  }
}
}  // namespace math
}  // namespace stan
#endif
