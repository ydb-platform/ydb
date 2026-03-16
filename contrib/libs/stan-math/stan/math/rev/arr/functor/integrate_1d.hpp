#ifndef STAN_MATH_REV_ARR_FUNCTOR_integrate_1d_HPP
#define STAN_MATH_REV_ARR_FUNCTOR_integrate_1d_HPP

#include <stan/math/prim/arr/fun/value_of.hpp>
#include <stan/math/prim/arr/functor/integrate_1d.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/rev/scal/fun/is_nan.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <type_traits>
#include <string>
#include <vector>
#include <functional>
#include <ostream>
#include <limits>

namespace stan {
namespace math {

/**
 * Calculate first derivative of f(x, param, std::ostream&)
 * with respect to the nth parameter. Uses nested reverse mode autodiff
 *
 * Gradients that evaluate to NaN are set to zero if the function itself
 * evaluates to zero. If the function is not zero and the gradient evaluates to
 * NaN, a std::domain_error is thrown
 */
template <typename F>
inline double gradient_of_f(const F &f, const double &x, const double &xc,
                            const std::vector<double> &theta_vals,
                            const std::vector<double> &x_r,
                            const std::vector<int> &x_i, size_t n,
                            std::ostream &msgs) {
  double gradient = 0.0;
  start_nested();
  std::vector<var> theta_var(theta_vals.size());
  try {
    for (size_t i = 0; i < theta_vals.size(); i++)
      theta_var[i] = theta_vals[i];
    var fx = f(x, xc, theta_var, x_r, x_i, &msgs);
    fx.grad();
    gradient = theta_var[n].adj();
    if (is_nan(gradient)) {
      if (fx.val() == 0) {
        gradient = 0;
      } else {
        domain_error("gradient_of_f", "The gradient of f", n,
                     "is nan for parameter ", "");
      }
    }
  } catch (const std::exception &e) {
    recover_memory_nested();
    throw;
  }
  recover_memory_nested();

  return gradient;
}

/**
 * Compute the integral of the single variable function f from a to b to within
 * a specified relative tolerance. a and b can be finite or infinite.
 *
 * f should be compatible with reverse mode autodiff and have the signature:
 *   var f(double x, double xc, const std::vector<var>& theta,
 *     const std::vector<double>& x_r, const std::vector<int> &x_i,
 * std::ostream* msgs)
 *
 * It should return the value of the function evaluated at x. Any errors
 * should be printed to the msgs stream.
 *
 * Integrals that cross zero are broken into two, and the separate integrals are
 * each integrated to the given relative tolerance.
 *
 * For integrals with finite limits, the xc argument is the distance to the
 * nearest boundary. So for a > 0, b > 0, it will be a - x for x closer to a,
 * and b - x for x closer to b. xc is computed in a way that avoids the
 * precision loss of computing a - x or b - x manually. For integrals that cross
 * zero, xc can take values a - x, -x, or b - x depending on which integration
 * limit it is nearest.
 *
 * If either limit is infinite, xc is set to NaN
 *
 * The integration algorithm terminates when
 *   \f[
 *     \frac{{|I_{n + 1} - I_n|}}{{|I|_{n + 1}}} < \text{relative tolerance}
 *   \f]
 * where \f$I_{n}\f$ is the nth estimate of the integral and \f$|I|_{n}\f$ is
 * the nth estimate of the norm of the integral.
 *
 * Integrals that cross zero are
 * split into two. In this case, each integral is separately integrated to the
 * given relative_tolerance.
 *
 * Gradients of f that evaluate to NaN when the function evaluates to zero are
 * set to zero themselves. This is due to the autodiff easily overflowing to NaN
 * when evaluating gradients near the maximum and minimum floating point values
 * (where the function should be zero anyway for the integral to exist)
 *
 * @tparam T_a type of first limit
 * @tparam T_b type of second limit
 * @tparam T_theta type of parameters
 * @tparam T Type of f
 * @param f the functor to integrate
 * @param a lower limit of integration
 * @param b upper limit of integration
 * @param theta additional parameters to be passed to f
 * @param x_r additional data to be passed to f
 * @param x_i additional integer data to be passed to f
 * @param[in, out] msgs the print stream for warning messages
 * @param relative_tolerance relative tolerance passed to Boost quadrature
 * @return numeric integral of function f
 */
template <typename F, typename T_a, typename T_b, typename T_theta>
inline typename std::enable_if<std::is_same<T_a, var>::value
                                   || std::is_same<T_b, var>::value
                                   || std::is_same<T_theta, var>::value,
                               var>::type
integrate_1d(const F &f, const T_a &a, const T_b &b,
             const std::vector<T_theta> &theta, const std::vector<double> &x_r,
             const std::vector<int> &x_i, std::ostream &msgs,
             const double relative_tolerance
             = std::sqrt(std::numeric_limits<double>::epsilon())) {
  static const char *function = "integrate_1d";
  check_less_or_equal(function, "lower limit", a, b);

  if (value_of(a) == value_of(b)) {
    if (is_inf(a))
      domain_error(function, "Integration endpoints are both", value_of(a), "",
                   "");
    return var(0.0);
  } else {
    double integral = integrate(
        std::bind<double>(f, std::placeholders::_1, std::placeholders::_2,
                          value_of(theta), x_r, x_i, &msgs),
        value_of(a), value_of(b), relative_tolerance);

    size_t N_theta_vars = is_var<T_theta>::value ? theta.size() : 0;
    std::vector<double> dintegral_dtheta(N_theta_vars);
    std::vector<var> theta_concat(N_theta_vars);

    if (N_theta_vars > 0) {
      std::vector<double> theta_vals = value_of(theta);

      for (size_t n = 0; n < N_theta_vars; ++n) {
        dintegral_dtheta[n] = integrate(
            std::bind<double>(gradient_of_f<F>, f, std::placeholders::_1,
                              std::placeholders::_2, theta_vals, x_r, x_i, n,
                              std::ref(msgs)),
            value_of(a), value_of(b), relative_tolerance);
        theta_concat[n] = theta[n];
      }
    }

    if (!is_inf(a) && is_var<T_a>::value) {
      theta_concat.push_back(a);
      dintegral_dtheta.push_back(
          -value_of(f(value_of(a), 0.0, theta, x_r, x_i, &msgs)));
    }

    if (!is_inf(b) && is_var<T_b>::value) {
      theta_concat.push_back(b);
      dintegral_dtheta.push_back(
          value_of(f(value_of(b), 0.0, theta, x_r, x_i, &msgs)));
    }

    return precomputed_gradients(integral, theta_concat, dintegral_dtheta);
  }
}

}  // namespace math
}  // namespace stan

#endif
