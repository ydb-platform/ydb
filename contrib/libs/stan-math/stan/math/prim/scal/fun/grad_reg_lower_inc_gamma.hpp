#ifndef STAN_MATH_PRIM_SCAL_FUN_LOWER_REG_INC_GAMMA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOWER_REG_INC_GAMMA_HPP

#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/gamma_p.hpp>
#include <stan/math/prim/scal/fun/log1p.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_gamma.hpp>
#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Computes the gradient of the lower regularized incomplete
 * gamma function.
 *
 * The lower incomlete gamma function
 * derivative w.r.t it's first parameter (a) seems to have no
 * standard source.  It also appears to have no widely known
 * approximate implementation.  Gautschi (1979) has a thorough
 * discussion of the calculation of the lower regularized
 * incomplete gamma function itself and some stability issues.
 *
 * Reference: Gautschi, Walter (1979) ACM Transactions on
 * mathematical software. 5(4):466-481
 *
 * We implemented calculations for d(gamma_p)/da by taking
 * derivatives of formulas suggested by Gauschi and others and
 * testing them against an outside source (Mathematica). We
 * took three implementations which can cover the range {a:[0,20],
 * z:[0,30]} with absolute error < 1e-10 with the exception of
 * values near (0,0) where the error is near 1e-5.  Relative error
 * is also <<1e-6 except for regions where the gradient approaches
 * zero.
 *
 * Gautschi suggests calculating the lower incomplete gamma
 * function for small to moderate values of $z$ using the
 * approximation:
 *
 * \f[
 *  \frac{\gamma(a,z)}{\Gamma(a)}=z^a e^-z
 *    \sum_n=0^\infty \frac{z^n}{\Gamma(a+n+1)}
 * \f]
 *
 * We write the derivative in the form:
 *
 * \f[
 *   \frac{d\gamma(a,z)\Gamma(a)}{da} = \frac{\log z}{e^z}
 *     \sum_n=0^\infty \frac{z^{a+n}}{\Gamma(a+n+1)}
 *   - \frac{1}{e^z}
 *     \sum_n=0^\infty \frac{z^{a+n}}{\Gamma(a+n+1)}\psi^0(a+n+1)
 * \f]
 *
 * This caclulation is sufficiently accurate for small $a$ and
 * small $z$.  For larger values and $a$ and $z$ we use it in its
 * log form:
 *
 * \f[
 *   \frac{d \gamma(a,z)\Gamma(a)}{da} = \frac{\log z}{e^z}
 *     \sum_n=0^\infty \exp[(a+n)\log z - \log\Gamma(a+n+1)]
 *   - \sum_n=0^\infty \exp[(a+n)\log z - \log\Gamma(a+n+1) +
 *       \log\psi^0(a+n+1)]
 * \f]
 *
 * For large $z$, Gauschi recommends using the upper incomplete
 * Gamma instead and the negative of its derivative turns out to be
 * more stable and accurate for larger $z$ and for some combinations
 * of $a$ and $z$. This is a log-scale implementation of the
 * derivative of the formulation suggested by Gauschi (1979). For
 * some values it defers to the negative of the gradient
 * for the gamma_q function. This is based on the suggestion by Gauschi
 * (1979) that for large values of $z$ it is better to
 * carry out calculations using the upper incomplete Gamma function.
 *
 * Branching for choice of implementation for the lower incomplete
 * regularized gamma function gradient. The derivative based on
 * Gautschi's formulation appears to be sufficiently accurate
 * everywhere except for large z and small to moderate a. The
 * intersection between the two regions is a radius 12 quarter circle
 * centered at a=0, z=30 although both implementations are
 * satisfactory near the intersection.
 *
 * Some limits that could be treated, e.g., infinite z should
 * return tgamma(a) * digamma(a), throw instead to match the behavior of,
 * e.g., boost::math::gamma_p
 *
 * @tparam T1 type of a
 * @tparam T2 type of z
 * @param[in] a shared with complete Gamma
 * @param[in] z value to integrate up to
 * @param[in] precision series terminates when increment falls below
 * this value.
 * @param[in] max_steps number of terms to sum before throwing
 * @throw std::domain_error if the series does not converge to
 * requested precision before max_steps.
 *
 */
template <typename T1, typename T2>
typename return_type<T1, T2>::type grad_reg_lower_inc_gamma(
    const T1& a, const T2& z, double precision = 1e-10, int max_steps = 1e5) {
  using std::exp;
  using std::log;
  using std::pow;
  typedef typename return_type<T1, T2>::type TP;

  if (is_nan(a) || is_nan(z))
    return std::numeric_limits<TP>::quiet_NaN();

  check_positive_finite("grad_reg_lower_inc_gamma", "a", a);

  if (z == 0.0)
    return 0.0;
  check_positive_finite("grad_reg_lower_inc_gamma", "z", z);

  if ((a < 0.8 && z > 15.0) || (a < 12.0 && z > 30.0)
      || a < sqrt(-756 - value_of_rec(z) * value_of_rec(z)
                  + 60 * value_of_rec(z))) {
    T1 tg = tgamma(a);
    T1 dig = digamma(a);
    return -grad_reg_inc_gamma(a, z, tg, dig, max_steps, precision);
  }

  T2 log_z = log(z);
  T2 emz = exp(-z);

  int n = 0;
  T1 a_plus_n = a;
  TP sum_a = 0.0;
  T1 lgamma_a_plus_1 = lgamma(a + 1);
  T1 lgamma_a_plus_n_plus_1 = lgamma_a_plus_1;
  TP term;
  while (true) {
    term = exp(a_plus_n * log_z - lgamma_a_plus_n_plus_1);
    sum_a += term;
    if (term <= precision)
      break;
    if (n >= max_steps)
      domain_error("grad_reg_lower_inc_gamma", "n (internal counter)",
                   max_steps, "exceeded ",
                   " iterations, gamma_p(a,z) gradient (a) "
                   "did not converge.");
    ++n;
    lgamma_a_plus_n_plus_1 += log1p(a_plus_n);
    ++a_plus_n;
  }

  n = 1;
  a_plus_n = a + 1;
  TP sum_b = digamma(a + 1) * exp(a * log_z - lgamma_a_plus_1);
  lgamma_a_plus_n_plus_1 = lgamma_a_plus_1 + log(a_plus_n);
  while (true) {
    term = exp(a_plus_n * log_z - lgamma_a_plus_n_plus_1)
           * digamma(a_plus_n + 1);
    sum_b += term;
    if (term <= precision)
      return emz * (log_z * sum_a - sum_b);
    if (n >= max_steps)
      domain_error("grad_reg_lower_inc_gamma", "n (internal counter)",
                   max_steps, "exceeded ",
                   " iterations, gamma_p(a,z) gradient (a) "
                   "did not converge.");
    ++n;
    lgamma_a_plus_n_plus_1 += log(a_plus_n + 1);
    ++a_plus_n;
  }
}

}  // namespace math
}  // namespace stan

#endif
