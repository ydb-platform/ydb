#ifndef STAN_MATH_PRIM_SCAL_FUN_GRAD_2F1_HPP
#define STAN_MATH_PRIM_SCAL_FUN_GRAD_2F1_HPP

#include <stan/math/prim/scal/fun/sign.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/check_2F1_converges.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Gradients of the hypergeometric function, 2F1.
 *
 * Calculate the gradients of the hypergeometric function (2F1)
 * as the power series stopping when the series converges
 * to within <code>precision</code> or throwing when the
 * function takes <code>max_steps</code> steps.
 *
 * This power-series representation converges for all gradients
 * under the same conditions as the 2F1 function itself.
 *
 * @tparam T type of arguments and result
 * @param[out] g_a1 g_a1 pointer to array of six values of type T, result.
 * @param[out] g_b1 g_b1 pointer to array of six values of type T, result.
 * @param[in] a1 a1 see generalized hypergeometric function definition.
 * @param[in] a2 a2 see generalized hypergeometric function definition.
 * @param[in] b1 b1 see generalized hypergeometric function definition.
 * @param[in] z z see generalized hypergeometric function definition.
 * @param[in] precision precision of the infinite sum.
 * @param[in] max_steps number of steps to take.
 */
template <typename T>
void grad_2F1(T& g_a1, T& g_b1, const T& a1, const T& a2, const T& b1,
              const T& z, const T& precision = 1e-10, int max_steps = 1e5) {
  check_2F1_converges("grad_2F1", a1, a2, b1, z);

  using std::exp;
  using std::fabs;
  using std::log;

  g_a1 = 0.0;
  g_b1 = 0.0;

  T log_g_old[2];
  for (auto& i : log_g_old)
    i = -std::numeric_limits<T>::infinity();

  T log_t_old = 0.0;
  T log_t_new = 0.0;

  T log_z = log(z);

  double log_t_new_sign = 1.0;
  double log_t_old_sign = 1.0;
  double log_g_old_sign[2];
  for (double& x : log_g_old_sign)
    x = 1.0;

  for (int k = 0; k <= max_steps; ++k) {
    T p = (a1 + k) * (a2 + k) / ((b1 + k) * (1 + k));
    if (p == 0)
      return;

    log_t_new += log(fabs(p)) + log_z;
    log_t_new_sign = p >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    T term = log_g_old_sign[0] * log_t_old_sign * exp(log_g_old[0] - log_t_old)
             + 1 / (a1 + k);
    log_g_old[0] = log_t_new + log(fabs(term));
    log_g_old_sign[0] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    term = log_g_old_sign[1] * log_t_old_sign * exp(log_g_old[1] - log_t_old)
           - 1 / (b1 + k);
    log_g_old[1] = log_t_new + log(fabs(term));
    log_g_old_sign[1] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    g_a1 += log_g_old_sign[0] > 0 ? exp(log_g_old[0]) : -exp(log_g_old[0]);
    g_b1 += log_g_old_sign[1] > 0 ? exp(log_g_old[1]) : -exp(log_g_old[1]);

    if (log_t_new <= log(precision))
      return;  // implicit abs

    log_t_old = log_t_new;
    log_t_old_sign = log_t_new_sign;
  }
  domain_error("grad_2F1", "k (internal counter)", max_steps, "exceeded ",
               " iterations, hypergeometric function gradient "
               "did not converge.");
  return;
}

}  // namespace math
}  // namespace stan
#endif
