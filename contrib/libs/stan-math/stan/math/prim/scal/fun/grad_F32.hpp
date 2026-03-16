#ifndef STAN_MATH_PRIM_SCAL_FUN_GRAD_F32_HPP
#define STAN_MATH_PRIM_SCAL_FUN_GRAD_F32_HPP

#include <stan/math/prim/scal/fun/inv.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/check_3F2_converges.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Gradients of the hypergeometric function, 3F2.
 *
 * Calculate the gradients of the hypergeometric function (3F2)
 * as the power series stopping when the series converges
 * to within <code>precision</code> or throwing when the
 * function takes <code>max_steps</code> steps.
 *
 * This power-series representation converges for all gradients
 * under the same conditions as the 3F2 function itself.
 *
 * @tparam T type of arguments and result
 * @param[out] g g pointer to array of six values of type T, result.
 * @param[in] a1 a1 see generalized hypergeometric function definition.
 * @param[in] a2 a2 see generalized hypergeometric function definition.
 * @param[in] a3 a3 see generalized hypergeometric function definition.
 * @param[in] b1 b1 see generalized hypergeometric function definition.
 * @param[in] b2 b2 see generalized hypergeometric function definition.
 * @param[in] z z see generalized hypergeometric function definition.
 * @param[in] precision precision of the infinite sum
 * @param[in] max_steps number of steps to take
 */
template <typename T>
void grad_F32(T* g, const T& a1, const T& a2, const T& a3, const T& b1,
              const T& b2, const T& z, const T& precision = 1e-6,
              int max_steps = 1e5) {
  check_3F2_converges("grad_F32", a1, a2, a3, b1, b2, z);

  using std::exp;
  using std::fabs;
  using std::log;

  for (int i = 0; i < 6; ++i)
    g[i] = 0.0;

  T log_g_old[6];
  for (auto& x : log_g_old)
    x = -std::numeric_limits<double>::infinity();

  T log_t_old = 0.0;
  T log_t_new = 0.0;

  T log_z = log(z);

  double log_t_new_sign = 1.0;
  double log_t_old_sign = 1.0;
  double log_g_old_sign[6];
  for (int i = 0; i < 6; ++i)
    log_g_old_sign[i] = 1.0;

  for (int k = 0; k <= max_steps; ++k) {
    T p = (a1 + k) * (a2 + k) * (a3 + k) / ((b1 + k) * (b2 + k) * (1 + k));
    if (p == 0)
      return;

    log_t_new += log(fabs(p)) + log_z;
    log_t_new_sign = p >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[0] = t_new * (g_old[0] / t_old + 1.0 / (a1 + k));
    T term = log_g_old_sign[0] * log_t_old_sign * exp(log_g_old[0] - log_t_old)
             + inv(a1 + k);
    log_g_old[0] = log_t_new + log(fabs(term));
    log_g_old_sign[0] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[1] = t_new * (g_old[1] / t_old + 1.0 / (a2 + k));
    term = log_g_old_sign[1] * log_t_old_sign * exp(log_g_old[1] - log_t_old)
           + inv(a2 + k);
    log_g_old[1] = log_t_new + log(fabs(term));
    log_g_old_sign[1] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[2] = t_new * (g_old[2] / t_old + 1.0 / (a3 + k));
    term = log_g_old_sign[2] * log_t_old_sign * exp(log_g_old[2] - log_t_old)
           + inv(a3 + k);
    log_g_old[2] = log_t_new + log(fabs(term));
    log_g_old_sign[2] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[3] = t_new * (g_old[3] / t_old - 1.0 / (b1 + k));
    term = log_g_old_sign[3] * log_t_old_sign * exp(log_g_old[3] - log_t_old)
           - inv(b1 + k);
    log_g_old[3] = log_t_new + log(fabs(term));
    log_g_old_sign[3] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[4] = t_new * (g_old[4] / t_old - 1.0 / (b2 + k));
    term = log_g_old_sign[4] * log_t_old_sign * exp(log_g_old[4] - log_t_old)
           - inv(b2 + k);
    log_g_old[4] = log_t_new + log(fabs(term));
    log_g_old_sign[4] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    //        g_old[5] = t_new * (g_old[5] / t_old + 1.0 / z);
    term = log_g_old_sign[5] * log_t_old_sign * exp(log_g_old[5] - log_t_old)
           + inv(z);
    log_g_old[5] = log_t_new + log(fabs(term));
    log_g_old_sign[5] = term >= 0.0 ? log_t_new_sign : -log_t_new_sign;

    for (int i = 0; i < 6; ++i) {
      g[i] += log_g_old_sign[i] * exp(log_g_old[i]);
    }

    if (log_t_new <= log(precision))
      return;  // implicit abs

    log_t_old = log_t_new;
    log_t_old_sign = log_t_new_sign;
  }
  domain_error("grad_F32", "k (internal counter)", max_steps, "exceeded ",
               " iterations, hypergeometric function gradient "
               "did not converge.");
  return;
}

}  // namespace math
}  // namespace stan
#endif
