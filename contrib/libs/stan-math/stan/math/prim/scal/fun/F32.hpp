#ifndef STAN_MATH_PRIM_SCAL_FUN_F32_HPP
#define STAN_MATH_PRIM_SCAL_FUN_F32_HPP

#include <stan/math/prim/scal/fun/sign.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/err/check_3F2_converges.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Hypergeometric function (3F2).
 *
 * Function reference: http://dlmf.nist.gov/16.2
 *
 * \f[
 *   _3F_2 \left(
 *     \begin{matrix}a_1 a_2 a3 \\ b_1 b_2\end{matrix}; z
 *     \right) = \sum_k=0^\infty
 * \frac{(a_1)_k(a_2)_k(a_3)_k}{(b_1)_k(b_2)_k}\frac{z^k}{k!} \f]
 *
 * Where $(a_1)_k$ is an upper shifted factorial.
 *
 * Calculate the hypergeometric function (3F2) as the power series
 * directly to within <code>precision</code> or until
 * <code>max_steps</code> terms.
 *
 * This function does not have a closed form but will converge if:
 *   - <code>|z|</code> is less than 1
 *   - <code>|z|</code> is equal to one and <code>b1 + b2 < a1 + a2 + a3</code>
 * This function is a rational polynomial if
 *   - <code>a1</code>, <code>a2</code>, or <code>a3</code> is a
 *     non-positive integer
 * This function can be treated as a rational polynomial if
 *   - <code>b1</code> or <code>b2</code> is a non-positive integer
 *     and the series is terminated prior to the final term.
 *
 * @tparam T type of arguments and result
 * @param[in] a1 a1 (always called with 1 from beta binomial cdfs)
 * @param[in] a2 a2 (always called with a2 > 1)
 * @param[in] a3 a3 (always called with int a3 <= 0)
 * @param[in] b1 b1 (always called with int b1 < |a3|)
 * @param[in] b2 b2 (always <= 1)
 * @param[in] z z (is always called with 1 from beta binomial cdfs)
 * @param[in] precision precision of the infinite sum. defaults to 1e-6
 * @param[in] max_steps number of steps to take. defaults to 1e5
 */
template <typename T>
T F32(const T& a1, const T& a2, const T& a3, const T& b1, const T& b2,
      const T& z, double precision = 1e-6, int max_steps = 1e5) {
  check_3F2_converges("F32", a1, a2, a3, b1, b2, z);

  using std::exp;
  using std::fabs;
  using std::log;

  T t_acc = 1.0;
  T log_t = 0.0;
  T log_z = log(z);
  double t_sign = 1.0;

  for (int k = 0; k <= max_steps; ++k) {
    T p = (a1 + k) * (a2 + k) * (a3 + k) / ((b1 + k) * (b2 + k) * (k + 1));
    if (p == 0.0)
      return t_acc;

    log_t += log(fabs(p)) + log_z;
    t_sign = p >= 0.0 ? t_sign : -t_sign;
    T t_new = t_sign > 0.0 ? exp(log_t) : -exp(log_t);
    t_acc += t_new;

    if (fabs(t_new) <= precision)
      return t_acc;

    if (is_inf(t_acc)) {
      domain_error("F32", "sum (output)", t_acc, "overflow ",
                   " hypergeometric function did not converge.");
    }
  }
  domain_error("F32", "k (internal counter)", max_steps, "exceeded ",
               " iterations, hypergeometric function did not converge.");
  return t_acc;  // to silence warning.
}

}  // namespace math
}  // namespace stan
#endif
