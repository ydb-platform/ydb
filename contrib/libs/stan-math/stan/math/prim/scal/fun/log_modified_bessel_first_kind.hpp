//  Copyright (c) 2006 Xiaogang Zhang
//  Copyright (c) 2007, 2017 John Maddock

#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_MODIFIED_BESSEL_FIRST_KIND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_MODIFIED_BESSEL_FIRST_KIND_HPP

#include <boost/math/tools/rational.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/inv.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/log_sum_exp.hpp>
#include <stan/math/prim/scal/fun/log1p.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <limits>

namespace stan {
namespace math {

/* Log of the modified Bessel function of the first kind,
 * which is better known as the Bessel I function. See
 * modified_bessel_first_kind.hpp for the function definition.
 * The derivatives are known to be incorrect for v = 0 because a
 * simple constant 0 is returned.
 *
 * @tparam T1 type of the order (v)
 * @tparam T2 type of argument (z)
 * @param v Order, can be a non-integer but must be at least -1
 * @param z Real non-negative number
 * @throws std::domain_error if either v or z is NaN, z is
 * negative, or v is less than -1
 * @return log of Bessel I function
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2, double>::type
log_modified_bessel_first_kind(const T1 v, const T2 z) {
  check_not_nan("log_modified_bessel_first_kind", "first argument (order)", v);
  check_not_nan("log_modified_bessel_first_kind", "second argument (variable)",
                z);
  check_nonnegative("log_modified_bessel_first_kind",
                    "second argument (variable)", z);
  check_greater_or_equal("log_modified_bessel_first_kind",
                         "first argument (order)", v, -1);

  using boost::math::tools::evaluate_polynomial;
  using std::log;
  using std::sqrt;

  typedef typename boost::math::tools::promote_args<T1, T2, double>::type T;

  if (z == 0) {
    if (v == 0)
      return 0.0;
    if (v > 0)
      return -std::numeric_limits<T>::infinity();
    return std::numeric_limits<T>::infinity();
  }
  if (is_inf(z))
    return z;
  if (v == 0) {
    // WARNING: will not autodiff for v = 0 correctly
    // modified from Boost's bessel_i0_imp in the double precision case,
    // which refers to:
    // Modified Bessel function of the first kind of order zero
    // we use the approximating forms derived in:
    // "Rational Approximations for the Modified Bessel Function of the
    // First Kind -- I0(x) for Computations with Double Precision"
    // by Pavel Holoborodko, see
    // http://www.advanpix.com/2015/11/11/rational-approximations-for-the-modified-bessel-function-of-the-first-kind-i0-computations-double-precision
    // The actual coefficients used are [Boost's] own, and extend
    // Pavel's work to precisions other than double.

    if (z < 7.75) {
      // Bessel I0 over[10 ^ -16, 7.75]
      // Max error in interpolated form : 3.042e-18
      // Max Error found at double precision = Poly : 5.106609e-16
      //                                       Cheb : 5.239199e-16
      static const double P[]
          = {1.00000000000000000e+00, 2.49999999999999909e-01,
             2.77777777777782257e-02, 1.73611111111023792e-03,
             6.94444444453352521e-05, 1.92901234513219920e-06,
             3.93675991102510739e-08, 6.15118672704439289e-10,
             7.59407002058973446e-12, 7.59389793369836367e-14,
             6.27767773636292611e-16, 4.34709704153272287e-18,
             2.63417742690109154e-20, 1.13943037744822825e-22,
             9.07926920085624812e-25};
      return log1p_exp(2 * log(z) - log(4.0)
                       + log(evaluate_polynomial(P, 0.25 * square(z))));
    }
    if (z < 500) {
      // Max error in interpolated form : 1.685e-16
      // Max Error found at double precision = Poly : 2.575063e-16
      //                                       Cheb : 2.247615e+00
      static const double P[]
          = {3.98942280401425088e-01,  4.98677850604961985e-02,
             2.80506233928312623e-02,  2.92211225166047873e-02,
             4.44207299493659561e-02,  1.30970574605856719e-01,
             -3.35052280231727022e+00, 2.33025711583514727e+02,
             -1.13366350697172355e+04, 4.24057674317867331e+05,
             -1.23157028595698731e+07, 2.80231938155267516e+08,
             -5.01883999713777929e+09, 7.08029243015109113e+10,
             -7.84261082124811106e+11, 6.76825737854096565e+12,
             -4.49034849696138065e+13, 2.24155239966958995e+14,
             -8.13426467865659318e+14, 2.02391097391687777e+15,
             -3.08675715295370878e+15, 2.17587543863819074e+15};
      return z + log(evaluate_polynomial(P, inv(z))) - 0.5 * log(z);
    }
    // Max error in interpolated form : 2.437e-18
    // Max Error found at double precision = Poly : 1.216719e-16
    static const double P[] = {3.98942280401432905e-01, 4.98677850491434560e-02,
                               2.80506308916506102e-02, 2.92179096853915176e-02,
                               4.53371208762579442e-02};
    return z + log(evaluate_polynomial(P, inv(z))) - 0.5 * log(z);
  }
  if (v == 1) {  // WARNING: will not autodiff for v = 1 correctly
    // modified from Boost's bessel_i1_imp in the double precision case
    // see credits above in the v == 0 case
    if (z < 7.75) {
      // Bessel I0 over[10 ^ -16, 7.75]
      // Max error in interpolated form: 5.639e-17
      // Max Error found at double precision = Poly: 1.795559e-16

      static const double P[]
          = {8.333333333333333803e-02, 6.944444444444341983e-03,
             3.472222222225921045e-04, 1.157407407354987232e-05,
             2.755731926254790268e-07, 4.920949692800671435e-09,
             6.834657311305621830e-11, 7.593969849687574339e-13,
             6.904822652741917551e-15, 5.220157095351373194e-17,
             3.410720494727771276e-19, 1.625212890947171108e-21,
             1.332898928162290861e-23};
      T a = square(z) * 0.25;
      T Q[3] = {1, 0.5, evaluate_polynomial(P, a)};
      return log(z) + log(evaluate_polynomial(Q, a)) - log(2.0);
    }
    if (z < 500) {
      // Max error in interpolated form: 1.796e-16
      // Max Error found at double precision = Poly: 2.898731e-16

      static const double P[]
          = {3.989422804014406054e-01,  -1.496033551613111533e-01,
             -4.675104253598537322e-02, -4.090895951581637791e-02,
             -5.719036414430205390e-02, -1.528189554374492735e-01,
             3.458284470977172076e+00,  -2.426181371595021021e+02,
             1.178785865993440669e+04,  -4.404655582443487334e+05,
             1.277677779341446497e+07,  -2.903390398236656519e+08,
             5.192386898222206474e+09,  -7.313784438967834057e+10,
             8.087824484994859552e+11,  -6.967602516005787001e+12,
             4.614040809616582764e+13,  -2.298849639457172489e+14,
             8.325554073334618015e+14,  -2.067285045778906105e+15,
             3.146401654361325073e+15,  -2.213318202179221945e+15};
      return z + log(evaluate_polynomial(P, inv(z))) - 0.5 * log(z);
    }
    // Max error in interpolated form: 1.320e-19
    // Max Error found at double precision = Poly: 7.065357e-17
    static const double P[]
        = {3.989422804014314820e-01, -1.496033551467584157e-01,
           -4.675105322571775911e-02, -4.090421597376992892e-02,
           -5.843630344778927582e-02};
    return z + log(evaluate_polynomial(P, inv(z))) - 0.5 * log(z);
  }
  if (z > 100) {
    // Boost does something like this in asymptotic_bessel_i_large_x
    T lim = (square(v) + 2.5) / (2 * z);
    lim *= lim;
    lim *= lim;
    lim /= 24;
    if (lim < (std::numeric_limits<double>::epsilon() * 10)) {
      T s = 1;
      T mu = 4 * square(v);
      T ex = 8 * z;
      T num = mu - 1;
      T denom = ex;
      s -= num / denom;
      num *= mu - 9;
      denom *= ex * 2;
      s += num / denom;
      num *= mu - 25;
      denom *= ex * 3;
      s -= num / denom;
      s = z - log(sqrt(2 * z * pi())) + log(s);
      return s;
    }
  }

  typename boost::math::tools::promote_args<T2>::type log_half_z = log(0.5 * z);
  typename boost::math::tools::promote_args<T1>::type lgam
      = v > -1 ? lgamma(v + 1.0) : 0;
  T lcons = (2.0 + v) * log_half_z;
  T out;
  if (v > -1) {
    out = log_sum_exp(v * log_half_z - lgam, lcons - lgamma(v + 2));
    lgam += log1p(v);
  } else {
    out = lcons;
  }

  double m = 2;
  double lfac = 0;
  T old_out;
  do {
    lfac += log(m);
    lgam += log(v + m);
    lcons += 2 * log_half_z;
    old_out = out;
    out = log_sum_exp(out, lcons - lfac - lgam);  // underflows eventually
    m++;
  } while (out > old_out || out < old_out);
  return out;
}

}  // namespace math
}  // namespace stan

#endif
