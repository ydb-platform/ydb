#ifndef STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_RNG_HPP
#define STAN_MATH_PRIM_MAT_PROB_ORDERED_LOGISTIC_RNG_HPP

#include <boost/random/uniform_01.hpp>
#include <boost/random/variate_generator.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/mat/prob/categorical_rng.hpp>

namespace stan {
namespace math {

template <class RNG>
inline int ordered_logistic_rng(
    double eta, const Eigen::Matrix<double, Eigen::Dynamic, 1>& c, RNG& rng) {
  using boost::variate_generator;

  static const char* function = "ordered_logistic";

  check_finite(function, "Location parameter", eta);
  check_greater(function, "Size of cut points parameter", c.size(), 0);
  for (int i = 1; i < c.size(); ++i) {
    check_greater(function, "Cut points parameter", c(i), c(i - 1));
  }
  check_finite(function, "Cut points parameter", c(c.size() - 1));
  check_finite(function, "Cut points parameter", c(0));

  Eigen::VectorXd cut(c.rows() + 1);
  cut(0) = 1 - inv_logit(eta - c(0));
  for (int j = 1; j < c.rows(); j++)
    cut(j) = inv_logit(eta - c(j - 1)) - inv_logit(eta - c(j));
  cut(c.rows()) = inv_logit(eta - c(c.rows() - 1));

  return categorical_rng(cut, rng);
}

}  // namespace math
}  // namespace stan
#endif
