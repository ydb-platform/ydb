#ifndef STAN_MATH_REV_MAT_FUN_SD_HPP
#define STAN_MATH_REV_MAT_FUN_SD_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/mean.hpp>
#include <stan/math/rev/core.hpp>
#include <boost/math/tools/promotion.hpp>
#include <cmath>
#include <vector>

namespace stan {
namespace math {

namespace internal {

// if x.size() = N, and x[i] = x[j] =
// then lim sd(x) -> 0 [ d/dx[n] sd(x) ] = sqrt(N) / N

inline var calc_sd(size_t size, const var* dtrs) {
  using std::sqrt;
  vari** varis = reinterpret_cast<vari**>(
      ChainableStack::instance().memalloc_.alloc(size * sizeof(vari*)));
  for (size_t i = 0; i < size; ++i)
    varis[i] = dtrs[i].vi_;
  double sum = 0.0;
  for (size_t i = 0; i < size; ++i)
    sum += dtrs[i].vi_->val_;
  double mean = sum / size;
  double sum_of_squares = 0;
  for (size_t i = 0; i < size; ++i) {
    double diff = dtrs[i].vi_->val_ - mean;
    sum_of_squares += diff * diff;
  }
  double variance = sum_of_squares / (size - 1);
  double sd = sqrt(variance);
  double* partials = reinterpret_cast<double*>(
      ChainableStack::instance().memalloc_.alloc(size * sizeof(double)));
  if (sum_of_squares < 1e-20) {
    double grad_limit = 1 / std::sqrt(static_cast<double>(size));
    for (size_t i = 0; i < size; ++i)
      partials[i] = grad_limit;
  } else {
    double multiplier = 1 / (sd * (size - 1));
    for (size_t i = 0; i < size; ++i)
      partials[i] = multiplier * (dtrs[i].vi_->val_ - mean);
  }
  return var(new stored_gradient_vari(sd, size, varis, partials));
}

}  // namespace internal

/**
 * Return the sample standard deviation of the specified standard
 * vector.  Raise domain error if size is not greater than zero.
 *
 * @param[in] v a vector
 * @return sample standard deviation of specified vector
 */
inline var sd(const std::vector<var>& v) {
  check_nonzero_size("sd", "v", v);
  if (v.size() == 1)
    return 0;
  return internal::calc_sd(v.size(), &v[0]);
}

/*
 * Return the sample standard deviation of the specified vector,
 * row vector, or matrix.  Raise domain error if size is not
 * greater than zero.
 *
 * @tparam R number of rows
 * @tparam C number of columns
 * @param[in] m input matrix
 * @return sample standard deviation of specified matrix
 */
template <int R, int C>
var sd(const Eigen::Matrix<var, R, C>& m) {
  check_nonzero_size("sd", "m", m);
  if (m.size() == 1)
    return 0;
  return internal::calc_sd(m.size(), &m(0));
}

}  // namespace math
}  // namespace stan
#endif
