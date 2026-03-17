#ifndef STAN_MATH_REV_MAT_FUN_VARIANCE_HPP
#define STAN_MATH_REV_MAT_FUN_VARIANCE_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/mean.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <vector>

namespace stan {
namespace math {

namespace internal {

inline var calc_variance(size_t size, const var* dtrs) {
  vari** varis = ChainableStack::instance().memalloc_.alloc_array<vari*>(size);
  for (size_t i = 0; i < size; ++i)
    varis[i] = dtrs[i].vi_;
  double sum = 0.0;
  for (size_t i = 0; i < size; ++i)
    sum += dtrs[i].vi_->val_;
  double mean = sum / size;
  double sum_of_squares = 0;
  double reciprocal_size_m1 = 1.0 / (size - 1);
  double two_over_size_m1 = 2.0 * reciprocal_size_m1;
  double* partials
      = ChainableStack::instance().memalloc_.alloc_array<double>(size);
  for (size_t i = 0; i < size; ++i) {
    double diff = dtrs[i].vi_->val_ - mean;
    sum_of_squares += diff * diff;
    partials[i] = two_over_size_m1 * diff;
  }
  double variance = sum_of_squares * reciprocal_size_m1;
  return var(new stored_gradient_vari(variance, size, varis, partials));
}

}  // namespace internal

/**
 * Return the sample variance of the specified standard
 * vector.  Raise domain error if size is not greater than zero.
 *
 * @param[in] v a vector
 * @return sample variance of specified vector
 */
inline var variance(const std::vector<var>& v) {
  check_nonzero_size("variance", "v", v);
  if (v.size() == 1)
    return 0;
  return internal::calc_variance(v.size(), &v[0]);
}

/*
 * Return the sample variance of the specified vector, row vector,
 * or matrix.  Raise domain error if size is not greater than
 * zero.
 *
 * @tparam R number of rows
 * @tparam C number of columns
 * @param[in] m input matrix
 * @return sample variance of specified matrix
 */
template <int R, int C>
var variance(const Eigen::Matrix<var, R, C>& m) {
  check_nonzero_size("variance", "m", m);
  if (m.size() == 1)
    return 0;
  return internal::calc_variance(m.size(), &m(0));
}

}  // namespace math
}  // namespace stan
#endif
