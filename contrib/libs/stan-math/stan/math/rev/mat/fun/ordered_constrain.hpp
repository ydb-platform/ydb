#ifndef STAN_MATH_REV_MAT_FUN_ORDERED_CONSTRAIN_HPP
#define STAN_MATH_REV_MAT_FUN_ORDERED_CONSTRAIN_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/mat/functor/adj_jac_apply.hpp>
#include <tuple>
#include <vector>

namespace stan {
namespace math {

namespace internal {
class ordered_constrain_op {
  int N_;
  double* exp_x_;

 public:
  /**
   * Return an increasing ordered vector derived from the specified
   * free vector.  The returned constrained vector will have the
   * same dimensionality as the specified free vector.
   *
   * @tparam size Number of arguments
   * @param needs_adj Boolean indicators of if adjoints of arguments will be
   * needed
   * @param x Free vector of scalars
   * @return Increasing ordered vector
   */
  template <std::size_t size>
  Eigen::VectorXd operator()(const std::array<bool, size>& needs_adj,
                             const Eigen::VectorXd& x) {
    N_ = x.size();

    Eigen::Matrix<double, Eigen::Dynamic, 1> y(N_);
    if (N_ == 0)
      return y;

    exp_x_ = ChainableStack::instance().memalloc_.alloc_array<double>(N_ - 1);

    y[0] = x[0];
    for (int n = 1; n < N_; ++n) {
      exp_x_[n - 1] = exp(x[n]);
      y[n] = y[n - 1] + exp_x_[n - 1];
    }
    return y;
  }

  /*
   * Compute the result of multiply the transpose of the adjoint vector times
   * the Jacobian of the ordered_constrain operator.
   *
   * @tparam size Number of adjoints to return
   * @param needs_adj Boolean indicators of if adjoints of arguments will be
   * needed
   * @param adj Eigen::VectorXd of adjoints at the output of the softmax
   * @return Eigen::VectorXd of adjoints propagated through softmax operation
   */
  template <std::size_t size>
  auto multiply_adjoint_jacobian(const std::array<bool, size>& needs_adj,
                                 const Eigen::VectorXd& adj) const {
    Eigen::VectorXd adj_times_jac(N_);
    double rolling_adjoint_sum = 0.0;

    if (N_ > 0) {
      for (int n = N_ - 1; n > 0; --n) {
        rolling_adjoint_sum += adj(n);
        adj_times_jac(n) = exp_x_[n - 1] * rolling_adjoint_sum;
      }
      adj_times_jac(0) = rolling_adjoint_sum + adj(0);
    }

    return std::make_tuple(adj_times_jac);
  }
};
}  // namespace internal

/**
 * Return an increasing ordered vector derived from the specified
 * free vector.  The returned constrained vector will have the
 * same dimensionality as the specified free vector.
 *
 * @param x Free vector of scalars
 * @return Increasing ordered vector
 */
inline Eigen::Matrix<var, Eigen::Dynamic, 1> ordered_constrain(
    const Eigen::Matrix<var, Eigen::Dynamic, 1>& x) {
  return adj_jac_apply<internal::ordered_constrain_op>(x);
}

}  // namespace math
}  // namespace stan
#endif
