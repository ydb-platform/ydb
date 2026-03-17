#ifndef STAN_MATH_REV_MAT_FUN_SIMPLEX_CONSTRAIN_HPP
#define STAN_MATH_REV_MAT_FUN_SIMPLEX_CONSTRAIN_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/rev/mat/functor/adj_jac_apply.hpp>
#include <tuple>
#include <vector>

namespace stan {
namespace math {

namespace internal {
class simplex_constrain_op {
  int N_;
  double* diag_;  // diagonal of the Jacobian of the operator
  double* z_;

 public:
  /**
   * Return the simplex corresponding to the specified free vector.
   * A simplex is a vector containing values greater than or equal
   * to 0 that sum to 1.  A vector with (K-1) unconstrained values
   * will produce a simplex of size K.
   *
   * The transform is based on a centered stick-breaking process.
   *
   * @tparam size Number of adjoints to return
   * @param needs_adj Boolean indicators of if adjoints of arguments will be
   * needed
   * @param y Free vector input of dimensionality K - 1
   * @return Simplex of dimensionality K
   */
  template <std::size_t size>
  Eigen::VectorXd operator()(const std::array<bool, size>& needs_adj,
                             const Eigen::VectorXd& y) {
    N_ = y.size();
    diag_ = ChainableStack::instance().memalloc_.alloc_array<double>(N_);
    z_ = ChainableStack::instance().memalloc_.alloc_array<double>(N_);

    Eigen::Matrix<double, Eigen::Dynamic, 1> x(N_ + 1);
    double stick_len(1.0);
    for (int k = 0; k < N_; ++k) {
      double log_N_minus_k = std::log(N_ - k);
      z_[k] = inv_logit(y(k) - log_N_minus_k);
      diag_[k] = stick_len * z_[k] * inv_logit(log_N_minus_k - y(k));
      x(k) = stick_len * z_[k];
      stick_len -= x(k);
    }
    x(N_) = stick_len;
    return x;
  }

  /*
   * Compute the result of multiply the transpose of the adjoint vector times
   * the Jacobian of the simplex_constrain operator.
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
    double acc = adj(N_);

    if (N_ > 0) {
      adj_times_jac(N_ - 1) = diag_[N_ - 1] * (adj(N_ - 1) - acc);
      for (int n = N_ - 1; --n >= 0;) {
        acc = adj(n + 1) * z_[n + 1] + (1 - z_[n + 1]) * acc;
        adj_times_jac(n) = diag_[n] * (adj(n) - acc);
      }
    }

    return std::make_tuple(adj_times_jac);
  }
};
}  // namespace internal

/**
 * Return the simplex corresponding to the specified free vector.
 * A simplex is a vector containing values greater than or equal
 * to 0 that sum to 1.  A vector with (K-1) unconstrained values
 * will produce a simplex of size K.
 *
 * The transform is based on a centered stick-breaking process.
 *
 * @param y Free vector input of dimensionality K - 1
 * @return Simplex of dimensionality K
 */
inline Eigen::Matrix<var, Eigen::Dynamic, 1> simplex_constrain(
    const Eigen::Matrix<var, Eigen::Dynamic, 1>& y) {
  return adj_jac_apply<internal::simplex_constrain_op>(y);
}

}  // namespace math
}  // namespace stan
#endif
