#ifndef STAN_MATH_REV_MAT_FUN_SOFTMAX_HPP
#define STAN_MATH_REV_MAT_FUN_SOFTMAX_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/softmax.hpp>
#include <stan/math/rev/mat/functor/adj_jac_apply.hpp>
#include <vector>
#include <tuple>

namespace stan {
namespace math {

namespace internal {
class softmax_op {
  int N_;
  double* y_;  // Holds the results of the softmax

 public:
  softmax_op() : N_(0), y_(NULL) {}

  /*
   * Compute the softmax of the unconstrained input vector
   *
   * @param alpha Unconstrained input vector.
   * @return Softmax of the input.
   */
  template <std::size_t size>
  Eigen::VectorXd operator()(const std::array<bool, size>& needs_adj,
                             const Eigen::VectorXd& alpha) {
    N_ = alpha.size();
    y_ = ChainableStack::instance().memalloc_.alloc_array<double>(N_);

    auto y = softmax(alpha);
    for (int n = 0; n < N_; ++n)
      y_[n] = y(n);
    return y;
  }

  /*
   * Compute the result of multiply the transpose of the adjoint vector times
   * the Jacobian of the softmax operator. It is more efficient to do this
   * without actually computing the Jacobian and doing the vector-matrix
   * product.
   *
   * @param adj Eigen::VectorXd of adjoints at the output of the softmax
   * @return Eigen::VectorXd of adjoints propagated through softmax operation
   */
  template <std::size_t size>
  std::tuple<Eigen::VectorXd> multiply_adjoint_jacobian(
      const std::array<bool, size>& needs_adj,
      const Eigen::VectorXd& adj) const {
    Eigen::VectorXd adj_times_jac(N_);
    Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, 1> > y(y_, N_);

    double adj_dot_y = adj.dot(y);
    for (int n = 0; n < N_; ++n) {
      adj_times_jac(n) = -y(n) * adj_dot_y + y(n) * adj(n);
    }

    return std::make_tuple(adj_times_jac);
  }
};
}  // namespace internal

/**
 * Return the softmax of the specified Eigen vector.  Softmax is
 * guaranteed to return a simplex.
 *
 * @param alpha Unconstrained input vector.
 * @return Softmax of the input.
 * @throw std::domain_error If the input vector is size 0.
 */
inline Eigen::Matrix<var, Eigen::Dynamic, 1> softmax(
    const Eigen::Matrix<var, Eigen::Dynamic, 1>& alpha) {
  check_nonzero_size("softmax", "alpha", alpha);

  return adj_jac_apply<internal::softmax_op>(alpha);
}

}  // namespace math
}  // namespace stan
#endif
