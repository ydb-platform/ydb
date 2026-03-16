#ifndef STAN_MATH_PRIM_MAT_FUN_WELFORD_VAR_ESTIMATOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_WELFORD_VAR_ESTIMATOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

class welford_var_estimator {
 public:
  explicit welford_var_estimator(int n)
      : m_(Eigen::VectorXd::Zero(n)), m2_(Eigen::VectorXd::Zero(n)) {
    restart();
  }

  void restart() {
    num_samples_ = 0;
    m_.setZero();
    m2_.setZero();
  }

  void add_sample(const Eigen::VectorXd& q) {
    ++num_samples_;

    Eigen::VectorXd delta(q - m_);
    m_ += delta / num_samples_;
    m2_ += delta.cwiseProduct(q - m_);
  }

  int num_samples() { return num_samples_; }

  void sample_mean(Eigen::VectorXd& mean) { mean = m_; }

  void sample_variance(Eigen::VectorXd& var) {
    if (num_samples_ > 1)
      var = m2_ / (num_samples_ - 1.0);
  }

 protected:
  double num_samples_;
  Eigen::VectorXd m_;
  Eigen::VectorXd m2_;
};

}  // namespace math
}  // namespace stan
#endif
