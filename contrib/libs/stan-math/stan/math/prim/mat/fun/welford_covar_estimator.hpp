#ifndef STAN_MATH_PRIM_MAT_FUN_WELFORD_COVAR_ESTIMATOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_WELFORD_COVAR_ESTIMATOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {

class welford_covar_estimator {
 public:
  explicit welford_covar_estimator(int n)
      : m_(Eigen::VectorXd::Zero(n)), m2_(Eigen::MatrixXd::Zero(n, n)) {
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
    m2_ += (q - m_) * delta.transpose();
  }

  int num_samples() { return num_samples_; }

  void sample_mean(Eigen::VectorXd& mean) { mean = m_; }

  void sample_covariance(Eigen::MatrixXd& covar) {
    if (num_samples_ > 1)
      covar = m2_ / (num_samples_ - 1.0);
  }

 protected:
  double num_samples_;
  Eigen::VectorXd m_;
  Eigen::MatrixXd m2_;
};

}  // namespace math
}  // namespace stan
#endif
