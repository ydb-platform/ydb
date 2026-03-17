#ifndef STAN_MCMC_SAMPLE_HPP
#define STAN_MCMC_SAMPLE_HPP

#include <Eigen/Dense>
#include <vector>
#include <string>

namespace stan {

  namespace mcmc {

    class sample {
    public:
      sample(const Eigen::VectorXd& q, double log_prob, double stat)
        : cont_params_(q), log_prob_(log_prob), accept_stat_(stat) {
      }

      sample(Eigen::VectorXd && q, double log_prob, double stat)
        : cont_params_(std::move(q)), log_prob_(log_prob), accept_stat_(stat) {
      }

      sample(const sample&) = default;

      sample(sample&&) = default;

      sample& operator=(const sample&) = default;

      sample& operator=(sample&&) = default;

      virtual ~sample() = default;

      int size_cont() const {
        return cont_params_.size();
      }

      double cont_params(int k) const {
        return cont_params_(k);
      }

      void cont_params(Eigen::VectorXd& x) const {
        x = cont_params_;
      }

      const Eigen::VectorXd& cont_params() const {
        return cont_params_;
      }

      inline double log_prob() const {
        return log_prob_;
      }

      inline double accept_stat() const {
        return accept_stat_;
      }

      static void get_sample_param_names(std::vector<std::string>& names) {
        names.push_back("lp__");
        names.push_back("accept_stat__");
      }

      void get_sample_params(std::vector<double>& values) {
        values.push_back(log_prob_);
        values.push_back(accept_stat_);
      }

    private:
      Eigen::VectorXd cont_params_;  // Continuous coordinates of sample
      double log_prob_;              // Log probability of sample
      double accept_stat_;           // Acceptance statistic of transition
    };

  }  // mcmc

}  // stan

#endif

