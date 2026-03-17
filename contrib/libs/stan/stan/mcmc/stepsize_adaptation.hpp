#ifndef STAN_MCMC_STEPSIZE_ADAPTATION_HPP
#define STAN_MCMC_STEPSIZE_ADAPTATION_HPP

#include <stan/mcmc/base_adaptation.hpp>
#include <cmath>

namespace stan {

  namespace mcmc {

    class stepsize_adaptation : public base_adaptation {
    public:
      stepsize_adaptation()
        : mu_(0.5), delta_(0.5), gamma_(0.05),
          kappa_(0.75), t0_(10) {
        restart();
      }

      void set_mu(double m) {
        mu_ = m;
      }

      void set_delta(double d) {
        if (d > 0 && d < 1)
          delta_ = d;
      }

      void set_gamma(double g) {
        if (g > 0)
          gamma_ = g;
      }

      void set_kappa(double k) {
        if (k > 0)
          kappa_ = k;
      }
      void set_t0(double t) {
        if (t > 0)
          t0_ = t;
      }

      double get_mu() {
        return mu_;
      }

      double get_delta() {
        return delta_;
      }

      double get_gamma() {
        return gamma_;
      }

      double get_kappa() {
        return kappa_;
      }

      double get_t0() {
        return t0_;
      }

      void restart() {
        counter_ = 0;
        s_bar_ = 0;
        x_bar_ = 0;
      }

      void learn_stepsize(double& epsilon, double adapt_stat) {
        ++counter_;

        adapt_stat = adapt_stat > 1 ? 1 : adapt_stat;

        // Nesterov Dual-Averaging of log(epsilon)
        const double eta = 1.0 / (counter_ + t0_);

        s_bar_ = (1.0 - eta) * s_bar_ + eta * (delta_ - adapt_stat);

        const double x = mu_ - s_bar_ * std::sqrt(counter_) / gamma_;
        const double x_eta = std::pow(counter_, - kappa_);

        x_bar_ = (1.0 - x_eta) * x_bar_ + x_eta * x;

        epsilon = std::exp(x);
      }

      void complete_adaptation(double& epsilon) {
        epsilon = std::exp(x_bar_);
      }

    protected:
      double counter_;  // Adaptation iteration
      double s_bar_;    // Moving average statistic
      double x_bar_;    // Moving average parameter
      double mu_;       // Asymptotic mean of parameter
      double delta_;    // Target value of statistic
      double gamma_;    // Adaptation scaling
      double kappa_;    // Adaptation shrinkage
      double t0_;       // Effective starting iteration
    };

  }  // mcmc

}  // stan

#endif
