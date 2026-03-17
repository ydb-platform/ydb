#ifndef STAN_MCMC_HMC_STATIC_ADAPT_DENSE_E_STATIC_HMC_HPP
#define STAN_MCMC_HMC_STATIC_ADAPT_DENSE_E_STATIC_HMC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/static/dense_e_static_hmc.hpp>
#include <stan/mcmc/stepsize_covar_adapter.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation using the endpoint
     * of trajectories with a static integration time with a
     * Gaussian-Euclidean disintegration and adative dense metric and
     * adaptive step size
     */
    template <class Model, class BaseRNG>
    class adapt_dense_e_static_hmc : public dense_e_static_hmc<Model, BaseRNG>,
                                     public stepsize_covar_adapter {
    public:
      adapt_dense_e_static_hmc(const Model& model, BaseRNG& rng)
        : dense_e_static_hmc<Model, BaseRNG>(model, rng),
        stepsize_covar_adapter(model.num_params_r()) { }

      ~adapt_dense_e_static_hmc() { }

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        sample s
          = dense_e_static_hmc<Model, BaseRNG>::transition(init_sample,
                                                           logger);

        if (this->adapt_flag_) {
          this->stepsize_adaptation_.learn_stepsize(this->nom_epsilon_,
                                                    s.accept_stat());
          this->update_L_();

          bool update = this->covar_adaptation_.learn_covariance
            (this->z_.inv_e_metric_, this->z_.q);

          if (update) {
            this->init_stepsize(logger);
            this->update_L_();

            this->stepsize_adaptation_.set_mu(log(10 * this->nom_epsilon_));
            this->stepsize_adaptation_.restart();
          }
        }
        return s;
      }

      void disengage_adaptation() {
        base_adapter::disengage_adaptation();
        this->stepsize_adaptation_.complete_adaptation(this->nom_epsilon_);
      }
    };

  }  // mcmc
}  // stan
#endif
