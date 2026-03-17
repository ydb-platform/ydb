#ifndef STAN_MCMC_HMC_NUTS_CLASSIC_ADAPT_DIAG_E_NUTS_CLASSIC_HPP
#define STAN_MCMC_HMC_NUTS_CLASSIC_ADAPT_DIAG_E_NUTS_CLASSIC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/stepsize_var_adapter.hpp>
#include <stan/mcmc/hmc/nuts_classic/diag_e_nuts_classic.hpp>

namespace stan {
  namespace mcmc {

    // The No-U-Turn Sampler (NUTS) on a
    // Euclidean manifold with diagonal metric
    // and adaptive stepsize

    template <class Model, class BaseRNG>
    class adapt_diag_e_nuts_classic:
      public diag_e_nuts_classic<Model, BaseRNG>,
      public stepsize_var_adapter {
    public:
        adapt_diag_e_nuts_classic(const Model& model, BaseRNG& rng):
          diag_e_nuts_classic<Model, BaseRNG>(model, rng),
          stepsize_var_adapter(model.num_params_r()) {}

      ~adapt_diag_e_nuts_classic() {}

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        sample s
          = diag_e_nuts_classic<Model, BaseRNG>::transition(init_sample,
                                                            logger);

        if (this->adapt_flag_) {
          this->stepsize_adaptation_.learn_stepsize(this->nom_epsilon_,
                                                    s.accept_stat());

          bool update = this->var_adaptation_.learn_variance(
                                              this->z_.inv_e_metric_,
                                              this->z_.q);

          if (update) {
            this->init_stepsize(logger);

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
