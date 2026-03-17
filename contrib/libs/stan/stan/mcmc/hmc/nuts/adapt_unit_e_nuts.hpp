#ifndef STAN_MCMC_HMC_NUTS_ADAPT_UNIT_E_NUTS_HPP
#define STAN_MCMC_HMC_NUTS_ADAPT_UNIT_E_NUTS_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/nuts/unit_e_nuts.hpp>
#include <stan/mcmc/stepsize_adapter.hpp>

namespace stan {
  namespace mcmc {
    /**
     * The No-U-Turn sampler (NUTS) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and unit metric
     * and adaptive step size
     */
    template <class Model, class BaseRNG>
    class adapt_unit_e_nuts: public unit_e_nuts<Model, BaseRNG>,
                             public stepsize_adapter {
    public:
      adapt_unit_e_nuts(const Model& model, BaseRNG& rng)
        : unit_e_nuts<Model, BaseRNG>(model, rng) {}

      ~adapt_unit_e_nuts() {}

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        sample s = unit_e_nuts<Model, BaseRNG>::transition(init_sample,
                                                           logger);

        if (this->adapt_flag_)
          this->stepsize_adaptation_.learn_stepsize(this->nom_epsilon_,
                                                    s.accept_stat());

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
