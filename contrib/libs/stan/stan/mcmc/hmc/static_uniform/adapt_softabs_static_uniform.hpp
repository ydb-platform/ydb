#ifndef STAN_MCMC_HMC_STATIC_ADAPT_SOFTABS_STATIC_HMC_HPP
#define STAN_MCMC_HMC_STATIC_ADAPT_SOFTABS_STATIC_HMC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/static_uniform/softabs_static_uniform.hpp>
#include <stan/mcmc/stepsize_adapter.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation that uniformly samples
     * from trajectories with a static integration time with a
     * Gaussian-Riemannian disintegration and SoftAbs metric and
     * adaptive step size
     */
    template <class Model, class BaseRNG>
    class adapt_softabs_static_uniform:
      public softabs_static_uniform<Model, BaseRNG>,
      public stepsize_adapter {
    public:
      adapt_softabs_static_uniform(const Model& model, BaseRNG& rng):
        softabs_static_uniform<Model, BaseRNG>(model, rng) { }

      ~adapt_softabs_static_uniform() { }

      sample transition(sample& init_sample, callbacks::logger& logger) {
        sample s
          = softabs_static_uniform<Model, BaseRNG>::transition(init_sample,
                                                               logger);
        if (this->adapt_flag_) {
          this->stepsize_adaptation_.learn_stepsize(this->nom_epsilon_,
                                                    s.accept_stat());
          this->update_L_();
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
