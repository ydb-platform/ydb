#ifndef STAN_MCMC_HMC_XHMC_ADAPT_SOFTABS_XHMC_HPP
#define STAN_MCMC_HMC_XHMC_ADAPT_SOFTABS_XHMC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/xhmc/softabs_xhmc.hpp>
#include <stan/mcmc/stepsize_adapter.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Exhausive Hamiltonian Monte Carlo (XHMC) with multinomial sampling
     * with a Gaussian-Riemannian disintegration and SoftAbs metric
     * and adaptive step size
     */
    template <class Model, class BaseRNG>
    class adapt_softabs_xhmc: public softabs_xhmc<Model, BaseRNG>,
                              public stepsize_adapter {
    public:
      adapt_softabs_xhmc(const Model& model, BaseRNG& rng)
        : softabs_xhmc<Model, BaseRNG>(model, rng) {}

      ~adapt_softabs_xhmc() {}

      sample transition(sample& init_sample, callbacks::logger& logger) {
        sample s
          = softabs_xhmc<Model, BaseRNG>::transition(init_sample,
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
