#ifndef STAN_MCMC_FIXED_PARAM_SAMPLER_HPP
#define STAN_MCMC_FIXED_PARAM_SAMPLER_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/base_mcmc.hpp>
#include <stan/mcmc/sample.hpp>

namespace stan {
  namespace mcmc {

    class fixed_param_sampler : public base_mcmc {
    public:
      fixed_param_sampler() { }

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        return init_sample;
      }
    };

  }  // mcmc
}  // stan
#endif
