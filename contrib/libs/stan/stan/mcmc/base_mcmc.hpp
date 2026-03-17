#ifndef STAN_MCMC_BASE_MCMC_HPP
#define STAN_MCMC_BASE_MCMC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/mcmc/sample.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace mcmc {

    class base_mcmc {
    public:
      base_mcmc() {}

      virtual ~base_mcmc() {}

      virtual sample
      transition(sample& init_sample, callbacks::logger& logger) = 0;

      virtual void get_sampler_param_names(std::vector<std::string>& names) {}

      virtual void get_sampler_params(std::vector<double>& values) {}

      virtual void
      write_sampler_state(callbacks::writer& writer) {}

      virtual void
      get_sampler_diagnostic_names(std::vector<std::string>& model_names,
                                   std::vector<std::string>& names) {}

      virtual void get_sampler_diagnostics(std::vector<double>& values) {}
    };

  }  // mcmc
}  // stan
#endif
