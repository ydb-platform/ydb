#ifndef STAN_MCMC_STEPSIZE_ADAPTER_HPP
#define STAN_MCMC_STEPSIZE_ADAPTER_HPP

#include <stan/mcmc/base_adapter.hpp>
#include <stan/mcmc/stepsize_adaptation.hpp>

namespace stan {

  namespace mcmc {

    class stepsize_adapter: public base_adapter {
    public:
      stepsize_adapter() { }

      stepsize_adaptation& get_stepsize_adaptation() {
        return stepsize_adaptation_;
      }

    protected:
      stepsize_adaptation stepsize_adaptation_;
    };

  }  // mcmc

}  // stan

#endif
