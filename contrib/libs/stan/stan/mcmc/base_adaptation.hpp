#ifndef STAN_MCMC_BASE_ADAPTATION_HPP
#define STAN_MCMC_BASE_ADAPTATION_HPP

namespace stan {

  namespace mcmc {

    class base_adaptation {
    public:
      virtual void restart() {}
    };

  }  // mcmc

}  // stan

#endif
