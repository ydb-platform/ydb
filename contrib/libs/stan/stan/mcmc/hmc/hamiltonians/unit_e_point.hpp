#ifndef STAN_MCMC_HMC_HAMILTONIANS_UNIT_E_POINT_HPP
#define STAN_MCMC_HMC_HAMILTONIANS_UNIT_E_POINT_HPP

#include <stan/mcmc/hmc/hamiltonians/ps_point.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Point in a phase space with a base
     * Euclidean manifold with unit metric
     */
    class unit_e_point: public ps_point {
    public:
      explicit unit_e_point(int n)
        : ps_point(n) {}
    };

    inline void write_metric(stan::callbacks::writer& writer) {
      writer("No free parameters for unit metric");
    }

  }  // mcmc
}  // stan

#endif
