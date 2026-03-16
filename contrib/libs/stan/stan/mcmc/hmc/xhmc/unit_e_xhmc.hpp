#ifndef STAN_MCMC_HMC_NUTS_UNIT_E_XHMC_HPP
#define STAN_MCMC_HMC_NUTS_UNIT_E_XHMC_HPP

#include <stan/mcmc/hmc/xhmc/base_xhmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Exhausive Hamiltonian Monte Carlo (XHMC) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and unit metric
     */
    template <class Model, class BaseRNG>
    class unit_e_xhmc
      : public base_xhmc<Model, unit_e_metric,
                         expl_leapfrog, BaseRNG> {
    public:
      unit_e_xhmc(const Model& model, BaseRNG& rng)
        : base_xhmc<Model, unit_e_metric, expl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
