#ifndef STAN_MCMC_HMC_STATIC_UNIT_E_STATIC_HMC_HPP
#define STAN_MCMC_HMC_STATIC_UNIT_E_STATIC_HMC_HPP

#include <stan/mcmc/hmc/hamiltonians/unit_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>
#include <stan/mcmc/hmc/static/base_static_hmc.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation using the endpoint
     * of trajectories with a static integration time with a
     * Gaussian-Euclidean disintegration and unit metric
     */
    template <class Model, class BaseRNG>
    class unit_e_static_hmc
      : public base_static_hmc<Model, unit_e_metric,
                               expl_leapfrog, BaseRNG> {
    public:
      unit_e_static_hmc(const Model& model, BaseRNG& rng)
        : base_static_hmc<Model, unit_e_metric,
                          expl_leapfrog, BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
