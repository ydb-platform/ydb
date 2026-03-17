#ifndef STAN_MCMC_HMC_STATIC_UNIFORM_DIAG_E_STATIC_UNIFORM_HPP
#define STAN_MCMC_HMC_STATIC_UNIFORM_DIAG_E_STATIC_UNIFORM_HPP

#include <stan/mcmc/hmc/static_uniform/base_static_uniform.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation that uniformly samples
     * from trajectories with a static integration time with a
     * Gaussian-Euclidean disintegration and diagonal metric
     */
    template <typename Model, class BaseRNG>
    class diag_e_static_uniform
      : public base_static_uniform<Model, diag_e_metric,
                                   expl_leapfrog, BaseRNG> {
    public:
      diag_e_static_uniform(const Model& model, BaseRNG& rng):
        base_static_uniform<Model, diag_e_metric,
                            expl_leapfrog, BaseRNG>(model, rng) {
      }
    };
  }  // mcmc
}  // stan

#endif
