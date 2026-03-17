#ifndef STAN_MCMC_HMC_STATIC_SOFTABS_STATIC_HMC_HPP
#define STAN_MCMC_HMC_STATIC_SOFTABS_STATIC_HMC_HPP

#include <stan/mcmc/hmc/hamiltonians/softabs_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/softabs_metric.hpp>
#include <stan/mcmc/hmc/integrators/impl_leapfrog.hpp>
#include <stan/mcmc/hmc/static/base_static_hmc.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation using the endpoint
     * of trajectories with a static integration time with a
     * Gaussian-Riemannian disintegration and SoftAbs metric
     */
    template <class Model, class BaseRNG>
    class softabs_static_hmc
      : public base_static_hmc<Model, softabs_metric,
                               impl_leapfrog, BaseRNG> {
    public:
      softabs_static_hmc(const Model& model, BaseRNG& rng)
        : base_static_hmc<Model, softabs_metric,
                          impl_leapfrog, BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
