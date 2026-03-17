#ifndef STAN_MCMC_HMC_NUTS_SOFTABS_XHMC_HPP
#define STAN_MCMC_HMC_NUTS_SOFTABS_XHMC_HPP

#include <stan/mcmc/hmc/xhmc/base_xhmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/softabs_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/softabs_metric.hpp>
#include <stan/mcmc/hmc/integrators/impl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Exhausive Hamiltonian Monte Carlo (XHMC) with multinomial sampling
     * with a Gaussian-Riemannian disintegration and SoftAbs metric
     */
    template <class Model, class BaseRNG>
    class softabs_xhmc
      : public base_xhmc<Model, softabs_metric,
                         impl_leapfrog, BaseRNG> {
    public:
      softabs_xhmc(const Model& model, BaseRNG& rng)
        : base_xhmc<Model, softabs_metric, impl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
