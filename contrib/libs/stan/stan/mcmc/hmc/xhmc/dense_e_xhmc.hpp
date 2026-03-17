#ifndef STAN_MCMC_HMC_NUTS_DENSE_E_XHMC_HPP
#define STAN_MCMC_HMC_NUTS_DENSE_E_XHMC_HPP

#include <stan/mcmc/hmc/xhmc/base_xhmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Exhausive Hamiltonian Monte Carlo (XHMC) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and dense metric
     */
    template <class Model, class BaseRNG>
    class dense_e_xhmc
      : public base_xhmc<Model, dense_e_metric,
                         expl_leapfrog, BaseRNG> {
    public:
      dense_e_xhmc(const Model& model, BaseRNG& rng)
        : base_xhmc<Model, dense_e_metric, expl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
