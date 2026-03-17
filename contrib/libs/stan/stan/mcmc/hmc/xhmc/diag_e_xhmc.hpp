#ifndef STAN_MCMC_HMC_NUTS_DIAG_E_XHMC_HPP
#define STAN_MCMC_HMC_NUTS_DIAG_E_XHMC_HPP

#include <stan/mcmc/hmc/xhmc/base_xhmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * Exhausive Hamiltonian Monte Carlo (XHMC) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and diagonal metric
     */
    template <class Model, class BaseRNG>
    class diag_e_xhmc
      : public base_xhmc<Model, diag_e_metric,
                         expl_leapfrog, BaseRNG> {
    public:
      diag_e_xhmc(const Model& model, BaseRNG& rng)
        : base_xhmc<Model, diag_e_metric, expl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
