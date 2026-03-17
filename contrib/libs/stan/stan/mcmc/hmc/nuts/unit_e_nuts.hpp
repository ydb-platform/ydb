#ifndef STAN_MCMC_HMC_NUTS_UNIT_E_NUTS_HPP
#define STAN_MCMC_HMC_NUTS_UNIT_E_NUTS_HPP

#include <stan/mcmc/hmc/nuts/base_nuts.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * The No-U-Turn sampler (NUTS) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and unit metric
     */
    template <class Model, class BaseRNG>
    class unit_e_nuts
      : public base_nuts<Model, unit_e_metric,
                         expl_leapfrog, BaseRNG> {
    public:
      unit_e_nuts(const Model& model, BaseRNG& rng)
        : base_nuts<Model, unit_e_metric, expl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
