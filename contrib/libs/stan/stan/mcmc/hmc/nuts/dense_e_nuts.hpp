#ifndef STAN_MCMC_HMC_NUTS_DENSE_E_NUTS_HPP
#define STAN_MCMC_HMC_NUTS_DENSE_E_NUTS_HPP

#include <stan/mcmc/hmc/nuts/base_nuts.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    /**
     * The No-U-Turn sampler (NUTS) with multinomial sampling
     * with a Gaussian-Euclidean disintegration and dense metric
     */
    template <class Model, class BaseRNG>
    class dense_e_nuts : public base_nuts<Model, dense_e_metric,
                                          expl_leapfrog, BaseRNG> {
    public:
      dense_e_nuts(const Model& model, BaseRNG& rng)
        : base_nuts<Model, dense_e_metric, expl_leapfrog,
                    BaseRNG>(model, rng) { }
    };

  }  // mcmc
}  // stan
#endif
