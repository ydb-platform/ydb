#ifndef STAN_MCMC_HMC_NUTS_CLASSIC_UNIT_E_NUTS_CLASSIC_HPP
#define STAN_MCMC_HMC_NUTS_CLASSIC_UNIT_E_NUTS_CLASSIC_HPP

#include <stan/mcmc/hmc/nuts_classic/base_nuts_classic.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/unit_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    // The No-U-Turn Sampler (NUTS) on a
    // Euclidean manifold with unit metric
    template <class Model, class BaseRNG>
    class unit_e_nuts_classic:
      public base_nuts_classic<Model, unit_e_metric,
                               expl_leapfrog, BaseRNG> {
    public:
      unit_e_nuts_classic(const Model& model, BaseRNG& rng):
        base_nuts_classic<Model, unit_e_metric,
                          expl_leapfrog, BaseRNG>(model, rng) { }

      bool compute_criterion(ps_point& start,
                             unit_e_point& finish,
                             Eigen::VectorXd& rho) {
        return finish.p.dot(rho - finish.p) > 0
               && start.p.dot(rho - start.p) > 0;
      }
    };

  }  // mcmc
}  // stan
#endif
