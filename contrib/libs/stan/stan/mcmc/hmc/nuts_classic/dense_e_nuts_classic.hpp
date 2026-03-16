#ifndef STAN_MCMC_HMC_NUTS_CLASSIC_DENSE_E_NUTS_CLASSIC_HPP
#define STAN_MCMC_HMC_NUTS_CLASSIC_DENSE_E_NUTS_CLASSIC_HPP

#include <stan/mcmc/hmc/nuts_classic/base_nuts_classic.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/dense_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    // The No-U-Turn Sampler (NUTS) on a
    // Euclidean manifold with dense metric
    template <class Model, class BaseRNG>
    class dense_e_nuts_classic:
      public base_nuts_classic<Model, dense_e_metric,
                               expl_leapfrog, BaseRNG> {
    public:
      dense_e_nuts_classic(const Model& model, BaseRNG& rng):
        base_nuts_classic<Model, dense_e_metric,
                          expl_leapfrog, BaseRNG>(model, rng) { }

      // Note that the points don't need to be swapped
      // here since start.inv_e_metric_ = finish.inv_e_metric_
      bool compute_criterion(ps_point& start,
                             dense_e_point& finish,
                             Eigen::VectorXd& rho) {
        return
          finish.p.transpose() * finish.inv_e_metric_ * (rho - finish.p) > 0
          &&
          start.p.transpose() * finish.inv_e_metric_ * (rho - start.p)  > 0;
      }
    };

  }  // mcmc
}  // stan
#endif
