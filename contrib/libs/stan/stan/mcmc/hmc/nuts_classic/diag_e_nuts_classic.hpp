#ifndef STAN_MCMC_HMC_NUTS_CLASSIC_DIAG_E_NUTS_CLASSIC_HPP
#define STAN_MCMC_HMC_NUTS_CLASSIC_DIAG_E_NUTS_CLASSIC_HPP

#include <stan/mcmc/hmc/nuts_classic/base_nuts_classic.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_point.hpp>
#include <stan/mcmc/hmc/hamiltonians/diag_e_metric.hpp>
#include <stan/mcmc/hmc/integrators/expl_leapfrog.hpp>

namespace stan {
  namespace mcmc {
    // The No-U-Turn Sampler (NUTS) on a
    // Euclidean manifold with diagonal metric
    template <class Model, class BaseRNG>
    class diag_e_nuts_classic:
      public base_nuts_classic<Model, diag_e_metric,
                               expl_leapfrog, BaseRNG> {
    public:
      diag_e_nuts_classic(const Model& model, BaseRNG& rng):
        base_nuts_classic<Model, diag_e_metric,
                          expl_leapfrog, BaseRNG>(model, rng) { }

      // Note that the points don't need to be swapped here
      // since start.inv_e_metric_ = finish.inv_e_metric_
      bool compute_criterion(ps_point& start,
                             diag_e_point& finish,
                             Eigen::VectorXd& rho) {
        return
          finish.inv_e_metric_.cwiseProduct(finish.p).dot(rho - finish.p) > 0
          &&
          finish.inv_e_metric_.cwiseProduct(start.p).dot(rho - start.p) > 0;
      }
    };

  }  // mcmc
}  // stan
#endif
