#ifndef STAN_ANALYZE_MCMC_COMPUTE_EFFECTIVE_SAMPLE_SIZE_HPP
#define STAN_ANALYZE_MCMC_COMPUTE_EFFECTIVE_SAMPLE_SIZE_HPP

#include <stan/math/prim/mat.hpp>
#include <algorithm>
#include <vector>

namespace stan {
  namespace analyze {

    /**
     * Returns the effective sample size for the specified parameter
     * across all kept samples.
     *
     * See more details in Stan reference manual section "Effective
     * Sample Size". http://mc-stan.org/users/documentation
     *
     * Current implementation assumes chains are all of equal size and
     * draws are stored in contiguous blocks of memory.
     *
     * @param std::vector stores pointers to arrays of chains
     * @param std::vector stores sizes of chains
     * @return effective sample size for the specified parameter
     */
    inline
    double compute_effective_sample_size(std::vector<const double*> draws,
                                         std::vector<size_t> sizes) {
      int num_chains = sizes.size();

      // need to generalize to each jagged draws per chain
      size_t num_draws = sizes[0];
      for (int chain = 1; chain < num_chains; ++chain) {
        num_draws = std::min(num_draws, sizes[chain]);
      }

      Eigen::Matrix<Eigen::VectorXd, Eigen::Dynamic, 1> acov(num_chains);
      Eigen::VectorXd chain_mean(num_chains);
      Eigen::VectorXd chain_var(num_chains);
      for (int chain = 0; chain < num_chains; ++chain) {
        Eigen::Map<const Eigen::Matrix<double, Eigen::Dynamic, 1>>
          draw(draws[chain], sizes[chain]);
        math::autocovariance<double>(draw, acov(chain));
        chain_mean(chain) = draw.mean();
        chain_var(chain) = acov(chain)(0) * num_draws / (num_draws - 1);
      }

      double mean_var = chain_var.mean();
      double var_plus = mean_var * (num_draws - 1) / num_draws;
      if (num_chains > 1)
        var_plus += math::variance(chain_mean);
      Eigen::VectorXd rho_hat_s(num_draws);
      rho_hat_s.setZero();
      Eigen::VectorXd acov_s(num_chains);
      for (int chain = 0; chain < num_chains; ++chain)
        acov_s(chain) = acov(chain)(1);
      double rho_hat_even = 1;
      double rho_hat_odd = 1 - (mean_var - acov_s.mean()) / var_plus;
      rho_hat_s(1) = rho_hat_odd;
      // Geyer's initial positive sequence
      int max_s = 1;
      for (size_t s = 1;
           (s < (num_draws - 2) && (rho_hat_even + rho_hat_odd) >= 0);
           s += 2) {
        for (int chain = 0; chain < num_chains; ++chain)
          acov_s(chain) = acov(chain)(s + 1);
        rho_hat_even = 1 - (mean_var - acov_s.mean()) / var_plus;
        for (int chain = 0; chain < num_chains; ++chain)
          acov_s(chain) = acov(chain)(s + 2);
        rho_hat_odd = 1 - (mean_var - acov_s.mean()) / var_plus;
        if ((rho_hat_even + rho_hat_odd) >= 0) {
          rho_hat_s(s + 1) = rho_hat_even;
          rho_hat_s(s + 2) = rho_hat_odd;
        }
        max_s = s + 2;
      }
      // Geyer's initial monotone sequence
      for (int s = 3; s <= max_s - 2; s += 2) {
        if (rho_hat_s(s + 1) + rho_hat_s(s + 2) >
            rho_hat_s(s - 1) + rho_hat_s(s)) {
          rho_hat_s(s + 1) = (rho_hat_s(s - 1) + rho_hat_s(s)) / 2;
          rho_hat_s(s + 2) = rho_hat_s(s + 1);
        }
      }

      return num_chains * num_draws / (1 + 2 * rho_hat_s.sum());
    }

    inline
    double compute_effective_sample_size(std::vector<const double*> draws,
                                         size_t size) {
      int num_chains = draws.size();
      std::vector<size_t> sizes(num_chains, size);
      return compute_effective_sample_size(draws, sizes);
    }
  }
}

#endif
