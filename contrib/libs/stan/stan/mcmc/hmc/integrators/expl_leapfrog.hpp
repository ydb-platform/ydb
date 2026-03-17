#ifndef STAN_MCMC_HMC_INTEGRATORS_EXPL_LEAPFROG_HPP
#define STAN_MCMC_HMC_INTEGRATORS_EXPL_LEAPFROG_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/integrators/base_leapfrog.hpp>
#include <Eigen/Dense>

namespace stan {
  namespace mcmc {

    template <class Hamiltonian>
    class expl_leapfrog : public base_leapfrog<Hamiltonian> {
    public:
      expl_leapfrog()
        : base_leapfrog<Hamiltonian>() {}

      void begin_update_p(typename Hamiltonian::PointType& z,
                          Hamiltonian& hamiltonian, double epsilon,
                          callbacks::logger& logger) {
        z.p -= epsilon * hamiltonian.dphi_dq(z, logger);
      }

      void update_q(typename Hamiltonian::PointType& z,
                    Hamiltonian& hamiltonian, double epsilon,
                    callbacks::logger& logger) {
        z.q += epsilon * hamiltonian.dtau_dp(z);
        hamiltonian.update_potential_gradient(z, logger);
      }

      void end_update_p(typename Hamiltonian::PointType& z,
                        Hamiltonian& hamiltonian, double epsilon,
                        callbacks::logger& logger) {
        z.p -= epsilon * hamiltonian.dphi_dq(z, logger);
      }
    };

  }  // mcmc
}  // stan
#endif
