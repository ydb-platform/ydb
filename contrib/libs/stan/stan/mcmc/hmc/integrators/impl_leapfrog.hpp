#ifndef STAN_MCMC_HMC_INTEGRATORS_IMPL_LEAPFROG_HPP
#define STAN_MCMC_HMC_INTEGRATORS_IMPL_LEAPFROG_HPP

#include <Eigen/Dense>
#include <stan/mcmc/hmc/integrators/base_leapfrog.hpp>

namespace stan {
  namespace mcmc {

    template <typename Hamiltonian>
    class impl_leapfrog: public base_leapfrog<Hamiltonian> {
    public:
      impl_leapfrog(): base_leapfrog<Hamiltonian>(),
                       max_num_fixed_point_(10),
                       fixed_point_threshold_(1e-8) {}

      void begin_update_p(typename Hamiltonian::PointType& z,
                          Hamiltonian& hamiltonian,
                          double epsilon,
                          callbacks::logger& logger) {
        hat_phi(z, hamiltonian, epsilon, logger);
        hat_tau(z, hamiltonian, epsilon, this->max_num_fixed_point_,
                logger);
      }

      void update_q(typename Hamiltonian::PointType& z,
                    Hamiltonian& hamiltonian,
                    double epsilon,
                    callbacks::logger& logger) {
        // hat{T} = dT/dp * d/dq
        Eigen::VectorXd q_init = z.q + 0.5 * epsilon * hamiltonian.dtau_dp(z);
        Eigen::VectorXd delta_q(z.q.size());

        for (int n = 0; n < this->max_num_fixed_point_; ++n) {
          delta_q = z.q;
          z.q.noalias() = q_init + 0.5 * epsilon * hamiltonian.dtau_dp(z);
          hamiltonian.update_metric(z, logger);

          delta_q -= z.q;
          if (delta_q.cwiseAbs().maxCoeff() < this->fixed_point_threshold_)
            break;
        }
        hamiltonian.update_gradients(z, logger);
      }

      void end_update_p(typename Hamiltonian::PointType& z,
                        Hamiltonian& hamiltonian,
                        double epsilon,
                        callbacks::logger& logger) {
        hat_tau(z, hamiltonian, epsilon, 1, logger);
        hat_phi(z, hamiltonian, epsilon, logger);
      }

      // hat{phi} = dphi/dq * d/dp
      void hat_phi(typename Hamiltonian::PointType& z,
                   Hamiltonian& hamiltonian,
                   double epsilon,
                   callbacks::logger& logger) {
        z.p -= epsilon * hamiltonian.dphi_dq(z, logger);
      }

      // hat{tau} = dtau/dq * d/dp
      void hat_tau(typename Hamiltonian::PointType& z,
                   Hamiltonian& hamiltonian,
                   double epsilon,
                   int num_fixed_point,
                   callbacks::logger& logger) {
        Eigen::VectorXd p_init = z.p;
        Eigen::VectorXd delta_p(z.p.size());

        for (int n = 0; n < num_fixed_point; ++n) {
          delta_p = z.p;
          z.p.noalias() = p_init - epsilon * hamiltonian.dtau_dq(z, logger);
          delta_p -= z.p;
          if (delta_p.cwiseAbs().maxCoeff() < this->fixed_point_threshold_)
            break;
        }
      }

      int max_num_fixed_point() {
        return this->max_num_fixed_point_;
      }

      void set_max_num_fixed_point(int n) {
        if (n > 0) this->max_num_fixed_point_ = n;
      }

      double fixed_point_threshold() {
        return this->fixed_point_threshold_;
      }

      void set_fixed_point_threshold(double t) {
        if (t > 0) this->fixed_point_threshold_ = t;
      }

    private:
      int max_num_fixed_point_;
      double fixed_point_threshold_;
    };

  }  // mcmc
}  // stan

#endif
