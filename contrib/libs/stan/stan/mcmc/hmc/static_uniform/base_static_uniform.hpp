#ifndef STAN_MCMC_HMC_UNIFORM_BASE_STATIC_UNIFORM_HPP
#define STAN_MCMC_HMC_UNIFORM_BASE_STATIC_UNIFORM_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/mcmc/hmc/base_hmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/ps_point.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace stan {

  namespace mcmc {
    /**
     * Hamiltonian Monte Carlo implementation that uniformly samples
     * from trajectories with a static integration time
     */
    template <class Model,
              template<class, class> class Hamiltonian,
              template<class> class Integrator,
              class BaseRNG>
    class base_static_uniform:
      public base_hmc<Model, Hamiltonian, Integrator, BaseRNG> {
    public:
      base_static_uniform(const Model& model, BaseRNG& rng)
        : base_hmc<Model, Hamiltonian, Integrator, BaseRNG>(model, rng),
          T_(1), energy_(0) {
        update_L_();
      }

      ~base_static_uniform() {}

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        this->sample_stepsize();

        this->seed(init_sample.cont_params());

        this->hamiltonian_.sample_p(this->z_, this->rand_int_);
        this->hamiltonian_.init(this->z_, logger);

        ps_point z_init(this->z_);
        double H0 = this->hamiltonian_.H(this->z_);

        ps_point z_sample(this->z_);
        double sum_prob = 1;
        double sum_metro_prob = 1;

        boost::random::uniform_int_distribution<> uniform(0, L_ - 1);
        int Lp = uniform(this->rand_int_);

        for (int l = 0; l < Lp; ++l) {
          this->integrator_.evolve(this->z_, this->hamiltonian_,
                                   -this->epsilon_,
                                   logger);

          double h = this->hamiltonian_.H(this->z_);
          if (boost::math::isnan(h))
            h = std::numeric_limits<double>::infinity();

          double prob = std::exp(H0 - h);
          sum_prob += prob;
          sum_metro_prob += prob > 1 ? 1 : prob;

          if (this->rand_uniform_() < prob / sum_prob)
            z_sample = this->z_;
        }

        this->z_.ps_point::operator=(z_init);

        for (int l = 0; l < L_ - 1 - Lp; ++l) {
          this->integrator_.evolve(this->z_, this->hamiltonian_,
                                   this->epsilon_,
                                   logger);

          double h = this->hamiltonian_.H(this->z_);
          if (boost::math::isnan(h))
            h = std::numeric_limits<double>::infinity();

          double prob = std::exp(H0 - h);
          sum_prob += prob;
          sum_metro_prob += prob > 1 ? 1 : prob;

          if (this->rand_uniform_() < prob / sum_prob)
            z_sample = this->z_;
        }

        double accept_prob = sum_metro_prob / static_cast<double>(L_);

        this->z_.ps_point::operator=(z_sample);
        this->energy_ = this->hamiltonian_.H(this->z_);
        return sample(this->z_.q,
                      - this->hamiltonian_.V(this->z_),
                      accept_prob);
      }

      void get_sampler_param_names(std::vector<std::string>& names) {
        names.push_back("stepsize__");
        names.push_back("int_time__");
        names.push_back("energy__");
      }

      void get_sampler_params(std::vector<double>& values) {
        values.push_back(this->epsilon_);
        values.push_back(this->T_);
        values.push_back(this->energy_);
      }

      void set_nominal_stepsize_and_T(const double e, const double t) {
        if (e > 0 && t > 0) {
          this->nom_epsilon_ = e;
          T_ = t;
          update_L_();
        }
      }

      void set_nominal_stepsize_and_L(const double e, const int l) {
        if (e > 0 && l > 0) {
          this->nom_epsilon_ = e;
          L_ = l;
          T_ = this->nom_epsilon_ * L_; }
      }

      void set_T(const double t) {
        if (t > 0) {
          T_ = t;
          update_L_();
        }
      }

      void set_nominal_stepsize(const double e) {
        if (e > 0) {
          this->nom_epsilon_ = e;
          update_L_();
        }
      }

      double get_T() {
        return this->T_;
      }

      int get_L() {
        return this->L_;
      }

    protected:
      double T_;
      int L_;
      double energy_;

      void update_L_() {
        L_ = static_cast<int>(T_ / this->nom_epsilon_);
        L_ = L_ < 1 ? 1 : L_;
      }
    };
  }  // mcmc
}  // stan
#endif
