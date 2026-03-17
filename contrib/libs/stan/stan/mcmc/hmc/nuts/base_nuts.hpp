#ifndef STAN_MCMC_HMC_NUTS_BASE_NUTS_HPP
#define STAN_MCMC_HMC_NUTS_BASE_NUTS_HPP

#include <stan/callbacks/logger.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <stan/math/prim/scal.hpp>
#include <stan/mcmc/hmc/base_hmc.hpp>
#include <stan/mcmc/hmc/hamiltonians/ps_point.hpp>
#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace stan {
  namespace mcmc {
    /**
     * The No-U-Turn sampler (NUTS) with multinomial sampling
     */
    template <class Model, template<class, class> class Hamiltonian,
              template<class> class Integrator, class BaseRNG>
    class base_nuts : public base_hmc<Model, Hamiltonian, Integrator, BaseRNG> {
    public:
      base_nuts(const Model& model, BaseRNG& rng)
        : base_hmc<Model, Hamiltonian, Integrator, BaseRNG>(model, rng),
          depth_(0), max_depth_(5), max_deltaH_(1000),
          n_leapfrog_(0), divergent_(false), energy_(0) {
      }

      /**
       * specialized constructor for specified diag mass matrix
       */
      base_nuts(const Model& model, BaseRNG& rng,
                Eigen::VectorXd& inv_e_metric)
        : base_hmc<Model, Hamiltonian, Integrator, BaseRNG>(model, rng,
                                                            inv_e_metric),
          depth_(0), max_depth_(5), max_deltaH_(1000),
          n_leapfrog_(0), divergent_(false), energy_(0) {
      }

      /**
       * specialized constructor for specified dense mass matrix
       */
      base_nuts(const Model& model, BaseRNG& rng,
                Eigen::MatrixXd& inv_e_metric)
        : base_hmc<Model, Hamiltonian, Integrator, BaseRNG>(model, rng,
                                                            inv_e_metric),
        depth_(0), max_depth_(5), max_deltaH_(1000),
        n_leapfrog_(0), divergent_(false), energy_(0) {
      }

      ~base_nuts() {}

      void set_metric(const Eigen::MatrixXd& inv_e_metric) {
        this->z_.set_metric(inv_e_metric);
      }

      void set_metric(const Eigen::VectorXd& inv_e_metric) {
        this->z_.set_metric(inv_e_metric);
      }

      void set_max_depth(int d) {
        if (d > 0)
          max_depth_ = d;
      }

      void set_max_delta(double d) {
        max_deltaH_ = d;
      }

      int get_max_depth() { return this->max_depth_; }
      double get_max_delta() { return this->max_deltaH_; }

      sample
      transition(sample& init_sample, callbacks::logger& logger) {
        // Initialize the algorithm
        this->sample_stepsize();

        this->seed(init_sample.cont_params());

        this->hamiltonian_.sample_p(this->z_, this->rand_int_);
        this->hamiltonian_.init(this->z_, logger);

        ps_point z_plus(this->z_);
        ps_point z_minus(z_plus);

        ps_point z_sample(z_plus);
        ps_point z_propose(z_plus);

        Eigen::VectorXd p_sharp_plus = this->hamiltonian_.dtau_dp(this->z_);
        Eigen::VectorXd p_sharp_dummy = p_sharp_plus;
        Eigen::VectorXd p_sharp_minus = p_sharp_plus;
        Eigen::VectorXd rho = this->z_.p;

        double log_sum_weight = 0;  // log(exp(H0 - H0))
        double H0 = this->hamiltonian_.H(this->z_);
        int n_leapfrog = 0;
        double sum_metro_prob = 0;

        // Build a trajectory until the NUTS criterion is no longer satisfied
        this->depth_ = 0;
        this->divergent_ = false;

        while (this->depth_ < this->max_depth_) {
          // Build a new subtree in a random direction
          Eigen::VectorXd rho_subtree = Eigen::VectorXd::Zero(rho.size());
          bool valid_subtree = false;
          double log_sum_weight_subtree
            = -std::numeric_limits<double>::infinity();

          if (this->rand_uniform_() > 0.5) {
            this->z_.ps_point::operator=(z_plus);
            valid_subtree
              = build_tree(this->depth_, z_propose,
                           p_sharp_dummy, p_sharp_plus, rho_subtree,
                           H0, 1, n_leapfrog,
                           log_sum_weight_subtree, sum_metro_prob,
                           logger);
            z_plus.ps_point::operator=(this->z_);
          } else {
            this->z_.ps_point::operator=(z_minus);
            valid_subtree
              = build_tree(this->depth_, z_propose,
                           p_sharp_dummy, p_sharp_minus, rho_subtree,
                           H0, -1, n_leapfrog,
                           log_sum_weight_subtree, sum_metro_prob,
                           logger);
            z_minus.ps_point::operator=(this->z_);
          }

          if (!valid_subtree) break;

          // Sample from an accepted subtree
          ++(this->depth_);

          if (log_sum_weight_subtree > log_sum_weight) {
            z_sample = z_propose;
          } else {
            double accept_prob
              = std::exp(log_sum_weight_subtree - log_sum_weight);
            if (this->rand_uniform_() < accept_prob)
              z_sample = z_propose;
          }

          log_sum_weight
            = math::log_sum_exp(log_sum_weight, log_sum_weight_subtree);

          // Break when NUTS criterion is no longer satisfied
          rho += rho_subtree;
          if (!compute_criterion(p_sharp_minus, p_sharp_plus, rho))
            break;
        }

        this->n_leapfrog_ = n_leapfrog;

        // Compute average acceptance probabilty across entire trajectory,
        // even over subtrees that may have been rejected
        double accept_prob
          = sum_metro_prob / static_cast<double>(n_leapfrog);

        this->z_.ps_point::operator=(z_sample);
        this->energy_ = this->hamiltonian_.H(this->z_);
        return sample(this->z_.q, -this->z_.V, accept_prob);
      }

      void get_sampler_param_names(std::vector<std::string>& names) {
        names.push_back("stepsize__");
        names.push_back("treedepth__");
        names.push_back("n_leapfrog__");
        names.push_back("divergent__");
        names.push_back("energy__");
      }

      void get_sampler_params(std::vector<double>& values) {
        values.push_back(this->epsilon_);
        values.push_back(this->depth_);
        values.push_back(this->n_leapfrog_);
        values.push_back(this->divergent_);
        values.push_back(this->energy_);
      }

      virtual bool compute_criterion(Eigen::VectorXd& p_sharp_minus,
                                     Eigen::VectorXd& p_sharp_plus,
                                     Eigen::VectorXd& rho) {
        return    p_sharp_plus.dot(rho) > 0
               && p_sharp_minus.dot(rho) > 0;
      }

      /**
       * Recursively build a new subtree to completion or until
       * the subtree becomes invalid.  Returns validity of the
       * resulting subtree.
       *
       * @param depth Depth of the desired subtree
       * @param z_propose State proposed from subtree
       * @param p_sharp_left p_sharp from left boundary of returned tree
       * @param p_sharp_right p_sharp from the right boundary of returned tree
       * @param rho Summed momentum across trajectory
       * @param H0 Hamiltonian of initial state
       * @param sign Direction in time to built subtree
       * @param n_leapfrog Summed number of leapfrog evaluations
       * @param log_sum_weight Log of summed weights across trajectory
       * @param sum_metro_prob Summed Metropolis probabilities across trajectory
       * @param logger Logger for messages
      */
      bool build_tree(int depth, ps_point& z_propose,
                      Eigen::VectorXd& p_sharp_left,
                      Eigen::VectorXd& p_sharp_right,
                      Eigen::VectorXd& rho,
                      double H0, double sign, int& n_leapfrog,
                      double& log_sum_weight, double& sum_metro_prob,
                      callbacks::logger& logger) {
        // Base case
        if (depth == 0) {
          this->integrator_.evolve(this->z_, this->hamiltonian_,
                                   sign * this->epsilon_,
                                   logger);
          ++n_leapfrog;

          double h = this->hamiltonian_.H(this->z_);
          if (boost::math::isnan(h))
            h = std::numeric_limits<double>::infinity();

          if ((h - H0) > this->max_deltaH_) this->divergent_ = true;

          log_sum_weight = math::log_sum_exp(log_sum_weight, H0 - h);

          if (H0 - h > 0)
            sum_metro_prob += 1;
          else
            sum_metro_prob += std::exp(H0 - h);

          z_propose = this->z_;
          rho += this->z_.p;

          p_sharp_left = this->hamiltonian_.dtau_dp(this->z_);
          p_sharp_right = p_sharp_left;

          return !this->divergent_;
        }
        // General recursion
        Eigen::VectorXd p_sharp_dummy(this->z_.p.size());

        // Build the left subtree
        double log_sum_weight_left = -std::numeric_limits<double>::infinity();
        Eigen::VectorXd rho_left = Eigen::VectorXd::Zero(rho.size());

        bool valid_left
          = build_tree(depth - 1, z_propose,
                       p_sharp_left, p_sharp_dummy, rho_left,
                       H0, sign, n_leapfrog,
                       log_sum_weight_left, sum_metro_prob,
                       logger);

        if (!valid_left) return false;

        // Build the right subtree
        ps_point z_propose_right(this->z_);

        double log_sum_weight_right = -std::numeric_limits<double>::infinity();
        Eigen::VectorXd rho_right = Eigen::VectorXd::Zero(rho.size());

        bool valid_right
          = build_tree(depth - 1, z_propose_right,
                       p_sharp_dummy, p_sharp_right, rho_right,
                       H0, sign, n_leapfrog,
                       log_sum_weight_right, sum_metro_prob,
                       logger);

        if (!valid_right) return false;

        // Multinomial sample from right subtree
        double log_sum_weight_subtree
          = math::log_sum_exp(log_sum_weight_left, log_sum_weight_right);
        log_sum_weight
          = math::log_sum_exp(log_sum_weight, log_sum_weight_subtree);

        if (log_sum_weight_right > log_sum_weight_subtree) {
          z_propose = z_propose_right;
        } else {
          double accept_prob
            = std::exp(log_sum_weight_right - log_sum_weight_subtree);
          if (this->rand_uniform_() < accept_prob)
            z_propose = z_propose_right;
        }

        Eigen::VectorXd rho_subtree = rho_left + rho_right;
        rho += rho_subtree;

        return compute_criterion(p_sharp_left, p_sharp_right, rho_subtree);
      }

      int depth_;
      int max_depth_;
      double max_deltaH_;

      int n_leapfrog_;
      bool divergent_;
      double energy_;
    };

  }  // mcmc
}  // stan
#endif
