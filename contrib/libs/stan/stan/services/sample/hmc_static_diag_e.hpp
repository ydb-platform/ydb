#ifndef STAN_SERVICES_SAMPLE_HMC_STATIC_DIAG_E_HPP
#define STAN_SERVICES_SAMPLE_HMC_STATIC_DIAG_E_HPP

#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat.hpp>
#include <stan/mcmc/fixed_param_sampler.hpp>
#include <stan/mcmc/hmc/static/diag_e_static_hmc.hpp>
#include <stan/services/error_codes.hpp>
#include <stan/services/util/run_sampler.hpp>
#include <stan/services/util/create_rng.hpp>
#include <stan/services/util/initialize.hpp>
#include <stan/services/util/inv_metric.hpp>

#include <vector>

namespace stan {
  namespace services {
    namespace sample {

      /**
       * Runs static HMC without adaptation using diagonal Euclidean metric
       * with a pre-specified Euclidean metric.
       *
       * @tparam Model Model class
       * @param[in] model Input model to test (with data already instantiated)
       * @param[in] init var context for initialization
       * @param[in] init_inv_metric var context exposing an initial diagonal
                    inverse Euclidean metric (must be positive definite)
       * @param[in] random_seed random seed for the random number generator
       * @param[in] chain chain id to advance the pseudo random number generator
       * @param[in] init_radius radius to initialize
       * @param[in] num_warmup Number of warmup samples
       * @param[in] num_samples Number of samples
       * @param[in] num_thin Number to thin the samples
       * @param[in] save_warmup Indicates whether to save the warmup iterations
       * @param[in] refresh Controls the output
       * @param[in] stepsize initial stepsize for discrete evolution
       * @param[in] stepsize_jitter uniform random jitter of stepsize
       * @param[in] int_time integration time
       * @param[in,out] interrupt Callback for interrupts
       * @param[in,out] logger Logger for messages
       * @param[in,out] init_writer Writer callback for unconstrained inits
       * @param[in,out] sample_writer Writer for draws
       * @param[in,out] diagnostic_writer Writer for diagnostic information
       * @return error_codes::OK if successful
       */
      template <class Model>
      int hmc_static_diag_e(Model& model, stan::io::var_context& init,
                            stan::io::var_context& init_inv_metric,
                            unsigned int random_seed, unsigned int chain,
                            double init_radius, int num_warmup, int num_samples,
                            int num_thin, bool save_warmup, int refresh,
                            double stepsize, double stepsize_jitter,
                            double int_time,
                            callbacks::interrupt& interrupt,
                            callbacks::logger& logger,
                            callbacks::writer& init_writer,
                            callbacks::writer& sample_writer,
                            callbacks::writer& diagnostic_writer) {
        boost::ecuyer1988 rng = util::create_rng(random_seed, chain);

        std::vector<int> disc_vector;
        std::vector<double> cont_vector
          = util::initialize(model, init, rng, init_radius, true,
                             logger, init_writer);

        Eigen::VectorXd inv_metric;
        try {
          inv_metric =
            util::read_diag_inv_metric(init_inv_metric, model.num_params_r(),
                                        logger);
          util::validate_diag_inv_metric(inv_metric, logger);
        } catch (const std::domain_error& e) {
          return error_codes::CONFIG;
        }

        stan::mcmc::diag_e_static_hmc<Model, boost::ecuyer1988>
          sampler(model, rng);

        sampler.set_metric(inv_metric);
        sampler.set_nominal_stepsize_and_T(stepsize, int_time);
        sampler.set_stepsize_jitter(stepsize_jitter);

        util::run_sampler(sampler, model, cont_vector, num_warmup, num_samples,
                          num_thin, refresh, save_warmup, rng, interrupt,
                          logger, sample_writer, diagnostic_writer);

        return error_codes::OK;
      }

      /**
       * Runs static HMC without adaptation using diagonal Euclidean metric.
       * with identity matrix as initial inv_metric.
       *
       * @tparam Model Model class
       * @param[in] model Input model to test (with data already instantiated)
       * @param[in] init var context for initialization
       * @param[in] random_seed random seed for the random number generator
       * @param[in] chain chain id to advance the pseudo random number generator
       * @param[in] init_radius radius to initialize
       * @param[in] num_warmup Number of warmup samples
       * @param[in] num_samples Number of samples
       * @param[in] num_thin Number to thin the samples
       * @param[in] save_warmup Indicates whether to save the warmup iterations
       * @param[in] refresh Controls the output
       * @param[in] stepsize initial stepsize for discrete evolution
       * @param[in] stepsize_jitter uniform random jitter of stepsize
       * @param[in] int_time integration time
       * @param[in,out] interrupt Callback for interrupts
       * @param[in,out] logger Logger for messages
       * @param[in,out] init_writer Writer callback for unconstrained inits
       * @param[in,out] sample_writer Writer for draws
       * @param[in,out] diagnostic_writer Writer for diagnostic information
       * @return error_codes::OK if successful
       */
      template <class Model>
      int hmc_static_diag_e(Model& model, stan::io::var_context& init,
                            unsigned int random_seed, unsigned int chain,
                            double init_radius, int num_warmup, int num_samples,
                            int num_thin, bool save_warmup, int refresh,
                            double stepsize, double stepsize_jitter,
                            double int_time,
                            callbacks::interrupt& interrupt,
                            callbacks::logger& logger,
                            callbacks::writer& init_writer,
                            callbacks::writer& sample_writer,
                            callbacks::writer& diagnostic_writer) {
        stan::io::dump dmp =
          util::create_unit_e_diag_inv_metric(model.num_params_r());
        stan::io::var_context& unit_e_metric = dmp;

        return hmc_static_diag_e(model, init, unit_e_metric,
                                 random_seed, chain, init_radius, num_warmup,
                                 num_samples, num_thin, save_warmup, refresh,
                                 stepsize, stepsize_jitter, int_time,
                                 interrupt, logger,
                                 init_writer, sample_writer, diagnostic_writer);
      }

    }
  }
}
#endif
