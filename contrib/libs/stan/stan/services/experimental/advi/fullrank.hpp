#ifndef STAN_SERVICES_EXPERIMENTAL_ADVI_FULLRANK_HPP
#define STAN_SERVICES_EXPERIMENTAL_ADVI_FULLRANK_HPP

#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/services/util/experimental_message.hpp>
#include <stan/services/util/initialize.hpp>
#include <stan/services/util/create_rng.hpp>
#include <stan/io/var_context.hpp>
#include <stan/variational/advi.hpp>
#include <boost/random/additive_combine.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace services {
    namespace experimental {
      namespace advi {

        /**
         * Runs full rank ADVI.
         *
         * @tparam Model A model implementation
         * @param[in] model Input model to test (with data already instantiated)
         * @param[in] init var context for initialization
         * @param[in] random_seed random seed for the random number generator
         * @param[in] chain chain id to advance the random number generator
         * @param[in] init_radius radius to initialize
         * @param[in] grad_samples number of samples for Monte Carlo estimate
         *   of gradients
         * @param[in] elbo_samples number of samples for Monte Carlo estimate
         *   of ELBO
         * @param[in] max_iterations maximum number of iterations
         * @param[in] tol_rel_obj convergence tolerance on the relative norm of
         *   the objective
         * @param[in] eta stepsize scaling parameter for variational inference
         * @param[in] adapt_engaged adaptation engaged?
         * @param[in] adapt_iterations number of iterations for eta adaptation
         * @param[in] eval_elbo evaluate ELBO every Nth iteration
         * @param[in] output_samples number of posterior samples to draw and
         *   save
         * @param[in,out] interrupt callback to be called every iteration
         * @param[in,out] logger Logger for messages
         * @param[in,out] init_writer Writer callback for unconstrained inits
         * @param[in,out] parameter_writer output for parameter values
         * @param[in,out] diagnostic_writer output for diagnostic values
         * @return error_codes::OK if successful
         */
        template <class Model>
        int fullrank(Model& model, stan::io::var_context& init,
                     unsigned int random_seed, unsigned int chain,
                     double init_radius, int grad_samples, int elbo_samples,
                     int max_iterations, double tol_rel_obj, double eta,
                     bool adapt_engaged, int adapt_iterations, int eval_elbo,
                     int output_samples,
                     callbacks::interrupt& interrupt,
                     callbacks::logger& logger,
                     callbacks::writer& init_writer,
                     callbacks::writer& parameter_writer,
                     callbacks::writer& diagnostic_writer) {
          util::experimental_message(logger);

          boost::ecuyer1988 rng = util::create_rng(random_seed, chain);

          std::vector<int> disc_vector;
          std::vector<double> cont_vector
            = util::initialize(model, init, rng, init_radius, true,
                               logger, init_writer);

          std::vector<std::string> names;
          names.push_back("lp__");
          names.push_back("log_p__");
          names.push_back("log_g__");
          model.constrained_param_names(names, true, true);
          parameter_writer(names);

          Eigen::VectorXd cont_params
            = Eigen::Map<Eigen::VectorXd>(&cont_vector[0],
                                          cont_vector.size(), 1);

          stan::variational::advi<Model,
                                  stan::variational::normal_fullrank,
                                  boost::ecuyer1988>
            cmd_advi(model, cont_params, rng, grad_samples,
                     elbo_samples, eval_elbo, output_samples);
          cmd_advi.run(eta, adapt_engaged, adapt_iterations,
                       tol_rel_obj, max_iterations,
                       logger, parameter_writer, diagnostic_writer);

          return 0;
        }
      }
    }
  }
}
#endif
