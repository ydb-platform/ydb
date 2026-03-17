#ifndef STAN_SERVICES_SAMPLE_FIXED_PARAM_HPP
#define STAN_SERVICES_SAMPLE_FIXED_PARAM_HPP

#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/mcmc/fixed_param_sampler.hpp>
#include <stan/services/error_codes.hpp>
#include <stan/services/util/mcmc_writer.hpp>
#include <stan/services/util/generate_transitions.hpp>
#include <stan/services/util/create_rng.hpp>
#include <stan/services/util/initialize.hpp>
#include <vector>

namespace stan {
  namespace services {
    namespace sample {

      /**
       * Runs the fixed parameter sampler.
       *
       * The fixed parameter sampler sets the parameters randomly once
       * on the unconstrained scale, then runs the model for the number
       * of iterations specified with the parameters fixed.
       *
       * @tparam Model Model class
       * @param[in] model Input model to test (with data already instantiated)
       * @param[in] init var context for initialization
       * @param[in] random_seed random seed for the random number generator
       * @param[in] chain chain id to advance the pseudo random number generator
       * @param[in] init_radius radius to initialize
       * @param[in] num_samples Number of samples
       * @param[in] num_thin Number to thin the samples
       * @param[in] refresh Controls the output
       * @param[in,out] interrupt Callback for interrupts
       * @param[in,out] logger Logger for messages
       * @param[in,out] init_writer Writer callback for unconstrained inits
       * @param[in,out] sample_writer Writer for draws
       * @param[in,out] diagnostic_writer Writer for diagnostic information
       * @return error_codes::OK if successful
       */
      template <class Model>
      int fixed_param(Model& model, stan::io::var_context& init,
                      unsigned int random_seed, unsigned int chain,
                      double init_radius, int num_samples,
                      int num_thin, int refresh,
                      callbacks::interrupt& interrupt,
                      callbacks::logger& logger,
                      callbacks::writer& init_writer,
                      callbacks::writer& sample_writer,
                      callbacks::writer& diagnostic_writer) {
        boost::ecuyer1988 rng = util::create_rng(random_seed, chain);

        std::vector<int> disc_vector;
        std::vector<double> cont_vector
          = util::initialize(model, init, rng, init_radius, false,
                             logger, init_writer);

        stan::mcmc::fixed_param_sampler sampler;
        util::mcmc_writer
          writer(sample_writer, diagnostic_writer, logger);
        Eigen::VectorXd cont_params(cont_vector.size());
        for (size_t i = 0; i < cont_vector.size(); i++)
          cont_params[i] = cont_vector[i];
        stan::mcmc::sample s(cont_params, 0, 0);

        // Headers
        writer.write_sample_names(s, sampler, model);
        writer.write_diagnostic_names(s, sampler, model);

        clock_t start = clock();

        util::generate_transitions(sampler, num_samples, 0, num_samples,
                                   num_thin, refresh, true, false, writer,
                                   s, model, rng, interrupt, logger);
        clock_t end = clock();

        double sampleDeltaT = static_cast<double>(end - start) / CLOCKS_PER_SEC;
        writer.write_timing(0.0, sampleDeltaT);

        return error_codes::OK;
      }

    }
  }
}
#endif
