#ifndef STAN_SERVICES_DIAGNOSE_DIAGNOSE_HPP
#define STAN_SERVICES_DIAGNOSE_DIAGNOSE_HPP

#include <stan/io/var_context.hpp>
#include <stan/io/chained_var_context.hpp>
#include <stan/io/random_var_context.hpp>
#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/model/test_gradients.hpp>
#include <stan/services/util/create_rng.hpp>
#include <stan/services/util/initialize.hpp>
#include <vector>

namespace stan {
  namespace services {
    namespace diagnose {

      /**
       * Checks the gradients of the model computed using reverse mode
       * autodiff against finite differences.
       *
       * This will test the first order gradients using reverse mode
       * at the value specified in cont_params. This method only
       * outputs to the logger.
       *
       * @tparam Model A model implementation
       * @param[in] model Input model to test (with data already instantiated)
       * @param[in] init var context for initialization
       * @param[in] random_seed random seed for the random number generator
       * @param[in] chain chain id to advance the pseudo random number generator
       * @param[in] init_radius radius to initialize
       * @param[in] epsilon epsilon to use for finite differences
       * @param[in] error amount of absolute error to allow
       * @param[in,out] interrupt interrupt callback
       * @param[in,out] logger Logger for messages
       * @param[in,out] init_writer Writer callback for unconstrained inits
       * @param[in,out] parameter_writer Writer callback for file output
       * @return the number of parameters that are not within epsilon
       * of the finite difference calculation
       */
      template <class Model>
      int diagnose(Model& model, stan::io::var_context& init,
                   unsigned int random_seed, unsigned int chain,
                   double init_radius, double epsilon, double error,
                   callbacks::interrupt& interrupt,
                   callbacks::logger& logger,
                   callbacks::writer& init_writer,
                   callbacks::writer& parameter_writer) {
        boost::ecuyer1988 rng = util::create_rng(random_seed, chain);

        std::vector<int> disc_vector;
        std::vector<double> cont_vector
          = util::initialize(model, init, rng, init_radius,
                             false,
                             logger, init_writer);

        logger.info("TEST GRADIENT MODE");

        int num_failed
          = stan::model::test_gradients<true, true>(model, cont_vector,
                                                    disc_vector, epsilon, error,
                                                    interrupt, logger,
                                                    parameter_writer);

        return num_failed;
      }

    }
  }
}
#endif
