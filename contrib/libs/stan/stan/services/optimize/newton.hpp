#ifndef STAN_SERVICES_OPTIMIZE_NEWTON_HPP
#define STAN_SERVICES_OPTIMIZE_NEWTON_HPP

#include <stan/io/var_context.hpp>
#include <stan/io/chained_var_context.hpp>
#include <stan/io/random_var_context.hpp>
#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/optimization/newton.hpp>
#include <stan/services/error_codes.hpp>
#include <stan/services/util/initialize.hpp>
#include <stan/services/util/create_rng.hpp>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace stan {
  namespace services {
    namespace optimize {

      /**
       * Runs the Newton algorithm for a model.
       *
       * @tparam Model A model implementation
       * @param[in] model the Stan model instantiated with data
       * @param[in] init var context for initialization
       * @param[in] random_seed random seed for the random number generator
       * @param[in] chain chain id to advance the pseudo random number generator
       * @param[in] init_radius radius to initialize
       * @param[in] num_iterations maximum number of iterations
       * @param[in] save_iterations indicates whether all the interations should
       *   be saved
       * @param[in,out] interrupt callback to be called every iteration
       * @param[in,out] logger Logger for messages
       * @param[in,out] init_writer Writer callback for unconstrained inits
       * @param[in,out] parameter_writer output for parameter values
       * @return error_codes::OK if successful
       */
      template <class Model>
      int newton(Model& model, stan::io::var_context& init,
                 unsigned int random_seed, unsigned int chain,
                 double init_radius, int num_iterations,
                 bool save_iterations,
                 callbacks::interrupt& interrupt,
                 callbacks::logger& logger,
                 callbacks::writer& init_writer,
                 callbacks::writer& parameter_writer) {
        boost::ecuyer1988 rng = util::create_rng(random_seed, chain);

        std::vector<int> disc_vector;
        std::vector<double> cont_vector
            = util::initialize<false>(model, init, rng, init_radius, false,
                                      logger, init_writer);


        double lp(0);
        try {
          std::stringstream message;
          lp = model.template log_prob<false, false>(cont_vector, disc_vector,
                                                     &message);
          logger.info(message);
        } catch (const std::exception& e) {
          logger.info("");
          logger.info("Informational Message: The current Metropolis"
                         " proposal is about to be rejected because of"
                         " the following issue:");
          logger.info(e.what());
          logger.info("If this warning occurs sporadically, such as"
                         " for highly constrained variable types like"
                         " covariance matrices, then the sampler is fine,");
          logger.info("but if this warning occurs often then your model"
                         " may be either severely ill-conditioned or"
                         " misspecified.");
          lp = -std::numeric_limits<double>::infinity();
        }

        std::stringstream msg;
        msg << "Initial log joint probability = " << lp;
        logger.info(msg);

        std::vector<std::string> names;
        names.push_back("lp__");
        model.constrained_param_names(names, true, true);
        parameter_writer(names);

        double lastlp = lp;
        for (int m = 0; m < num_iterations; m++) {
          if (save_iterations) {
            std::vector<double> values;
            std::stringstream ss;
            model.write_array(rng, cont_vector, disc_vector, values,
                              true, true, &ss);
            if (ss.str().length() > 0)
              logger.info(ss);
            values.insert(values.begin(), lp);
            parameter_writer(values);
          }
          interrupt();
          lastlp = lp;
          lp = stan::optimization::newton_step(model, cont_vector, disc_vector);

          std::stringstream msg2;
          msg2 << "Iteration "
               << std::setw(2) << (m + 1) << "."
               << " Log joint probability = " << std::setw(10) << lp
               << ". Improved by " << (lp - lastlp) << ".";
          logger.info(msg2);

          if (std::fabs(lp - lastlp) <= 1e-8)
            break;
        }

        {
          std::vector<double> values;
          std::stringstream ss;
          model.write_array(rng, cont_vector, disc_vector, values,
                            true, true, &ss);
          if (ss.str().length() > 0)
            logger.info(ss);
          values.insert(values.begin(), lp);
          parameter_writer(values);
        }
        return error_codes::OK;
      }

    }
  }
}
#endif
