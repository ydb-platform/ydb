#ifndef STAN_SERVICES_UTIL_INITIALIZE_HPP
#define STAN_SERVICES_UTIL_INITIALIZE_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/io/var_context.hpp>
#include <stan/io/random_var_context.hpp>
#include <stan/io/chained_var_context.hpp>
#include <stan/model/log_prob_grad.hpp>
#include <stan/math/prim/arr/fun/sum.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace stan {
namespace services {
namespace util {

/**
 * Returns a valid initial value of the parameters of the model
 * on the unconstrained scale.
 *
 * For identical inputs (model, init, rng, init_radius), this
 * function will produce the same initialization.
 *
 * Initialization first tries to use the provided
 * <code>stan::io::var_context</code>, then it will generate
 * random uniform values from -init_radius to +init_radius for missing
 * parameters.
 *
 * When the <code>var_context</code> provides all variables or
 * the init_radius is 0, this function will only evaluate the
 * log probability of the model with the unconstrained
 * parameters once to see if it's valid.
 *
 * When at least some of the initialization is random, it will
 * randomly initialize until it finds a set of unconstrained
 * parameters that are valid or it hits <code>MAX_INIT_TRIES =
 * 100</code> (hard-coded).
 *
 * Valid initialization is defined as a finite, non-NaN value for the
 * evaluation of the log probability density function and all its
 * gradients.
 *
 * @tparam Jacobian indicates whether to include the Jacobian term when
 *   evaluating the log density function
 * @tparam Model the type of the model class
 * @tparam RNG the type of the random number generator
 *
 * @param[in] model the model
 * @param[in] init a var_context with initial values
 * @param[in,out] rng random number generator
 * @param[in] init_radius the radius for generating random values.
 *   A value of 0 indicates that the unconstrained parameters (not
 *   provided by init) should be initialized with 0.
 * @param[in] print_timing indicates whether a timing message should
 *   be printed to the logger
 * @param[in,out] logger logger for messages
 * @param[in,out] init_writer init writer (on the unconstrained scale)
 * @throws exception passed through from the model if the model has a
 *   fatal error (not a std::domain_error)
 * @throws std::domain_error if the model can not be initialized and
 *   the model does not have a fatal error (only allows for
 *   std::domain_error)
 * @return valid unconstrained parameters for the model
 */
template <bool Jacobian = true, class Model, class RNG>
std::vector<double> initialize(Model& model,
                               stan::io::var_context& init,
                               RNG& rng,
                               double init_radius,
                               bool print_timing,
                               stan::callbacks::logger& logger,
                               stan::callbacks::writer&
                               init_writer) {
  std::vector<double> unconstrained;
  std::vector<int> disc_vector;

  bool is_fully_initialized = true;
  bool any_initialized = false;
  std::vector<std::string> param_names;
  model.get_param_names(param_names);
  for (size_t n = 0; n < param_names.size(); n++) {
    is_fully_initialized &= init.contains_r(param_names[n]);
    any_initialized |= init.contains_r(param_names[n]);
  }

  bool is_initialized_with_zero = init_radius == 0.0;

  int MAX_INIT_TRIES = is_fully_initialized || is_initialized_with_zero
                       ? 1 : 100;
  int num_init_tries = 0;
  for (; num_init_tries < MAX_INIT_TRIES; num_init_tries++) {
    std::stringstream msg;
    try {
      stan::io::random_var_context
          random_context(model, rng, init_radius, is_initialized_with_zero);

      if (!any_initialized) {
        unconstrained = random_context.get_unconstrained();
      } else {
        stan::io::chained_var_context context(init, random_context);

        model.transform_inits(context,
                              disc_vector,
                              unconstrained,
                              &msg);
      }
    } catch (std::domain_error& e) {
      if (msg.str().length() > 0)
        logger.info(msg);
      logger.info("Rejecting initial value:");
      logger.info("  Error evaluating the log probability"
                  " at the initial value.");
      logger.info(e.what());
      continue;
    } catch (std::exception& e) {
      if (msg.str().length() > 0)
        logger.info(msg);
      logger.info("Unrecoverable error evaluating the log probability"
                  " at the initial value.");
      logger.info(e.what());
      throw;
    }

    msg.str("");
    double log_prob(0);
    try {
      // we evaluate the log_prob function with propto=false
      // because we're evaluating with `double` as the type of
      // the parameters.
      log_prob = model.template log_prob<false, Jacobian>
                 (unconstrained, disc_vector, &msg);
      if (msg.str().length() > 0)
        logger.info(msg);
    } catch (std::domain_error& e) {
      if (msg.str().length() > 0)
        logger.info(msg);
      logger.info("Rejecting initial value:");
      logger.info("  Error evaluating the log probability"
                  " at the initial value.");
      logger.info(e.what());
      continue;
    } catch (std::exception& e) {
      if (msg.str().length() > 0)
        logger.info(msg);
      logger.info("Unrecoverable error evaluating the log probability"
                  " at the initial value.");
      logger.info(e.what());
      throw;
    }
    if (!boost::math::isfinite(log_prob)) {
      logger.info("Rejecting initial value:");
      logger.info("  Log probability evaluates to log(0),"
                  " i.e. negative infinity.");
      logger.info("  Stan can't start sampling from this"
                  " initial value.");
      continue;
    }
    std::stringstream log_prob_msg;
    std::vector<double> gradient;
    clock_t start_check = clock();
    try {
      // we evaluate this with propto=true since we're
      // evaluating with autodiff variables
      log_prob = stan::model::log_prob_grad<true, Jacobian>
                 (model, unconstrained, disc_vector,
                  gradient, &log_prob_msg);
    } catch (const std::exception& e) {
      if (log_prob_msg.str().length() > 0)
        logger.info(log_prob_msg);
      logger.info(e.what());
      throw;
    }
    clock_t end_check = clock();
    double deltaT = static_cast<double>(end_check - start_check)
                    / CLOCKS_PER_SEC;
    if (log_prob_msg.str().length() > 0)
      logger.info(log_prob_msg);

    bool gradient_ok = boost::math::isfinite(stan::math::sum(gradient));

    if (!gradient_ok) {
      logger.info("Rejecting initial value:");
      logger.info("  Gradient evaluated at the initial value"
                  " is not finite.");
      logger.info("  Stan can't start sampling from this"
                  " initial value.");
    }
    if (gradient_ok && print_timing) {
      logger.info("");
      std::stringstream msg1;
      msg1 << "Gradient evaluation took " << deltaT << " seconds";
      logger.info(msg1);

      std::stringstream msg2;
      msg2 << "1000 transitions using 10 leapfrog steps"
           << " per transition would take"
           << " " << 1e4 * deltaT << " seconds.";
      logger.info(msg2);

      logger.info("Adjust your expectations accordingly!");
      logger.info("");
      logger.info("");
    }
    if (gradient_ok) {
      init_writer(unconstrained);
      return unconstrained;
    }
  }

  if (!is_initialized_with_zero) {
    logger.info("");
    std::stringstream msg;
    msg << "Initialization between (-" << init_radius
        << ", " << init_radius << ") failed after"
        << " " << MAX_INIT_TRIES <<  " attempts. ";
    logger.info(msg);
    logger.info(" Try specifying initial values,"
                " reducing ranges of constrained values,"
                " or reparameterizing the model.");
  }
  throw std::domain_error("Initialization failed.");
}

}
}
}
#endif
